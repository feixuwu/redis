// ClientConnection implementation - to be completed in task 4
#include "client_connection.h"
#include "worker_thread.h"
#include "backend_manager.h"
#include "mux_backend_connection.h"
#include "resp_parser.h"
#include "logger.h"
#include "config.h"

#include <unistd.h>
#include <sys/socket.h>
#include <cstring>
#include <algorithm>

namespace proxy {

// Get proxy password - we need access to config through worker -> backend_mgr
// For now, use a global config reference set during init
static const std::string* g_proxy_password = nullptr;

void setGlobalProxyPassword(const std::string* pwd) {
    g_proxy_password = pwd;
}

ClientConnection::ClientConnection(int fd, uint16_t listen_port, WorkerThread* worker)
    : fd_(fd), listen_port_(listen_port), worker_(worker) {
    recv_buf_.reserve(4096);
    send_buf_.reserve(4096);

    // If no proxy password configured, auto-authenticate
    if (getProxyPassword().empty()) {
        state_ = ClientState::AUTHENTICATED;
    }
}

ClientConnection::~ClientConnection() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

const std::string& ClientConnection::getProxyPassword() const {
    static const std::string empty;
    return g_proxy_password ? *g_proxy_password : empty;
}

void ClientConnection::setStreaming(MuxBackendConnection* conn, uint32_t stream_id) {
    backend_conn_ = conn;
    stream_id_ = stream_id;
    state_ = ClientState::STREAMING;
}

void ClientConnection::close() {
    if (state_ == ClientState::CLOSING) return;
    state_ = ClientState::CLOSING;

    // Close backend stream if bound
    if (backend_conn_ && stream_id_ != 0) {
        backend_conn_->closeStream(stream_id_);
        backend_conn_ = nullptr;
        stream_id_ = 0;
    }

    // Remove from worker (which will close the fd and destroy this object)
    worker_->removeConnection(this);
}

void ClientConnection::onReadable() {
    if (state_ == ClientState::CLOSING) return;

    // Read data from client
    char buf[8192];
    while (true) {
        ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
        if (n > 0) {
            recv_buf_.insert(recv_buf_.end(), buf, buf + n);
            // Protect against excessive memory usage from abnormal clients
            if (recv_buf_.size() > 4 * 1024 * 1024) {
                LOG_WARN("Client fd=%d recv buffer overflow (%zu bytes), closing",
                         fd_, recv_buf_.size());
                close();
                return;
            }
        } else if (n == 0) {
            // Client disconnected
            LOG_DEBUG("Client fd=%d disconnected", fd_);
            close();
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            LOG_ERROR("recv() fd=%d failed: %s", fd_, strerror(errno));
            close();
            return;
        }
    }

    // Process received data
    if (!isAuthenticated()) {
        // Try to parse and handle AUTH command
        if (!tryHandleAuth()) {
            return; // Need more data or error already sent
        }
    }

    // If authenticated and have data, forward to backend
    if (isAuthenticated() && !recv_buf_.empty()) {
        forwardToBackend();
    }
}

void ClientConnection::onWritable() {
    flushSendBuffer();
}

bool ClientConnection::tryHandleAuth() {
    if (recv_buf_.empty()) return false;

    RespParser::Command cmd;
    bool parsed = RespParser::parse(recv_buf_.data(), recv_buf_.size(), cmd);
    if (!parsed) {
        // Not enough data yet - check if we have \r\n for inline detection
        if (recv_buf_.size() > 65536) {
            // Too much data without valid command, close connection
            sendError("ERR Protocol error: too big inline request");
            close();
        }
        return false;
    }

    if (cmd.name == "AUTH") {
        // Handle AUTH command
        if (cmd.args.empty()) {
            sendError("ERR wrong number of arguments for 'auth' command");
        } else if (getProxyPassword().empty()) {
            sendError("ERR Client sent AUTH, but no password is set");
        } else if (cmd.args[0] == getProxyPassword()) {
            state_ = ClientState::AUTHENTICATED;
            sendReply("+OK\r\n");
            LOG_DEBUG("Client fd=%d authenticated successfully", fd_);
        } else {
            sendError("WRONGPASS invalid username-password pair or user is disabled.");
        }

        // Consume processed bytes
        recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + cmd.bytes_consumed);
        return true;
    } else if (cmd.name == "QUIT") {
        sendReply("+OK\r\n");
        // Consume and then close
        recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + cmd.bytes_consumed);
        close();
        return true;
    } else if (cmd.name == "PING") {
        sendReply("+PONG\r\n");
        recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + cmd.bytes_consumed);
        return false; // Still not authenticated
    } else {
        // Not AUTH, not authenticated
        sendError("NOAUTH Authentication required.");
        recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + cmd.bytes_consumed);
        return false;
    }
}

void ClientConnection::forwardToBackend() {
    // Ensure we have a backend stream
    if (!ensureBackendStream()) {
        return;
    }

    // Forward all data in recv buffer to backend
    if (!recv_buf_.empty()) {
        backend_conn_->sendData(stream_id_, recv_buf_.data(), recv_buf_.size());
        recv_buf_.clear();
    }
}

void ClientConnection::retryForwardToBackend() {
    // Called when backend connection becomes ready and we have buffered data
    if (!recv_buf_.empty() && backend_conn_ && stream_id_ != 0) {
        backend_conn_->sendData(stream_id_, recv_buf_.data(), recv_buf_.size());
        recv_buf_.clear();
    }
}

bool ClientConnection::ensureBackendStream() {
    if (backend_conn_ && stream_id_ != 0) {
        return true; // Already connected
    }

    // Look up backend info for this listen port
    BackendManager* mgr = worker_->getBackendManager();
    std::string addr;
    uint16_t port;
    std::string password;
    if (!mgr->getBackendAddr(listen_port_, addr, port, password)) {
        LOG_ERROR("Client fd=%d: no backend configured for listen port %d", fd_, listen_port_);
        sendError("ERR no backend configured for this port");
        close();
        return false;
    }

    // Get or create a MUX backend connection
    MuxBackendConnection* mux_conn = worker_->getOrCreateBackendConn(addr, port, password);
    if (!mux_conn) {
        LOG_ERROR("Client fd=%d: failed to get backend connection to %s:%d", fd_, addr.c_str(), port);
        sendError("ERR backend connection failed");
        close();
        return false;
    }

    // If the connection is not ready yet (still connecting/handshaking),
    // we need to wait - buffer data and return false
    if (!mux_conn->isReady()) {
        // The connection is still being established
        // Add this client to the pending queue - will be notified when ready
        LOG_DEBUG("Client fd=%d: backend not ready yet, adding to pending queue", fd_);
        mux_conn->addPendingClient(this);
        backend_conn_ = mux_conn;  // Track the connection we're waiting for
        return false;
    }

    // Create a new stream on the backend connection
    uint32_t sid = mux_conn->createStream(this);
    if (sid == 0) {
        LOG_ERROR("Client fd=%d: failed to create stream on backend %s:%d", fd_, addr.c_str(), port);
        sendError("ERR backend stream creation failed");
        close();
        return false;
    }

    setStreaming(mux_conn, sid);
    LOG_DEBUG("Client fd=%d bound to backend %s:%d stream=%u", fd_, addr.c_str(), port, sid);
    return true;
}

void ClientConnection::sendReply(const std::string& reply) {
    appendToSendBuffer(reply.data(), reply.size());
}

void ClientConnection::sendError(const std::string& error) {
    std::string resp = "-" + error + "\r\n";
    appendToSendBuffer(resp.data(), resp.size());
}

void ClientConnection::appendToSendBuffer(const char* data, size_t len) {
    send_buf_.insert(send_buf_.end(), data, data + len);

    // Try to flush immediately
    flushSendBuffer();

    // If still have pending data, register for write events
    if (hasPendingSend()) {
        worker_->getEventLoop().modifyEvent(fd_, EVENT_READABLE | EVENT_WRITABLE);
    }
}

bool ClientConnection::hasPendingSend() const {
    return send_offset_ < send_buf_.size();
}

void ClientConnection::flushSendBuffer() {
    while (send_offset_ < send_buf_.size()) {
        ssize_t n = ::send(fd_, send_buf_.data() + send_offset_,
                           send_buf_.size() - send_offset_, MSG_NOSIGNAL);
        if (n > 0) {
            send_offset_ += n;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            LOG_ERROR("send() fd=%d failed: %s", fd_, strerror(errno));
            close();
            return;
        }
    }

    // Compact send buffer
    if (send_offset_ > 0 && send_offset_ == send_buf_.size()) {
        send_buf_.clear();
        send_offset_ = 0;
        // Remove write interest if no more data
        worker_->getEventLoop().modifyEvent(fd_, EVENT_READABLE);
    } else if (send_offset_ > send_buf_.size() / 2) {
        send_buf_.erase(send_buf_.begin(), send_buf_.begin() + send_offset_);
        send_offset_ = 0;
    }
}

} // namespace proxy
