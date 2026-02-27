// MuxBackendConnection implementation - to be completed in task 6
#include "mux_backend_connection.h"
#include "client_connection.h"
#include "ipc_proxy_frontend.h"
#include "worker_thread.h"
#include "event_loop.h"
#include "resp_parser.h"
#include "logger.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>

namespace proxy {

MuxBackendConnection::MuxBackendConnection(const std::string& addr, uint16_t port,
                                             const std::string& password, bool mux_enabled,
                                             WorkerThread* worker)
    : addr_(addr), port_(port), password_(password),
      mux_enabled_(mux_enabled), worker_(worker),
      max_streams_(mux_enabled ? 1024 : 1) {
    send_buf_.reserve(8192);
    recv_buf_.reserve(8192);
}

MuxBackendConnection::~MuxBackendConnection() {
    if (fd_ >= 0) {
        worker_->getEventLoop().removeEvent(fd_);
        ::close(fd_);
        fd_ = -1;
    }
}

bool MuxBackendConnection::connect() {
    fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd_ < 0) {
        LOG_ERROR("socket() failed for backend %s:%d: %s", addr_.c_str(), port_, strerror(errno));
        return false;
    }

    // Disable Nagle
    int nodelay = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (inet_pton(AF_INET, addr_.c_str(), &addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid backend address: %s", addr_.c_str());
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    int ret = ::connect(fd_, (struct sockaddr*)&addr, sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        LOG_ERROR("connect() to %s:%d failed: %s", addr_.c_str(), port_, strerror(errno));
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    state_ = MuxConnState::CONNECTING;

    // Register for write to detect connection completion
    worker_->getEventLoop().addEvent(fd_, EVENT_WRITABLE,
        [this](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                LOG_ERROR("Backend %s:%d connection error", addr_.c_str(), port_);
                onConnectionError();
                return;
            }
            if (events & EVENT_WRITABLE) {
                if (state_ == MuxConnState::CONNECTING) {
                    // Check if connect succeeded
                    int err = 0;
                    socklen_t len = sizeof(err);
                    getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
                    if (err != 0) {
                        LOG_ERROR("Backend %s:%d connect failed: %s", addr_.c_str(), port_, strerror(err));
                        onConnectionError();
                        return;
                    }
                    onConnected();
                } else {
                    onWritable();
                }
            }
            if (events & EVENT_READABLE) {
                onReadable();
            }
        });

    LOG_DEBUG("Connecting to backend %s:%d (fd=%d)", addr_.c_str(), port_, fd_);
    return true;
}

void MuxBackendConnection::onConnected() {
    LOG_INFO("Connected to backend %s:%d (fd=%d)", addr_.c_str(), port_, fd_);

    if (mux_enabled_) {
        // Start MUX handshake: send HELLO 3 MULTIPLEX
        state_ = MuxConnState::HANDSHAKING;
        startHandshake();
    } else {
        // Non-MUX mode: check if we need AUTH
        if (!password_.empty()) {
            state_ = MuxConnState::AUTHENTICATING;
            std::string auth_cmd = "*2\r\n$4\r\nAUTH\r\n$" +
                std::to_string(password_.size()) + "\r\n" + password_ + "\r\n";
            send_buf_.insert(send_buf_.end(), auth_cmd.begin(), auth_cmd.end());
            flushSendBuffer();
        } else {
            state_ = MuxConnState::READY;
            flushPendingStreams();
        }
    }

    // Switch to read+write monitoring
    worker_->getEventLoop().modifyEvent(fd_, EVENT_READABLE | EVENT_WRITABLE);
    worker_->getEventLoop().setCallback(fd_,
        [this](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                onConnectionError();
                return;
            }
            if (events & EVENT_READABLE) {
                onReadable();
            }
            if (events & EVENT_WRITABLE) {
                onWritable();
            }
        });
}

void MuxBackendConnection::startHandshake() {
    // Send HELLO 3 MULTIPLEX as plain RESP (before MUX mode)
    std::string hello = "*3\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$9\r\nMULTIPLEX\r\n";
    send_buf_.insert(send_buf_.end(), hello.begin(), hello.end());
    flushSendBuffer();
}

void MuxBackendConnection::onReadable() {
    char buf[16384];
    while (true) {
        ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
        if (n > 0) {
            recv_buf_.insert(recv_buf_.end(), buf, buf + n);
        } else if (n == 0) {
            LOG_INFO("Backend %s:%d disconnected (fd=%d)", addr_.c_str(), port_, fd_);
            onConnectionError();
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            LOG_ERROR("recv() from backend %s:%d failed: %s", addr_.c_str(), port_, strerror(errno));
            onConnectionError();
            return;
        }
    }

    if (state_ == MuxConnState::HANDSHAKING) {
        handleHandshakeReply();
    } else if (state_ == MuxConnState::AUTHENTICATING) {
        handleAuthReply();
    } else if (state_ == MuxConnState::READY || state_ == MuxConnState::GOAWAY) {
        if (mux_enabled_) {
            processRecvBuffer();
        } else {
            processNonMuxRecvBuffer();
        }
    }
}

void MuxBackendConnection::handleHandshakeReply() {
    // Look for complete RESP response (simplified: look for a map/array response)
    // HELLO response is a RESP3 map or RESP2 array
    // We just need to detect "+OK" or a map response to know MUX is enabled

    // Simple check: look for "mux" in the response as indicator
    std::string data(recv_buf_.begin(), recv_buf_.end());

    // Check for error
    if (!data.empty() && data[0] == '-') {
        LOG_ERROR("HELLO handshake failed with backend %s:%d: %s",
                  addr_.c_str(), port_, data.c_str());
        onConnectionError();
        return;
    }

    // For simplicity, consume all data after seeing \r\n at the end
    // A proper implementation would fully parse RESP3, but the HELLO response
    // always ends with \r\n and we can detect completion
    size_t last_crlf = data.rfind("\r\n");
    if (last_crlf == std::string::npos) return; // Need more data

    // We got the HELLO response, now MUX mode is active
    recv_buf_.clear();

    LOG_INFO("MUX handshake complete with backend %s:%d", addr_.c_str(), port_);

    // If backend has password, send AUTH via MUX frame
    if (!password_.empty()) {
        state_ = MuxConnState::AUTHENTICATING;
        // Send AUTH as MUX DATA frame on stream 0
        std::string auth_cmd = "*2\r\n$4\r\nAUTH\r\n$" +
            std::to_string(password_.size()) + "\r\n" + password_ + "\r\n";
        sendFrame(MUX_FRAME_DATA, 0, auth_cmd.data(), auth_cmd.size());
    } else {
        state_ = MuxConnState::READY;
        flushPendingStreams();
    }
}

void MuxBackendConnection::handleAuthReply() {
    if (mux_enabled_) {
        // Parse MUX frames to find AUTH response
        processRecvBuffer();
        // The frame handler will check for auth response on stream 0
    } else {
        // Non-MUX: look for +OK or -ERR
        std::string data(recv_buf_.begin(), recv_buf_.end());
        size_t crlf = data.find("\r\n");
        if (crlf == std::string::npos) return;

        std::string response = data.substr(0, crlf);
        recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + crlf + 2);

        if (response[0] == '+') {
            LOG_INFO("Backend %s:%d AUTH successful", addr_.c_str(), port_);
            state_ = MuxConnState::READY;
            flushPendingStreams();
        } else {
            LOG_ERROR("Backend %s:%d AUTH failed: %s", addr_.c_str(), port_, response.c_str());
            onConnectionError();
        }
    }
}

void MuxBackendConnection::onWritable() {
    flushSendBuffer();
}

void MuxBackendConnection::flushSendBuffer() {
    while (send_offset_ < send_buf_.size()) {
        ssize_t n = ::send(fd_, send_buf_.data() + send_offset_,
                           send_buf_.size() - send_offset_, MSG_NOSIGNAL);
        if (n > 0) {
            send_offset_ += n;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            if (errno == EINTR) continue;
            LOG_ERROR("send() to backend %s:%d failed: %s", addr_.c_str(), port_, strerror(errno));
            onConnectionError();
            return;
        }
    }

    // Compact buffer
    if (send_offset_ > 0 && send_offset_ == send_buf_.size()) {
        send_buf_.clear();
        send_offset_ = 0;
        // Only need read events when nothing to write
        if (fd_ >= 0) {
            worker_->getEventLoop().modifyEvent(fd_, EVENT_READABLE);
        }
    } else if (send_offset_ > send_buf_.size() / 2) {
        send_buf_.erase(send_buf_.begin(), send_buf_.begin() + send_offset_);
        send_offset_ = 0;
    }
}

void MuxBackendConnection::close() {
    if (state_ == MuxConnState::CLOSED) return;
    state_ = MuxConnState::CLOSED;

    // Close all streams - close corresponding client connections
    for (auto& [stream_id, client] : streams_) {
        if (client) {
            client->close();
        }
    }
    streams_.clear();
    active_stream_count_ = 0;

    if (fd_ >= 0) {
        worker_->getEventLoop().removeEvent(fd_);
        ::close(fd_);
        fd_ = -1;
    }

    LOG_INFO("MUX connection to %s:%d closed", addr_.c_str(), port_);
}

void MuxBackendConnection::onConnectionError() {
    LOG_ERROR("Backend %s:%d connection error, closing %u streams",
              addr_.c_str(), port_, active_stream_count_);
    close();
    // Remove from worker's pool
    worker_->removeMuxConnection(this);
}

uint64_t MuxBackendConnection::createStream(ClientConnection* client) {
    if (!canCreateStream()) return 0;

    uint64_t stream_id = next_stream_id_;
    next_stream_id_ += 2; // Odd IDs for client-initiated streams

    streams_[stream_id] = client;
    active_stream_count_++;

    // Send STREAM_OPEN frame
    if (mux_enabled_) {
        sendFrame(MUX_FRAME_STREAM_OPEN, stream_id, nullptr, 0);
    }

    LOG_DEBUG("Created stream %llu on backend %s:%d (active=%u)",
              (unsigned long long)stream_id, addr_.c_str(), port_, active_stream_count_);
    return stream_id;
}

void MuxBackendConnection::closeStream(uint64_t stream_id) {
    auto it = streams_.find(stream_id);
    if (it == streams_.end()) return;

    streams_.erase(it);
    active_stream_count_--;

    // Send STREAM_CLOSE frame
    if (mux_enabled_ && state_ != MuxConnState::CLOSED) {
        sendFrame(MUX_FRAME_STREAM_CLOSE, stream_id, nullptr, 0);
    }

    LOG_DEBUG("Closed stream %llu on backend %s:%d (active=%u)",
              (unsigned long long)stream_id, addr_.c_str(), port_, active_stream_count_);

    // If in GOAWAY state and no more streams, close the connection
    if (state_ == MuxConnState::GOAWAY && active_stream_count_ == 0) {
        close();
        worker_->removeMuxConnection(this);
    }
}

ClientConnection* MuxBackendConnection::getStreamClient(uint64_t stream_id) const {
    auto it = streams_.find(stream_id);
    return (it != streams_.end()) ? it->second : nullptr;
}

bool MuxBackendConnection::canCreateStream() const {
    if (state_ != MuxConnState::READY) return false;
    if (active_stream_count_ >= max_streams_) return false;
    return true;
}

void MuxBackendConnection::sendData(uint64_t stream_id, const char* data, size_t len) {
    if (mux_enabled_) {
        sendFrame(MUX_FRAME_DATA, stream_id, data, len);
    } else {
        // Non-MUX: send directly
        send_buf_.insert(send_buf_.end(), data, data + len);
        if (fd_ >= 0) {
            worker_->getEventLoop().modifyEvent(fd_, EVENT_READABLE | EVENT_WRITABLE);
        }
        flushSendBuffer();
    }
}

void MuxBackendConnection::sendFrame(uint8_t type, uint64_t stream_id,
                                      const char* payload, size_t payload_len) {
    unsigned char header[MUX_FRAME_HEADER_SIZE];
    encodeFrameHeader(header, type, stream_id, (uint32_t)payload_len);

    send_buf_.insert(send_buf_.end(), header, header + MUX_FRAME_HEADER_SIZE);
    if (payload && payload_len > 0) {
        send_buf_.insert(send_buf_.end(), payload, payload + payload_len);
    }

    if (fd_ >= 0) {
        worker_->getEventLoop().modifyEvent(fd_, EVENT_READABLE | EVENT_WRITABLE);
    }
}

void MuxBackendConnection::encodeFrameHeader(unsigned char* buf, uint8_t flags,
                                               uint64_t stream_id, uint32_t payload_len) {
    buf[0] = MUX_FRAME_MAGIC;
    buf[1] = flags;
    buf[2] = (stream_id >> 56) & 0xFF;
    buf[3] = (stream_id >> 48) & 0xFF;
    buf[4] = (stream_id >> 40) & 0xFF;
    buf[5] = (stream_id >> 32) & 0xFF;
    buf[6] = (stream_id >> 24) & 0xFF;
    buf[7] = (stream_id >> 16) & 0xFF;
    buf[8] = (stream_id >> 8) & 0xFF;
    buf[9] = stream_id & 0xFF;
    buf[10] = (payload_len >> 24) & 0xFF;
    buf[11] = (payload_len >> 16) & 0xFF;
    buf[12] = (payload_len >> 8) & 0xFF;
    buf[13] = payload_len & 0xFF;
}

bool MuxBackendConnection::decodeFrameHeader(const unsigned char* buf, uint8_t& flags,
                                               uint64_t& stream_id, uint32_t& payload_len) {
    if (buf[0] != MUX_FRAME_MAGIC) return false;
    flags = buf[1];
    stream_id = ((uint64_t)buf[2] << 56) | ((uint64_t)buf[3] << 48) |
                ((uint64_t)buf[4] << 40) | ((uint64_t)buf[5] << 32) |
                ((uint64_t)buf[6] << 24) | ((uint64_t)buf[7] << 16) |
                ((uint64_t)buf[8] << 8) | (uint64_t)buf[9];
    payload_len = ((uint32_t)buf[10] << 24) | ((uint32_t)buf[11] << 16) |
                  ((uint32_t)buf[12] << 8) | (uint32_t)buf[13];
    return true;
}

void MuxBackendConnection::processRecvBuffer() {
    while (!recv_buf_.empty()) {
        if (!parsing_payload_) {
            // Reading frame header
            size_t need = MUX_FRAME_HEADER_SIZE - header_bytes_read_;
            size_t avail = recv_buf_.size();
            size_t copy = std::min(need, avail);

            memcpy(frame_header_buf_ + header_bytes_read_, recv_buf_.data(), copy);
            header_bytes_read_ += copy;
            recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + copy);

            if (header_bytes_read_ < MUX_FRAME_HEADER_SIZE) {
                break; // Need more data
            }

            // Decode header
            if (!decodeFrameHeader(frame_header_buf_, current_flags_,
                                    current_stream_id_, current_payload_len_)) {
                LOG_ERROR("Invalid MUX frame from backend %s:%d", addr_.c_str(), port_);
                onConnectionError();
                return;
            }

            header_bytes_read_ = 0;

            if (current_payload_len_ > MUX_MAX_PAYLOAD_SIZE) {
                LOG_ERROR("MUX frame payload too large: %u", current_payload_len_);
                onConnectionError();
                return;
            }

            if (current_payload_len_ == 0) {
                // No payload, dispatch immediately
                handleFrame(current_flags_ & MUX_FRAME_TYPE_MASK, current_stream_id_, nullptr, 0);
            } else {
                parsing_payload_ = true;
                frame_payload_buf_.clear();
                frame_payload_buf_.reserve(current_payload_len_);
            }
        }

        if (parsing_payload_) {
            size_t need = current_payload_len_ - frame_payload_buf_.size();
            size_t avail = recv_buf_.size();
            size_t copy = std::min(need, avail);

            frame_payload_buf_.insert(frame_payload_buf_.end(),
                                       recv_buf_.begin(), recv_buf_.begin() + copy);
            recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + copy);

            if (frame_payload_buf_.size() < current_payload_len_) {
                break; // Need more data
            }

            // Full frame received
            handleFrame(current_flags_ & MUX_FRAME_TYPE_MASK, current_stream_id_,
                        frame_payload_buf_.data(), frame_payload_buf_.size());
            frame_payload_buf_.clear();
            parsing_payload_ = false;
        }
    }
}

void MuxBackendConnection::handleFrame(uint8_t type, uint64_t stream_id,
                                        const char* payload, size_t len) {
    switch (type) {
        case MUX_FRAME_DATA: {
            if (state_ == MuxConnState::AUTHENTICATING && stream_id == 0) {
                // This is the AUTH response
                if (payload && len > 0 && payload[0] == '+') {
                    LOG_INFO("Backend %s:%d MUX AUTH successful", addr_.c_str(), port_);
                    state_ = MuxConnState::READY;
                    flushPendingStreams();
                } else {
                    std::string resp(payload, std::min(len, (size_t)100));
                    LOG_ERROR("Backend %s:%d MUX AUTH failed: %s", addr_.c_str(), port_, resp.c_str());
                    onConnectionError();
                }
                return;
            }

            // Forward data to client
            ClientConnection* client = getStreamClient(stream_id);
            if (client) {
                client->appendToSendBuffer(payload, len);
            } else if (ipc_frontend_ && ipc_frontend_->isProxied(stream_id)) {
                // 热升级: Client 还在旧进程，通过 IPC 回传
                ipc_frontend_->forwardToOldProcess(stream_id, payload, len);
            } else {
                LOG_WARN("DATA frame for unknown stream %llu on backend %s:%d",
                         (unsigned long long)stream_id, addr_.c_str(), port_);
            }
            break;
        }
        case MUX_FRAME_STREAM_CLOSE: {
            ClientConnection* client = getStreamClient(stream_id);
            if (client) {
                client->close();
            }
            streams_.erase(stream_id);
            if (active_stream_count_ > 0) active_stream_count_--;
            break;
        }
        case MUX_FRAME_PING: {
            // Send PONG
            sendFrame(MUX_FRAME_PONG, 0, payload, len);
            break;
        }
        case MUX_FRAME_GOAWAY: {
            LOG_INFO("Received GOAWAY from backend %s:%d", addr_.c_str(), port_);
            state_ = MuxConnState::GOAWAY;
            // No new streams, existing ones continue
            if (active_stream_count_ == 0) {
                close();
                worker_->removeMuxConnection(this);
            }
            break;
        }
        default:
            LOG_DEBUG("Unknown MUX frame type %d from backend %s:%d", type, addr_.c_str(), port_);
            break;
    }
}

void MuxBackendConnection::processNonMuxRecvBuffer() {
    // In non-MUX mode, there's exactly one stream (or zero for the connection itself)
    // Forward all received data to the bound client
    if (recv_buf_.empty()) return;

    // Non-MUX: stream_id 0 is used as the single stream
    // Find the client bound to this connection
    ClientConnection* client = nullptr;
    for (auto& [sid, c] : streams_) {
        client = c;
        break;
    }

    if (client) {
        client->appendToSendBuffer(recv_buf_.data(), recv_buf_.size());
        recv_buf_.clear();
    }
}

void MuxBackendConnection::flushPendingStreams() {
    // Called when connection becomes READY
    // Process any pending clients waiting for connection
    LOG_DEBUG("Backend %s:%d ready, %u active streams, %zu pending clients",
              addr_.c_str(), port_, active_stream_count_, pending_clients_.size());

    auto pending = std::move(pending_clients_);
    pending_clients_.clear();

    for (auto* client : pending) {
        uint64_t sid = createStream(client);
        if (sid == 0) {
            LOG_ERROR("Failed to create stream for pending client fd=%d", client->getFd());
            client->sendError("ERR backend stream creation failed");
            client->close();
            continue;
        }
        client->setStreamingMux(this, sid);
        client->retryForwardToBackend();
    }
}

void MuxBackendConnection::addPendingClient(ClientConnection* client) {
    pending_clients_.push_back(client);
}

// ========== 热升级支持方法 ==========

void MuxBackendConnection::rebindStream(uint64_t stream_id, ClientConnection* new_client) {
    auto it = streams_.find(stream_id);
    if (it != streams_.end()) {
        it->second = new_client;
        LOG_DEBUG("Rebound stream %llu to new client fd=%d on backend %s:%d",
                  (unsigned long long)stream_id,
                  new_client ? new_client->getFd() : -1,
                  addr_.c_str(), port_);
    } else {
        // stream 不存在，直接添加
        streams_[stream_id] = new_client;
        active_stream_count_++;
        LOG_DEBUG("Added stream %llu for migrated client on backend %s:%d",
                  (unsigned long long)stream_id, addr_.c_str(), port_);
    }
}

MuxBackendConnection::FrameParseState MuxBackendConnection::getFrameParseState() const {
    FrameParseState state;
    memcpy(state.frame_header_buf, frame_header_buf_, sizeof(frame_header_buf_));
    state.header_bytes_read = header_bytes_read_;
    state.parsing_payload = parsing_payload_;
    state.current_flags = current_flags_;
    state.current_stream_id = current_stream_id_;
    state.current_payload_len = current_payload_len_;
    state.frame_payload_buf = frame_payload_buf_;
    state.recv_buf = recv_buf_;
    state.send_buf = send_buf_;
    state.send_offset = send_offset_;
    return state;
}

void MuxBackendConnection::restoreFrameParseState(const FrameParseState& state) {
    memcpy(frame_header_buf_, state.frame_header_buf, sizeof(frame_header_buf_));
    header_bytes_read_ = state.header_bytes_read;
    parsing_payload_ = state.parsing_payload;
    current_flags_ = state.current_flags;
    current_stream_id_ = state.current_stream_id;
    current_payload_len_ = state.current_payload_len;
    frame_payload_buf_ = state.frame_payload_buf;
    recv_buf_ = state.recv_buf;
    send_buf_ = state.send_buf;
    send_offset_ = state.send_offset;
}

void MuxBackendConnection::restoreFrameParseState(
    const unsigned char* header_buf, size_t header_bytes_read,
    bool parsing_payload, uint8_t flags,
    uint64_t stream_id, uint32_t payload_len,
    const char* payload_buf, size_t payload_buf_len) {
    if (header_buf && header_bytes_read > 0) {
        size_t copy_len = std::min(header_bytes_read, sizeof(frame_header_buf_));
        memcpy(frame_header_buf_, header_buf, copy_len);
    }
    header_bytes_read_ = header_bytes_read;
    parsing_payload_ = parsing_payload;
    current_flags_ = flags;
    current_stream_id_ = stream_id;
    current_payload_len_ = payload_len;
    if (payload_buf && payload_buf_len > 0) {
        frame_payload_buf_.assign(payload_buf, payload_buf + payload_buf_len);
    }
}

void MuxBackendConnection::adoptFd(int fd) {
    // 接管一个已有的 fd（从旧进程迁移过来，不重新 connect）
    if (fd_ >= 0 && fd_ != fd) {
        ::close(fd_);
    }
    fd_ = fd;
    state_ = MuxConnState::READY;
    LOG_INFO("Backend %s:%d adopted fd=%d", addr_.c_str(), port_, fd);
}

int MuxBackendConnection::detachFd() {
    // 分离 fd 所有权（不 close），防止析构时 close
    int f = fd_;
    fd_ = -1;
    return f;
}

void MuxBackendConnection::injectRecvBuffer(const std::vector<char>& data) {
    if (data.empty()) return;
    recv_buf_.insert(recv_buf_.end(), data.begin(), data.end());
    LOG_DEBUG("Backend %s:%d: injected %zu bytes into recv_buf",
             addr_.c_str(), port_, data.size());
}

void MuxBackendConnection::injectSendBuffer(const std::vector<char>& data) {
    if (data.empty()) return;
    send_buf_.insert(send_buf_.end(), data.begin(), data.end());
    LOG_DEBUG("Backend %s:%d: injected %zu bytes into send_buf",
             addr_.c_str(), port_, data.size());
}

} // namespace proxy
