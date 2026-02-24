// AdminServer implementation
#include "admin_server.h"
#include "proxy_server.h"
#include "logger.h"
#include "resp_parser.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <sstream>

namespace proxy {

AdminServer::AdminServer(ProxyServer* server)
    : server_(server), start_time_(std::chrono::steady_clock::now()) {}

AdminServer::~AdminServer() {
    shutdown();
}

bool AdminServer::init(uint16_t port, EventLoop& loop) {
    loop_ = &loop;

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("AdminServer: socket() failed: %s", strerror(errno));
        return false;
    }

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("AdminServer: bind() port %d failed: %s", port, strerror(errno));
        ::close(fd);
        return false;
    }

    if (listen(fd, 16) < 0) {
        LOG_ERROR("AdminServer: listen() port %d failed: %s", port, strerror(errno));
        ::close(fd);
        return false;
    }

    listen_fd_ = fd;

    loop_->addEvent(listen_fd_, EVENT_READABLE,
        [this](int /*fd*/, uint32_t /*events*/) {
            onNewConnection(listen_fd_);
        });

    LOG_INFO("AdminServer listening on port %d", port);
    return true;
}

void AdminServer::shutdown() {
    if (listen_fd_ >= 0 && loop_) {
        loop_->removeEvent(listen_fd_);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    for (auto* client : clients_) {
        if (loop_) loop_->removeEvent(client->fd);
        ::close(client->fd);
        delete client;
    }
    clients_.clear();
}

void AdminServer::onNewConnection(int /*listen_fd*/) {
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept4(listen_fd_, (struct sockaddr*)&client_addr,
                                &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            LOG_ERROR("AdminServer: accept4() failed: %s", strerror(errno));
            break;
        }

        int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        auto* client = new AdminClient();
        client->fd = client_fd;
        clients_.push_back(client);

        loop_->addEvent(client_fd, EVENT_READABLE,
            [this, client](int /*fd*/, uint32_t events) {
                if (events & EVENT_ERROR) {
                    closeClient(client);
                    return;
                }
                if (events & EVENT_READABLE) {
                    onClientReadable(client);
                }
                if (events & EVENT_WRITABLE) {
                    onClientWritable(client);
                }
            });

        LOG_DEBUG("AdminServer: new admin connection fd=%d", client_fd);
    }
}

void AdminServer::onClientReadable(AdminClient* client) {
    char buf[4096];
    ssize_t n = ::read(client->fd, buf, sizeof(buf));
    if (n <= 0) {
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        closeClient(client);
        return;
    }

    client->recv_buf.insert(client->recv_buf.end(), buf, buf + n);

    // Try to parse commands from the buffer
    while (!client->recv_buf.empty()) {
        RespParser::Command cmd;
        if (!RespParser::parse(client->recv_buf.data(), client->recv_buf.size(), cmd)) {
            break;
        }

        processCommand(client, cmd.name, cmd.args);
        client->recv_buf.erase(client->recv_buf.begin(),
                               client->recv_buf.begin() + cmd.bytes_consumed);
    }
}

void AdminServer::onClientWritable(AdminClient* client) {
    if (client->send_buf.empty() || client->send_offset >= client->send_buf.size()) {
        // Nothing to write, remove WRITABLE interest
        loop_->modifyEvent(client->fd, EVENT_READABLE);
        client->send_buf.clear();
        client->send_offset = 0;
        return;
    }

    size_t remaining = client->send_buf.size() - client->send_offset;
    ssize_t n = ::write(client->fd, client->send_buf.data() + client->send_offset, remaining);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        closeClient(client);
        return;
    }

    client->send_offset += n;
    if (client->send_offset >= client->send_buf.size()) {
        client->send_buf.clear();
        client->send_offset = 0;
        loop_->modifyEvent(client->fd, EVENT_READABLE);
    }
}

void AdminServer::sendResponse(AdminClient* client, const std::string& response) {
    client->send_buf.insert(client->send_buf.end(), response.begin(), response.end());
    loop_->modifyEvent(client->fd, EVENT_READABLE | EVENT_WRITABLE);
}

void AdminServer::closeClient(AdminClient* client) {
    loop_->removeEvent(client->fd);
    ::close(client->fd);
    auto it = std::find(clients_.begin(), clients_.end(), client);
    if (it != clients_.end()) {
        clients_.erase(it);
    }
    delete client;
}

void AdminServer::processCommand(AdminClient* client, const std::string& cmd,
                                  const std::vector<std::string>& args) {
    if (cmd == "PROXY") {
        if (args.empty()) {
            sendResponse(client, respError("ERR wrong number of arguments for 'PROXY' command"));
            return;
        }

        std::string subcmd = args[0];
        std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(), ::toupper);

        std::vector<std::string> sub_args(args.begin() + 1, args.end());

        if (subcmd == "INFO") {
            handleProxyInfo(client);
        } else if (subcmd == "STATS") {
            handleProxyStats(client);
        } else if (subcmd == "BACKEND") {
            handleProxyBackend(client, sub_args);
        } else if (subcmd == "CONNECTIONS") {
            handleProxyConnections(client);
        } else if (subcmd == "UPGRADE") {
            handleProxyUpgrade(client, sub_args);
        } else if (subcmd == "LOG") {
            handleProxyLogLevel(client, sub_args);
        } else {
            sendResponse(client, respError("ERR unknown PROXY subcommand '" + subcmd + "'"));
        }
    } else if (cmd == "PING") {
        sendResponse(client, respSimpleString("PONG"));
    } else if (cmd == "QUIT") {
        sendResponse(client, respSimpleString("OK"));
        // Close after send
    } else {
        sendResponse(client, respError("ERR unknown command '" + cmd + "'"));
    }
}

void AdminServer::handleProxyInfo(AdminClient* client) {
    auto now = std::chrono::steady_clock::now();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        now - server_->getStartTime()).count();

    const auto& workers = server_->getWorkers();
    size_t total_conns = 0;
    uint64_t total_requests = 0;
    uint64_t total_bytes_in = 0;
    uint64_t total_bytes_out = 0;
    size_t total_mux_conns = 0;
    size_t total_streams = 0;

    for (const auto& w : workers) {
        total_conns += w->getConnectionCount();
        total_requests += w->getRequests();
        total_bytes_in += w->getBytesIn();
        total_bytes_out += w->getBytesOut();
        total_mux_conns += w->getMuxConnectionCount();
        total_streams += w->getTotalStreamCount();
    }

    std::ostringstream oss;
    oss << "# Server\r\n";
    oss << "redis_mux_proxy_version:1.0.0\r\n";
    oss << "uptime_in_seconds:" << uptime << "\r\n";
    oss << "process_id:" << getpid() << "\r\n";
    oss << "\r\n";
    oss << "# Workers\r\n";
    oss << "worker_threads:" << workers.size() << "\r\n";
    oss << "\r\n";
    oss << "# Clients\r\n";
    oss << "connected_clients:" << total_conns << "\r\n";
    oss << "\r\n";
    oss << "# Stats\r\n";
    oss << "total_requests:" << total_requests << "\r\n";
    oss << "total_bytes_in:" << total_bytes_in << "\r\n";
    oss << "total_bytes_out:" << total_bytes_out << "\r\n";
    oss << "\r\n";
    oss << "# MUX\r\n";
    oss << "mux_enabled:" << (server_->getConfig().getMuxConfig().enabled ? "yes" : "no") << "\r\n";
    oss << "mux_connections:" << total_mux_conns << "\r\n";
    oss << "mux_streams:" << total_streams << "\r\n";
    oss << "\r\n";
    oss << "# Backends\r\n";
    auto backends = server_->getBackendManager().getAllBackends();
    oss << "backend_count:" << backends.size() << "\r\n";

    sendResponse(client, respBulkString(oss.str()));
}

void AdminServer::handleProxyStats(AdminClient* client) {
    auto backends = server_->getBackendManager().getAllBackends();
    const auto& workers = server_->getWorkers();

    std::vector<std::string> entries;
    for (const auto& backend : backends) {
        // Per-backend stats
        size_t backend_streams = 0;
        size_t backend_mux_conns = 0;

        std::string key = backend.config.backend_addr + ":" +
                          std::to_string(backend.config.backend_port);

        entries.push_back(respBulkString("listen_port"));
        entries.push_back(respInteger(backend.config.listen_port));
        entries.push_back(respBulkString("backend_addr"));
        entries.push_back(respBulkString(key));
        entries.push_back(respBulkString("active"));
        entries.push_back(respBulkString(backend.active ? "yes" : "no"));

        // Count connections for this backend across all workers
        for (const auto& w : workers) {
            backend_mux_conns += w->getMuxConnectionCount();
            backend_streams += w->getTotalStreamCount();
        }

        entries.push_back(respBulkString("mux_connections"));
        entries.push_back(respInteger(static_cast<int64_t>(backend_mux_conns)));
        entries.push_back(respBulkString("streams"));
        entries.push_back(respInteger(static_cast<int64_t>(backend_streams)));
    }

    sendResponse(client, respArray(entries));
}

void AdminServer::handleProxyBackend(AdminClient* client, const std::vector<std::string>& args) {
    if (args.empty()) {
        sendResponse(client, respError("ERR usage: PROXY BACKEND ADD|REMOVE|LIST [args...]"));
        return;
    }

    std::string action = args[0];
    std::transform(action.begin(), action.end(), action.begin(), ::toupper);

    if (action == "LIST") {
        auto backends = server_->getBackendManager().getAllBackends();
        std::vector<std::string> entries;
        for (const auto& b : backends) {
            std::string desc = std::to_string(b.config.listen_port) + " -> " +
                               b.config.backend_addr + ":" +
                               std::to_string(b.config.backend_port) +
                               (b.active ? " (active)" : " (inactive)");
            entries.push_back(respBulkString(desc));
        }
        sendResponse(client, respArray(entries));

    } else if (action == "ADD") {
        // PROXY BACKEND ADD <listen_port> <backend_addr> <backend_port> [password]
        if (args.size() < 4) {
            sendResponse(client, respError(
                "ERR usage: PROXY BACKEND ADD <listen_port> <backend_addr> <backend_port> [password]"));
            return;
        }

        BackendConfig cfg;
        try {
            cfg.listen_port = static_cast<uint16_t>(std::stoi(args[1]));
            cfg.backend_addr = args[2];
            cfg.backend_port = static_cast<uint16_t>(std::stoi(args[3]));
        } catch (...) {
            sendResponse(client, respError("ERR invalid port number"));
            return;
        }
        if (args.size() > 4) {
            cfg.backend_password = args[4];
        }

        if (server_->addBackend(cfg)) {
            sendResponse(client, respSimpleString("OK"));
        } else {
            sendResponse(client, respError("ERR failed to add backend"));
        }

    } else if (action == "REMOVE") {
        // PROXY BACKEND REMOVE <listen_port>
        if (args.size() < 2) {
            sendResponse(client, respError("ERR usage: PROXY BACKEND REMOVE <listen_port>"));
            return;
        }

        uint16_t port;
        try {
            port = static_cast<uint16_t>(std::stoi(args[1]));
        } catch (...) {
            sendResponse(client, respError("ERR invalid port number"));
            return;
        }

        if (server_->removeBackend(port)) {
            sendResponse(client, respSimpleString("OK"));
        } else {
            sendResponse(client, respError("ERR failed to remove backend"));
        }

    } else {
        sendResponse(client, respError("ERR unknown BACKEND subcommand '" + action + "'"));
    }
}

void AdminServer::handleProxyConnections(AdminClient* client) {
    const auto& workers = server_->getWorkers();
    std::vector<std::string> entries;

    for (const auto& w : workers) {
        std::string info = "worker=" + std::to_string(w->getId()) +
                           " clients=" + std::to_string(w->getConnectionCount()) +
                           " mux_conns=" + std::to_string(w->getMuxConnectionCount()) +
                           " streams=" + std::to_string(w->getTotalStreamCount()) +
                           " requests=" + std::to_string(w->getRequests()) +
                           " bytes_in=" + std::to_string(w->getBytesIn()) +
                           " bytes_out=" + std::to_string(w->getBytesOut());
        entries.push_back(respBulkString(info));
    }

    sendResponse(client, respArray(entries));
}

void AdminServer::handleProxyUpgrade(AdminClient* client, const std::vector<std::string>& args) {
    if (args.empty()) {
        // PROXY UPGRADE - trigger upgrade
        sendResponse(client, respError("ERR hot upgrade not yet implemented"));
    } else {
        std::string subcmd = args[0];
        std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(), ::toupper);
        if (subcmd == "STATUS") {
            sendResponse(client, respBulkString("idle"));
        } else {
            sendResponse(client, respError("ERR usage: PROXY UPGRADE [STATUS]"));
        }
    }
}

void AdminServer::handleProxyLogLevel(AdminClient* client, const std::vector<std::string>& args) {
    if (args.empty() || args.size() < 2) {
    // Show current level
        std::string current;
        auto level = Logger::instance().getLevel();
        switch (level) {
            case LogLevel::LOG_LVL_DEBUG: current = "DEBUG"; break;
            case LogLevel::LOG_LVL_INFO:  current = "INFO"; break;
            case LogLevel::LOG_LVL_WARN:  current = "WARN"; break;
            case LogLevel::LOG_LVL_ERROR: current = "ERROR"; break;
        }
        sendResponse(client, respBulkString(current));
        return;
    }

    std::string level_str = args[0];
    if (args.size() >= 2) level_str = args[1];
    std::transform(level_str.begin(), level_str.end(), level_str.begin(), ::toupper);

    LogLevel level;
    if (level_str == "DEBUG") level = LogLevel::LOG_LVL_DEBUG;
    else if (level_str == "INFO") level = LogLevel::LOG_LVL_INFO;
    else if (level_str == "WARN") level = LogLevel::LOG_LVL_WARN;
    else if (level_str == "ERROR") level = LogLevel::LOG_LVL_ERROR;
    else {
        sendResponse(client, respError("ERR invalid log level: " + level_str +
                                        " (valid: DEBUG, INFO, WARN, ERROR)"));
        return;
    }

    Logger::instance().setLevel(level);
    LOG_INFO("Log level changed to %s via admin command", level_str.c_str());
    sendResponse(client, respSimpleString("OK"));
}

std::string AdminServer::respSimpleString(const std::string& s) { return "+" + s + "\r\n"; }
std::string AdminServer::respError(const std::string& s) { return "-" + s + "\r\n"; }
std::string AdminServer::respBulkString(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string AdminServer::respInteger(int64_t val) { return ":" + std::to_string(val) + "\r\n"; }
std::string AdminServer::respArray(const std::vector<std::string>& items) {
    std::string result = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) result += item;
    return result;
}

} // namespace proxy
