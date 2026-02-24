#pragma once

#include "event_loop.h"
#include "resp_parser.h"
#include <string>
#include <vector>
#include <functional>
#include <chrono>

namespace proxy {

// Forward declarations
class ProxyServer;

class AdminServer {
public:
    explicit AdminServer(ProxyServer* server);
    ~AdminServer();

    // Non-copyable
    AdminServer(const AdminServer&) = delete;
    AdminServer& operator=(const AdminServer&) = delete;

    // Lifecycle
    bool init(uint16_t port, EventLoop& loop);
    void shutdown();

private:
    ProxyServer* server_;
    EventLoop* loop_ = nullptr;
    int listen_fd_ = -1;
    std::chrono::steady_clock::time_point start_time_;

    // Admin client connections
    struct AdminClient {
        int fd;
        std::vector<char> recv_buf;
        std::vector<char> send_buf;
        size_t send_offset = 0;
    };

    std::vector<AdminClient*> clients_;

    void onNewConnection(int listen_fd);
    void onClientReadable(AdminClient* client);
    void onClientWritable(AdminClient* client);
    void processCommand(AdminClient* client, const std::string& cmd,
                        const std::vector<std::string>& args);
    void sendResponse(AdminClient* client, const std::string& response);
    void closeClient(AdminClient* client);

    // Command handlers
    void handleProxyInfo(AdminClient* client);
    void handleProxyStats(AdminClient* client);
    void handleProxyBackend(AdminClient* client, const std::vector<std::string>& args);
    void handleProxyConnections(AdminClient* client);
    void handleProxyUpgrade(AdminClient* client, const std::vector<std::string>& args);
    void handleProxyLogLevel(AdminClient* client, const std::vector<std::string>& args);

    // RESP encoding helpers
    static std::string respSimpleString(const std::string& s);
    static std::string respError(const std::string& s);
    static std::string respBulkString(const std::string& s);
    static std::string respInteger(int64_t val);
    static std::string respArray(const std::vector<std::string>& items);
};

} // namespace proxy
