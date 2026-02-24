#pragma once

#include "config.h"
#include "event_loop.h"
#include "worker_thread.h"
#include "backend_manager.h"
#include "admin_server.h"

#include <vector>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <chrono>

namespace proxy {

class AdminServer;
class HotUpgradeManager;

class ProxyServer {
public:
    explicit ProxyServer(Config& config);
    ~ProxyServer();

    ProxyServer(const ProxyServer&) = delete;
    ProxyServer& operator=(const ProxyServer&) = delete;

    // Lifecycle
    bool init();
    void run();
    void shutdown();

    // Access
    Config& getConfig() { return config_; }
    BackendManager& getBackendManager() { return backend_mgr_; }
    const std::vector<std::unique_ptr<WorkerThread>>& getWorkers() const { return workers_; }
    EventLoop& getMainLoop() { return main_loop_; }
    bool isRunning() const { return running_.load(); }
    std::chrono::steady_clock::time_point getStartTime() const { return start_time_; }

    // Dynamic backend management (called from admin commands)
    bool addBackend(const BackendConfig& backend);
    bool removeBackend(uint16_t listen_port);

private:
    Config& config_;
    EventLoop main_loop_;
    BackendManager backend_mgr_;
    std::vector<std::unique_ptr<WorkerThread>> workers_;
    std::unique_ptr<AdminServer> admin_server_;
    std::atomic<uint32_t> next_worker_{0};
    std::atomic<bool> running_{false};
    std::chrono::steady_clock::time_point start_time_;

    // Listening sockets: listen_fd -> listen_port
    std::unordered_map<int, uint16_t> listen_fds_;

    bool setupListenSocket(uint16_t port);
    void closeListenSocket(uint16_t port);
    void onNewConnection(int listen_fd);
    WorkerThread& selectWorker();
    void setupSignalHandlers();
};

} // namespace proxy