#pragma once

#include "event_loop.h"
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <functional>
#include <unordered_map>
#include <memory>
#include <string>

namespace proxy {

class ClientConnection;
class MuxBackendConnection;
class BackendManager;

class WorkerThread {
public:
    explicit WorkerThread(int id, BackendManager* backend_mgr, bool mux_enabled, uint32_t max_streams);
    ~WorkerThread();

    WorkerThread(const WorkerThread&) = delete;
    WorkerThread& operator=(const WorkerThread&) = delete;

    // Lifecycle
    void start();
    void stop();

    // Dispatch a new client fd to this worker (called from main thread)
    void dispatchClient(int client_fd, uint16_t listen_port);

    // Dispatch a task to close all connections for a backend
    void dispatchCloseBackend(uint16_t listen_port);

    // Get worker's event loop
    EventLoop& getEventLoop() { return event_loop_; }

    int getId() const { return id_; }
    size_t getConnectionCount() const;

    // Connection management (called within worker thread)
    void addConnection(ClientConnection* conn);
    void removeConnection(ClientConnection* conn);

    // MUX connection pool: get or create a backend connection for given backend
    MuxBackendConnection* getOrCreateBackendConn(const std::string& addr, uint16_t port,
                                                   const std::string& password);

    // Remove a MUX backend connection from the pool (when it's closed)
    void removeMuxConnection(MuxBackendConnection* conn);

    BackendManager* getBackendManager() { return backend_mgr_; }
    bool isMuxEnabled() const { return mux_enabled_; }

    // Hot upgrade support: get info about all active client connections
    struct ClientConnInfo {
        int fd;
        uint16_t listen_port;
        bool authenticated;
        uint64_t stream_id;
    };
    std::vector<ClientConnInfo> getClientConnInfos() const;

    // Hot upgrade: dispatch pause reads task to worker thread (thread-safe)
    void dispatchPauseReads();
    // Wait for pause reads to complete
    bool waitPauseDone(int timeout_ms = 500);

    // Hot upgrade: dispatch detach all clients task to worker thread (thread-safe)
    void dispatchDetachAll();
    // Wait for detach to complete
    bool waitDetachDone(int timeout_ms = 500);
    // Get the detached client infos (after detach completed)
    std::vector<ClientConnInfo> getDetachedInfos();

    // Statistics
    size_t getMuxConnectionCount() const;
    size_t getTotalStreamCount() const;

    // Per-worker stats (atomic for cross-thread reads)
    std::atomic<uint64_t> stat_requests{0};
    std::atomic<uint64_t> stat_bytes_in{0};
    std::atomic<uint64_t> stat_bytes_out{0};

    void incrRequests(uint64_t n = 1) { stat_requests.fetch_add(n, std::memory_order_relaxed); }
    void incrBytesIn(uint64_t n) { stat_bytes_in.fetch_add(n, std::memory_order_relaxed); }
    void incrBytesOut(uint64_t n) { stat_bytes_out.fetch_add(n, std::memory_order_relaxed); }
    uint64_t getRequests() const { return stat_requests.load(std::memory_order_relaxed); }
    uint64_t getBytesIn() const { return stat_bytes_in.load(std::memory_order_relaxed); }
    uint64_t getBytesOut() const { return stat_bytes_out.load(std::memory_order_relaxed); }

private:
    int id_;
    EventLoop event_loop_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    BackendManager* backend_mgr_;
    bool mux_enabled_;
    uint32_t max_streams_;

    // Notification mechanism
    int notify_fd_ = -1;

    struct PendingTask {
        enum Type { NEW_CLIENT, CLOSE_BACKEND, PAUSE_READS, DETACH_ALL };
        Type type;
        int fd = -1;
        uint16_t listen_port = 0;
    };

    // Atomic flag: set after pause/detach completes in worker thread
    std::atomic<bool> pause_done_{false};
    std::atomic<bool> detach_done_{false};

    // Storage for detached client fd infos (filled during DETACH_ALL)
    std::vector<ClientConnInfo> detached_infos_;

    std::mutex pending_mutex_;
    std::vector<PendingTask> pending_tasks_;

    // Active client connections owned by this worker
    std::unordered_map<int, std::unique_ptr<ClientConnection>> connections_; // fd -> ClientConnection

    // MUX connection pool: "addr:port" -> vector of MUX connections
    std::unordered_map<std::string, std::vector<std::unique_ptr<MuxBackendConnection>>> mux_pools_;

    void threadMain();
    void processPendingTasks();
    void handleNewClient(int client_fd, uint16_t listen_port);
    void closeBackendConnections(uint16_t listen_port);
    void pauseClientReads();    // Internal: runs in worker thread
    void detachAllClients();    // Internal: runs in worker thread
};

} // namespace proxy