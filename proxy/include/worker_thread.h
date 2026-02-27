#pragma once

#include "event_loop.h"
#include "hot_upgrade.h"  // MuxBackendTransferMeta, StreamMapEntry
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
class IPCProxyBackend;
class IPCProxyFrontend;
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
    // 通过 fd 直接从 connections_ map 中移除（热升级迁移用，fd 已从 epoll 移除且已 detach）
    void eraseConnection(int fd);

    // MUX connection pool: get or create a backend connection for given backend
    MuxBackendConnection* getOrCreateBackendConn(const std::string& addr, uint16_t port,
                                                   const std::string& password);

    // Remove a MUX backend connection from the pool (when it's closed)
    void removeMuxConnection(MuxBackendConnection* conn);

    // 通过 stream_id 查找对应的 MuxBackendConnection（跨线程安全：只读遍历连接池）
    MuxBackendConnection* findBackendByStreamId(uint64_t stream_id) const;

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

    // ========== 热升级 V2: 后端连接迁移 ==========

    // 后端连接迁移结果（一个 MuxBackendConnection 的快照）
    struct BackendTransferResult {
        int backend_fd;                  // 后端 socket fd（已从 epoll 摘除）
        std::string addr;
        uint16_t port;
        std::string password;
        bool mux_enabled;
        uint64_t next_stream_id;
        uint32_t active_stream_count;
        // 帧解析状态通过 getFrameParseState() 序列化
        MuxBackendConnection* orig_conn;  // 原始连接指针（用于匹配）
        int ipc_fd;                       // socketpair[0]（旧进程端）
        // stream_map: { stream_id, client_fd }
        std::vector<std::pair<uint64_t, int>> stream_map;
    };

    // 分派: 将所有后端 MUX 连接的 fd 摘除、创建 IPC 代理，返回结果
    void dispatchTransferBackends();
    bool waitTransferDone(int timeout_ms = 5000);
    std::vector<BackendTransferResult> getTransferResults();

    // 获取 IPC 代理后端列表（在 transferBackends 之后使用）
    std::vector<IPCProxyBackend*> getIPCProxyBackends() const;

    // 分派: 开始迁移某个 stream
    void dispatchMigrateStream(IPCProxyBackend* proxy, uint64_t stream_id);

    // ========== 热升级 V2: 事件驱动的迁移定时器 ==========

    // 迁移完成回调类型
    using MigrateDoneCallback = std::function<void(int client_fd,
                                                    const ClientMigrateMeta& meta,
                                                    const std::vector<char>& recv_buf,
                                                    const std::vector<char>& send_buf)>;
    using AllDoneCallback = std::function<void()>;

    // 分派: 在 Worker 线程中注册迁移定时器（事件驱动，不阻塞 event loop）
    void dispatchStartMigrateTimer(int interval_ms, int batch_size,
                                    MigrateDoneCallback on_stream_done,
                                    AllDoneCallback on_all_done);

    // ========== 新进程侧: 接收迁移过来的后端连接和 IPC fd ==========

    // 分派: 在 Worker 线程中恢复后端 MUX 连接
    void dispatchRestoreBackend(int backend_fd, const MuxBackendTransferMeta& meta,
                                 const std::vector<StreamMapEntry>& stream_map,
                                 const std::vector<char>& frame_payload_buf,
                                 const std::vector<char>& recv_buf,
                                 const std::vector<char>& send_buf);

    // 分派: 在 Worker 线程中创建 IPCProxyFrontend
    void dispatchCreateIPCFrontend(int ipc_fd, const std::string& backend_key);

    // 分派: 在 Worker 线程中接受迁移过来的客户端（完整恢复路径）
    void dispatchAcceptMigratedClient(int fd, uint16_t listen_port,
                                       uint64_t stream_id, bool authenticated,
                                       MuxBackendConnection* backend,
                                       std::vector<char>* recv_buf,
                                       std::vector<char>* send_buf);

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
        enum Type { NEW_CLIENT, CLOSE_BACKEND, PAUSE_READS, DETACH_ALL,
                    TRANSFER_BACKENDS, MIGRATE_STREAM, ACCEPT_MIGRATED_CLIENT,
                    START_MIGRATE_TIMER, RESTORE_BACKEND, CREATE_IPC_FRONTEND };
        Type type;
        int fd = -1;
        uint16_t listen_port = 0;
        // MIGRATE_STREAM 参数
        IPCProxyBackend* proxy_backend = nullptr;
        uint64_t stream_id = 0;
        // ACCEPT_MIGRATED_CLIENT 参数
        uint64_t migrated_stream_id = 0;
        bool migrated_authenticated = false;
        std::vector<char>* migrated_recv_buf = nullptr;
        std::vector<char>* migrated_send_buf = nullptr;
        MuxBackendConnection* target_backend = nullptr;
        // START_MIGRATE_TIMER 参数
        int migrate_interval_ms = 0;
        int migrate_batch_size = 0;
        MigrateDoneCallback migrate_stream_done_cb;
        AllDoneCallback migrate_all_done_cb;
        // RESTORE_BACKEND 参数
        MuxBackendTransferMeta* restore_meta = nullptr;
        std::vector<StreamMapEntry>* restore_stream_map = nullptr;
        std::vector<char>* restore_frame_payload = nullptr;
        std::vector<char>* restore_recv_buf = nullptr;
        std::vector<char>* restore_send_buf = nullptr;
        // CREATE_IPC_FRONTEND 参数
        std::string backend_key;
    };

    // Atomic flag: set after pause/detach/transfer completes in worker thread
    std::atomic<bool> pause_done_{false};
    std::atomic<bool> detach_done_{false};
    std::atomic<bool> transfer_done_{false};

    // Storage for detached client fd infos (filled during DETACH_ALL)
    std::vector<ClientConnInfo> detached_infos_;

    // Storage for backend transfer results (filled during TRANSFER_BACKENDS)
    std::vector<BackendTransferResult> transfer_results_;

    // IPC 代理后端（热升级期间，旧进程侧的虚拟后端）
    std::vector<std::unique_ptr<IPCProxyBackend>> ipc_proxy_backends_;

    // IPC 代理前端（热升级期间，新进程侧）
    std::vector<std::unique_ptr<IPCProxyFrontend>> ipc_proxy_frontends_;

    // 迁移定时器回调
    MigrateDoneCallback migrate_stream_done_cb_;
    AllDoneCallback migrate_all_done_cb_;
    TimerId migrate_timer_id_ = 0;  // 0 表示无定时器
    int migrate_batch_size_ = 10;  // 每次 timer 回调迁移的 stream 数量

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
    void transferBackends();    // Internal: runs in worker thread (Phase 1)
    void migrateStream(IPCProxyBackend* proxy, uint64_t stream_id); // Internal
    void startMigrateTimer(int interval_ms, int batch_size); // Internal: 启动迁移定时器
    void onMigrateTimerFired();  // Internal: 定时器触发，迁移一批 stream
    void restoreBackend(int fd, MuxBackendTransferMeta* meta,
                         std::vector<StreamMapEntry>* stream_map,
                         std::vector<char>* frame_payload,
                         std::vector<char>* recv_buf,
                         std::vector<char>* send_buf); // Internal: 新进程恢复后端
    void createIPCFrontend(int ipc_fd, const std::string& backend_key); // Internal
    void acceptMigratedClient(int fd, uint16_t listen_port, uint64_t stream_id,
                               bool authenticated, MuxBackendConnection* backend,
                               std::vector<char>* recv_buf, std::vector<char>* send_buf); // Internal
};

} // namespace proxy