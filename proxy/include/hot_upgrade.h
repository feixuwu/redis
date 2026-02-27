#pragma once

#include <string>
#include <functional>
#include <cstdint>
#include <vector>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <atomic>

#include "event_loop.h"  // TimerId

namespace proxy {

// Forward declarations
class ProxyServer;
class WorkerThread;
class MuxBackendConnection;
class IPCProxyBackend;
class IPCProxyFrontend;

// Hot upgrade states
enum class UpgradeState {
    IDLE,                    // 无升级进行中
    BACKEND_TRANSFER,        // Phase 1: 正在传递后端 MUX fd
    IPC_PROXY,               // Phase 2: IPC 代理期，逐步搬迁 Client
    WAITING_ACK,             // Phase 3: 所有 Client 迁移完毕，等待确认
    COMPLETED,               // 升级完成
    FAILED                   // 升级失败
};

// 后端 MUX 连接传递元数据
struct MuxBackendTransferMeta {
    int fd;                           // 后端 socket fd
    char addr[64];                    // 后端地址
    uint16_t port;                    // 后端端口
    char password[64];                // 认证密码
    uint8_t mux_enabled;              // 是否启用 MUX
    uint64_t next_stream_id;          // 下一个 stream ID
    uint32_t active_stream_count;     // 活跃 stream 数量

    // 帧解析状态机
    unsigned char frame_header_buf[14]; // MUX_FRAME_HEADER_SIZE
    size_t header_bytes_read;
    uint8_t parsing_payload;
    uint8_t current_flags;
    uint64_t current_stream_id;
    uint32_t current_payload_len;

    // Stream 映射表大小
    uint32_t stream_map_count;
    // 后跟: StreamMapEntry[stream_map_count] { uint64_t stream_id; int client_fd; }

    // 变长缓冲区大小
    uint32_t frame_payload_buf_len;
    uint32_t recv_buf_len;
    uint32_t send_buf_len;
    // 后跟: frame_payload_buf 数据
    // 后跟: recv_buf 数据
    // 后跟: send_buf 数据
};

// Stream 映射表条目
struct StreamMapEntry {
    uint64_t stream_id;
    int client_fd;    // 对应的客户端 fd（用于旧进程侧标识）
};

// 客户端迁移元数据（前向声明，完整定义在 ipc_proxy_backend.h 中）
struct ClientMigrateMeta;

// UDS 消息类型
constexpr uint8_t MSG_HANDSHAKE      = 0x01;
constexpr uint8_t MSG_HANDSHAKE_ACK  = 0x02;
constexpr uint8_t MSG_LISTEN_FD      = 0x03;
constexpr uint8_t MSG_BACKEND_FD     = 0x04;  // 传递后端 MUX fd
constexpr uint8_t MSG_IPC_FD         = 0x05;  // 传递 IPC socketpair fd
constexpr uint8_t MSG_CLIENT_FD      = 0x06;  // 传递客户端 fd
constexpr uint8_t MSG_TRANSFER_DONE  = 0x07;
constexpr uint8_t MSG_ACK            = 0x08;
constexpr uint8_t MSG_ROLLBACK       = 0x09;

class HotUpgradeManager {
public:
    explicit HotUpgradeManager(ProxyServer* server);
    ~HotUpgradeManager();

    // Non-copyable
    HotUpgradeManager(const HotUpgradeManager&) = delete;
    HotUpgradeManager& operator=(const HotUpgradeManager&) = delete;

    // ========== 旧进程侧 ==========

    // 发起热升级（旧进程调用）
    bool startUpgrade(const std::string& new_binary_path = "");

    // ========== 新进程侧 ==========

    // 从旧进程接收 fd 和状态（新进程调用，注册到 event loop 后异步接收）
    bool receiveFromOldProcess(const std::string& socket_path);

    // ========== 状态查询 ==========

    UpgradeState getState() const { return state_; }
    std::string getStateString() const;

private:
    ProxyServer* server_;
    UpgradeState state_ = UpgradeState::IDLE;
    std::string socket_path_;
    int uds_fd_ = -1;
    pid_t new_process_pid_ = -1;

    // ========== 迁移调度配置 ==========

    // 迁移定时器间隔（毫秒），默认 1ms
    int migrate_interval_ms_ = 1;
    // 每次 timer 回调最多迁移的 stream 数量
    int migrate_batch_size_ = 10;

    // ========== 旧进程侧: 迁移调度器 ==========

    struct MigrationScheduler {
        std::chrono::steady_clock::time_point start_time;
        std::chrono::steady_clock::time_point deadline;
        int total_streams = 0;
        std::atomic<int> migrated_streams{0};
        int current_backend_idx = 0;
    };
    MigrationScheduler scheduler_;

    // 已完成迁移的 Worker 计数
    std::atomic<int> workers_done_count_{0};

    // Worker 线程设置此标志，主线程检测后执行 finalizeUpgrade()
    std::atomic<bool> finalize_pending_{false};
    // 主线程检查 finalize_pending_ 的定时器 ID
    TimerId finalize_check_timer_ = 0;

    // 异步等待 ACK 的超时定时器 ID
    TimerId event_loop_ack_timer_ = 0;

    // UDS 发送互斥锁（多 Worker 回调共用 uds_fd_）
    std::mutex uds_mutex_;

    // ========== 新进程侧: 接收计数 ==========
    int received_listen_fds_ = 0;
    int received_backend_fds_ = 0;
    int received_client_fds_ = 0;

    // 已恢复的后端连接映射: "addr:port" → { worker, fd }
    struct RestoredBackendInfo {
        WorkerThread* worker;
        int fd;
    };
    std::unordered_map<std::string, RestoredBackendInfo> restored_backends_;

    // stream_id → Worker 映射（在 receiveBackendConnection 中构建，主线程读写，无竞争）
    // 用于 receiveClientConnection() 中快速定位目标 Worker，避免跨线程访问 mux_pools_
    std::unordered_map<uint64_t, WorkerThread*> stream_to_worker_;

    // ========== fd 传递工具方法 ==========

    bool sendFd(int uds_fd, int fd_to_send, const void* metadata, size_t meta_len);
    int recvFd(int uds_fd, void* metadata, size_t meta_len);

    // 发送变长数据（元数据之后的 buffer 数据）
    bool sendData(int uds_fd, const void* data, size_t len);
    bool recvData(int uds_fd, void* buf, size_t len);

    // ========== 旧进程侧: 三阶段迁移 ==========

    // Phase 1: 传递后端 MUX fd + 建立 IPC 桥（通过 Worker dispatch）
    bool transferBackendConnections();

    // Phase 2: 在每个 Worker 中注册迁移定时器（事件驱动，不阻塞）
    void startMigrationTimers();

    // 某个 stream 迁移完成后，通过 UDS 把 client fd 发给新进程
    void sendMigratedClient(int client_fd, const ClientMigrateMeta& meta,
                            const std::vector<char>& recv_buf,
                            const std::vector<char>& send_buf);

    // 某个 Worker 的所有 stream 迁移完毕时回调
    void onWorkerMigrationDone();

    // 注册 UDS fd 到主线程 event loop
    void registerUDSWatcher();

    // Phase 1 完成后旧进程关闭 listen fd（保证迁移收敛）
    void closeListenFds();

    // Phase 3: 完成确认
    bool finalizeUpgrade();

    // ========== 新进程侧: 异步接收 ==========

    // UDS fd 可读时的回调（event loop 驱动）
    void onUDSReadable();

    // UDS 连接错误处理
    void onUDSError();

    // 接收后端 MUX fd 并重建连接
    bool receiveBackendConnection();

    // 接收 IPC socketpair fd 并创建 IPCProxyFrontend
    bool receiveIPCFd();

    // 接收客户端 fd 并通过 acceptMigratedClient 完整恢复
    bool receiveClientConnection();

    // ========== 兼容旧结构 ==========
    struct ConnectionMetadata {
        int fd;
        uint16_t listen_port;
        bool authenticated;
        uint64_t stream_id;
        char backend_addr[64];
        uint16_t backend_port;
        uint32_t recv_buf_len;
        uint32_t send_buf_len;
    };
};

} // namespace proxy
