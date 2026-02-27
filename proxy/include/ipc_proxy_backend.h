#pragma once

#include "i_backend_connection.h"
#include "mux_backend_connection.h"  // MUX 帧常量

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

namespace proxy {

class ClientConnection;
class WorkerThread;

// IPC 帧类型（新增，仅在 IPC 通道上使用，不与标准 MUX 帧冲突）
constexpr uint8_t IPC_FRAME_FENCE     = 0x0A;  // 排空栅栏: 某 stream 的请求到此为止
constexpr uint8_t IPC_FRAME_FENCE_ACK = 0x0B;  // 排空确认: 某 stream 的回复全部回传完毕

// 客户端迁移元数据（通过 sendmsg(SCM_RIGHTS) 传递 client fd 时附带）
struct ClientMigrateMeta {
    uint64_t stream_id;       // 对应的后端 stream ID
    uint16_t listen_port;     // 监听端口
    uint8_t  authenticated;   // 认证状态 (0/1)
    uint32_t recv_buf_len;    // 接收缓冲区残余长度
    uint32_t send_buf_len;    // 发送缓冲区残余长度
    // 后跟: recv_buf 数据
    // 后跟: send_buf 数据
};

/**
 * IPCProxyBackend - 旧进程侧的 IPC 代理后端
 *
 * 在热升级期间，替代真正的 MuxBackendConnection。
 * 实现与 MuxBackendConnection 相同的 IBackendConnection 接口，
 * 但把数据通过 socketpair 转发到新进程，而不是直接写后端 fd。
 *
 * 对 ClientConnection 完全透明——只需 setStreaming(ipc_proxy, stream_id)。
 */
class IPCProxyBackend : public IBackendConnection {
public:
    /**
     * 构造 IPCProxyBackend
     * @param ipc_fd socketpair 的旧进程端 (socketpair[0])
     * @param worker 所属的 WorkerThread
     */
    IPCProxyBackend(int ipc_fd, WorkerThread* worker);
    ~IPCProxyBackend();

    // 不可复制
    IPCProxyBackend(const IPCProxyBackend&) = delete;
    IPCProxyBackend& operator=(const IPCProxyBackend&) = delete;

    // ========== IBackendConnection 接口 ==========

    // 发送请求数据 → 编码为 MUX DATA 帧写入 IPC 通道
    void sendData(uint64_t stream_id, const char* data, size_t len) override;

    // 关闭 stream（发送 STREAM_CLOSE 帧到 IPC 通道）
    void closeStream(uint64_t stream_id) override;

    // 通过 stream_id 查找对应的 ClientConnection
    ClientConnection* getStreamClient(uint64_t stream_id) const override;

    // 活跃 stream 数量
    uint32_t getActiveStreamCount() const override;

    // ========== Stream 管理 ==========

    // 注册一个 stream（旧进程切换后端时调用）
    void addStream(uint64_t stream_id, ClientConnection* client);

    // 移除一个 stream（迁移完成时调用）
    void removeStream(uint64_t stream_id);

    // 获取所有 stream
    const std::unordered_map<uint64_t, ClientConnection*>& getStreams() const { return streams_; }

    // ========== I/O 处理 ==========

    // IPC 通道可读时调用（从新进程接收回复数据或 FENCE_ACK）
    void onReadable();

    // IPC 通道可写时调用（刷新发送缓冲区）
    void onWritable();

    // ========== 迁移相关 ==========

    // 单 stream 迁移状态
    enum class StreamMigrateState {
        ACTIVE,       // 正常代理中
        DRAINING,     // 已停止客户端读取，正在排空
        WAITING_ACK   // 已发送 FENCE，等待 FENCE_ACK
    };

    // 发起单个 stream 的迁移
    void beginMigrate(uint64_t stream_id);

    // 检查某 stream 是否正在迁移中
    bool isMigrating(uint64_t stream_id) const;

    // 获取迁移状态表
    const std::unordered_map<uint64_t, StreamMigrateState>& getMigrateStates() const {
        return migrate_state_;
    }

    // 设置迁移完成回调（完成一个 client 迁移时调用）
    using MigrateCallback = std::function<void(int client_fd, const ClientMigrateMeta& meta,
                                                const std::vector<char>& recv_buf,
                                                const std::vector<char>& send_buf)>;
    void setMigrateCallback(MigrateCallback cb) { migrate_callback_ = std::move(cb); }

    // 获取 IPC fd
    int getIpcFd() const { return ipc_fd_; }

    // 检查 IPC 通道是否仍有效
    bool isAlive() const { return ipc_fd_ >= 0; }

private:
    int ipc_fd_;              // socketpair[0]
    WorkerThread* worker_;

    // Stream 映射: stream_id → ClientConnection*
    std::unordered_map<uint64_t, ClientConnection*> streams_;

    // 每个 stream 的迁移状态
    std::unordered_map<uint64_t, StreamMigrateState> migrate_state_;

    // 迁移完成回调
    MigrateCallback migrate_callback_;

    // 帧解析状态机（复用 MUX 帧格式）
    unsigned char frame_header_buf_[MUX_FRAME_HEADER_SIZE];
    size_t header_bytes_read_ = 0;
    bool parsing_payload_ = false;
    uint8_t current_flags_ = 0;
    uint64_t current_stream_id_ = 0;
    uint32_t current_payload_len_ = 0;
    std::vector<char> frame_payload_buf_;

    // 发送缓冲区
    std::vector<char> send_buf_;
    size_t send_offset_ = 0;

    // 接收缓冲区
    std::vector<char> recv_buf_;

    // 编码并发送一个 MUX 帧到 IPC 通道
    void sendFrame(uint8_t type, uint64_t stream_id, const char* payload, size_t payload_len);

    // 解析接收缓冲区中的帧
    void processRecvBuffer();

    // 处理解析出的帧
    void handleFrame(uint8_t type, uint64_t stream_id, const char* payload, size_t len);

    // 完成单个 stream 的迁移（收到 FENCE_ACK 后调用）
    void completeMigration(uint64_t stream_id);

    // 刷新发送缓冲区
    void flushSendBuffer();
};

} // namespace proxy
