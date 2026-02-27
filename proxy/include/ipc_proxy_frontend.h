#pragma once

#include "ipc_proxy_backend.h"  // ClientMigrateMeta, IPC_FRAME_* 常量
#include "mux_backend_connection.h"  // MUX 帧常量

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <functional>

namespace proxy {

class ClientConnection;
class MuxBackendConnection;
class WorkerThread;

/**
 * IPCProxyFrontend - 新进程侧的 IPC 代理前端
 *
 * 在热升级期间，从 IPC 通道接收旧进程转发的请求，
 * 转发到真正的 MuxBackendConnection（已迁移到本进程的后端连接）。
 * 当后端返回回复时，判断 stream 是否仍在旧进程，如果是则通过 IPC 回传。
 *
 * 同时处理 FENCE/FENCE_ACK 协议，配合旧进程逐步迁移客户端。
 */
class IPCProxyFrontend {
public:
    /**
     * 构造 IPCProxyFrontend
     * @param ipc_fd socketpair 的新进程端 (socketpair[1])
     * @param real_backend 迁移过来的真正后端连接
     * @param worker 所属的 WorkerThread
     */
    IPCProxyFrontend(int ipc_fd, MuxBackendConnection* real_backend, WorkerThread* worker);
    ~IPCProxyFrontend();

    // 不可复制
    IPCProxyFrontend(const IPCProxyFrontend&) = delete;
    IPCProxyFrontend& operator=(const IPCProxyFrontend&) = delete;

    // ========== I/O 处理 ==========

    // IPC 通道可读时调用（从旧进程接收请求数据或 FENCE）
    void onIPCReadable();

    // IPC 通道可写时调用（刷新发送缓冲区）
    void onIPCWritable();

    // ========== 后端回复路由 ==========

    // 判断某个 stream 是否仍由 IPC 代理（Client 还在旧进程）
    bool isProxied(uint64_t stream_id) const;

    // 后端回复到达时，转发给旧进程（通过 IPC 通道）
    void forwardToOldProcess(uint64_t stream_id, const char* data, size_t len);

    // ========== Stream 管理 ==========

    // 初始化时注册所有被代理的 stream
    void addProxiedStream(uint64_t stream_id);

    // 当客户端 fd 从旧进程迁移过来后，从代理集合中移除
    void removeProxiedStream(uint64_t stream_id);

    // 获取所有代理中的 stream
    const std::unordered_map<uint64_t, uint32_t>& getProxiedStreams() const { return proxied_; }

    // ========== 客户端 fd 接收 ==========

    // 设置接收到客户端 fd 时的回调
    using ClientReceivedCallback = std::function<void(int client_fd, const ClientMigrateMeta& meta,
                                                       const std::vector<char>& recv_buf,
                                                       const std::vector<char>& send_buf)>;
    void setClientReceivedCallback(ClientReceivedCallback cb) { client_received_cb_ = std::move(cb); }

    // 从 IPC 通道接收一个客户端 fd + 元数据（FENCE_ACK 之后调用）
    void receiveClientFd();

    // ========== 状态 ==========

    int getIpcFd() const { return ipc_fd_; }
    bool isAlive() const { return ipc_fd_ >= 0; }
    bool hasProxiedStreams() const { return !proxied_.empty(); }

    // 获取关联的真正后端连接
    MuxBackendConnection* getRealBackend() const { return real_backend_; }

private:
    int ipc_fd_;                       // socketpair[1]
    MuxBackendConnection* real_backend_; // 迁移过来的真正后端连接
    WorkerThread* worker_;

    // 被代理的 stream: stream_id → in_flight 计数
    struct ProxiedStreamInfo {
        uint32_t in_flight = 0;        // 已转发给 Redis 但未收到回复的请求数
        bool fence_received = false;    // 是否收到了 FENCE
    };
    std::unordered_map<uint64_t, ProxiedStreamInfo> proxied_info_;

    // 简单集合版本（stream_id → in_flight），用于快速查询
    std::unordered_map<uint64_t, uint32_t> proxied_;

    // 客户端 fd 接收回调
    ClientReceivedCallback client_received_cb_;

    // 帧解析状态机（复用 MUX 帧格式）
    unsigned char frame_header_buf_[MUX_FRAME_HEADER_SIZE];
    size_t header_bytes_read_ = 0;
    bool parsing_payload_ = false;
    uint8_t current_flags_ = 0;
    uint64_t current_stream_id_ = 0;
    uint32_t current_payload_len_ = 0;
    std::vector<char> frame_payload_buf_;

    // 发送缓冲区（回传给旧进程的数据）
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

    // 尝试发送 FENCE_ACK（当 fence_received && in_flight == 0）
    void tryAckFence(uint64_t stream_id);

    // 刷新发送缓冲区
    void flushSendBuffer();
};

} // namespace proxy
