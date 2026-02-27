#pragma once

#include <cstdint>
#include <cstddef>

namespace proxy {

class ClientConnection;

/**
 * IBackendConnection - 后端连接抽象基类
 *
 * 让 ClientConnection 对后端的实际实现透明：
 * - MuxBackendConnection: 真正的后端 MUX 连接
 * - IPCProxyBackend: IPC 代理桥，用于热升级期间的数据中转
 */
class IBackendConnection {
public:
    virtual ~IBackendConnection() = default;

    // 发送数据到后端（编码为 MUX DATA 帧）
    virtual void sendData(uint64_t stream_id, const char* data, size_t len) = 0;

    // 关闭一个 stream
    virtual void closeStream(uint64_t stream_id) = 0;

    // 通过 stream_id 查找对应的 ClientConnection
    virtual ClientConnection* getStreamClient(uint64_t stream_id) const = 0;

    // 查询 stream 数量
    virtual uint32_t getActiveStreamCount() const = 0;
};

} // namespace proxy
