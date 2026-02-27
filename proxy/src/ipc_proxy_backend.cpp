// IPCProxyBackend 实现 - 旧进程侧的 IPC 代理后端
#include "ipc_proxy_backend.h"
#include "client_connection.h"
#include "worker_thread.h"
#include "logger.h"

#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <algorithm>

namespace proxy {

IPCProxyBackend::IPCProxyBackend(int ipc_fd, WorkerThread* worker)
    : ipc_fd_(ipc_fd), worker_(worker) {
    memset(frame_header_buf_, 0, sizeof(frame_header_buf_));
}

IPCProxyBackend::~IPCProxyBackend() {
    if (ipc_fd_ >= 0) {
        ::close(ipc_fd_);
        ipc_fd_ = -1;
    }
}

// ========== IBackendConnection 接口实现 ==========

void IPCProxyBackend::sendData(uint64_t stream_id, const char* data, size_t len) {
    if (ipc_fd_ < 0) return;
    sendFrame(MUX_FRAME_DATA, stream_id, data, len);
}

void IPCProxyBackend::closeStream(uint64_t stream_id) {
    if (ipc_fd_ < 0) return;
    sendFrame(MUX_FRAME_STREAM_CLOSE, stream_id, nullptr, 0);
    streams_.erase(stream_id);
    migrate_state_.erase(stream_id);
}

ClientConnection* IPCProxyBackend::getStreamClient(uint64_t stream_id) const {
    auto it = streams_.find(stream_id);
    if (it != streams_.end()) {
        return it->second;
    }
    return nullptr;
}

uint32_t IPCProxyBackend::getActiveStreamCount() const {
    return static_cast<uint32_t>(streams_.size());
}

// ========== Stream 管理 ==========

void IPCProxyBackend::addStream(uint64_t stream_id, ClientConnection* client) {
    streams_[stream_id] = client;
    migrate_state_[stream_id] = StreamMigrateState::ACTIVE;
}

void IPCProxyBackend::removeStream(uint64_t stream_id) {
    streams_.erase(stream_id);
    migrate_state_.erase(stream_id);
}

// ========== I/O 处理 ==========

void IPCProxyBackend::onReadable() {
    if (ipc_fd_ < 0) return;

    // 从 IPC 通道读取数据
    char buf[65536];
    while (true) {
        ssize_t n = ::read(ipc_fd_, buf, sizeof(buf));
        if (n > 0) {
            recv_buf_.insert(recv_buf_.end(), buf, buf + n);
        } else if (n == 0) {
            // IPC 通道关闭
            LOG_WARN("IPCProxyBackend: IPC channel closed by new process");
            ::close(ipc_fd_);
            ipc_fd_ = -1;
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            LOG_ERROR("IPCProxyBackend: read error: %s", strerror(errno));
            ::close(ipc_fd_);
            ipc_fd_ = -1;
            return;
        }
    }

    // 解析帧
    processRecvBuffer();
}

void IPCProxyBackend::onWritable() {
    flushSendBuffer();
}

// ========== 迁移相关 ==========

void IPCProxyBackend::beginMigrate(uint64_t stream_id) {
    auto it = streams_.find(stream_id);
    if (it == streams_.end()) {
        LOG_WARN("IPCProxyBackend::beginMigrate: stream %llu not found",
                 (unsigned long long)stream_id);
        return;
    }

    ClientConnection* client = it->second;
    migrate_state_[stream_id] = StreamMigrateState::DRAINING;

    // Step 1: 停止从客户端读取新请求
    client->setPaused(true);

    // Step 2: 显式 flush client 的 recv_buf_ 中的残余数据
    // paused=true 后不会再触发 onReadable，所以必须手动转发残余数据
    // forwardToBackend() 内部会调用 backend_conn_->sendData()，
    // 此时 backend_conn_ 已经是 this（IPCProxyBackend），所以残余数据
    // 会通过 IPC 通道发到新进程
    client->flushRecvBuffer();

    // Step 3: 发送 FENCE 帧
    // TCP 有序性保证：FENCE 一定在所有 DATA 帧之后到达新进程
    sendFrame(IPC_FRAME_FENCE, stream_id, nullptr, 0);
    migrate_state_[stream_id] = StreamMigrateState::WAITING_ACK;

    LOG_INFO("IPCProxyBackend: began migration for stream %llu, FENCE sent",
             (unsigned long long)stream_id);
}

bool IPCProxyBackend::isMigrating(uint64_t stream_id) const {
    auto it = migrate_state_.find(stream_id);
    if (it == migrate_state_.end()) return false;
    return it->second != StreamMigrateState::ACTIVE;
}

// ========== 帧编解码 ==========

void IPCProxyBackend::sendFrame(uint8_t type, uint64_t stream_id,
                                  const char* payload, size_t payload_len) {
    if (ipc_fd_ < 0) return;

    // 编码帧头
    unsigned char header[MUX_FRAME_HEADER_SIZE];
    header[0] = MUX_FRAME_MAGIC;   // Magic
    header[1] = type;               // Flags (帧类型)

    // Stream ID (8 bytes, big-endian)
    header[2] = (stream_id >> 56) & 0xFF;
    header[3] = (stream_id >> 48) & 0xFF;
    header[4] = (stream_id >> 40) & 0xFF;
    header[5] = (stream_id >> 32) & 0xFF;
    header[6] = (stream_id >> 24) & 0xFF;
    header[7] = (stream_id >> 16) & 0xFF;
    header[8] = (stream_id >> 8) & 0xFF;
    header[9] = stream_id & 0xFF;

    // Payload length (4 bytes, big-endian)
    uint32_t plen = static_cast<uint32_t>(payload_len);
    header[10] = (plen >> 24) & 0xFF;
    header[11] = (plen >> 16) & 0xFF;
    header[12] = (plen >> 8) & 0xFF;
    header[13] = plen & 0xFF;

    // 追加到发送缓冲区
    send_buf_.insert(send_buf_.end(), header, header + MUX_FRAME_HEADER_SIZE);
    if (payload && payload_len > 0) {
        send_buf_.insert(send_buf_.end(), payload, payload + payload_len);
    }

    // 尝试立即发送
    flushSendBuffer();
}

void IPCProxyBackend::processRecvBuffer() {
    while (true) {
        if (!parsing_payload_) {
            // 正在读帧头
            size_t need = MUX_FRAME_HEADER_SIZE - header_bytes_read_;
            size_t avail = recv_buf_.size();
            if (avail == 0) break;

            size_t copy = std::min(need, avail);
            memcpy(frame_header_buf_ + header_bytes_read_, recv_buf_.data(), copy);
            header_bytes_read_ += copy;
            recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + copy);

            if (header_bytes_read_ < MUX_FRAME_HEADER_SIZE) {
                break; // 帧头不完整，等更多数据
            }

            // 解析帧头
            if (frame_header_buf_[0] != MUX_FRAME_MAGIC) {
                LOG_ERROR("IPCProxyBackend: bad magic byte 0x%02x", frame_header_buf_[0]);
                ::close(ipc_fd_);
                ipc_fd_ = -1;
                return;
            }

            current_flags_ = frame_header_buf_[1];
            current_stream_id_ = 0;
            for (int i = 0; i < 8; i++) {
                current_stream_id_ = (current_stream_id_ << 8) | frame_header_buf_[2 + i];
            }
            current_payload_len_ = 0;
            for (int i = 0; i < 4; i++) {
                current_payload_len_ = (current_payload_len_ << 8) | frame_header_buf_[10 + i];
            }

            header_bytes_read_ = 0;

            if (current_payload_len_ == 0) {
                // 无 payload 的帧，直接处理
                handleFrame(current_flags_ & MUX_FRAME_TYPE_MASK,
                           current_stream_id_, nullptr, 0);
                continue;
            }

            parsing_payload_ = true;
            frame_payload_buf_.clear();
            frame_payload_buf_.reserve(current_payload_len_);
        }

        // 正在读 payload
        size_t need = current_payload_len_ - frame_payload_buf_.size();
        size_t avail = recv_buf_.size();
        if (avail == 0) break;

        size_t copy = std::min(need, avail);
        frame_payload_buf_.insert(frame_payload_buf_.end(),
                                  recv_buf_.begin(), recv_buf_.begin() + copy);
        recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + copy);

        if (frame_payload_buf_.size() < current_payload_len_) {
            break; // payload 不完整，等更多数据
        }

        // 帧完整，处理
        handleFrame(current_flags_ & MUX_FRAME_TYPE_MASK,
                   current_stream_id_,
                   frame_payload_buf_.data(),
                   frame_payload_buf_.size());
        parsing_payload_ = false;
    }
}

void IPCProxyBackend::handleFrame(uint8_t type, uint64_t stream_id,
                                   const char* payload, size_t len) {
    switch (type) {
    case MUX_FRAME_DATA: {
        // 从新进程回传的后端回复 → 转发给本地的 ClientConnection
        ClientConnection* client = getStreamClient(stream_id);
        if (client) {
            client->appendToSendBuffer(payload, len);
        } else {
            LOG_WARN("IPCProxyBackend: DATA for unknown stream %llu",
                     (unsigned long long)stream_id);
        }
        break;
    }
    case IPC_FRAME_FENCE_ACK: {
        // 新进程确认某 stream 的所有回复已回传完毕
        LOG_INFO("IPCProxyBackend: received FENCE_ACK for stream %llu",
                 (unsigned long long)stream_id);
        completeMigration(stream_id);
        break;
    }
    case MUX_FRAME_STREAM_CLOSE: {
        // 新进程通知某 stream 已关闭（后端断连等）
        ClientConnection* client = getStreamClient(stream_id);
        if (client) {
            client->close();
        }
        streams_.erase(stream_id);
        migrate_state_.erase(stream_id);
        break;
    }
    default:
        LOG_WARN("IPCProxyBackend: unknown frame type 0x%02x for stream %llu",
                 type, (unsigned long long)stream_id);
        break;
    }
}

void IPCProxyBackend::completeMigration(uint64_t stream_id) {
    auto it = streams_.find(stream_id);
    if (it == streams_.end()) {
        LOG_WARN("IPCProxyBackend::completeMigration: stream %llu not found",
                 (unsigned long long)stream_id);
        return;
    }

    ClientConnection* client = it->second;

    // 确保客户端的 send_buf_ 全部刷给客户端
    client->flushSendBuffer();

    // 收集迁移元数据
    ClientMigrateMeta meta{};
    meta.stream_id = stream_id;
    meta.listen_port = client->getListenPort();
    meta.authenticated = client->isAuthenticated() ? 1 : 0;
    meta.recv_buf_len = 0;  // 将在下面填充
    meta.send_buf_len = 0;

    int client_fd = client->getFd();

    // Step 1: 先从 epoll 中移除 fd，防止 detach 后仍触发事件
    if (worker_ && client_fd >= 0) {
        worker_->getEventLoop().removeEvent(client_fd);
    }

    // Step 2: 提取 client 的残余 buffer 数据（迁移给新进程）
    std::vector<char> recv_buf = client->extractRecvBuffer();
    std::vector<char> send_buf = client->extractSendBuffer();

    meta.recv_buf_len = static_cast<uint32_t>(recv_buf.size());
    meta.send_buf_len = static_cast<uint32_t>(send_buf.size());

    // Step 3: 分离 fd（防止析构时 close）
    client->detachFd();

    // Step 4: 从 Worker 的 connections_ 中移除该客户端（使用保存的 client_fd）
    // 注意：必须在 detachFd 之后通过保存的 fd 值直接 erase
    // 因为 detachFd 后 getFd() 返回 -1，removeConnection 无法正确工作
    if (worker_) {
        worker_->eraseConnection(client_fd);
    }

    // 如果有迁移回调，则调用（主线程通过回调把 fd + buffer 传给新进程）
    if (migrate_callback_) {
        migrate_callback_(client_fd, meta, recv_buf, send_buf);
    }

    // 清理
    streams_.erase(stream_id);
    migrate_state_.erase(stream_id);

    LOG_INFO("IPCProxyBackend: completed migration for stream %llu, client_fd=%d",
             (unsigned long long)stream_id, client_fd);
}

void IPCProxyBackend::flushSendBuffer() {
    if (ipc_fd_ < 0) return;

    while (send_offset_ < send_buf_.size()) {
        ssize_t n = ::write(ipc_fd_,
                           send_buf_.data() + send_offset_,
                           send_buf_.size() - send_offset_);
        if (n > 0) {
            send_offset_ += n;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 写缓冲区满，注册 EPOLLOUT 事件，等待写就绪后继续发送
                if (worker_) {
                    worker_->getEventLoop().modifyEvent(ipc_fd_, EVENT_READABLE | EVENT_WRITABLE);
                }
                break;
            }
            LOG_ERROR("IPCProxyBackend: write error: %s", strerror(errno));
            ::close(ipc_fd_);
            ipc_fd_ = -1;
            return;
        }
    }

    // 清理已发送的数据
    if (send_offset_ > 0) {
        send_buf_.erase(send_buf_.begin(), send_buf_.begin() + send_offset_);
        send_offset_ = 0;
    }

    // 如果发送缓冲区已全部发完，取消 EPOLLOUT 监听，避免无效的写就绪事件
    if (send_buf_.empty() && worker_) {
        worker_->getEventLoop().modifyEvent(ipc_fd_, EVENT_READABLE);
    }
}

} // namespace proxy
