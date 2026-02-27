// IPCProxyFrontend 实现 - 新进程侧的 IPC 代理前端
#include "ipc_proxy_frontend.h"
#include "client_connection.h"
#include "mux_backend_connection.h"
#include "worker_thread.h"
#include "logger.h"
#include "resp_parser.h"  // RESP 协议解析器（用于精确计数）

#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <algorithm>

namespace proxy {

IPCProxyFrontend::IPCProxyFrontend(int ipc_fd, MuxBackendConnection* real_backend,
                                     WorkerThread* worker)
    : ipc_fd_(ipc_fd), real_backend_(real_backend), worker_(worker) {
    memset(frame_header_buf_, 0, sizeof(frame_header_buf_));
}

IPCProxyFrontend::~IPCProxyFrontend() {
    if (ipc_fd_ >= 0) {
        ::close(ipc_fd_);
        ipc_fd_ = -1;
    }
}

// ========== I/O 处理 ==========

void IPCProxyFrontend::onIPCReadable() {
    if (ipc_fd_ < 0) return;

    // 从 IPC 通道读取数据
    char buf[65536];
    while (true) {
        ssize_t n = ::read(ipc_fd_, buf, sizeof(buf));
        if (n > 0) {
            recv_buf_.insert(recv_buf_.end(), buf, buf + n);
        } else if (n == 0) {
            // IPC 通道关闭（旧进程退出）
            LOG_INFO("IPCProxyFrontend: IPC channel closed by old process");
            ::close(ipc_fd_);
            ipc_fd_ = -1;
            return;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            LOG_ERROR("IPCProxyFrontend: read error: %s", strerror(errno));
            ::close(ipc_fd_);
            ipc_fd_ = -1;
            return;
        }
    }

    // 解析帧
    processRecvBuffer();
}

void IPCProxyFrontend::onIPCWritable() {
    flushSendBuffer();
}

// ========== 后端回复路由 ==========

bool IPCProxyFrontend::isProxied(uint64_t stream_id) const {
    return proxied_info_.count(stream_id) > 0;
}

void IPCProxyFrontend::forwardToOldProcess(uint64_t stream_id,
                                             const char* data, size_t len) {
    if (ipc_fd_ < 0) return;

    auto it = proxied_info_.find(stream_id);
    if (it == proxied_info_.end()) {
        LOG_WARN("IPCProxyFrontend::forwardToOldProcess: stream %llu not proxied",
                 (unsigned long long)stream_id);
        return;
    }

    // 回传 DATA 帧给旧进程
    sendFrame(MUX_FRAME_DATA, stream_id, data, len);

    // 用 RESP 解析器精确计算回复中的完整响应数量
    // 一个 DATA 帧可能包含多个或部分 RESP 响应
    uint32_t resp_count = RespParser::countResponses(data, len);
    if (it->second.in_flight >= resp_count) {
        it->second.in_flight -= resp_count;
    } else {
        LOG_WARN("IPCProxyFrontend: in_flight underflow for stream %llu "
                 "(in_flight=%u, resp_count=%u)",
                 (unsigned long long)stream_id, it->second.in_flight, resp_count);
        it->second.in_flight = 0;
    }

    // 检查是否可以发送 FENCE_ACK
    if (it->second.fence_received) {
        tryAckFence(stream_id);
    }
}

// ========== Stream 管理 ==========

void IPCProxyFrontend::addProxiedStream(uint64_t stream_id) {
    proxied_info_[stream_id] = ProxiedStreamInfo{0, false};
    proxied_[stream_id] = 0;
}

void IPCProxyFrontend::removeProxiedStream(uint64_t stream_id) {
    proxied_info_.erase(stream_id);
    proxied_.erase(stream_id);
}

// ========== 客户端 fd 接收 ==========

void IPCProxyFrontend::receiveClientFd() {
    if (ipc_fd_ < 0) return;

    // 通过 recvmsg + SCM_RIGHTS 接收 client fd + 元数据
    ClientMigrateMeta meta{};
    struct msghdr msg{};
    struct iovec iov;
    char cmsgbuf[CMSG_SPACE(sizeof(int))];

    iov.iov_base = &meta;
    iov.iov_len = sizeof(meta);
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);

    ssize_t n = recvmsg(ipc_fd_, &msg, 0);
    if (n < 0) {
        LOG_ERROR("IPCProxyFrontend::receiveClientFd: recvmsg failed: %s", strerror(errno));
        return;
    }

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (!cmsg || cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        LOG_ERROR("IPCProxyFrontend::receiveClientFd: no fd in control message");
        return;
    }

    int client_fd;
    memcpy(&client_fd, CMSG_DATA(cmsg), sizeof(int));

    LOG_INFO("IPCProxyFrontend: received client fd=%d for stream %llu",
             client_fd, (unsigned long long)meta.stream_id);

    // 读取后续的 buffer 数据
    std::vector<char> recv_buf;
    std::vector<char> send_buf;

    if (meta.recv_buf_len > 0) {
        recv_buf.resize(meta.recv_buf_len);
        size_t total_read = 0;
        while (total_read < meta.recv_buf_len) {
            ssize_t r = ::read(ipc_fd_, recv_buf.data() + total_read,
                              meta.recv_buf_len - total_read);
            if (r <= 0) {
                LOG_ERROR("IPCProxyFrontend: failed to read recv_buf data");
                break;
            }
            total_read += r;
        }
    }

    if (meta.send_buf_len > 0) {
        send_buf.resize(meta.send_buf_len);
        size_t total_read = 0;
        while (total_read < meta.send_buf_len) {
            ssize_t r = ::read(ipc_fd_, send_buf.data() + total_read,
                              meta.send_buf_len - total_read);
            if (r <= 0) {
                LOG_ERROR("IPCProxyFrontend: failed to read send_buf data");
                break;
            }
            total_read += r;
        }
    }

    // 从代理集合中移除
    removeProxiedStream(meta.stream_id);

    // 调用回调，让 Worker 创建本地的 ClientConnection
    if (client_received_cb_) {
        client_received_cb_(client_fd, meta, recv_buf, send_buf);
    }
}

// ========== 帧编解码 ==========

void IPCProxyFrontend::sendFrame(uint8_t type, uint64_t stream_id,
                                   const char* payload, size_t payload_len) {
    if (ipc_fd_ < 0) return;

    // 编码帧头
    unsigned char header[MUX_FRAME_HEADER_SIZE];
    header[0] = MUX_FRAME_MAGIC;
    header[1] = type;

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

void IPCProxyFrontend::processRecvBuffer() {
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
                break;
            }

            // 解析帧头
            if (frame_header_buf_[0] != MUX_FRAME_MAGIC) {
                LOG_ERROR("IPCProxyFrontend: bad magic byte 0x%02x", frame_header_buf_[0]);
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
            break;
        }

        handleFrame(current_flags_ & MUX_FRAME_TYPE_MASK,
                   current_stream_id_,
                   frame_payload_buf_.data(),
                   frame_payload_buf_.size());
        parsing_payload_ = false;
    }
}

void IPCProxyFrontend::handleFrame(uint8_t type, uint64_t stream_id,
                                     const char* payload, size_t len) {
    switch (type) {
    case MUX_FRAME_DATA: {
        // 旧进程转发来的客户端请求 → 转发给真正的后端
        auto it = proxied_info_.find(stream_id);
        if (it != proxied_info_.end()) {
            // 用 RESP 解析器精确计算 payload 中的命令数量
            // 一个 DATA 帧可能包含多个 pipeline 命令
            uint32_t cmd_count = RespParser::countCommands(payload, len);
            it->second.in_flight += cmd_count;
            proxied_[stream_id] = it->second.in_flight;
            real_backend_->sendData(stream_id, payload, len);
        } else {
            LOG_WARN("IPCProxyFrontend: DATA for non-proxied stream %llu",
                     (unsigned long long)stream_id);
        }
        break;
    }
    case IPC_FRAME_FENCE: {
        // 旧进程标记某 stream 的请求到此为止
        auto it = proxied_info_.find(stream_id);
        if (it != proxied_info_.end()) {
            it->second.fence_received = true;
            LOG_INFO("IPCProxyFrontend: received FENCE for stream %llu (in_flight=%u)",
                     (unsigned long long)stream_id, it->second.in_flight);
            tryAckFence(stream_id);
        } else {
            LOG_WARN("IPCProxyFrontend: FENCE for non-proxied stream %llu",
                     (unsigned long long)stream_id);
        }
        break;
    }
    case MUX_FRAME_STREAM_CLOSE: {
        // 旧进程通知关闭某 stream
        proxied_info_.erase(stream_id);
        proxied_.erase(stream_id);
        real_backend_->closeStream(stream_id);
        break;
    }
    default:
        LOG_WARN("IPCProxyFrontend: unknown frame type 0x%02x for stream %llu",
                 type, (unsigned long long)stream_id);
        break;
    }
}

void IPCProxyFrontend::tryAckFence(uint64_t stream_id) {
    auto it = proxied_info_.find(stream_id);
    if (it == proxied_info_.end()) return;

    if (it->second.fence_received && it->second.in_flight == 0) {
        // 所有回复都已回传给旧进程 → 发送 FENCE_ACK
        sendFrame(IPC_FRAME_FENCE_ACK, stream_id, nullptr, 0);
        LOG_INFO("IPCProxyFrontend: sent FENCE_ACK for stream %llu",
                 (unsigned long long)stream_id);
        // 注意：不立即从 proxied_info_ 移除
        // 等收到旧进程传来的 client fd 后再移除（在 receiveClientFd 中）
    }
}

void IPCProxyFrontend::flushSendBuffer() {
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
            LOG_ERROR("IPCProxyFrontend: write error: %s", strerror(errno));
            ::close(ipc_fd_);
            ipc_fd_ = -1;
            return;
        }
    }

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
