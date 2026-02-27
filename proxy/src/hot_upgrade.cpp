// HotUpgradeManager V2 实现 - 后端 MUX fd 迁移 + IPC 代理 + 逐步搬迁 Client
#include "hot_upgrade.h"
#include "proxy_server.h"
#include "worker_thread.h"
#include "mux_backend_connection.h"
#include "ipc_proxy_backend.h"
#include "ipc_proxy_frontend.h"
#include "client_connection.h"
#include "logger.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cerrno>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>

namespace proxy {

HotUpgradeManager::HotUpgradeManager(ProxyServer* server) : server_(server) {
}

HotUpgradeManager::~HotUpgradeManager() {
    if (uds_fd_ >= 0) {
        ::close(uds_fd_);
        uds_fd_ = -1;
    }
}

// ========== 旧进程侧: 发起热升级 ==========

bool HotUpgradeManager::startUpgrade(const std::string& new_binary_path) {
    if (state_ != UpgradeState::IDLE) {
        LOG_ERROR("Hot upgrade already in progress (state=%s)", getStateString().c_str());
        return false;
    }

    socket_path_ = server_->getConfig().getHotUpgradeConfig().socket_path;
    state_ = UpgradeState::BACKEND_TRANSFER;
    LOG_INFO("Starting hot upgrade V2 (MUX fd transfer + IPC proxy)...");

    // Step 1: 创建 UDS 通信通道
    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        LOG_ERROR("Failed to create UDS: %s", strerror(errno));
        state_ = UpgradeState::FAILED;
        return false;
    }

    unlink(socket_path_.c_str());

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("Failed to bind UDS '%s': %s", socket_path_.c_str(), strerror(errno));
        ::close(server_fd);
        state_ = UpgradeState::FAILED;
        return false;
    }

    if (listen(server_fd, 1) < 0) {
        LOG_ERROR("Failed to listen UDS: %s", strerror(errno));
        ::close(server_fd);
        unlink(socket_path_.c_str());
        state_ = UpgradeState::FAILED;
        return false;
    }

    // Step 2: 确定新二进制路径
    std::string binary = new_binary_path;
    if (binary.empty()) {
        char exe_path[1024];
        ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
        if (len <= 0) {
            LOG_ERROR("Failed to read /proc/self/exe: %s", strerror(errno));
            ::close(server_fd);
            unlink(socket_path_.c_str());
            state_ = UpgradeState::FAILED;
            return false;
        }
        exe_path[len] = '\0';
        binary = exe_path;
    }

    // Step 3: Fork 并 exec 新进程
    pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("fork() failed: %s", strerror(errno));
        ::close(server_fd);
        unlink(socket_path_.c_str());
        state_ = UpgradeState::FAILED;
        return false;
    }

    if (pid == 0) {
        // 子进程: exec 新二进制
        std::string upgrade_arg = "--upgrade-from=" + socket_path_;
        std::string cfg_path = server_->getConfig().getConfigPath();
        if (!cfg_path.empty()) {
            execl(binary.c_str(), binary.c_str(),
                  "-c", cfg_path.c_str(),
                  upgrade_arg.c_str(), nullptr);
        } else {
            execl(binary.c_str(), binary.c_str(),
                  upgrade_arg.c_str(), nullptr);
        }
        LOG_ERROR("execl failed: %s", strerror(errno));
        _exit(1);
    }

    LOG_INFO("Forked new process pid=%d, binary=%s", pid, binary.c_str());
    new_process_pid_ = pid;

    // Step 4: 等待新进程连接（这里短暂同步等待握手，握手本身很快）
    uint32_t timeout = server_->getConfig().getHotUpgradeConfig().timeout;

    struct timeval tv;
    tv.tv_sec = timeout;
    tv.tv_usec = 0;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int client_fd = accept(server_fd, nullptr, nullptr);
    if (client_fd < 0) {
        LOG_ERROR("Timeout waiting for new process to connect: %s", strerror(errno));
        ::close(server_fd);
        unlink(socket_path_.c_str());
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
        state_ = UpgradeState::FAILED;
        return false;
    }

    ::close(server_fd);
    uds_fd_ = client_fd;

    // 设置 UDS fd 为非阻塞
    int flags = fcntl(uds_fd_, F_GETFL, 0);
    fcntl(uds_fd_, F_SETFL, flags | O_NONBLOCK);

    // Step 5: 握手（新进程发来 MSG_HANDSHAKE，回复 MSG_HANDSHAKE_ACK）
    // 握手消息很小，这里短暂同步完成
    fcntl(uds_fd_, F_SETFL, flags); // 临时设回阻塞模式
    uint8_t msg_type = 0;
    if (::read(uds_fd_, &msg_type, 1) != 1 || msg_type != MSG_HANDSHAKE) {
        LOG_ERROR("Invalid handshake from new process (got type=%d)", msg_type);
        ::close(uds_fd_);
        uds_fd_ = -1;
        unlink(socket_path_.c_str());
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
        state_ = UpgradeState::FAILED;
        return false;
    }

    msg_type = MSG_HANDSHAKE_ACK;
    ::write(uds_fd_, &msg_type, 1);
    fcntl(uds_fd_, F_SETFL, flags | O_NONBLOCK); // 恢复非阻塞
    LOG_INFO("Handshake complete with new process");

    // Step 6: 传递监听 fd（新进程已用 SO_REUSEPORT 自己 bind，这里传递只是备用兼容）
    const auto& listen_fds = server_->getListenFds();
    for (const auto& [fd, port] : listen_fds) {
        msg_type = MSG_LISTEN_FD;
        ::write(uds_fd_, &msg_type, 1);

        ConnectionMetadata meta{};
        meta.fd = fd;
        meta.listen_port = port;
        if (!sendFd(uds_fd_, fd, &meta, sizeof(meta))) {
            LOG_ERROR("Failed to send listen fd=%d for port=%d", fd, port);
        } else {
            LOG_INFO("Sent listen fd=%d for port=%d", fd, port);
        }
    }

    // ========== Phase 1: 传递后端 MUX fd + 建立 IPC 桥 ==========
    if (!transferBackendConnections()) {
        LOG_ERROR("Phase 1 failed: backend connection transfer");
        msg_type = MSG_ROLLBACK;
        ::write(uds_fd_, &msg_type, 1);
        ::close(uds_fd_);
        uds_fd_ = -1;
        unlink(socket_path_.c_str());
        kill(pid, SIGTERM);
        waitpid(pid, nullptr, 0);
        state_ = UpgradeState::FAILED;
        return false;
    }

    // ========== Phase 1 完成后，旧进程关闭 listen fd，保证迁移收敛 ==========
    closeListenFds();

    // ========== Phase 2: 事件驱动 + 逐批搬迁 Client ==========
    state_ = UpgradeState::IPC_PROXY;
    scheduler_.start_time = std::chrono::steady_clock::now();
    scheduler_.migrated_streams = 0;
    scheduler_.current_backend_idx = 0;

    // 统计总 stream 数并动态计算超时
    scheduler_.total_streams = 0;
    const auto& workers = server_->getWorkers();
    for (const auto& worker : workers) {
        for (auto* proxy : worker->getIPCProxyBackends()) {
            scheduler_.total_streams += proxy->getActiveStreamCount();
        }
    }

    // 动态超时: max(30s, stream_count * 15ms)
    uint32_t dynamic_timeout = std::max(timeout,
        static_cast<uint32_t>((scheduler_.total_streams * 15 + 999) / 1000));
    scheduler_.deadline = scheduler_.start_time + std::chrono::seconds(dynamic_timeout);

    LOG_INFO("Phase 2: IPC proxy active, %d streams to migrate (timeout=%us)",
             scheduler_.total_streams, dynamic_timeout);

    // 在每个 Worker 线程中注册迁移定时器（事件驱动，不阻塞 event loop）
    startMigrationTimers();

    // 注册 UDS fd 到主线程 event loop，接收新进程的 ACK 等异步消息
    // （Phase 3 的 finalizeUpgrade 也通过事件驱动触发）
    registerUDSWatcher();

    return true;
}

// ========== Phase 1: 传递后端 MUX fd（通过 Worker dispatch 安全执行）==========

bool HotUpgradeManager::transferBackendConnections() {
    LOG_INFO("Phase 1: Transferring backend MUX connections via Worker dispatch...");

    const auto& workers = server_->getWorkers();

    // 第一步：dispatch 到每个 Worker 线程中执行后端迁移
    for (const auto& worker : workers) {
        worker->dispatchTransferBackends();
    }

    // 第二步：等待所有 Worker 完成（每个 Worker 5 秒超时）
    for (const auto& worker : workers) {
        if (!worker->waitTransferDone(5000)) {
            LOG_ERROR("Phase 1: Worker %d transfer timeout", worker->getId());
            return false;
        }
    }

    // 第三步：收集结果，通过 UDS 发送给新进程
    for (const auto& worker : workers) {
        auto results = worker->getTransferResults();

        for (auto& result : results) {
            // 发送 MSG_BACKEND_FD: 后端 MUX fd + 元数据
            uint8_t msg_type = MSG_BACKEND_FD;
            ::write(uds_fd_, &msg_type, 1);

            MuxBackendTransferMeta meta{};
            meta.fd = result.backend_fd;
            strncpy(meta.addr, result.addr.c_str(), sizeof(meta.addr) - 1);
            meta.port = result.port;
            strncpy(meta.password, result.password.c_str(), sizeof(meta.password) - 1);
            meta.mux_enabled = result.mux_enabled ? 1 : 0;
            meta.next_stream_id = result.next_stream_id;
            meta.active_stream_count = result.active_stream_count;
            meta.stream_map_count = static_cast<uint32_t>(result.stream_map.size());

            // ★ 关键修复：从原始连接获取帧解析状态和缓冲区数据
            // 如果迁移瞬间后端连接有半帧数据或未解析的数据，必须完整传递
            // 否则新进程会从错误的帧边界开始解析，导致整个连接损坏
            auto parse_state = result.orig_conn->getFrameParseState();
            memcpy(meta.frame_header_buf, parse_state.frame_header_buf,
                   sizeof(meta.frame_header_buf));
            meta.header_bytes_read = parse_state.header_bytes_read;
            meta.parsing_payload = parse_state.parsing_payload ? 1 : 0;
            meta.current_flags = parse_state.current_flags;
            meta.current_stream_id = parse_state.current_stream_id;
            meta.current_payload_len = parse_state.current_payload_len;
            meta.frame_payload_buf_len = static_cast<uint32_t>(parse_state.frame_payload_buf.size());
            meta.recv_buf_len = static_cast<uint32_t>(parse_state.recv_buf.size());
            meta.send_buf_len = static_cast<uint32_t>(parse_state.send_buf.size());

            if (!sendFd(uds_fd_, result.backend_fd, &meta, sizeof(meta))) {
                LOG_ERROR("Phase 1: Failed to send backend fd=%d for %s:%d",
                          result.backend_fd, result.addr.c_str(), result.port);
                return false;
            }

            // 发送 stream 映射表
            if (meta.stream_map_count > 0) {
                std::vector<StreamMapEntry> entries;
                for (const auto& [sid, cfd] : result.stream_map) {
                    entries.push_back({sid, cfd});
                }
                sendData(uds_fd_, entries.data(), entries.size() * sizeof(StreamMapEntry));
            }

            // ★ 关键修复：发送变长缓冲区数据（帧 payload、recv_buf、send_buf）
            if (meta.frame_payload_buf_len > 0) {
                sendData(uds_fd_, parse_state.frame_payload_buf.data(),
                         parse_state.frame_payload_buf.size());
            }
            if (meta.recv_buf_len > 0) {
                sendData(uds_fd_, parse_state.recv_buf.data(),
                         parse_state.recv_buf.size());
            }
            if (meta.send_buf_len > 0) {
                sendData(uds_fd_, parse_state.send_buf.data(),
                         parse_state.send_buf.size());
            }

            LOG_INFO("Phase 1: Sent backend fd=%d for %s:%d (streams=%u, "
                     "recv_buf=%u, send_buf=%u, frame_payload=%u)",
                     result.backend_fd, result.addr.c_str(), result.port,
                     result.active_stream_count,
                     meta.recv_buf_len, meta.send_buf_len, meta.frame_payload_buf_len);

            // 发送 MSG_IPC_FD: socketpair[1]（新进程端）
            msg_type = MSG_IPC_FD;
            ::write(uds_fd_, &msg_type, 1);

            struct {
                char backend_addr[64];
                uint16_t backend_port;
            } ipc_meta{};
            strncpy(ipc_meta.backend_addr, result.addr.c_str(), sizeof(ipc_meta.backend_addr) - 1);
            ipc_meta.backend_port = result.port;

            if (!sendFd(uds_fd_, result.ipc_fd, &ipc_meta, sizeof(ipc_meta))) {
                LOG_ERROR("Phase 1: Failed to send IPC fd for %s:%d",
                          result.addr.c_str(), result.port);
                return false;
            }

            LOG_INFO("Phase 1: Sent IPC fd=%d for %s:%d",
                     result.ipc_fd, result.addr.c_str(), result.port);
        }
    }

    LOG_INFO("Phase 1: Backend transfer complete");
    return true;
}

// ========== Phase 2: 事件驱动的迁移定时器 ==========

void HotUpgradeManager::startMigrationTimers() {
    LOG_INFO("Phase 2: Starting migration timers on each Worker...");

    const auto& workers = server_->getWorkers();

    for (const auto& worker : workers) {
        // 在每个 Worker 线程注册迁移定时器
        // 定时器回调在 Worker 线程中执行，安全访问 Worker 的数据
        worker->dispatchStartMigrateTimer(
            migrate_interval_ms_,
            migrate_batch_size_,
            // 迁移完成一个 stream 的回调：通过 UDS 把 client fd 发给新进程
            [this](int client_fd, const ClientMigrateMeta& meta,
                   const std::vector<char>& recv_buf, const std::vector<char>& send_buf) {
                sendMigratedClient(client_fd, meta, recv_buf, send_buf);
                scheduler_.migrated_streams++;

                LOG_DEBUG("Phase 2: Migrated stream %llu (client_fd=%d), %d/%d done",
                         (unsigned long long)meta.stream_id, client_fd,
                         scheduler_.migrated_streams.load(), scheduler_.total_streams);
            },
            // 全部完成回调
            [this]() {
                onWorkerMigrationDone();
            }
        );
    }
}

void HotUpgradeManager::sendMigratedClient(int client_fd, const ClientMigrateMeta& meta,
                                              const std::vector<char>& recv_buf,
                                              const std::vector<char>& send_buf) {
    std::lock_guard<std::mutex> lock(uds_mutex_);

    // 问题 F 修复：将 msg_type + sendFd(meta) + buffer 数据作为一个整体发送
    // sendFd 使用 sendmsg 发送 fd + 元数据（原子操作），然后 sendData 发送 buffer
    // 整个过程在 mutex 保护下完成，保证消息完整性

    // 先发 msg_type 标识字节
    uint8_t msg_type = MSG_CLIENT_FD;
    if (!sendData(uds_fd_, &msg_type, 1)) {
        LOG_ERROR("Phase 2: Failed to send msg_type for client fd=%d", client_fd);
        return;
    }

    // 构建带 buffer 长度的元数据
    struct {
        uint64_t stream_id;
        uint16_t listen_port;
        uint8_t authenticated;
        uint32_t recv_buf_len;
        uint32_t send_buf_len;
    } client_meta{};
    client_meta.stream_id = meta.stream_id;
    client_meta.listen_port = meta.listen_port;
    client_meta.authenticated = meta.authenticated;
    client_meta.recv_buf_len = static_cast<uint32_t>(recv_buf.size());
    client_meta.send_buf_len = static_cast<uint32_t>(send_buf.size());

    if (!sendFd(uds_fd_, client_fd, &client_meta, sizeof(client_meta))) {
        LOG_ERROR("Phase 2: Failed to send client fd=%d", client_fd);
        // 消息边界已破坏，关闭 UDS 通道触发回滚
        ::close(uds_fd_);
        uds_fd_ = -1;
        return;
    }

    // 发送 buffer 数据
    if (!recv_buf.empty()) {
        if (!sendData(uds_fd_, recv_buf.data(), recv_buf.size())) {
            LOG_ERROR("Phase 2: Failed to send recv_buf for client fd=%d", client_fd);
            ::close(uds_fd_);
            uds_fd_ = -1;
            return;
        }
    }
    if (!send_buf.empty()) {
        if (!sendData(uds_fd_, send_buf.data(), send_buf.size())) {
            LOG_ERROR("Phase 2: Failed to send send_buf for client fd=%d", client_fd);
            ::close(uds_fd_);
            uds_fd_ = -1;
            return;
        }
    }
}

void HotUpgradeManager::onWorkerMigrationDone() {
    // 检查是否所有 Worker 的迁移都完成了
    workers_done_count_++;
    const auto& workers = server_->getWorkers();
    if (workers_done_count_ >= static_cast<int>(workers.size())) {
        LOG_INFO("Phase 2: All workers migration complete, %d/%d streams migrated",
                 scheduler_.migrated_streams.load(), scheduler_.total_streams);

        // 注意：此回调可能从任意 Worker 线程触发，finalizeUpgrade() 需要操作主线程的
        // event loop，因此必须通过 wakeup + pending flag 调度到主线程执行
        finalize_pending_.store(true);
        server_->getMainLoop().wakeup();
    }
}

void HotUpgradeManager::registerUDSWatcher() {
    // 注册 UDS fd 到主线程 event loop，监听新进程发来的异步消息
    // 主要用于检测新进程崩溃、接收回滚请求等
    // 同时周期性检查 finalize_pending_ 标志（从 Worker 线程设置）
    server_->getMainLoop().addEvent(uds_fd_, EVENT_READABLE,
        [this](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                LOG_ERROR("Old process: UDS error, new process may have crashed");
                // 尝试回滚：重新接管后端连接
                state_ = UpgradeState::FAILED;
                return;
            }
            if (events & EVENT_READABLE) {
                // 读取新进程发来的消息（例如 MSG_ROLLBACK 请求）
                uint8_t msg_type = 0;
                ssize_t n = ::read(uds_fd_, &msg_type, 1);
                if (n <= 0) {
                    if (n == 0) {
                        LOG_WARN("Old process: UDS connection closed by new process");
                    }
                    return;
                }
                if (msg_type == MSG_ROLLBACK) {
                    LOG_WARN("Old process: received rollback request from new process");
                    state_ = UpgradeState::FAILED;
                }
            }
        });

    // 注册一个周期性定时器检查 finalize_pending_ 标志
    // Worker 线程通过 wakeup() 唤醒主线程后，主线程在这个定时器中检查并执行 finalizeUpgrade
    finalize_check_timer_ = server_->getMainLoop().addTimer(10, /* 10ms */
        [this]() {
            if (finalize_pending_.exchange(false)) {
                finalizeUpgrade();
                // 完成后取消此检查定时器
                server_->getMainLoop().cancelTimer(finalize_check_timer_);
                finalize_check_timer_ = 0;
            }
        }, true /* repeat */);

    LOG_INFO("Old process: UDS fd=%d registered to main event loop for monitoring", uds_fd_);
}

// ========== Phase 1 完成后: 关闭 listen fd ==========

void HotUpgradeManager::closeListenFds() {
    // Phase 1 完成后，旧进程主动关闭所有 listen fd
    // 从此所有新连接只落在新进程上（通过 SO_REUSEPORT）
    // 这是保证迁移收敛的关键优化：存量连接数单调递减
    const auto& listen_fds = server_->getListenFds();
    for (const auto& [fd, port] : listen_fds) {
        server_->getMainLoop().removeEvent(fd);
        ::close(fd);
        LOG_INFO("Hot-upgrade: closed listen fd=%d port=%d, stop accepting new connections",
                 fd, port);
    }
    // 注意：这里只关闭 fd，不修改 listen_fds_ map（它属于 ProxyServer）
    // ProxyServer 退出时会清理
    LOG_INFO("Hot-upgrade: all listen fds closed, old process now in migration-only mode");
}

// ========== Phase 3: 完成确认 ==========

bool HotUpgradeManager::finalizeUpgrade() {
    state_ = UpgradeState::WAITING_ACK;

    uint8_t msg_type = MSG_TRANSFER_DONE;
    ::write(uds_fd_, &msg_type, 1);
    LOG_INFO("All streams migrated, waiting for ACK from new process (async)...");

    // 先移除 registerUDSWatcher() 中注册的旧事件回调，再重新注册新的 ACK 处理回调
    // 避免对同一个 fd 重复 addEvent 导致 epoll 的 EEXIST 错误
    server_->getMainLoop().removeEvent(uds_fd_);

    // 异步等待 ACK：注册 UDS fd 读事件到主线程 event loop
    server_->getMainLoop().addEvent(uds_fd_, EVENT_READABLE,
        [this](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                LOG_ERROR("Hot upgrade V2: UDS error while waiting for ACK");
                server_->getMainLoop().removeEvent(uds_fd_);
                if (event_loop_ack_timer_) {
                    server_->getMainLoop().cancelTimer(event_loop_ack_timer_);
                    event_loop_ack_timer_ = 0;
                }
                ::close(uds_fd_);
                uds_fd_ = -1;
                unlink(socket_path_.c_str());
                state_ = UpgradeState::FAILED;
                return;
            }
            if (events & EVENT_READABLE) {
                uint8_t ack_msg = 0;
                ssize_t n = ::read(uds_fd_, &ack_msg, 1);
                if (n == 1 && ack_msg == MSG_ACK) {
                    LOG_INFO("Hot upgrade V2 ACK received, shutting down old process");
                    state_ = UpgradeState::COMPLETED;
                    server_->getMainLoop().removeEvent(uds_fd_);
                    if (event_loop_ack_timer_) {
                        server_->getMainLoop().cancelTimer(event_loop_ack_timer_);
                        event_loop_ack_timer_ = 0;
                    }
                    ::close(uds_fd_);
                    uds_fd_ = -1;
                    unlink(socket_path_.c_str());
                    server_->shutdown();
                } else if (n <= 0) {
                    LOG_ERROR("Hot upgrade V2: failed to read ACK (n=%zd)", n);
                }
            }
        });

    // 设置超时定时器
    uint32_t timeout = server_->getConfig().getHotUpgradeConfig().timeout;
    event_loop_ack_timer_ = server_->getMainLoop().addTimer(timeout * 1000,
        [this]() {
            if (state_ == UpgradeState::WAITING_ACK) {
                LOG_ERROR("Hot upgrade V2 failed: ACK timeout after %u seconds",
                         server_->getConfig().getHotUpgradeConfig().timeout);
                server_->getMainLoop().removeEvent(uds_fd_);
                ::close(uds_fd_);
                uds_fd_ = -1;
                unlink(socket_path_.c_str());
                state_ = UpgradeState::FAILED;
            }
        }, false /* one-shot */);

    // 唤醒主线程 event loop 处理新注册的事件
    server_->getMainLoop().wakeup();

    return true;  // 异步操作已启动
}

// ========== 新进程侧: 异步接收 ==========

bool HotUpgradeManager::receiveFromOldProcess(const std::string& socket_path) {
    LOG_INFO("New process: connecting to old process via UDS '%s'", socket_path.c_str());

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        LOG_ERROR("Failed to create UDS: %s", strerror(errno));
        return false;
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

    // 重试连接
    int retries = 5;
    while (retries > 0) {
        if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            break;
        }
        retries--;
        if (retries > 0) {
            LOG_INFO("Retrying connection to old process (%d retries left)...", retries);
            usleep(200000);
        }
    }

    if (retries == 0) {
        LOG_ERROR("Failed to connect to old process UDS: %s", strerror(errno));
        ::close(fd);
        return false;
    }

    uds_fd_ = fd;

    // 发送握手（同步，握手消息很小）
    uint8_t msg_type = MSG_HANDSHAKE;
    ::write(uds_fd_, &msg_type, 1);

    // 等待 ACK
    if (::read(uds_fd_, &msg_type, 1) != 1 || msg_type != MSG_HANDSHAKE_ACK) {
        LOG_ERROR("Handshake with old process failed");
        ::close(uds_fd_);
        uds_fd_ = -1;
        return false;
    }

    LOG_INFO("Handshake with old process successful");

    // 设置非阻塞模式
    int flags = fcntl(uds_fd_, F_GETFL, 0);
    fcntl(uds_fd_, F_SETFL, flags | O_NONBLOCK);

    // 将 UDS fd 注册到主线程 event loop，异步接收消息
    // 这样 event loop 可以同时处理：
    //   - listen fd 的 accept（新连接不受影响）
    //   - UDS fd 的读取（接收旧进程传来的 fd）
    //   - 已迁移的 backend fd / IPC fd 的 I/O
    server_->getMainLoop().addEvent(uds_fd_, EVENT_READABLE,
        [this](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                LOG_ERROR("UDS connection error");
                onUDSError();
                return;
            }
            if (events & EVENT_READABLE) {
                onUDSReadable();
            }
        });

    LOG_INFO("UDS fd=%d registered to event loop, ready to receive asynchronously", uds_fd_);
    return true;
}

void HotUpgradeManager::onUDSReadable() {
    // 问题 A 修复：全程保持非阻塞模式，不再临时切换为阻塞模式
    // 旧进程在 mutex 保护下通过 sendData() 循环写入完整消息（保证原子性）
    // 新进程在 recvFd/recvData 中也使用循环读取（阻塞语义由 sendData 的完整性保证）
    //
    // 但注意：recvmsg/recvData 在非阻塞模式下如果数据未完全到达会返回 EAGAIN
    // 对于 sendmsg(SCM_RIGHTS) 传递的消息，内核保证 sendmsg 的 iov 数据和 cmsg
    // 在 recvmsg 时原子可见。但后续的 buffer 数据可能还未到达。
    //
    // 解决方案：处理每种消息类型时，如果 recvData 遇到 EAGAIN，
    // 临时设为阻塞模式仅对该消息的 payload 部分进行阻塞接收，
    // 完成后立即恢复非阻塞。这样只有 payload 接收是短暂阻塞的（微秒级），
    // 不会像之前整个 switch 都在阻塞模式下那样卡住 event loop。
    //
    // 更好的方案：对于 fd 传递（recvmsg），内核保证原子性，所以 recvFd 没问题。
    // 对于后续的 recvData（stream_map、buffer 等），这些数据量通常很小，
    // 且旧进程在 mutex 下连续 write，内核缓冲区足够，几乎不会出现 partial。
    // 但为了绝对安全，我们在 recvData 内部已有循环读取逻辑。
    // 唯一的问题是 recvData 中 read() 如果返回 EAGAIN，当前实现会报错退出。
    // 我们需要在 recvData 中处理 EAGAIN，短暂 spin 等待数据到达。

    // 每次 epoll 触发时处理所有可读消息
    while (true) {
        uint8_t msg_type = 0;
        ssize_t n = ::read(uds_fd_, &msg_type, 1);
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break; // 无更多数据，返回 event loop
            }
            if (n == 0) {
                LOG_WARN("UDS connection closed by old process");
            } else {
                LOG_ERROR("UDS read error: %s", strerror(errno));
            }
            onUDSError();
            return;
        }

        bool msg_ok = true;

        switch (msg_type) {
        case MSG_TRANSFER_DONE: {
            LOG_INFO("Transfer complete: %d listen, %d backend, %d client fds",
                     received_listen_fds_, received_backend_fds_, received_client_fds_);

            // 发送 ACK
            uint8_t ack = MSG_ACK;
            ::write(uds_fd_, &ack, 1);

            // 清理 UDS fd
            server_->getMainLoop().removeEvent(uds_fd_);
            ::close(uds_fd_);
            uds_fd_ = -1;
            LOG_INFO("Hot upgrade V2 receive complete, new process fully operational");
            return;
        }
        case MSG_LISTEN_FD: {
            ConnectionMetadata meta{};
            int received_fd = recvFd(uds_fd_, &meta, sizeof(meta));
            if (received_fd >= 0) {
                received_listen_fds_++;
                LOG_INFO("Received listen fd=%d for port=%d (closing, we use SO_REUSEPORT)",
                         received_fd, meta.listen_port);
                ::close(received_fd); // 新进程已用 SO_REUSEPORT 自己 bind
            } else {
                LOG_ERROR("Failed to receive listen fd, message boundary may be corrupted");
                msg_ok = false;
            }
            break;
        }
        case MSG_BACKEND_FD: {
            if (receiveBackendConnection()) {
                received_backend_fds_++;
            } else {
                LOG_ERROR("Failed to receive backend connection, aborting");
                msg_ok = false;
            }
            break;
        }
        case MSG_IPC_FD: {
            if (!receiveIPCFd()) {
                LOG_ERROR("Failed to receive IPC fd, aborting");
                msg_ok = false;
            }
            break;
        }
        case MSG_CLIENT_FD: {
            if (receiveClientConnection()) {
                received_client_fds_++;
            } else {
                LOG_ERROR("Failed to receive client connection, aborting");
                msg_ok = false;
            }
            break;
        }
        case MSG_ROLLBACK: {
            LOG_WARN("Old process requested rollback");
            server_->getMainLoop().removeEvent(uds_fd_);
            ::close(uds_fd_);
            uds_fd_ = -1;
            return;
        }
        default:
            LOG_WARN("Unknown message type: %d", msg_type);
            msg_ok = false;
            break;
        }

        // 问题 D 修复：如果某个消息处理失败，消息边界可能已错乱
        // 此时继续读取会导致把 buffer 数据误判为 msg_type
        // 最安全的做法是关闭 UDS 连接，触发旧进程检测到连接断开并回滚
        if (!msg_ok) {
            LOG_ERROR("Message processing failed, closing UDS to prevent boundary corruption");
            onUDSError();
            return;
        }
    }
}

void HotUpgradeManager::onUDSError() {
    if (uds_fd_ >= 0) {
            server_->getMainLoop().removeEvent(uds_fd_);
        ::close(uds_fd_);
        uds_fd_ = -1;
    }
    LOG_ERROR("UDS connection lost");
}

bool HotUpgradeManager::receiveBackendConnection() {
    MuxBackendTransferMeta meta{};
    int received_fd = recvFd(uds_fd_, &meta, sizeof(meta));
    if (received_fd < 0) {
        LOG_ERROR("Failed to receive backend fd");
        return false;
    }

    LOG_INFO("Received backend fd=%d for %s:%d (streams=%u)",
             received_fd, meta.addr, meta.port, meta.active_stream_count);

    // 读取 stream 映射表
    std::vector<StreamMapEntry> stream_map(meta.stream_map_count);
    if (meta.stream_map_count > 0) {
        if (!recvData(uds_fd_, stream_map.data(),
                     meta.stream_map_count * sizeof(StreamMapEntry))) {
            LOG_ERROR("Failed to receive stream map");
            ::close(received_fd);
            return false;
        }
    }

    // 读取变长缓冲区数据
    std::vector<char> frame_payload_buf(meta.frame_payload_buf_len);
    std::vector<char> recv_buf(meta.recv_buf_len);
    std::vector<char> send_buf(meta.send_buf_len);

    if (meta.frame_payload_buf_len > 0) {
        recvData(uds_fd_, frame_payload_buf.data(), meta.frame_payload_buf_len);
    }
    if (meta.recv_buf_len > 0) {
        recvData(uds_fd_, recv_buf.data(), meta.recv_buf_len);
    }
    if (meta.send_buf_len > 0) {
        recvData(uds_fd_, send_buf.data(), meta.send_buf_len);
    }

    // 在新进程的 Worker 中重建 MuxBackendConnection
    // 选择一个 Worker（按 round-robin 分配或使用固定映射）
    const auto& workers = server_->getWorkers();
    if (workers.empty()) {
        LOG_ERROR("No workers available to accept backend connection");
        ::close(received_fd);
        return false;
    }

    int worker_idx = received_backend_fds_ % workers.size();
    auto* worker = workers[worker_idx].get();

    // 通过 Worker 的 dispatch 机制在 Worker 线程中创建 MuxBackendConnection
    worker->dispatchRestoreBackend(received_fd, meta, stream_map,
                                    frame_payload_buf, recv_buf, send_buf);

    // 保存 backend 标识以便后续 IPC fd 关联
    std::string key = std::string(meta.addr) + ":" + std::to_string(meta.port);
    restored_backends_[key] = {worker, received_fd};

    // 建立 stream_id → Worker 映射（主线程构建，主线程读取，无竞争）
    // 这样在 receiveClientConnection() 时可以直接通过 stream_id 找到目标 Worker
    for (const auto& entry : stream_map) {
        stream_to_worker_[entry.stream_id] = worker;
    }

    LOG_INFO("Backend connection %s:%d dispatched to Worker %d (fd=%d, stream_map_size=%zu)",
             meta.addr, meta.port, worker_idx, received_fd, stream_map.size());
    return true;
}

bool HotUpgradeManager::receiveIPCFd() {
    // 接收 socketpair[1] 和关联的后端连接标识
    struct {
        char backend_addr[64];
        uint16_t backend_port;
    } ipc_meta{};

    int ipc_fd = recvFd(uds_fd_, &ipc_meta, sizeof(ipc_meta));
    if (ipc_fd < 0) {
        LOG_ERROR("Failed to receive IPC fd");
        return false;
    }

    LOG_INFO("Received IPC fd=%d for backend %s:%d",
             ipc_fd, ipc_meta.backend_addr, ipc_meta.backend_port);

    // 查找对应的已恢复后端连接
    std::string key = std::string(ipc_meta.backend_addr) + ":" + std::to_string(ipc_meta.backend_port);
    auto it = restored_backends_.find(key);
    if (it == restored_backends_.end()) {
        LOG_ERROR("No restored backend found for %s", key.c_str());
        ::close(ipc_fd);
        return false;
    }

    auto* worker = it->second.worker;

    // 通过 Worker 的 dispatch 在 Worker 线程中创建 IPCProxyFrontend
    worker->dispatchCreateIPCFrontend(ipc_fd, key);

    LOG_INFO("IPC fd=%d dispatched to Worker for backend %s", ipc_fd, key.c_str());
    return true;
}

bool HotUpgradeManager::receiveClientConnection() {
    // 接收通过 FENCE_ACK 协议完成迁移的客户端 fd + 完整元数据和 buffer
    struct {
        uint64_t stream_id;
        uint16_t listen_port;
        uint8_t authenticated;
        uint32_t recv_buf_len;
        uint32_t send_buf_len;
    } client_meta{};

    int client_fd = recvFd(uds_fd_, &client_meta, sizeof(client_meta));
    if (client_fd < 0) {
        LOG_ERROR("Failed to receive client fd");
        return false;
    }

    // 读取 buffer 数据
    auto* recv_buf = new std::vector<char>(client_meta.recv_buf_len);
    auto* send_buf = new std::vector<char>(client_meta.send_buf_len);

    if (client_meta.recv_buf_len > 0) {
        recvData(uds_fd_, recv_buf->data(), client_meta.recv_buf_len);
    }
    if (client_meta.send_buf_len > 0) {
        recvData(uds_fd_, send_buf->data(), client_meta.send_buf_len);
    }

    LOG_INFO("Received client fd=%d stream=%llu port=%d auth=%d recv_buf=%u send_buf=%u",
             client_fd, (unsigned long long)client_meta.stream_id,
             client_meta.listen_port, client_meta.authenticated,
             client_meta.recv_buf_len, client_meta.send_buf_len);

    // 通过 Worker dispatch 调用 acceptMigratedClient（完整恢复路径）
    // 需要找到对应的 Worker 和 MuxBackendConnection
    const auto& workers = server_->getWorkers();
    if (workers.empty()) {
        LOG_ERROR("No workers available to accept client connection");
        ::close(client_fd);
        delete recv_buf;
        delete send_buf;
        return false;
    }

    // 通过 stream_id 确定目标 Worker：遍历各 Worker 的 IPCProxyFrontend 的代理表
    // 因为新进程的 IPCProxyFrontend 持有 proxied stream → real_backend 的映射
    // 注意：不能跨线程直接访问 Worker 的 mux_pools_（data race）
    // 所以使用 dispatch 机制：将 stream_id 传给 Worker，Worker 在自己的线程内查找 backend
    //
    // 选择目标 Worker 的策略：
    //   - 每个 Worker 在 Phase 1 恢复了若干 MuxBackendConnection
    //   - stream_id 一定属于某个已恢复的 backend
    //   - 通过 restored_backends_ 反查 Worker
    WorkerThread* target_worker = nullptr;

    // 从 restored_backends_ 中找到哪个 Worker 拥有此 stream_id 对应的 backend
    // restored_backends_ 是主线程构建的，这里在主线程读取，是安全的
    for (const auto& [key, info] : restored_backends_) {
        // 我们没有在主线程维护 stream→backend 的映射
        // 但可以将所有可能的 Worker 作为候选，让 Worker 线程内自行查找
        // 这里选择一个简单策略：按 stream_id dispatch 给所有 Worker 尝试
        // 但更优化的是：记住 Phase 1 时每个 Worker 恢复的 backend 的 stream range
        (void)key;
        (void)info;
    }

    // 最安全的方案：dispatch 到各 Worker 线程，让 Worker 在线程内通过
    // findBackendByStreamId 查找。第一个找到的 Worker 接管此 client。
    // 但 dispatch 是异步的，多个 Worker 无法协调。
    //
    // 实际优化：Phase 1 的 receiveBackendConnection() 已经记录了
    // restored_backends_["addr:port"] → { worker, fd }。
    // 我们可以把 worker 列表传给 dispatch，但每个 backend 在一个 Worker 上。
    // 所以只需要把 client dispatch 给拥有该 stream 的 Worker 即可。
    //
    // 新方案：不传 backend 指针，改为传 stream_id，让 Worker 线程内自行查找。
    // 因为不知道哪个 Worker 拥有此 stream，所以收集所有唯一 Worker，逐个尝试。
    // 但这会导致一个 client 被 dispatch 给多个 Worker（只有一个会成功绑定）。
    //
    // 更好的方案：在 receiveBackendConnection() 时记录 stream_id → worker 映射。
    // 但 stream_map 数据在那时已经有了！我们可以建立这个映射。
    //
    // 最终方案：使用 stream_to_worker_ map（在 receiveBackendConnection 中填充）
    auto sw_it = stream_to_worker_.find(client_meta.stream_id);
    if (sw_it != stream_to_worker_.end()) {
        target_worker = sw_it->second;
    } else {
        // 找不到映射（可能 stream 在 Phase 1 时未记录），round-robin fallback
        LOG_WARN("No worker mapping for stream %llu, assigning round-robin",
                 (unsigned long long)client_meta.stream_id);
        static int rr_idx = 0;
        int worker_idx = rr_idx++ % workers.size();
        target_worker = workers[worker_idx].get();
    }

    // 传 stream_id 而不传 backend 指针，Worker 线程内自行通过 stream_id 查找 backend
    // 这样避免了跨线程传递裸指针可能失效的问题
    target_worker->dispatchAcceptMigratedClient(
        client_fd, client_meta.listen_port,
        client_meta.stream_id,
        client_meta.authenticated != 0,
        nullptr,  // backend = nullptr，Worker 线程内通过 stream_id 自行查找
        recv_buf, send_buf);

    return true;
}

// ========== 工具方法 ==========

std::string HotUpgradeManager::getStateString() const {
    switch (state_) {
        case UpgradeState::IDLE:             return "idle";
        case UpgradeState::BACKEND_TRANSFER: return "backend_transfer";
        case UpgradeState::IPC_PROXY:        return "ipc_proxy";
        case UpgradeState::WAITING_ACK:      return "waiting_ack";
        case UpgradeState::COMPLETED:        return "completed";
        case UpgradeState::FAILED:           return "failed";
    }
    return "unknown";
}

bool HotUpgradeManager::sendFd(int uds_fd, int fd_to_send,
                                const void* metadata, size_t meta_len) {
    struct msghdr msg{};
    struct iovec iov;
    char cmsgbuf[CMSG_SPACE(sizeof(int))];

    iov.iov_base = const_cast<void*>(metadata);
    iov.iov_len = meta_len;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &fd_to_send, sizeof(int));

    ssize_t n = sendmsg(uds_fd, &msg, 0);
    if (n < 0) {
        LOG_ERROR("sendmsg() failed: %s", strerror(errno));
        return false;
    }
    return true;
}

int HotUpgradeManager::recvFd(int uds_fd, void* metadata, size_t meta_len) {
    struct msghdr msg{};
    struct iovec iov;
    char cmsgbuf[CMSG_SPACE(sizeof(int))];

    iov.iov_base = metadata;
    iov.iov_len = meta_len;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    msg.msg_control = cmsgbuf;
    msg.msg_controllen = sizeof(cmsgbuf);

    ssize_t n = recvmsg(uds_fd, &msg, 0);
    if (n < 0) {
        LOG_ERROR("recvmsg() failed: %s", strerror(errno));
        return -1;
    }

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (!cmsg || cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        LOG_ERROR("No fd received in control message");
        return -1;
    }

    int received_fd;
    memcpy(&received_fd, CMSG_DATA(cmsg), sizeof(int));
    return received_fd;
}

bool HotUpgradeManager::sendData(int uds_fd, const void* data, size_t len) {
    const char* p = static_cast<const char*>(data);
    size_t sent = 0;
    int eagain_retries = 0;
    constexpr int MAX_EAGAIN_RETRIES = 1000;  // 最多重试 1000 次（~100ms）

    while (sent < len) {
        ssize_t n = ::write(uds_fd, p + sent, len - sent);
        if (n > 0) {
            sent += n;
            eagain_retries = 0;
        } else if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 非阻塞模式下写缓冲区满，短暂等待后重试
                if (++eagain_retries > MAX_EAGAIN_RETRIES) {
                    LOG_ERROR("sendData: EAGAIN timeout after %d retries (sent %zu/%zu bytes)",
                             MAX_EAGAIN_RETRIES, sent, len);
                    return false;
                }
                usleep(100);  // 100μs
                continue;
            }
            LOG_ERROR("sendData failed: %s (sent %zu/%zu bytes)",
                     strerror(errno), sent, len);
            return false;
        }
    }
    return true;
}

bool HotUpgradeManager::recvData(int uds_fd, void* buf, size_t len) {
    char* p = static_cast<char*>(buf);
    size_t received = 0;
    int eagain_retries = 0;
    constexpr int MAX_EAGAIN_RETRIES = 100;  // 最多重试 100 次（~10ms）

    while (received < len) {
        ssize_t n = ::read(uds_fd, p + received, len - received);
        if (n > 0) {
            received += n;
            eagain_retries = 0;  // 重置计数
        } else if (n == 0) {
            LOG_ERROR("recvData: connection closed (received %zu/%zu bytes)", received, len);
            return false;
        } else {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 非阻塞模式下数据尚未到达，短暂等待后重试
                // 旧进程的 sendData 保证了完整写入，数据应很快到达
                if (++eagain_retries > MAX_EAGAIN_RETRIES) {
                    LOG_ERROR("recvData: EAGAIN timeout after %d retries (received %zu/%zu bytes)",
                             MAX_EAGAIN_RETRIES, received, len);
                    return false;
                }
                usleep(100);  // 100μs
                continue;
            }
            LOG_ERROR("recvData failed: %s (received %zu/%zu bytes)",
                     strerror(errno), received, len);
            return false;
        }
    }
    return true;
}

} // namespace proxy
