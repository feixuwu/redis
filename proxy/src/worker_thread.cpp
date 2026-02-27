// WorkerThread implementation - to be completed in task 3
#include "worker_thread.h"
#include "client_connection.h"
#include "mux_backend_connection.h"
#include "ipc_proxy_backend.h"
#include "ipc_proxy_frontend.h"
#include "backend_manager.h"
#include "logger.h"

#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <signal.h>
#include <chrono>

namespace proxy {

WorkerThread::WorkerThread(int id, BackendManager* backend_mgr, bool mux_enabled, uint32_t max_streams)
    : id_(id), backend_mgr_(backend_mgr), mux_enabled_(mux_enabled), max_streams_(max_streams) {}

WorkerThread::~WorkerThread() {
    stop();
    if (notify_fd_ >= 0) {
        ::close(notify_fd_);
        notify_fd_ = -1;
    }
}

void WorkerThread::start() {
    notify_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd_ < 0) {
        LOG_ERROR("Worker %d: eventfd creation failed: %s", id_, strerror(errno));
        return;
    }

    event_loop_.addEvent(notify_fd_, EVENT_READABLE,
        [this](int /*fd*/, uint32_t /*events*/) {
            uint64_t val;
            (void)::read(notify_fd_, &val, sizeof(val));
            processPendingTasks();
        });

    running_ = true;
    thread_ = std::thread(&WorkerThread::threadMain, this);
    LOG_INFO("Worker %d started", id_);
}

void WorkerThread::stop() {
    if (!running_) return;
    running_ = false;
    event_loop_.stop();
    if (thread_.joinable()) {
        thread_.join();
    }

    // Close all client connections
    connections_.clear();

    // Close all MUX connections
    mux_pools_.clear();

    LOG_INFO("Worker %d stopped", id_);
}

void WorkerThread::dispatchClient(int client_fd, uint16_t listen_port) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_tasks_.push_back(PendingTask{PendingTask::NEW_CLIENT, client_fd, listen_port});
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

void WorkerThread::dispatchCloseBackend(uint16_t listen_port) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_tasks_.push_back(PendingTask{PendingTask::CLOSE_BACKEND, -1, listen_port});
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

size_t WorkerThread::getConnectionCount() const {
    return connections_.size();
}

void WorkerThread::addConnection(ClientConnection* conn) {
    (void)conn;
}

void WorkerThread::removeConnection(ClientConnection* conn) {
    int fd = conn->getFd();
    event_loop_.removeEvent(fd);
    connections_.erase(fd);
}

void WorkerThread::eraseConnection(int fd) {
    // 直接从 connections_ map 中移除（fd 已从 epoll 移除，ClientConnection fd 已 detach）
    // 移除会触发 unique_ptr 析构 ClientConnection，但因 fd 已 detach 不会 close
    connections_.erase(fd);
}

MuxBackendConnection* WorkerThread::getOrCreateBackendConn(const std::string& addr, uint16_t port,
                                                             const std::string& password) {
    std::string key = addr + ":" + std::to_string(port);

    auto& pool = mux_pools_[key];

    // Find an existing connection that can accept new streams
    for (auto& conn : pool) {
        if (conn->canCreateStream()) {
            return conn.get();
        }
    }

    // Create a new MUX connection
    auto conn = std::make_unique<MuxBackendConnection>(addr, port, password, mux_enabled_, this);
    if (!conn->connect()) {
        LOG_ERROR("Worker %d: failed to connect to backend %s:%d", id_, addr.c_str(), port);
        return nullptr;
    }

    MuxBackendConnection* raw = conn.get();
    pool.push_back(std::move(conn));

    LOG_DEBUG("Worker %d: created new MUX connection to %s:%d (pool size=%zu)",
              id_, addr.c_str(), port, pool.size());
    return raw;
}

void WorkerThread::removeMuxConnection(MuxBackendConnection* conn) {
    std::string key = conn->getAddr() + ":" + std::to_string(conn->getPort());
    auto pool_it = mux_pools_.find(key);
    if (pool_it == mux_pools_.end()) return;

    auto& pool = pool_it->second;
    for (auto it = pool.begin(); it != pool.end(); ++it) {
        if (it->get() == conn) {
            pool.erase(it);
            break;
        }
    }

    if (pool.empty()) {
        mux_pools_.erase(pool_it);
    }
}

MuxBackendConnection* WorkerThread::findBackendByStreamId(uint64_t stream_id) const {
    // 遍历所有 MUX 连接池，查找包含指定 stream_id 的 backend
    for (const auto& [key, pool] : mux_pools_) {
        for (const auto& conn : pool) {
            const auto& streams = conn->getStreams();
            if (streams.count(stream_id) > 0) {
                return conn.get();
            }
        }
    }
    return nullptr;
}

size_t WorkerThread::getMuxConnectionCount() const {
    size_t count = 0;
    for (const auto& [key, pool] : mux_pools_) {
        count += pool.size();
    }
    return count;
}

size_t WorkerThread::getTotalStreamCount() const {
    size_t count = 0;
    for (const auto& [key, pool] : mux_pools_) {
        for (const auto& conn : pool) {
            count += conn->getActiveStreamCount();
        }
    }
    return count;
}

void WorkerThread::threadMain() {
    sigset_t mask;
    sigfillset(&mask);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);

    try {
        LOG_DEBUG("Worker %d event loop starting", id_);
        event_loop_.run();
        LOG_DEBUG("Worker %d event loop finished", id_);
    } catch (const std::exception& e) {
        LOG_ERROR("Worker %d caught exception: %s", id_, e.what());
        // Close all connections in this worker
        connections_.clear();
        mux_pools_.clear();
    } catch (...) {
        LOG_ERROR("Worker %d caught unknown exception", id_);
        connections_.clear();
        mux_pools_.clear();
    }
}

void WorkerThread::processPendingTasks() {
    std::vector<PendingTask> tasks;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        tasks.swap(pending_tasks_);
    }

    for (auto& task : tasks) {
        switch (task.type) {
            case PendingTask::NEW_CLIENT:
                handleNewClient(task.fd, task.listen_port);
                break;
            case PendingTask::CLOSE_BACKEND:
                closeBackendConnections(task.listen_port);
                break;
            case PendingTask::PAUSE_READS:
                pauseClientReads();
                pause_done_.store(true);
                break;
            case PendingTask::DETACH_ALL:
                detachAllClients();
                detach_done_.store(true);
                break;
            case PendingTask::TRANSFER_BACKENDS:
                transferBackends();
                transfer_done_.store(true);
                break;
            case PendingTask::MIGRATE_STREAM:
                if (task.proxy_backend) {
                    migrateStream(task.proxy_backend, task.stream_id);
                }
                break;
            case PendingTask::ACCEPT_MIGRATED_CLIENT:
                acceptMigratedClient(task.fd, task.listen_port,
                                     task.migrated_stream_id,
                                     task.migrated_authenticated,
                                     task.target_backend,
                                     task.migrated_recv_buf,
                                     task.migrated_send_buf);
                break;
            case PendingTask::START_MIGRATE_TIMER:
                migrate_stream_done_cb_ = std::move(task.migrate_stream_done_cb);
                migrate_all_done_cb_ = std::move(task.migrate_all_done_cb);
                startMigrateTimer(task.migrate_interval_ms, task.migrate_batch_size);
                break;
            case PendingTask::RESTORE_BACKEND:
                restoreBackend(task.fd, task.restore_meta, task.restore_stream_map,
                               task.restore_frame_payload, task.restore_recv_buf,
                               task.restore_send_buf);
                break;
            case PendingTask::CREATE_IPC_FRONTEND:
                createIPCFrontend(task.fd, task.backend_key);
                break;
        }
    }
}

void WorkerThread::handleNewClient(int client_fd, uint16_t listen_port) {
    auto conn = std::make_unique<ClientConnection>(client_fd, listen_port, this);

    ClientConnection* raw = conn.get();
    connections_[client_fd] = std::move(conn);

    event_loop_.addEvent(client_fd, EVENT_READABLE,
        [raw](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                raw->close();
                return;
            }
            if (events & EVENT_READABLE) {
                raw->onReadable();
            }
            if (events & EVENT_WRITABLE) {
                raw->onWritable();
            }
        });

    LOG_DEBUG("Worker %d: new client fd=%d for listen_port=%d", id_, client_fd, listen_port);
}

std::vector<WorkerThread::ClientConnInfo> WorkerThread::getClientConnInfos() const {
    std::vector<ClientConnInfo> infos;
    for (const auto& [fd, conn] : connections_) {
        ClientConnInfo info;
        info.fd = fd;
        info.listen_port = conn->getListenPort();
        info.authenticated = conn->isAuthenticated();
        info.stream_id = conn->getStreamId();
        infos.push_back(info);
    }
    return infos;
}

void WorkerThread::dispatchPauseReads() {
    pause_done_.store(false);
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_tasks_.push_back(PendingTask{PendingTask::PAUSE_READS, -1, 0});
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

bool WorkerThread::waitPauseDone(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!pause_done_.load() && std::chrono::steady_clock::now() < deadline) {
        usleep(1000); // 1ms
    }
    return pause_done_.load();
}

void WorkerThread::dispatchDetachAll() {
    detach_done_.store(false);
    detached_infos_.clear();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_tasks_.push_back(PendingTask{PendingTask::DETACH_ALL, -1, 0});
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

bool WorkerThread::waitDetachDone(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!detach_done_.load() && std::chrono::steady_clock::now() < deadline) {
        usleep(1000); // 1ms
    }
    return detach_done_.load();
}

std::vector<WorkerThread::ClientConnInfo> WorkerThread::getDetachedInfos() {
    return std::move(detached_infos_);
}

void WorkerThread::pauseClientReads() {
    // Stop reading from client fds to prevent new requests.
    // Two-pronged approach:
    // 1. Set paused flag on each ClientConnection (prevents reading even if
    //    this epoll batch already has READABLE events for the fd)
    // 2. Modify epoll to WRITABLE-only (prevents future epoll_wait from
    //    returning READABLE for these fds)
    for (auto& [fd, conn] : connections_) {
        conn->setPaused(true);
        event_loop_.modifyEvent(fd, EVENT_WRITABLE);
    }
    LOG_INFO("Worker %d: paused reads on %zu client connections", id_, connections_.size());
}

void WorkerThread::detachAllClients() {
    // Collect client info and detach fds
    // This runs in the worker thread, so it's safe to access connections_
    detached_infos_.clear();

    std::vector<int> fds;
    for (auto& [fd, conn] : connections_) {
        ClientConnInfo info;
        info.fd = fd;
        info.listen_port = conn->getListenPort();
        info.authenticated = conn->isAuthenticated();
        info.stream_id = conn->getStreamId();
        detached_infos_.push_back(info);

        event_loop_.removeEvent(fd);
        conn->detachFd();  // Prevent close on destruction
        fds.push_back(fd);
    }

    // Now safe to clear - destructors won't close fds
    connections_.clear();

    // NOTE: Do NOT clear mux_pools_ here. The MUX backend connections may still have
    // in-flight responses in the kernel buffer. They will be cleaned up when the
    // old process exits. The new process creates its own MUX connections.

    LOG_INFO("Worker %d: detached %zu client connections for hot upgrade", id_, fds.size());
}

void WorkerThread::closeBackendConnections(uint16_t listen_port) {
    // Close client connections for this backend
    std::vector<int> fds_to_close;
    for (auto& [fd, conn] : connections_) {
        if (conn->getListenPort() == listen_port) {
            fds_to_close.push_back(fd);
        }
    }

    for (int fd : fds_to_close) {
        event_loop_.removeEvent(fd);
        connections_.erase(fd);
    }

    // Also close MUX connections for this backend
    std::string backend_addr;
    uint16_t backend_port;
    std::string backend_password;
    if (backend_mgr_->getBackendAddr(listen_port, backend_addr, backend_port, backend_password)) {
        std::string key = backend_addr + ":" + std::to_string(backend_port);
        mux_pools_.erase(key);
    }

    LOG_INFO("Worker %d: closed %zu connections for backend port %d",
             id_, fds_to_close.size(), listen_port);
}

// ========== 热升级 V2: 后端连接迁移 ==========

void WorkerThread::dispatchTransferBackends() {
    transfer_done_.store(false);
    transfer_results_.clear();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        PendingTask task{};
        task.type = PendingTask::TRANSFER_BACKENDS;
        pending_tasks_.push_back(task);
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

bool WorkerThread::waitTransferDone(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!transfer_done_.load() && std::chrono::steady_clock::now() < deadline) {
        usleep(1000);
    }
    return transfer_done_.load();
}

std::vector<WorkerThread::BackendTransferResult> WorkerThread::getTransferResults() {
    return std::move(transfer_results_);
}

std::vector<IPCProxyBackend*> WorkerThread::getIPCProxyBackends() const {
    std::vector<IPCProxyBackend*> result;
    for (const auto& proxy : ipc_proxy_backends_) {
        result.push_back(proxy.get());
    }
    return result;
}

void WorkerThread::dispatchMigrateStream(IPCProxyBackend* proxy, uint64_t stream_id) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        PendingTask task{};
        task.type = PendingTask::MIGRATE_STREAM;
        task.proxy_backend = proxy;
        task.stream_id = stream_id;
        pending_tasks_.push_back(task);
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

void WorkerThread::transferBackends() {
    // 在 Worker 线程中执行：转移所有后端 MUX 连接
    // 对每个 MuxBackendConnection:
    //   1. 从 epoll 移除 backend_fd
    //   2. 刷完 send_buf_
    //   3. 创建 socketpair
    //   4. 创建 IPCProxyBackend(socketpair[0])
    //   5. 对每个 Client: setStreaming(ipc_proxy, same_stream_id)
    //   6. 记录结果（backend_fd + socketpair[1] 将由主线程传给新进程）

    transfer_results_.clear();

    for (auto& [key, pool] : mux_pools_) {
        for (auto& mux_conn : pool) {
            if (mux_conn->getState() == MuxConnState::CLOSED) continue;

            int backend_fd = mux_conn->getFd();
            if (backend_fd < 0) continue;

            // Step 1: 从 epoll 移除
            event_loop_.removeEvent(backend_fd);

            // Step 2: 刷完 send_buf_
            mux_conn->flushSendBuffer();

            // Step 3: 创建 socketpair
            int sv[2];
            if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv) < 0) {
                LOG_ERROR("Worker %d: socketpair failed: %s", id_, strerror(errno));
                continue;
            }

            // Step 4: 创建 IPCProxyBackend
            auto ipc_proxy = std::make_unique<IPCProxyBackend>(sv[0], this);

            // Step 5: 收集 stream 映射并切换 Client 的后端
            BackendTransferResult result;
            result.backend_fd = backend_fd;
            result.addr = mux_conn->getAddr();
            result.port = mux_conn->getPort();
            result.password = mux_conn->getPassword();
            result.mux_enabled = mux_conn->isMuxEnabled();
            result.next_stream_id = mux_conn->getNextStreamId();
            result.active_stream_count = mux_conn->getActiveStreamCount();
            result.orig_conn = mux_conn.get();
            result.ipc_fd = sv[1]; // 新进程端的 socketpair fd

            for (const auto& [stream_id, client] : mux_conn->getStreams()) {
                if (client) {
                    result.stream_map.push_back({stream_id, client->getFd()});
                    // 切换客户端的后端为 IPC 代理
                    ipc_proxy->addStream(stream_id, client);
                    client->setStreaming(static_cast<IBackendConnection*>(ipc_proxy.get()), stream_id);
                }
            }

            // 注册 IPC fd 到 epoll
            IPCProxyBackend* proxy_raw = ipc_proxy.get();
            event_loop_.addEvent(sv[0], EVENT_READABLE,
                [proxy_raw](int /*fd*/, uint32_t events) {
                    if (events & EVENT_ERROR) {
                        return;
                    }
                    if (events & EVENT_READABLE) {
                        proxy_raw->onReadable();
                    }
                    if (events & EVENT_WRITABLE) {
                        proxy_raw->onWritable();
                    }
                });

            // ★ 安全措施：将原始 MuxBackendConnection 的 fd 分离（不 close）
            // 防止旧进程退出时析构 MuxBackendConnection 导致 close(backend_fd)
            // 必须用 detachFd() 而非 adoptFd(-1)，因为 adoptFd(-1) 会 close 原 fd
            // 而此 fd 即将通过 sendmsg(SCM_RIGHTS) 传给新进程
            mux_conn->detachFd();

            transfer_results_.push_back(std::move(result));
            ipc_proxy_backends_.push_back(std::move(ipc_proxy));

            LOG_INFO("Worker %d: transferred backend %s:%d (fd=%d, streams=%u, ipc_fd=%d)",
                     id_, result.addr.c_str(), result.port, backend_fd,
                     result.active_stream_count, sv[1]);
        }
    }

    // 不清理 mux_pools_，因为 MuxBackendConnection 对象还持有 fd
    // 但是 fd 已经从 epoll 移除，不会再触发 I/O

    LOG_INFO("Worker %d: transfer complete, %zu backend connections transferred",
             id_, transfer_results_.size());
}

void WorkerThread::migrateStream(IPCProxyBackend* proxy, uint64_t stream_id) {
    // 在 Worker 线程中执行：发起单个 stream 的迁移
    proxy->beginMigrate(stream_id);
}

// ========== 热升级 V2: 事件驱动的迁移定时器 ==========

void WorkerThread::dispatchStartMigrateTimer(int interval_ms, int batch_size,
                                               MigrateDoneCallback on_stream_done,
                                               AllDoneCallback on_all_done) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        PendingTask task{};
        task.type = PendingTask::START_MIGRATE_TIMER;
        task.migrate_interval_ms = interval_ms;
        task.migrate_batch_size = batch_size;
        task.migrate_stream_done_cb = std::move(on_stream_done);
        task.migrate_all_done_cb = std::move(on_all_done);
        pending_tasks_.push_back(std::move(task));
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

void WorkerThread::startMigrateTimer(int interval_ms, int batch_size) {
    // 在 Worker 线程中执行：注册定时器，每次触发迁移一批 stream
    // 定时器在 event loop 中执行，不会阻塞客户端 I/O
    LOG_INFO("Worker %d: starting migrate timer (interval=%dms, batch=%d)",
             id_, interval_ms, batch_size);

    // 保存批量大小
    migrate_batch_size_ = batch_size;

    // 对每个 IPC 代理后端设置迁移完成回调
    for (auto& proxy : ipc_proxy_backends_) {
        proxy->setMigrateCallback(
            [this](int client_fd, const ClientMigrateMeta& meta,
                   const std::vector<char>& recv_buf, const std::vector<char>& send_buf) {
                if (migrate_stream_done_cb_) {
                    migrate_stream_done_cb_(client_fd, meta, recv_buf, send_buf);
                }
            });
    }

    // 注册定时器（repeat 模式，每次间隔触发）
    migrate_timer_id_ = event_loop_.addTimer(interval_ms,
        [this]() {
            onMigrateTimerFired();
            // 检查是否还有待迁移的 stream
            bool has_more = false;
            for (const auto& proxy : ipc_proxy_backends_) {
                if (proxy->getActiveStreamCount() > 0) {
                    has_more = true;
                    break;
                }
            }
            // 同时检查是否还有空闲连接（无 stream 绑定的客户端）
            if (!has_more) {
                for (const auto& [fd, conn] : connections_) {
                    if (conn->getStreamId() == 0) {
                        has_more = true;
                        break;
                    }
                }
            }
            if (!has_more) {
                LOG_INFO("Worker %d: all streams migrated, stopping timer", id_);
                if (migrate_all_done_cb_) {
                    migrate_all_done_cb_();
                }
                // 取消定时器
                event_loop_.cancelTimer(migrate_timer_id_);
                migrate_timer_id_ = 0;
            }
        }, true /* repeat */);
}

void WorkerThread::onMigrateTimerFired() {
    // 定时器回调：在 Worker 线程中执行，不阻塞 event loop
    // 每次迁移一批 stream（最多 migrate_batch_size_ 个）
    int migrated = 0;

    // ---- 第一部分：迁移有活跃 stream 的连接（通过 FENCE 协议）----
    for (auto& proxy : ipc_proxy_backends_) {
        if (migrated >= migrate_batch_size_) break;
        if (proxy->getActiveStreamCount() == 0) continue;

        // 先收集待迁移的 stream_id 列表，避免在遍历 getMigrateStates() 的
        // 同时调用 beginMigrate() 修改同一个 map（迭代器失效 = UB）
        std::vector<uint64_t> to_migrate;
        for (const auto& [stream_id, state] : proxy->getMigrateStates()) {
            if (state == IPCProxyBackend::StreamMigrateState::ACTIVE) {
                to_migrate.push_back(stream_id);
                if (migrated + static_cast<int>(to_migrate.size()) >= migrate_batch_size_) break;
            }
        }

        // 遍历结束后再逐个发起迁移
        for (uint64_t sid : to_migrate) {
            if (migrated >= migrate_batch_size_) break;
            proxy->beginMigrate(sid);
            migrated++;
        }
    }

    if (migrated > 0) {
        LOG_DEBUG("Worker %d: migrate timer fired, initiated %d stream migrations", id_, migrated);
    }

    // ---- 第二部分：直接迁移空闲连接（没有绑定 stream 的客户端）----
    // 空闲连接没有 in-flight 请求，无需 FENCE 协议，直接 detach + 发送即可
    std::vector<int> idle_fds;
    for (const auto& [fd, conn] : connections_) {
        if (conn->getStreamId() == 0) {
            idle_fds.push_back(fd);
        }
    }

    int idle_migrated = 0;
    for (int fd : idle_fds) {
        auto it = connections_.find(fd);
        if (it == connections_.end()) continue;
        ClientConnection* client = it->second.get();

        // 构造迁移元数据
        ClientMigrateMeta meta{};
        meta.stream_id = 0;  // 空闲连接，无 stream
        meta.listen_port = client->getListenPort();
        meta.authenticated = client->isAuthenticated() ? 1 : 0;

        // 从 epoll 移除
        event_loop_.removeEvent(fd);

        // 刷完 send buffer
        client->flushSendBuffer();

        // 提取缓冲区
        std::vector<char> recv_buf = client->extractRecvBuffer();
        std::vector<char> send_buf = client->extractSendBuffer();
        meta.recv_buf_len = static_cast<uint32_t>(recv_buf.size());
        meta.send_buf_len = static_cast<uint32_t>(send_buf.size());

        // 分离 fd（防止析构时 close）
        client->detachFd();

        // 从 connections_ 移除（触发 ~ClientConnection，但 fd 已 detach）
        connections_.erase(it);

        // 通过回调发送给新进程
        if (migrate_stream_done_cb_) {
            migrate_stream_done_cb_(fd, meta, recv_buf, send_buf);
        }
        idle_migrated++;
    }

    if (idle_migrated > 0) {
        LOG_INFO("Worker %d: directly migrated %d idle connections (no stream bound)",
                 id_, idle_migrated);
    }
}

// ========== 新进程侧: 恢复后端连接 ==========

void WorkerThread::dispatchRestoreBackend(int backend_fd, const MuxBackendTransferMeta& meta,
                                            const std::vector<StreamMapEntry>& stream_map,
                                            const std::vector<char>& frame_payload_buf,
                                            const std::vector<char>& recv_buf,
                                            const std::vector<char>& send_buf) {
    // 堆上分配数据，由 Worker 线程释放
    auto* meta_copy = new MuxBackendTransferMeta(meta);
    auto* sm_copy = new std::vector<StreamMapEntry>(stream_map);
    auto* fp_copy = new std::vector<char>(frame_payload_buf);
    auto* rb_copy = new std::vector<char>(recv_buf);
    auto* sb_copy = new std::vector<char>(send_buf);

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        PendingTask task{};
        task.type = PendingTask::RESTORE_BACKEND;
        task.fd = backend_fd;
        task.restore_meta = meta_copy;
        task.restore_stream_map = sm_copy;
        task.restore_frame_payload = fp_copy;
        task.restore_recv_buf = rb_copy;
        task.restore_send_buf = sb_copy;
        pending_tasks_.push_back(std::move(task));
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

void WorkerThread::restoreBackend(int fd, MuxBackendTransferMeta* meta,
                                    std::vector<StreamMapEntry>* stream_map,
                                    std::vector<char>* frame_payload,
                                    std::vector<char>* recv_buf,
                                    std::vector<char>* send_buf) {
    // 在 Worker 线程中执行：用迁移过来的 fd 重建 MuxBackendConnection
    std::string addr(meta->addr);
    uint16_t port = meta->port;
    std::string password(meta->password);
    bool mux_enabled = meta->mux_enabled != 0;

    auto conn = std::make_unique<MuxBackendConnection>(
        addr, port, password, mux_enabled, this);

    // 使用迁移过来的 fd（不重新连接）
    conn->adoptFd(fd);
    conn->setNextStreamId(meta->next_stream_id);

    // ★ 关键修复：恢复 stream 映射表和 active_stream_count
    // 这些数据从旧进程传来的 stream_map 中恢复。
    // 此时 client 指针为 nullptr（客户端还在旧进程），后续通过：
    //   1. createIPCFrontend() 注册为 proxied（IPC 代理期间）
    //   2. acceptMigratedClient() 中 rebindStream() 绑定到真正的本地 ClientConnection
    if (stream_map) {
        for (const auto& entry : *stream_map) {
            conn->rebindStream(entry.stream_id, nullptr);
        }
    }

    // 恢复帧解析状态机
    conn->restoreFrameParseState(
        meta->frame_header_buf, meta->header_bytes_read,
        meta->parsing_payload != 0, meta->current_flags,
        meta->current_stream_id, meta->current_payload_len,
        frame_payload ? frame_payload->data() : nullptr,
        frame_payload ? frame_payload->size() : 0);

    // 注入缓冲区数据
    if (recv_buf && !recv_buf->empty()) {
        conn->injectRecvBuffer(*recv_buf);
    }
    if (send_buf && !send_buf->empty()) {
        conn->injectSendBuffer(*send_buf);
    }

    // 注册到 epoll
    MuxBackendConnection* raw = conn.get();
    event_loop_.addEvent(fd, EVENT_READABLE,
        [raw](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                raw->onConnectionError();
                return;
            }
            if (events & EVENT_READABLE) {
                raw->onReadable();
            }
            if (events & EVENT_WRITABLE) {
                raw->onWritable();
            }
        });

    // 添加到连接池
    std::string key = addr + ":" + std::to_string(port);
    mux_pools_[key].push_back(std::move(conn));

    LOG_INFO("Worker %d: restored backend %s:%d (fd=%d, streams=%u)",
             id_, addr.c_str(), port, fd, meta->active_stream_count);

    // 清理堆上分配的数据
    delete meta;
    delete stream_map;
    delete frame_payload;
    delete recv_buf;
    delete send_buf;
}

void WorkerThread::dispatchCreateIPCFrontend(int ipc_fd, const std::string& backend_key) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        PendingTask task{};
        task.type = PendingTask::CREATE_IPC_FRONTEND;
        task.fd = ipc_fd;
        task.backend_key = backend_key;
        pending_tasks_.push_back(std::move(task));
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

void WorkerThread::createIPCFrontend(int ipc_fd, const std::string& backend_key) {
    // 在 Worker 线程中执行：创建 IPCProxyFrontend 并关联到对应的 MuxBackendConnection
    auto pool_it = mux_pools_.find(backend_key);
    if (pool_it == mux_pools_.end() || pool_it->second.empty()) {
        LOG_ERROR("Worker %d: no backend found for key '%s'", id_, backend_key.c_str());
        ::close(ipc_fd);
        return;
    }

    // 使用第一个后端连接（通常迁移过来的只有一个）
    MuxBackendConnection* real_backend = pool_it->second.back().get();

    auto frontend = std::make_unique<IPCProxyFrontend>(ipc_fd, real_backend, this);
    IPCProxyFrontend* frontend_raw = frontend.get();

    // ★ 关键修复：注册所有当前 stream 为代理状态（它们的客户端还在旧进程）
    // restoreBackend() 已经用 stream_map 恢复了 streams_，所以 getStreams() 现在返回正确的映射
    for (const auto& [stream_id, client] : real_backend->getStreams()) {
        frontend->addProxiedStream(stream_id);
    }

    // ★ 关键修复：将 IPCProxyFrontend 关联到 MuxBackendConnection
    // 这样 handleFrame(DATA) 中发现 stream 的 client 为 nullptr 时，
    // 会检查 ipc_frontend_->isProxied(stream_id)，将回复通过 IPC 回传给旧进程
    real_backend->setIPCProxyFrontend(frontend_raw);

    // 注册 IPC fd 到 epoll
    event_loop_.addEvent(ipc_fd, EVENT_READABLE,
        [frontend_raw](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                return;
            }
            if (events & EVENT_READABLE) {
                frontend_raw->onIPCReadable();
            }
            if (events & EVENT_WRITABLE) {
                frontend_raw->onIPCWritable();
            }
        });

    ipc_proxy_frontends_.push_back(std::move(frontend));

    // ★ 修复：主动处理 restoreBackend 阶段注入的 recv_buf 中残留的帧数据
    // 此时 ipc_frontend_ 已设置好，proxied streams 已注册，可以正确路由
    // 如果不主动处理，这些数据要等到 Redis 发送新数据触发 onReadable() 后才会被解析
    // 在 Redis 空闲时（如 PING 间隔长），这些回复帧可能长时间不被处理
    real_backend->processRecvBuffer();

    LOG_INFO("Worker %d: created IPCProxyFrontend for backend '%s' (ipc_fd=%d, proxied_streams=%zu)",
             id_, backend_key.c_str(), ipc_fd, real_backend->getStreams().size());
}

void WorkerThread::dispatchAcceptMigratedClient(int fd, uint16_t listen_port,
                                                  uint64_t stream_id, bool authenticated,
                                                  MuxBackendConnection* backend,
                                                  std::vector<char>* recv_buf,
                                                  std::vector<char>* send_buf) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        PendingTask task{};
        task.type = PendingTask::ACCEPT_MIGRATED_CLIENT;
        task.fd = fd;
        task.listen_port = listen_port;
        task.migrated_stream_id = stream_id;
        task.migrated_authenticated = authenticated;
        task.target_backend = backend;
        task.migrated_recv_buf = recv_buf;
        task.migrated_send_buf = send_buf;
        pending_tasks_.push_back(std::move(task));
    }
    uint64_t val = 1;
    (void)::write(notify_fd_, &val, sizeof(val));
}

void WorkerThread::acceptMigratedClient(int fd, uint16_t listen_port,
                                          uint64_t stream_id, bool authenticated,
                                          MuxBackendConnection* backend,
                                          std::vector<char>* recv_buf,
                                          std::vector<char>* send_buf) {
    // 在新进程的 Worker 线程中执行：接收从旧进程迁移过来的客户端
    auto conn = std::make_unique<ClientConnection>(fd, listen_port, this);
    ClientConnection* raw = conn.get();

    // 恢复认证状态
    if (authenticated) {
        raw->setAuthenticated();
    }

    // 注入 buffer 数据
    if (recv_buf && !recv_buf->empty()) {
        raw->injectRecvBuffer(*recv_buf);
        delete recv_buf;
    } else {
        delete recv_buf;
    }
    if (send_buf && !send_buf->empty()) {
        raw->injectSendBuffer(*send_buf);
        delete send_buf;
    } else {
        delete send_buf;
    }

    // 如果 backend 为 nullptr，在 Worker 线程内通过 stream_id 安全查找
    // 这避免了跨线程传递裸指针可能失效的问题
    if (!backend && stream_id > 0) {
        backend = findBackendByStreamId(stream_id);
        if (!backend) {
            LOG_WARN("Worker %d: no backend found for migrated stream %llu, "
                     "client will need to re-establish",
                     id_, (unsigned long long)stream_id);
        }
    }

    // 绑定到后端 stream
    if (backend && stream_id > 0) {
        // ★ 关键：rebindStream 将 streams_[stream_id] 从 nullptr 更新为 raw
        // 此后 handleFrame(DATA) 中 getStreamClient() 就能找到这个 client，
        // 回复直接走本地路由而不再走 IPC
        backend->rebindStream(stream_id, raw);
        raw->setStreamingMux(backend, stream_id);

        // ★ 关键修复：从 IPCProxyFrontend 的 proxied 集合中移除此 stream
        // 否则 handleFrame(DATA) 中虽然 getStreamClient() 能找到 client，
        // 但后续如果有 in_flight 回复仍走旧逻辑
        IPCProxyFrontend* ipc_fe = backend->getIPCProxyFrontend();
        if (ipc_fe) {
            ipc_fe->removeProxiedStream(stream_id);
        }
    }

    connections_[fd] = std::move(conn);

    // 注册 epoll
    event_loop_.addEvent(fd, EVENT_READABLE,
        [raw](int /*fd*/, uint32_t events) {
            if (events & EVENT_ERROR) {
                raw->close();
                return;
            }
            if (events & EVENT_READABLE) {
                raw->onReadable();
            }
            if (events & EVENT_WRITABLE) {
                raw->onWritable();
            }
        });

    // ★ 关键修复：如果注入了 recv_buf（旧进程 Client 有已接收但未转发的数据），
    // 必须主动触发转发，否则如果客户端不再发新数据，这些请求永远不会被处理
    if (backend && stream_id > 0 && !raw->getRecvBuf().empty()) {
        raw->retryForwardToBackend();
    }

    LOG_INFO("Worker %d: accepted migrated client fd=%d stream=%llu",
             id_, fd, (unsigned long long)stream_id);
}

} // namespace proxy