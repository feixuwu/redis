// WorkerThread implementation - to be completed in task 3
#include "worker_thread.h"
#include "client_connection.h"
#include "mux_backend_connection.h"
#include "backend_manager.h"
#include "logger.h"

#include <sys/eventfd.h>
#include <unistd.h>
#include <cstring>
#include <signal.h>

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

} // namespace proxy