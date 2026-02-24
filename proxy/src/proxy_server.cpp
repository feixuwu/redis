#include "proxy_server.h"
#include "logger.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <csignal>
#include <cstring>

namespace proxy {

// Global pointer for signal handler
static ProxyServer* g_server = nullptr;

static void signalHandler(int sig) {
    if (sig == SIGTERM || sig == SIGINT) {
        if (g_server) {
            LOG_INFO("Received signal %d, shutting down...", sig);
            g_server->shutdown();
        }
    }
}

ProxyServer::ProxyServer(Config& config)
    : config_(config), start_time_(std::chrono::steady_clock::now()) {}

ProxyServer::~ProxyServer() {
    shutdown();
}

bool ProxyServer::init() {
    // Initialize backend manager
    if (!backend_mgr_.init(config_.getBackends())) {
        LOG_ERROR("Failed to initialize backend manager");
        return false;
    }

    // Determine worker count
    int num_workers = config_.getWorkers();
    if (num_workers <= 0) {
        num_workers = static_cast<int>(std::thread::hardware_concurrency());
        if (num_workers <= 0) num_workers = 4;
    }

    // Create worker threads
    bool mux_enabled = config_.getMuxConfig().enabled;
    uint32_t max_streams = config_.getMuxConfig().max_streams_per_connection;
    for (int i = 0; i < num_workers; i++) {
        workers_.push_back(std::make_unique<WorkerThread>(i, &backend_mgr_, mux_enabled, max_streams));
    }

    // Setup listening sockets for each backend
    for (const auto& backend : config_.getBackends()) {
        if (!setupListenSocket(backend.listen_port)) {
            LOG_ERROR("Failed to setup listen socket for port %d", backend.listen_port);
            return false;
        }
    }

    // Setup signal handlers
    setupSignalHandlers();

    // Initialize admin server
    uint16_t admin_port = config_.getAdminPort();
    if (admin_port > 0) {
        admin_server_ = std::make_unique<AdminServer>(this);
        if (!admin_server_->init(admin_port, main_loop_)) {
            LOG_WARN("Failed to initialize admin server on port %d", admin_port);
            admin_server_.reset();
        }
    }

    LOG_INFO("ProxyServer initialized: %d workers, %zu backends, admin_port=%d",
             num_workers, config_.getBackends().size(), admin_port);
    return true;
}

void ProxyServer::run() {
    running_ = true;
    g_server = this;

    // Start worker threads
    for (auto& worker : workers_) {
        worker->start();
    }

    LOG_INFO("ProxyServer running, entering main event loop");

    // Run main thread event loop (handles accepts and admin)
    main_loop_.run();

    LOG_INFO("Main event loop exited");
}

void ProxyServer::shutdown() {
    if (!running_.exchange(false)) return;

    LOG_INFO("ProxyServer shutting down gracefully...");

    // Phase 1: Stop accepting new connections
    for (auto& [fd, port] : listen_fds_) {
        main_loop_.removeEvent(fd);
        ::close(fd);
    }
    listen_fds_.clear();
    LOG_INFO("Stopped accepting new connections");

    // Shutdown admin server
    if (admin_server_) {
        admin_server_->shutdown();
    }

    // Phase 2: Wait for existing connections to drain (with timeout)
    uint32_t timeout_secs = config_.getShutdownTimeout();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_secs);

    while (std::chrono::steady_clock::now() < deadline) {
        size_t total_conns = 0;
        for (const auto& worker : workers_) {
            total_conns += worker->getConnectionCount();
        }
        if (total_conns == 0) {
            LOG_INFO("All connections drained");
            break;
        }
        LOG_INFO("Waiting for %zu connections to drain (timeout=%us)...", total_conns, timeout_secs);
        usleep(500000); // 500ms
    }

    // Phase 3: Force close - stop all workers
    // Stop main loop
    main_loop_.stop();

    // Stop worker threads (this will close all remaining connections)
    for (auto& worker : workers_) {
        worker->stop();
    }

    LOG_INFO("ProxyServer shutdown complete");
}

bool ProxyServer::setupListenSocket(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("socket() failed: %s", strerror(errno));
        return false;
    }

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("bind() port %d failed: %s", port, strerror(errno));
        ::close(fd);
        return false;
    }

    if (listen(fd, 511) < 0) {
        LOG_ERROR("listen() port %d failed: %s", port, strerror(errno));
        ::close(fd);
        return false;
    }

    // Register in main event loop
    main_loop_.addEvent(fd, EVENT_READABLE,
        [this, fd](int /*listen_fd*/, uint32_t /*events*/) {
            onNewConnection(fd);
        });

    listen_fds_[fd] = port;
    LOG_INFO("Listening on port %d (fd=%d)", port, fd);
    return true;
}

void ProxyServer::closeListenSocket(uint16_t port) {
    for (auto it = listen_fds_.begin(); it != listen_fds_.end(); ++it) {
        if (it->second == port) {
            int fd = it->first;
            main_loop_.removeEvent(fd);
            ::close(fd);
            listen_fds_.erase(it);
            LOG_INFO("Closed listen socket for port %d", port);
            return;
        }
    }
}

void ProxyServer::onNewConnection(int listen_fd) {
    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept4(listen_fd, (struct sockaddr*)&client_addr,
                                &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            LOG_ERROR("accept4() failed: %s", strerror(errno));
            break;
        }

        // Disable Nagle's algorithm
        int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        uint16_t listen_port = listen_fds_[listen_fd];
        WorkerThread& worker = selectWorker();
        worker.dispatchClient(client_fd, listen_port);

        char addr_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, addr_str, sizeof(addr_str));
        LOG_DEBUG("New connection from %s:%d on port %d -> worker %d",
                  addr_str, ntohs(client_addr.sin_port), listen_port, worker.getId());
    }
}

WorkerThread& ProxyServer::selectWorker() {
    uint32_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    return *workers_[idx];
}

void ProxyServer::setupSignalHandlers() {
    // Ignore SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    // Handle SIGTERM and SIGINT
    struct sigaction sa{};
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
}

bool ProxyServer::addBackend(const BackendConfig& backend) {
    // Add to backend manager
    if (!backend_mgr_.addBackend(backend)) {
        return false;
    }

    // Create listen socket
    if (!setupListenSocket(backend.listen_port)) {
        backend_mgr_.removeBackend(backend.listen_port);
        return false;
    }

    // Add to config
    config_.addBackend(backend);
    return true;
}

bool ProxyServer::removeBackend(uint16_t listen_port) {
    // Close listen socket
    closeListenSocket(listen_port);

    // Notify all workers to close connections for this backend
    for (auto& worker : workers_) {
        worker->dispatchCloseBackend(listen_port);
    }

    // Remove from backend manager and config
    backend_mgr_.removeBackend(listen_port);
    config_.removeBackend(listen_port);
    return true;
}

} // namespace proxy