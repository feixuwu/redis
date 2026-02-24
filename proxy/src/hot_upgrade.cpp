// HotUpgradeManager implementation
#include "hot_upgrade.h"
#include "proxy_server.h"
#include "worker_thread.h"
#include "logger.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <chrono>
#include <thread>
#include <vector>

namespace proxy {

// Message types for UDS protocol
constexpr uint8_t MSG_HANDSHAKE     = 0x01;
constexpr uint8_t MSG_HANDSHAKE_ACK = 0x02;
constexpr uint8_t MSG_LISTEN_FD     = 0x03;
constexpr uint8_t MSG_CLIENT_FD     = 0x04;
constexpr uint8_t MSG_TRANSFER_DONE = 0x05;
constexpr uint8_t MSG_ACK           = 0x06;
constexpr uint8_t MSG_ROLLBACK      = 0x07;

HotUpgradeManager::HotUpgradeManager(ProxyServer* server) : server_(server) {
}

HotUpgradeManager::~HotUpgradeManager() {
    if (uds_fd_ >= 0) {
        ::close(uds_fd_);
        uds_fd_ = -1;
    }
}

bool HotUpgradeManager::startUpgrade(const std::string& new_binary_path) {
    if (state_ != UpgradeState::IDLE) {
        LOG_ERROR("Hot upgrade already in progress (state=%s)", getStateString().c_str());
        return false;
    }

    socket_path_ = server_->getConfig().getHotUpgradeConfig().socket_path;
    state_ = UpgradeState::TRANSFERRING;
    LOG_INFO("Starting hot upgrade...");

    // Step 1: Create UDS for communication
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

    // Step 2: Determine binary path
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

    // Step 3: Build command line for new process
    // Pass config file and upgrade-from flag
    std::string config_arg = "-c";
    // Try to get config path (use default if not available)
    std::string config_path = "proxy.yaml";

    // Step 4: Fork and exec new process
    pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("fork() failed: %s", strerror(errno));
        ::close(server_fd);
        unlink(socket_path_.c_str());
        state_ = UpgradeState::FAILED;
        return false;
    }

    if (pid == 0) {
        // Child process: exec new binary with --upgrade-from flag and config path
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
        // If exec fails
        LOG_ERROR("execl failed: %s", strerror(errno));
        _exit(1);
    }

    LOG_INFO("Forked new process pid=%d, binary=%s", pid, binary.c_str());

    // Step 5: Wait for new process to connect
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

    // Step 6: Wait for handshake from new process
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

    // Send handshake ACK
    msg_type = MSG_HANDSHAKE_ACK;
    ::write(uds_fd_, &msg_type, 1);
    LOG_INFO("Handshake complete with new process");

    // Step 7: Transfer all listen fds with metadata
    const auto& listen_fds = server_->getListenFds();
    for (const auto& [fd, port] : listen_fds) {
        msg_type = MSG_LISTEN_FD;
        ::write(uds_fd_, &msg_type, 1);

        ConnectionMetadata meta{};
        meta.fd = fd;
        meta.listen_port = port;
        meta.authenticated = false;
        meta.stream_id = 0;
        meta.backend_port = 0;
        meta.recv_buf_len = 0;
        meta.send_buf_len = 0;
        memset(meta.backend_addr, 0, sizeof(meta.backend_addr));

        if (!sendFd(uds_fd_, fd, &meta, sizeof(meta))) {
            LOG_ERROR("Failed to send listen fd=%d for port=%d", fd, port);
        } else {
            LOG_INFO("Sent listen fd=%d for port=%d", fd, port);
        }
    }

    // Step 7b: Collect and transfer all client fds from workers
    // First stop accepting new connections on listen fds
    LOG_INFO("Collecting client connections from all workers...");
    int total_client_fds = 0;
    const auto& workers = server_->getWorkers();

    // Phase 1: Dispatch pause reads to all workers (thread-safe)
    for (const auto& worker : workers) {
        worker->dispatchPauseReads();
    }
    // Wait for all workers to complete pause
    for (const auto& worker : workers) {
        worker->waitPauseDone(500);
    }

    // Phase 2: Wait for in-flight requests to complete
    // Now workers have stopped reading from client fds, but can still write responses.
    // Give time for any pending Redis responses to be forwarded back to clients.
    usleep(100000); // 100ms

    // Phase 3: Dispatch detach to all workers (thread-safe)
    for (const auto& worker : workers) {
        worker->dispatchDetachAll();
    }
    // Wait for all workers to complete detach
    for (const auto& worker : workers) {
        worker->waitDetachDone(500);
    }

    // Phase 4: Collect client infos from workers (safe now - detach is done)
    struct ClientFdInfo {
        int fd;
        uint16_t listen_port;
        bool authenticated;
        uint64_t stream_id;
    };
    std::vector<ClientFdInfo> all_clients;

    for (const auto& worker : workers) {
        auto infos = worker->getDetachedInfos();
        for (const auto& info : infos) {
            all_clients.push_back({info.fd, info.listen_port, info.authenticated, info.stream_id});
        }
    }

    LOG_INFO("Transferring %zu client fds...", all_clients.size());

    // Now send all client fds
    for (const auto& ci : all_clients) {
        msg_type = MSG_CLIENT_FD;
        ::write(uds_fd_, &msg_type, 1);

        ConnectionMetadata meta{};
        meta.fd = ci.fd;
        meta.listen_port = ci.listen_port;
        meta.authenticated = ci.authenticated;
        meta.stream_id = ci.stream_id;
        meta.backend_port = 0;
        meta.recv_buf_len = 0;
        meta.send_buf_len = 0;
        memset(meta.backend_addr, 0, sizeof(meta.backend_addr));

        if (!sendFd(uds_fd_, ci.fd, &meta, sizeof(meta))) {
            LOG_ERROR("Failed to send client fd=%d for port=%d", ci.fd, ci.listen_port);
            ::close(ci.fd);
        } else {
            total_client_fds++;
            // Close our copy of the fd - new process has it now
            ::close(ci.fd);
        }
    }

    LOG_INFO("Transferred %d client fds", total_client_fds);

    // Step 8: Signal transfer done
    state_ = UpgradeState::WAITING_ACK;
    msg_type = MSG_TRANSFER_DONE;
    ::write(uds_fd_, &msg_type, 1);
    LOG_INFO("All fds transferred, waiting for ACK from new process...");

    // Step 9: Wait for ACK with timeout
    tv.tv_sec = timeout;
    tv.tv_usec = 0;
    setsockopt(uds_fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (::read(uds_fd_, &msg_type, 1) == 1 && msg_type == MSG_ACK) {
        LOG_INFO("Hot upgrade ACK received, shutting down old process");
        state_ = UpgradeState::COMPLETED;
        ::close(uds_fd_);
        uds_fd_ = -1;
        unlink(socket_path_.c_str());
        server_->shutdown();
        return true;
    }

    // Timeout or error - rollback
    LOG_ERROR("Hot upgrade failed: no ACK received, rolling back");
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

    // Retry connection a few times (old process may not be ready yet)
    int retries = 5;
    while (retries > 0) {
        if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
            break;
        }
        retries--;
        if (retries > 0) {
            LOG_INFO("Retrying connection to old process (%d retries left)...", retries);
            usleep(200000); // 200ms
        }
    }

    if (retries == 0) {
        LOG_ERROR("Failed to connect to old process UDS: %s", strerror(errno));
        ::close(fd);
        return false;
    }

    uds_fd_ = fd;

    // Send handshake
    uint8_t msg_type = MSG_HANDSHAKE;
    ::write(uds_fd_, &msg_type, 1);

    // Wait for ACK
    if (::read(uds_fd_, &msg_type, 1) != 1 || msg_type != MSG_HANDSHAKE_ACK) {
        LOG_ERROR("Handshake with old process failed");
        ::close(uds_fd_);
        uds_fd_ = -1;
        return false;
    }

    LOG_INFO("Handshake with old process successful");

    // Receive fds and metadata
    int received_listen_fds = 0;
    int received_client_fds = 0;

    while (true) {
        if (::read(uds_fd_, &msg_type, 1) != 1) {
            LOG_ERROR("Failed to read message type from old process");
            break;
        }

        if (msg_type == MSG_TRANSFER_DONE) {
            LOG_INFO("Transfer from old process complete: %d listen fds, %d client fds",
                     received_listen_fds, received_client_fds);
            break;
        }

        if (msg_type == MSG_LISTEN_FD) {
            // Receive listen fd and metadata
            ConnectionMetadata meta{};
            int received_fd = recvFd(uds_fd_, &meta, sizeof(meta));
            if (received_fd >= 0) {
                received_listen_fds++;
                LOG_INFO("Received listen fd=%d for port=%d (original fd=%d)",
                         received_fd, meta.listen_port, meta.fd);
                // New process already has its own listen sockets from init()
                ::close(received_fd);
            } else {
                LOG_ERROR("Failed to receive listen fd");
            }
        } else if (msg_type == MSG_CLIENT_FD) {
            // Receive client fd and metadata
            ConnectionMetadata meta{};
            int received_fd = recvFd(uds_fd_, &meta, sizeof(meta));
            if (received_fd >= 0) {
                received_client_fds++;
                // Dispatch client fd to a worker via round-robin
                const auto& workers = server_->getWorkers();
                if (!workers.empty()) {
                    int worker_idx = received_client_fds % workers.size();
                    workers[worker_idx]->dispatchClient(received_fd, meta.listen_port);
                    LOG_DEBUG("Restored client fd=%d to worker %d (port=%d)",
                             received_fd, worker_idx, meta.listen_port);
                } else {
                    LOG_ERROR("No workers available to restore client fd=%d", received_fd);
                    ::close(received_fd);
                }
            } else {
                LOG_ERROR("Failed to receive client fd");
            }
        } else if (msg_type == MSG_ROLLBACK) {
            LOG_WARN("Old process requested rollback");
            ::close(uds_fd_);
            uds_fd_ = -1;
            return false;
        } else {
            LOG_WARN("Unknown message type: %d", msg_type);
        }
    }

    // Send ACK
    msg_type = MSG_ACK;
    ::write(uds_fd_, &msg_type, 1);

    ::close(uds_fd_);
    uds_fd_ = -1;

    LOG_INFO("Hot upgrade receive complete, new process ready");
    return true;
}

std::string HotUpgradeManager::getStateString() const {
    switch (state_) {
        case UpgradeState::IDLE:         return "idle";
        case UpgradeState::TRANSFERRING: return "transferring";
        case UpgradeState::WAITING_ACK:  return "waiting_ack";
        case UpgradeState::COMPLETED:    return "completed";
        case UpgradeState::FAILED:       return "failed";
    }
    return "unknown";
}

bool HotUpgradeManager::sendFd(int uds_fd, int fd_to_send,
                                const void* metadata, size_t meta_len) {
    struct msghdr msg{};
    struct iovec iov;
    char cmsgbuf[CMSG_SPACE(sizeof(int))];

    // Send metadata as the iov data
    iov.iov_base = const_cast<void*>(metadata);
    iov.iov_len = meta_len;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    // Attach fd via SCM_RIGHTS
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

} // namespace proxy
