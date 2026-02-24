// HotUpgradeManager implementation
#include "hot_upgrade.h"
#include "proxy_server.h"
#include "logger.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <chrono>
#include <thread>

namespace proxy {

// Message types for UDS protocol
constexpr uint8_t MSG_HANDSHAKE     = 0x01;
constexpr uint8_t MSG_HANDSHAKE_ACK = 0x02;
constexpr uint8_t MSG_LISTEN_FD     = 0x03;
constexpr uint8_t MSG_CLIENT_FD     = 0x04;
constexpr uint8_t MSG_TRANSFER_DONE = 0x05;
constexpr uint8_t MSG_ACK           = 0x06;
constexpr uint8_t MSG_ROLLBACK      = 0x07;

static constexpr int MAX_FDS_PER_MSG = 1;

HotUpgradeManager::HotUpgradeManager(ProxyServer* server) : server_(server) {
    socket_path_ = server_->getConfig().getHotUpgradeConfig().socket_path;
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
        // Use /proc/self/exe
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

    // Step 3: Fork and exec new process
    pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("fork() failed: %s", strerror(errno));
        ::close(server_fd);
        unlink(socket_path_.c_str());
        state_ = UpgradeState::FAILED;
        return false;
    }

    if (pid == 0) {
        // Child process: exec new binary with --upgrade-from flag
        std::string upgrade_arg = "--upgrade-from=" + socket_path_;
        execl(binary.c_str(), binary.c_str(), upgrade_arg.c_str(), nullptr);
        // If exec fails
        _exit(1);
    }

    LOG_INFO("Forked new process pid=%d", pid);

    // Step 4: Wait for new process to connect
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

    // Step 5: Wait for handshake from new process
    uint8_t msg_type = 0;
    if (::read(uds_fd_, &msg_type, 1) != 1 || msg_type != MSG_HANDSHAKE) {
        LOG_ERROR("Invalid handshake from new process");
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

    // Step 6: Transfer all listen fds with metadata
    // Note: In a full implementation, we would iterate all listen_fds_ and
    // client connections, sending each one with its metadata.
    // For now, send TRANSFER_DONE to signal completion.

    state_ = UpgradeState::WAITING_ACK;
    msg_type = MSG_TRANSFER_DONE;
    ::write(uds_fd_, &msg_type, 1);

    // Step 7: Wait for ACK with timeout
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

    if (::connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
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
    while (true) {
        if (::read(uds_fd_, &msg_type, 1) != 1) {
            LOG_ERROR("Failed to read message type from old process");
            break;
        }

        if (msg_type == MSG_TRANSFER_DONE) {
            LOG_INFO("Transfer from old process complete");
            break;
        }

        if (msg_type == MSG_LISTEN_FD) {
            // Receive listen fd and metadata
            // Implementation: recvFd + restore listen socket
            LOG_INFO("Received listen fd from old process");
        } else if (msg_type == MSG_CLIENT_FD) {
            // Receive client fd and metadata
            // Implementation: recvFd + restore client connection
            LOG_INFO("Received client fd from old process");
        } else if (msg_type == MSG_ROLLBACK) {
            LOG_WARN("Old process requested rollback");
            ::close(uds_fd_);
            uds_fd_ = -1;
            return false;
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
