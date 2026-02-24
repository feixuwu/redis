#pragma once

#include <string>
#include <functional>
#include <cstdint>

namespace proxy {

// Forward declarations
class ProxyServer;

// Hot upgrade states
enum class UpgradeState {
    IDLE,            // No upgrade in progress
    TRANSFERRING,    // Transferring fds and state
    WAITING_ACK,     // Waiting for new process acknowledgement
    COMPLETED,       // Upgrade completed successfully
    FAILED           // Upgrade failed
};

class HotUpgradeManager {
public:
    explicit HotUpgradeManager(ProxyServer* server);
    ~HotUpgradeManager();

    // Non-copyable
    HotUpgradeManager(const HotUpgradeManager&) = delete;
    HotUpgradeManager& operator=(const HotUpgradeManager&) = delete;

    // Initiate hot upgrade (old process side)
    bool startUpgrade(const std::string& new_binary_path = "");

    // Receive fds from old process (new process side)
    bool receiveFromOldProcess(const std::string& socket_path);

    // Get current upgrade state
    UpgradeState getState() const { return state_; }
    std::string getStateString() const;

private:
    ProxyServer* server_;
    UpgradeState state_ = UpgradeState::IDLE;
    std::string socket_path_;
    int uds_fd_ = -1;

    // Send fd via Unix Domain Socket using SCM_RIGHTS
    bool sendFd(int uds_fd, int fd_to_send, const void* metadata, size_t meta_len);
    int recvFd(int uds_fd, void* metadata, size_t meta_len);

    // Serialize/deserialize connection state
    struct ConnectionMetadata {
        int fd;
        uint16_t listen_port;
        bool authenticated;
        uint32_t stream_id;
        char backend_addr[64];
        uint16_t backend_port;
        uint32_t recv_buf_len;
        uint32_t send_buf_len;
    };
};

} // namespace proxy
