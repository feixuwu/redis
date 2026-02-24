#pragma once

#include "config.h"
#include <unordered_map>
#include <mutex>
#include <cstdint>
#include <string>
#include <vector>

namespace proxy {

// Runtime backend state
struct Backend {
    BackendConfig config;
    int listen_fd = -1;
    bool active = true;
};

class BackendManager {
public:
    BackendManager() = default;
    ~BackendManager() = default;

    // Initialize from config
    bool init(const std::vector<BackendConfig>& backends);

    // Dynamic backend management
    bool addBackend(const BackendConfig& config);
    bool removeBackend(uint16_t listen_port);

    // Lookup
    Backend* getBackendByListenPort(uint16_t listen_port);
    Backend* getBackendByListenFd(int listen_fd);
    std::vector<Backend> getAllBackends() const;

    // Get backend address for a listen port
    bool getBackendAddr(uint16_t listen_port, std::string& addr, uint16_t& port, std::string& password);

private:
    mutable std::mutex mutex_;
    std::unordered_map<uint16_t, Backend> backends_;  // listen_port -> Backend
    std::unordered_map<int, uint16_t> fd_to_port_;    // listen_fd -> listen_port
};

} // namespace proxy
