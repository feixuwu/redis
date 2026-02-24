// BackendManager implementation - to be completed in task 3
#include "backend_manager.h"
#include "logger.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

namespace proxy {

bool BackendManager::init(const std::vector<BackendConfig>& backends) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& cfg : backends) {
        Backend backend;
        backend.config = cfg;
        backend.active = true;
        backends_[cfg.listen_port] = backend;
        LOG_INFO("Backend registered: :%d -> %s:%d",
                 cfg.listen_port, cfg.backend_addr.c_str(), cfg.backend_port);
    }
    return true;
}

bool BackendManager::addBackend(const BackendConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (backends_.count(config.listen_port)) {
        LOG_WARN("Backend already exists for listen port %d", config.listen_port);
        return false;
    }

    Backend backend;
    backend.config = config;
    backend.active = true;
    backends_[config.listen_port] = backend;
    LOG_INFO("Backend added: :%d -> %s:%d",
             config.listen_port, config.backend_addr.c_str(), config.backend_port);
    return true;
}

bool BackendManager::removeBackend(uint16_t listen_port) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = backends_.find(listen_port);
    if (it == backends_.end()) {
        LOG_WARN("Backend not found for listen port %d", listen_port);
        return false;
    }

    // Remove fd mapping if exists
    if (it->second.listen_fd >= 0) {
        fd_to_port_.erase(it->second.listen_fd);
    }

    backends_.erase(it);
    LOG_INFO("Backend removed: listen port %d", listen_port);
    return true;
}

Backend* BackendManager::getBackendByListenPort(uint16_t listen_port) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = backends_.find(listen_port);
    if (it == backends_.end()) return nullptr;
    return &it->second;
}

Backend* BackendManager::getBackendByListenFd(int listen_fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto fd_it = fd_to_port_.find(listen_fd);
    if (fd_it == fd_to_port_.end()) return nullptr;
    auto it = backends_.find(fd_it->second);
    if (it == backends_.end()) return nullptr;
    return &it->second;
}

std::vector<Backend> BackendManager::getAllBackends() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Backend> result;
    result.reserve(backends_.size());
    for (const auto& [port, backend] : backends_) {
        result.push_back(backend);
    }
    return result;
}

bool BackendManager::getBackendAddr(uint16_t listen_port, std::string& addr,
                                     uint16_t& port, std::string& password) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = backends_.find(listen_port);
    if (it == backends_.end()) return false;
    addr = it->second.config.backend_addr;
    port = it->second.config.backend_port;
    password = it->second.config.backend_password;
    return true;
}

} // namespace proxy