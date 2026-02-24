#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace proxy {

// Backend Redis instance configuration
struct BackendConfig {
    uint16_t listen_port;
    std::string backend_addr;
    uint16_t backend_port;
    std::string backend_password;
};

// MUX settings
struct MuxConfig {
    bool enabled = true;
    uint32_t max_streams_per_connection = 1024;
};

// Log settings
struct LogConfig {
    std::string level = "INFO";
    std::string file;              // empty = stdout
    uint32_t max_size_mb = 100;
    uint32_t max_files = 5;
};

// Hot upgrade settings
struct HotUpgradeConfig {
    std::string socket_path = "/tmp/redis-mux-proxy.sock";
    uint32_t timeout = 30;
};

// Main configuration
class Config {
public:
    Config() = default;
    ~Config() = default;

    // Load configuration from YAML file
    bool loadFromFile(const std::string& filepath);

    // Override config values from command line arguments
    void applyCommandLine(int argc, char* argv[]);

    // Config file path
    void setConfigPath(const std::string& path) { config_path_ = path; }
    const std::string& getConfigPath() const { return config_path_; }

    // Accessors
    int getWorkers() const { return workers_; }
    const std::string& getProxyPassword() const { return proxy_password_; }
    uint16_t getAdminPort() const { return admin_port_; }
    const MuxConfig& getMuxConfig() const { return mux_config_; }
    const LogConfig& getLogConfig() const { return log_config_; }
    const HotUpgradeConfig& getHotUpgradeConfig() const { return hot_upgrade_config_; }
    const std::vector<BackendConfig>& getBackends() const { return backends_; }
    uint32_t getShutdownTimeout() const { return shutdown_timeout_; }

    // Mutators (for dynamic config changes via admin commands)
    void addBackend(const BackendConfig& backend);
    bool removeBackend(uint16_t listen_port);

private:
    int workers_ = 0;
    std::string config_path_;
    std::string proxy_password_;
    uint16_t admin_port_ = 9090;
    MuxConfig mux_config_;
    LogConfig log_config_;
    HotUpgradeConfig hot_upgrade_config_;
    std::vector<BackendConfig> backends_;
    uint32_t shutdown_timeout_ = 30;

    // YAML parsing helpers
    bool parseYaml(const std::string& content);
    std::string trim(const std::string& s);
    std::string unquote(const std::string& s);
};

} // namespace proxy
