#include "config.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <getopt.h>

namespace proxy {

std::string Config::trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

std::string Config::unquote(const std::string& s) {
    if (s.size() >= 2 && ((s.front() == '"' && s.back() == '"') ||
                          (s.front() == '\'' && s.back() == '\''))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

bool Config::loadFromFile(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Failed to open config file: " << filepath << std::endl;
        return false;
    }

    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
    file.close();

    return parseYaml(content);
}

// Simple YAML-like parser (supports flat keys, nested keys with indent, and list items)
bool Config::parseYaml(const std::string& content) {
    std::istringstream stream(content);
    std::string line;
    std::string section;
    std::string subsection;
    bool in_backend_item = false;
    BackendConfig current_backend{};

    auto finishBackendItem = [&]() {
        if (in_backend_item && current_backend.listen_port != 0) {
            backends_.push_back(current_backend);
        }
        current_backend = BackendConfig{};
        in_backend_item = false;
    };

    while (std::getline(stream, line)) {
        // Remove comments
        size_t comment_pos = line.find('#');
        if (comment_pos != std::string::npos) {
            // Make sure it's not inside quotes
            bool in_quotes = false;
            for (size_t i = 0; i < comment_pos; i++) {
                if (line[i] == '"' || line[i] == '\'') in_quotes = !in_quotes;
            }
            if (!in_quotes) line = line.substr(0, comment_pos);
        }

        std::string trimmed = trim(line);
        if (trimmed.empty()) continue;

        // Determine indentation level
        size_t indent = line.find_first_not_of(" \t");

        // List item in backends section
        if (trimmed.size() >= 2 && trimmed[0] == '-' && trimmed[1] == ' ' && section == "backends") {
            finishBackendItem();
            in_backend_item = true;
            // Parse inline key: "- listen_port: 6479"
            std::string item = trim(trimmed.substr(2));
            size_t colon = item.find(':');
            if (colon != std::string::npos) {
                std::string key = trim(item.substr(0, colon));
                std::string val = trim(unquote(trim(item.substr(colon + 1))));
                if (key == "listen_port") current_backend.listen_port = (uint16_t)std::stoi(val);
                else if (key == "backend_addr") current_backend.backend_addr = val;
                else if (key == "backend_port") current_backend.backend_port = (uint16_t)std::stoi(val);
                else if (key == "backend_password") current_backend.backend_password = val;
            }
            continue;
        }

        // Key-value pair
        size_t colon = trimmed.find(':');
        if (colon == std::string::npos) continue;

        std::string key = trim(trimmed.substr(0, colon));
        std::string val = trim(trimmed.substr(colon + 1));
        val = unquote(val);

        // Top-level or section header
        if (indent == 0) {
            if (val.empty()) {
                // Section header
                finishBackendItem();
                section = key;
                subsection.clear();
            } else {
                // Top-level key-value
                finishBackendItem();
                section.clear();
                subsection.clear();

                if (key == "workers") workers_ = std::stoi(val);
                else if (key == "proxy_password") proxy_password_ = val;
                else if (key == "admin_port") admin_port_ = (uint16_t)std::stoi(val);
                else if (key == "shutdown_timeout") shutdown_timeout_ = (uint32_t)std::stoi(val);
            }
        } else if (indent >= 2 && in_backend_item) {
            // Backend list item continuation
            if (key == "listen_port") current_backend.listen_port = (uint16_t)std::stoi(val);
            else if (key == "backend_addr") current_backend.backend_addr = val;
            else if (key == "backend_port") current_backend.backend_port = (uint16_t)std::stoi(val);
            else if (key == "backend_password") current_backend.backend_password = val;
        } else if (indent >= 2) {
            // Nested key within a section
            if (section == "mux") {
                if (key == "enabled") mux_config_.enabled = (val == "true" || val == "1");
                else if (key == "max_streams_per_connection") mux_config_.max_streams_per_connection = (uint32_t)std::stoi(val);
            } else if (section == "log") {
                if (key == "level") log_config_.level = val;
                else if (key == "file") log_config_.file = val;
                else if (key == "max_size_mb") log_config_.max_size_mb = (uint32_t)std::stoi(val);
                else if (key == "max_files") log_config_.max_files = (uint32_t)std::stoi(val);
            } else if (section == "hot_upgrade") {
                if (key == "socket_path") hot_upgrade_config_.socket_path = val;
                else if (key == "timeout") hot_upgrade_config_.timeout = (uint32_t)std::stoi(val);
            }
        }
    }

    finishBackendItem();
    return true;
}

void Config::applyCommandLine(int argc, char* argv[]) {
    static struct option long_options[] = {
        {"config",      required_argument, nullptr, 'c'},
        {"workers",     required_argument, nullptr, 'w'},
        {"admin-port",  required_argument, nullptr, 'a'},
        {"log-level",   required_argument, nullptr, 'l'},
        {"log-file",    required_argument, nullptr, 'f'},
        {"password",    required_argument, nullptr, 'p'},
        {"help",        no_argument,       nullptr, 'h'},
        {nullptr,       0,                 nullptr,  0 }
    };

    // Reset getopt state
    optind = 1;

    int opt;
    while ((opt = getopt_long(argc, argv, "c:w:a:l:f:p:h", long_options, nullptr)) != -1) {
        switch (opt) {
            case 'c':
                // Config file path handled in main before calling this
                break;
            case 'w':
                workers_ = std::stoi(optarg);
                break;
            case 'a':
                admin_port_ = (uint16_t)std::stoi(optarg);
                break;
            case 'l':
                log_config_.level = optarg;
                break;
            case 'f':
                log_config_.file = optarg;
                break;
            case 'p':
                proxy_password_ = optarg;
                break;
            case 'h':
            default:
                break;
        }
    }
}

void Config::addBackend(const BackendConfig& backend) {
    // Check for duplicate listen port
    for (auto& b : backends_) {
        if (b.listen_port == backend.listen_port) {
            b = backend; // Update existing
            return;
        }
    }
    backends_.push_back(backend);
}

bool Config::removeBackend(uint16_t listen_port) {
    auto it = std::remove_if(backends_.begin(), backends_.end(),
        [listen_port](const BackendConfig& b) {
            return b.listen_port == listen_port;
        });
    if (it == backends_.end()) return false;
    backends_.erase(it, backends_.end());
    return true;
}

} // namespace proxy
