#pragma once

#include <string>
#include <mutex>
#include <fstream>
#include <cstdint>
#include <atomic>

namespace proxy {

enum class LogLevel : int {
    LOG_LVL_DEBUG = 0,
    LOG_LVL_INFO = 1,
    LOG_LVL_WARN = 2,
    LOG_LVL_ERROR = 3
};

class Logger {
public:
    static Logger& instance();

    // Initialize logger with config
    bool init(const std::string& filepath, const std::string& level,
              uint32_t max_size_mb = 100, uint32_t max_files = 5);

    // Log methods
    void debug(const char* fmt, ...);
    void info(const char* fmt, ...);
    void warn(const char* fmt, ...);
    void error(const char* fmt, ...);

    // Dynamic level adjustment
    void setLevel(LogLevel level);
    void setLevel(const std::string& level);
    LogLevel getLevel() const { return level_.load(); }
    std::string getLevelString() const;

    static LogLevel parseLevelString(const std::string& level);
    static std::string levelToString(LogLevel level);

private:
    Logger() = default;
    ~Logger();
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void log(LogLevel level, const char* fmt, va_list args);
    void rotateIfNeeded();
    void rotate();
    std::string currentTimestamp();

    std::atomic<LogLevel> level_{LogLevel::LOG_LVL_INFO};
    std::string filepath_;
    std::ofstream file_;
    std::mutex mutex_;
    uint32_t max_size_mb_ = 100;
    uint32_t max_files_ = 5;
    bool use_stdout_ = true;
};

// Convenience macros
#define LOG_DEBUG(...) proxy::Logger::instance().debug(__VA_ARGS__)
#define LOG_INFO(...)  proxy::Logger::instance().info(__VA_ARGS__)
#define LOG_WARN(...)  proxy::Logger::instance().warn(__VA_ARGS__)
#define LOG_ERROR(...) proxy::Logger::instance().error(__VA_ARGS__)
#define LOG_LEVEL_DEBUG proxy::LogLevel::LOG_LVL_DEBUG
#define LOG_LEVEL_INFO  proxy::LogLevel::LOG_LVL_INFO
#define LOG_LEVEL_WARN  proxy::LogLevel::LOG_LVL_WARN
#define LOG_LEVEL_ERROR proxy::LogLevel::LOG_LVL_ERROR

} // namespace proxy
