#include "logger.h"
#include <iostream>
#include <cstdarg>
#include <ctime>
#include <sys/stat.h>
#include <algorithm>
#include <filesystem>

namespace proxy {

Logger& Logger::instance() {
    static Logger inst;
    return inst;
}

Logger::~Logger() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool Logger::init(const std::string& filepath, const std::string& level,
                  uint32_t max_size_mb, uint32_t max_files) {
    std::lock_guard<std::mutex> lock(mutex_);

    level_.store(parseLevelString(level));
    max_size_mb_ = max_size_mb;
    max_files_ = max_files;

    if (filepath.empty()) {
        use_stdout_ = true;
        return true;
    }

    filepath_ = filepath;
    use_stdout_ = false;

    file_.open(filepath_, std::ios::app);
    if (!file_.is_open()) {
        std::cerr << "Failed to open log file: " << filepath_ << std::endl;
        use_stdout_ = true;
        return false;
    }

    return true;
}

void Logger::setLevel(LogLevel level) {
    level_.store(level);
}

void Logger::setLevel(const std::string& level) {
    level_.store(parseLevelString(level));
}

std::string Logger::getLevelString() const {
    return levelToString(level_.load());
}

LogLevel Logger::parseLevelString(const std::string& level) {
    std::string upper = level;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    if (upper == "DEBUG") return LogLevel::LOG_LVL_DEBUG;
    if (upper == "INFO") return LogLevel::LOG_LVL_INFO;
    if (upper == "WARN" || upper == "WARNING") return LogLevel::LOG_LVL_WARN;
    if (upper == "ERROR") return LogLevel::LOG_LVL_ERROR;
    return LogLevel::LOG_LVL_INFO;
}

std::string Logger::levelToString(LogLevel level) {
    switch (level) {
        case LogLevel::LOG_LVL_DEBUG: return "DEBUG";
        case LogLevel::LOG_LVL_INFO:  return "INFO";
        case LogLevel::LOG_LVL_WARN:  return "WARN";
        case LogLevel::LOG_LVL_ERROR: return "ERROR";
    }
    return "INFO";
}

std::string Logger::currentTimestamp() {
    time_t now = time(nullptr);
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    return std::string(buf);
}

void Logger::log(LogLevel level, const char* fmt, va_list args) {
    if (level < level_.load()) return;

    char message[4096];
    vsnprintf(message, sizeof(message), fmt, args);

    std::string timestamp = currentTimestamp();
    std::string level_str = levelToString(level);

    std::lock_guard<std::mutex> lock(mutex_);

    if (use_stdout_) {
        FILE* out = (level >= LogLevel::LOG_LVL_WARN) ? stderr : stdout;
        fprintf(out, "[%s] [%s] %s\n", timestamp.c_str(), level_str.c_str(), message);
        fflush(out);
    } else {
        rotateIfNeeded();
        file_ << "[" << timestamp << "] [" << level_str << "] " << message << "\n";
        file_.flush();
    }
}

void Logger::rotateIfNeeded() {
    if (use_stdout_ || filepath_.empty()) return;

    // Check file size
    file_.seekp(0, std::ios::end);
    auto size = file_.tellp();
    if (size >= 0 && (uint64_t)size >= (uint64_t)max_size_mb_ * 1024 * 1024) {
        rotate();
    }
}

void Logger::rotate() {
    file_.close();

    // Rename existing log files
    for (int i = (int)max_files_ - 1; i >= 1; i--) {
        std::string old_name = filepath_ + "." + std::to_string(i);
        std::string new_name = filepath_ + "." + std::to_string(i + 1);
        std::rename(old_name.c_str(), new_name.c_str());
    }

    // Rename current log file
    std::string backup = filepath_ + ".1";
    std::rename(filepath_.c_str(), backup.c_str());

    // Remove excess log files
    std::string excess = filepath_ + "." + std::to_string(max_files_ + 1);
    std::remove(excess.c_str());

    // Open new log file
    file_.open(filepath_, std::ios::app);
}

void Logger::debug(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::LOG_LVL_DEBUG, fmt, args);
    va_end(args);
}

void Logger::info(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::LOG_LVL_INFO, fmt, args);
    va_end(args);
}

void Logger::warn(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::LOG_LVL_WARN, fmt, args);
    va_end(args);
}

void Logger::error(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::LOG_LVL_ERROR, fmt, args);
    va_end(args);
}

} // namespace proxy
