#pragma once

#include <cstdint>
#include <functional>
#include <vector>
#include <unordered_map>

struct epoll_event;

namespace proxy {

using EventCallback = std::function<void(int fd, uint32_t events)>;
using TimerCallback = std::function<void()>;

// Event flags (matching EPOLLIN/EPOLLOUT/EPOLLERR bit positions)
constexpr uint32_t EVENT_READABLE  = 0x01;
constexpr uint32_t EVENT_WRITABLE  = 0x04;
constexpr uint32_t EVENT_ERROR     = 0x08;

using TimerId = uint64_t;

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // Event management
    bool addEvent(int fd, uint32_t events, EventCallback cb);
    bool modifyEvent(int fd, uint32_t events);
    bool removeEvent(int fd);

    // Modify event callback
    bool setCallback(int fd, EventCallback cb);

    // Timer management
    TimerId addTimer(uint64_t interval_ms, TimerCallback cb, bool repeat = false);
    void cancelTimer(TimerId id);

    // Run the event loop (blocking)
    void run();

    // Stop the event loop
    void stop();

    // Wake up the event loop from another thread
    void wakeup();

    bool isRunning() const { return running_; }

private:
    static constexpr int MAX_EVENTS = 1024;

    int epoll_fd_ = -1;
    int wakeup_fd_ = -1;
    bool running_ = false;

    struct EventEntry {
        int fd;
        uint32_t events;
        EventCallback callback;
    };

    std::unordered_map<int, EventEntry> events_;

    struct TimerEntry {
        TimerId id;
        uint64_t interval_ms;
        uint64_t next_fire_ms;
        TimerCallback callback;
        bool repeat;
        bool cancelled;
    };

    std::vector<TimerEntry> timers_;
    TimerId next_timer_id_ = 1;

    uint64_t currentTimeMs() const;
    int processTimers();
};

} // namespace proxy
