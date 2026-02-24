// EventLoop implementation - to be completed in task 3
#include "event_loop.h"
#include "logger.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <algorithm>

namespace proxy {

EventLoop::EventLoop() {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("epoll_create1 failed: %s", strerror(errno));
        return;
    }

    wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0) {
        LOG_ERROR("eventfd failed: %s", strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return;
    }

    // Register wakeup fd in epoll
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = wakeup_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev) < 0) {
        LOG_ERROR("epoll_ctl add wakeup_fd failed: %s", strerror(errno));
    }
}

EventLoop::~EventLoop() {
    if (wakeup_fd_ >= 0) ::close(wakeup_fd_);
    if (epoll_fd_ >= 0) ::close(epoll_fd_);
}

bool EventLoop::addEvent(int fd, uint32_t events, EventCallback cb) {
    struct epoll_event ev{};
    ev.events = 0;
    if (events & EVENT_READABLE) ev.events |= EPOLLIN;
    if (events & EVENT_WRITABLE) ev.events |= EPOLLOUT;
    ev.events |= EPOLLERR | EPOLLHUP;
    ev.data.fd = fd;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        LOG_ERROR("epoll_ctl ADD fd=%d failed: %s", fd, strerror(errno));
        return false;
    }

    events_[fd] = EventEntry{fd, events, std::move(cb)};
    return true;
}

bool EventLoop::modifyEvent(int fd, uint32_t events) {
    auto it = events_.find(fd);
    if (it == events_.end()) return false;

    struct epoll_event ev{};
    ev.events = 0;
    if (events & EVENT_READABLE) ev.events |= EPOLLIN;
    if (events & EVENT_WRITABLE) ev.events |= EPOLLOUT;
    ev.events |= EPOLLERR | EPOLLHUP;
    ev.data.fd = fd;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        LOG_ERROR("epoll_ctl MOD fd=%d failed: %s", fd, strerror(errno));
        return false;
    }

    it->second.events = events;
    return true;
}

bool EventLoop::removeEvent(int fd) {
    auto it = events_.find(fd);
    if (it == events_.end()) return false;

    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    events_.erase(it);
    return true;
}

bool EventLoop::setCallback(int fd, EventCallback cb) {
    auto it = events_.find(fd);
    if (it == events_.end()) return false;
    it->second.callback = std::move(cb);
    return true;
}

TimerId EventLoop::addTimer(uint64_t interval_ms, TimerCallback cb, bool repeat) {
    TimerId id = next_timer_id_++;
    uint64_t now = currentTimeMs();
    timers_.push_back(TimerEntry{id, interval_ms, now + interval_ms, std::move(cb), repeat, false});
    return id;
}

void EventLoop::cancelTimer(TimerId id) {
    for (auto& t : timers_) {
        if (t.id == id) {
            t.cancelled = true;
            return;
        }
    }
}

uint64_t EventLoop::currentTimeMs() const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
}

int EventLoop::processTimers() {
    if (timers_.empty()) return 1000; // Default 1 second timeout

    uint64_t now = currentTimeMs();
    int min_timeout = 1000;

    // Process expired timers
    for (size_t i = 0; i < timers_.size(); ) {
        auto& t = timers_[i];
        if (t.cancelled) {
            timers_.erase(timers_.begin() + i);
            continue;
        }
        if (now >= t.next_fire_ms) {
            TimerCallback cb = t.callback; // Copy before potential modification
            if (t.repeat) {
                t.next_fire_ms = now + t.interval_ms;
                i++;
            } else {
                timers_.erase(timers_.begin() + i);
            }
            cb(); // Fire callback
            continue; // Re-check index after erase
        }
        int diff = (int)(t.next_fire_ms - now);
        if (diff < min_timeout) min_timeout = diff;
        i++;
    }

    return min_timeout > 0 ? min_timeout : 0;
}

void EventLoop::run() {
    running_ = true;
    struct epoll_event events[MAX_EVENTS];

    while (running_) {
        int timeout = processTimers();
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout);

        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("epoll_wait failed: %s", strerror(errno));
            break;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            if (fd == wakeup_fd_) {
                // Drain the eventfd
                uint64_t val;
                (void)::read(wakeup_fd_, &val, sizeof(val));
                continue;
            }

            auto it = events_.find(fd);
            if (it == events_.end()) continue;

            uint32_t ev = 0;
            if (events[i].events & (EPOLLIN | EPOLLHUP)) ev |= EVENT_READABLE;
            if (events[i].events & EPOLLOUT) ev |= EVENT_WRITABLE;
            if (events[i].events & EPOLLERR) ev |= EVENT_ERROR;

            it->second.callback(fd, ev);
        }
    }
}

void EventLoop::stop() {
    running_ = false;
    wakeup();
}

void EventLoop::wakeup() {
    uint64_t val = 1;
    (void)::write(wakeup_fd_, &val, sizeof(val));
}

} // namespace proxy
