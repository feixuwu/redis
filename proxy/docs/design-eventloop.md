# Multi-Thread Architecture & Event Loop Design

## 1. Overview

The Redis MUX Proxy uses a **main thread + N worker threads** architecture:

- **Main thread**: accepts new connections, dispatches them to workers, handles signals, runs the admin server
- **Worker threads**: each runs an independent event loop, handles full lifecycle of assigned client connections (read, forward, write back)

```
┌─────────────────────────────────────────────────────────────┐
│                       Main Thread                            │
│                                                              │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   │
│   │ Listen :6479  │   │ Listen :6480  │   │ Admin :9090  │   │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   │
│          │ accept()          │ accept()          │           │
│          └─────────┬─────────┘                   │           │
│                    │                              │           │
│          round-robin dispatch               RESP commands    │
│                    │                                         │
│   ┌────────────────┴────────────────────────┐               │
│   │          eventfd notification           │               │
│   ▼                ▼                ▼       │               │
├───────────┬────────────────┬────────────────┤               │
│ Worker 0  │   Worker 1     │   Worker N-1   │               │
│ EventLoop │   EventLoop    │   EventLoop    │               │
│           │                │                │               │
│ clients[] │   clients[]    │   clients[]    │               │
│ mux_conns │   mux_conns    │   mux_conns    │               │
└───────────┴────────────────┴────────────────┘               │
```

## 2. EventLoop Class Design

### Core Interface

```cpp
class EventLoop {
public:
    // Create epoll instance and wakeup eventfd
    EventLoop();
    ~EventLoop();

    // Event management - register fd with callback
    bool addEvent(int fd, uint32_t events, EventCallback cb);
    bool modifyEvent(int fd, uint32_t events);
    bool removeEvent(int fd);

    // Timer management
    TimerId addTimer(uint64_t interval_ms, TimerCallback cb, bool repeat);
    void cancelTimer(TimerId id);

    // Main loop: epoll_wait + dispatch callbacks + process timers
    void run();
    void stop();

    // Cross-thread wakeup via eventfd write
    void wakeup();
};
```

### Implementation Details

- **epoll**: uses `epoll_create1(EPOLL_CLOEXEC)` for event multiplexing
- **Wakeup mechanism**: an `eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)` registered in epoll for cross-thread notification
- **Timer**: maintained as a sorted vector of `TimerEntry` structs; `epoll_wait` timeout is set to the nearest timer deadline
- **Event dispatch**: `struct EventEntry { int fd; uint32_t events; EventCallback callback; }` stored in `unordered_map<int, EventEntry>`

### Event Loop Cycle

```
while (running_) {
    int timeout = processTimers();          // fire expired timers, compute next timeout
    int n = epoll_wait(epfd, events, MAX, timeout);
    for (int i = 0; i < n; i++) {
        if (events[i].data.fd == wakeup_fd_) {
            // drain eventfd, then process pending tasks
            handleWakeup();
        } else {
            auto it = events_.find(events[i].data.fd);
            if (it != events_.end()) {
                it->second.callback(events[i].data.fd, events[i].events);
            }
        }
    }
}
```

## 3. WorkerThread Class Design

### Core Interface

```cpp
class WorkerThread {
public:
    explicit WorkerThread(int id);

    void start();   // spawn std::thread running threadMain()
    void stop();    // set running_ = false, wakeup event loop, join thread

    // Called by main thread to dispatch a new client fd
    void dispatchClient(int client_fd, uint16_t listen_port);

    EventLoop& getEventLoop();
    size_t getConnectionCount() const;

private:
    int id_;
    EventLoop event_loop_;
    std::thread thread_;
    std::atomic<bool> running_;

    // Cross-thread notification
    int notify_fd_;    // eventfd for new client notification

    // Protected by mutex, accessed by main thread (producer) and worker (consumer)
    std::mutex pending_mutex_;
    std::vector<PendingClient> pending_clients_;

    void threadMain();
    void handleNewClients();
};
```

### Thread Lifecycle

1. `start()`: creates eventfd, registers it in worker's event loop, spawns `std::thread(&WorkerThread::threadMain, this)`
2. `threadMain()`: calls `event_loop_.run()` (blocking loop)
3. `stop()`: sets `running_ = false`, calls `event_loop_.stop()`, then `thread_.join()`

### Connection Dispatch (Main → Worker)

```
Main thread:                          Worker thread:
                                      
1. accept(listen_fd) → client_fd      
2. select worker (round-robin)        
3. lock pending_mutex_                
4. push {client_fd, listen_port}      
5. unlock pending_mutex_              
6. write(notify_fd_, 1)  ──────────→  eventfd readable
                                      7. read(notify_fd_)
                                      8. lock pending_mutex_
                                      9. swap pending_clients_ → local
                                      10. unlock pending_mutex_
                                      11. for each {fd, port}:
                                          - create ClientConnection
                                          - register in event_loop_
                                          - lookup backend info
                                          - create/bind MUX stream
```

## 4. Main Thread Responsibilities

### Accept Loop

The main thread creates listening sockets for each backend mapping, registers them in its own event loop:

```cpp
for (auto& backend : config.getBackends()) {
    int listen_fd = createListenSocket(backend.listen_port);
    main_loop_.addEvent(listen_fd, EVENT_READABLE, [this, listen_fd](int fd, uint32_t) {
        onNewConnection(fd);
    });
}
```

`onNewConnection()`:
```cpp
void ProxyServer::onNewConnection(int listen_fd) {
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    int client_fd = accept4(listen_fd, (struct sockaddr*)&addr, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (client_fd < 0) return;

    uint16_t listen_port = listen_fds_[listen_fd];
    WorkerThread& worker = selectWorker();
    worker.dispatchClient(client_fd, listen_port);
}
```

### Worker Selection

Round-robin strategy:
```cpp
WorkerThread& ProxyServer::selectWorker() {
    uint32_t idx = next_worker_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    return *workers_[idx];
}
```

### Signal Handling

- **SIGTERM / SIGINT**: set a global `shutting_down` flag, call `main_loop_.stop()`, then gracefully shutdown
- **SIGUSR2**: trigger hot upgrade flow
- **SIGPIPE**: ignored (SIG_IGN)

Signals are blocked in worker threads via `pthread_sigmask()` before spawning them; only the main thread handles signals.

## 5. Dynamic Backend Add/Remove

### Adding a Backend at Runtime

Triggered by admin command `PROXY BACKEND ADD <listen_port> <addr:port> [password]`:

```
1. Main thread: validate parameters
2. Create listen socket → bind → listen
3. Register listen_fd in main_loop_
4. Add to BackendManager
5. Notify all workers of new backend info (for future connections)
   (Workers only need the mapping info, not the listen socket)
```

No cross-thread communication is needed because:
- The main thread owns all listen sockets
- Workers look up backend info from shared BackendManager (read-only after init, or protected by mutex)

### Removing a Backend at Runtime

Triggered by admin command `PROXY BACKEND REMOVE <listen_port>`:

```
1. Main thread: remove listen_fd from event loop
2. Close listen_fd (stops new connections)
3. Notify workers to close all connections on this listen_port:
   - For each worker: post a "close_backend" task via dispatchTask()
   - Worker processes task: iterates its connections, closes those matching listen_port
4. Remove from BackendManager
```

### Cross-Thread Task Dispatch

Extend the dispatch mechanism to support generic tasks (not just new connections):

```cpp
struct PendingTask {
    enum Type { NEW_CLIENT, CLOSE_BACKEND, CONFIG_UPDATE };
    Type type;
    int fd;               // For NEW_CLIENT
    uint16_t listen_port; // For NEW_CLIENT and CLOSE_BACKEND
};
```

Both `dispatchClient()` and `dispatchTask()` use the same `notify_fd_` + `pending_mutex_` mechanism.

## 6. Data Flow Summary

### Request Path (Client → Redis)

```
Client TCP → Worker EventLoop (EPOLLIN on client_fd)
  → ClientConnection::onReadable()
    → read() into recv_buf
    → if not authenticated: parse AUTH, handle locally
    → if authenticated:
      → MuxBackendConnection::sendData(stream_id, data, len)
        → encodeFrame(DATA, stream_id, payload)
        → append to send_buf
        → register EPOLLOUT on backend_fd
```

### Response Path (Redis → Client)

```
Redis TCP → Worker EventLoop (EPOLLIN on backend_fd)
  → MuxBackendConnection::onReadable()
    → read() into recv_buf
    → processRecvBuffer() → decodeFrame()
      → for each DATA frame:
        → lookup stream_id → ClientConnection*
        → ClientConnection::appendToSendBuffer(payload)
        → register EPOLLOUT on client_fd
```

## 7. Thread Safety Rules

1. **No shared mutable state between workers**: each worker owns its connections and MUX connections exclusively
2. **Main → Worker communication**: only via eventfd + mutex-protected queue
3. **BackendManager**: protected by mutex for add/remove operations (rare), lock-free for read-only lookups
4. **Statistics counters**: per-worker atomic counters, aggregated on demand by admin commands (read from main thread, no locks needed for atomics)
5. **Config**: immutable after loading; dynamic changes go through BackendManager
