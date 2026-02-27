# Proxy 热升级设计方案 V2

## 1. 概述

热升级允许在不中断客户端连接的情况下替换正在运行的 proxy 二进制。
本方案的核心设计是：**先将后端 MUX 连接 fd 迁移到新进程，通过 IPC 代理桥接，再逐步搬迁客户端连接**。

### 1.1 设计目标

- **零连接中断**：客户端 TCP 连接全程不断开，客户端完全无感知
- **零数据丢失**：迁移过程中不丢失任何请求和回复数据
- **渐进式迁移**：逐连接搬迁，避免全局暂停导致的卡顿
- **后端连接复用**：后端 MUX 连接原封不动迁移，Redis 侧无感知
- **事件驱动**：所有阶段均基于 epoll 事件循环驱动，不阻塞主线程
- **大规模稳定**：5000+ 前端连接场景下，热升级稳定可靠，客户影响小

### 1.2 现有方案的问题

旧方案（Pause-Drain-Detach）存在以下问题：

| 问题 | 影响 |
|------|------|
| `usleep(100ms)` 盲等 in-flight | pipeline / 慢查询场景不可靠 |
| `recv_buf_` / `send_buf_` 未传递 | 数据丢失、协议错乱 |
| 认证状态未恢复 | 已认证客户端收到 `NOAUTH` |
| 全局暂停所有连接 | 所有客户端同时被卡住 |
| 后端 MUX 连接不传递 | 每个迁移的客户端都要重新建连 |

### 1.3 核心思路

1. **迁移后端 MUX fd 到新进程**：新进程掌握所有后端 MUX 连接的读写权
2. **IPC 代理桥接**：未迁移的客户端仍在旧进程，通过 IPC 通道转发数据
3. **逐步搬迁客户端**：用 Drain-Fence-Takeover 协议安全地迁移每个客户端
4. **全程事件驱动**：调度迁移用定时器回调，不用同步 while+usleep 阻塞

## 2. 整体架构

### 2.1 连接模型回顾

```
Worker Thread N (旧进程)
├── ClientConnection fd=10 (stream_id=1) ──┐
├── ClientConnection fd=11 (stream_id=3) ──┼──→ MuxBackendConn fd=100 (→ Redis:6379)
├── ClientConnection fd=12 (stream_id=5) ──┘
│
├── ClientConnection fd=13 (stream_id=1) ──┐
└── ClientConnection fd=14 (stream_id=3) ──┼──→ MuxBackendConn fd=101 (→ Redis:6380)
```

**关键约束**：多个 ClientConnection 共享同一个 MuxBackendConnection 的 TCP fd。
后端 MUX 连接不可拆分——同一 fd 上的所有 stream 必须由同一个进程读写。

### 2.2 三阶段架构

```
阶段一: 迁移后端 MUX fd    阶段二: IPC 代理期        阶段三: 逐个迁移 Client
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ backend_fd → 新进程   │  │ Client→旧→IPC→新→Redis│  │ Client fd → 新进程   │
│ 创建 IPC socketpair  │  │ Redis→新→IPC→旧→Client│  │ 从 IPC 代理切换本地  │
│ 建立 IPC 代理桥      │  │ 逐连接 Drain-Fence    │  │ 旧进程退出          │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### 2.3 事件驱动架构（关键设计）

**所有阶段均通过事件循环驱动，绝不阻塞主线程或 Worker 线程的 event loop。**

- **Phase 1（后端迁移）**：通过 Worker 的 `dispatchTransferBackends()` 在 Worker 线程中安全执行
- **Phase 2（逐步搬迁）**：通过旧进程主线程 / Worker 线程的 **定时器回调** 驱动，每次 timer 回调迁移一批 stream
- **新进程接收**：UDS fd 注册到新进程 event loop，异步接收消息，不阻塞 accept 新连接

```
旧进程 Worker 线程:
  ┌──────────────────────────────────────┐
  │ epoll_wait() 正常事件循环             │
  │  ├── client fd → onReadable/Writable │ ← 正常处理客户端 I/O
  │  ├── ipc fd → onReadable/Writable    │ ← IPC 代理 I/O（FENCE_ACK 等）
  │  └── timer → onMigrateTimer()        │ ← 定时迁移一批 stream
  └──────────────────────────────────────┘
```

## 3. IPC 代理桥设计

### 3.1 核心思想：复用 MUX 帧协议

**不发明新协议**。IPC 通道（socketpair）上传输的就是标准 MUX 帧格式：

```
+--------+--------+------------------+-----------+-----------+
| Magic  | Flags  |     Stream ID    | Payload   | Payload   |
| 1 byte | 1 byte |     8 bytes      | Len(4B)   | N bytes   |
+--------+--------+------------------+-----------+-----------+
```

IPC 通道对旧进程来说就像一个"虚拟的 MuxBackendConnection"；
IPC 通道对新进程来说就像一组"虚拟的 ClientConnection"。

### 3.2 两个 IPC 组件

#### IPCProxyBackend（旧进程侧）

- 实现与 `MuxBackendConnection` 相同的接口（`IBackendConnection` 抽象基类）
- `sendData(stream_id, data, len)`：编码为 MUX DATA 帧，写入 socketpair
- `onReadable()`：从 socketpair 读数据，解析 MUX 帧，路由回复到 ClientConnection
- 旧进程的 ClientConnection 通过 `setStreaming(ipc_proxy, same_stream_id)` 切换后端

```
ClientConnection::forwardToBackend()
  → backend_conn_->sendData(stream_id_, data, len)
  → IPCProxyBackend::sendData() // 透明替换，Client 代码无需修改
  → write(socketpair[0], MUX_DATA_FRAME)
```

#### IPCProxyFrontend（新进程侧）

- 从 socketpair 读取 MUX 帧，转发给真正的 MuxBackendConnection
- 从 MuxBackendConnection 的 handleFrame 回调中，判断 stream 归属：
  - `proxied_streams_` 中的 → 通过 IPC 回传给旧进程
  - 已迁移到本地的 → 直接调用 `client->appendToSendBuffer()`

```
新进程 MuxBackendConnection::handleFrame(DATA, stream_id, payload):
  if (ipc_frontend_->isProxied(stream_id)):
      ipc_frontend_->forwardToOldProcess(stream_id, payload, len)  // IPC 回传
  else:
      local_client->appendToSendBuffer(payload, len)               // 本地路由
```

### 3.3 IPC 通道对应关系

每个 MuxBackendConnection 对应一个独立的 socketpair + 一对 IPCProxyBackend/IPCProxyFrontend。

```
旧进程 Worker 0:
  MuxBC_1 (→ Redis:6379, streams {1,3,5})
    → socketpair_1 → IPCProxyBackend_1
  MuxBC_2 (→ Redis:6380, streams {1,3})
    → socketpair_2 → IPCProxyBackend_2

新进程:
  real_backend_1 (fd从旧进程迁移) + IPCProxyFrontend_1
  real_backend_2 (fd从旧进程迁移) + IPCProxyFrontend_2
```

## 4. IPC 帧类型

整个 IPC 通道上只需 **4 种帧类型**：

| Flags 值 | 帧类型 | 方向 | 说明 |
|----------|--------|------|------|
| `0x01` (DATA) | 数据帧 | 双向 | 转发请求（旧→新）或回复（新→旧） |
| `0x03` (STREAM_CLOSE) | 关闭帧 | 双向 | stream 错误/关闭通知 |
| `0x0A` (FENCE) | 排空栅栏 | 旧→新 | 标记某 stream 的请求到此为止 |
| `0x0B` (FENCE_ACK) | 排空确认 | 新→旧 | 确认某 stream 的回复全部回传完毕 |

新增帧类型使用高位（0x0A/0x0B），不和标准 MUX 帧冲突。

## 5. Drain-Fence-Takeover 三步迁移协议

这是保证 **状态 1（源端代理）→ 状态 2（目标端本地）不丢包** 的核心协议。

### 5.1 客户端连接的三态模型

```
状态1: 源端代理                状态2: 目标端本地
┌─────────────────────┐      ┌──────────────────────┐
│ 前端 fd 在旧进程     │      │ 前端 fd 在新进程      │
│ 后端 MUX fd 在新进程  │  →   │ 后端 MUX fd 在新进程  │
│ 请求: C→旧→IPC→新→R  │      │ 请求: C→新→R          │
│ 回复: R→新→IPC→旧→C  │      │ 回复: R→新→C          │
└─────────────────────┘      └──────────────────────┘
```

### 5.2 详细时序

```
旧进程                    IPC通道                   新进程
  │                         │                         │
  │ Step 1: Drain           │                         │
  │ 停止从 client fd 读取    │                         │
  │ 显式调用                │                         │
  │   client->flushRecvBuf()│                         │
  │ 转发完 recv_buf_ 残余   │                         │
  │ ─── FENCE(stream=3) ──>│───────────────────────> │
  │                         │                         │ 收到 FENCE
  │                         │                         │ 从此不会再有 stream=3 的新请求
  │                         │                         │ 等待 in_flight[3] → 0
  │                         │                         │ （RESP 命令级别精确计数）
  │                         │                         │
  │                         │ <──DATA(stream=3,resp)──│ 回传最后的回复
  │ 正常写给客户端           │                         │
  │                         │                         │
  │                         │                         │ in_flight[3] == 0
  │                         │<── FENCE_ACK(stream=3)──│
  │                         │                         │
  │ Step 3: Takeover        │                         │
  │ 收到 FENCE_ACK          │                         │
  │ flush send_buf_ → 客户端│                         │
  │ 提取 recv_buf_ 残余     │                         │
  │ 提取 send_buf_ 残余     │                         │
  │ detach client fd        │                         │
  │ ── sendmsg(fd + meta    │                         │
  │    + recv_buf + send_buf)│──────────────────────> │
  │ 从 streams_ 移除        │                         │ acceptMigratedClient(fd)
  │                         │                         │ 恢复认证状态
  │                         │                         │ 注入 recv_buf / send_buf
  │                         │                         │ rebindStream(stream_id)
  │                         │                         │ 从 proxied_ 移除
  │                         │                         │ 开始本地服务
  │                         │                         │
```

### 5.3 不丢包的保证

利用 **TCP (socketpair) 有序性** 天然保证：

| 保证 | 原因 |
|------|------|
| FENCE 之前的请求不丢 | 旧进程**显式 flush recv_buf**后才发 FENCE，TCP 有序 → 新进程先收到所有 DATA |
| FENCE_ACK 之前的回复不丢 | 新进程发完所有回复 DATA 帧才发 FENCE_ACK，TCP 有序 → 旧进程先收到所有回复 |
| 切换后无残留 | 收到 FENCE_ACK 时，管道中双向都没有 stream=N 的数据 |
| buffer 不丢 | completeMigration 显式提取 client 的 recv_buf_ 和 send_buf_ 并传给新进程 |

### 5.4 in-flight 跟踪（RESP 命令级别）

在新进程的 IPCProxyFrontend 中**按 RESP 命令粒度**精确跟踪：

```cpp
struct ProxiedStream {
    uint32_t in_flight = 0;       // 已转发给 Redis 但未收到回复的 RESP 命令数
    bool fence_received = false;   // 是否收到了 FENCE
};
```

**关键设计**：一个 IPC DATA 帧可能包含多个 pipeline 命令，一个回复 DATA 帧也可能包含多个或部分 RESP 响应。因此：

- **请求侧**：从 IPC DATA 帧的 payload 中**用 RESP 解析器**计数命令数量，`in_flight += cmd_count`
- **回复侧**：从后端回复中**用 RESP 解析器**计数完整响应数量，`in_flight -= resp_count`
- 只有 `fence_received && in_flight == 0` 才发送 FENCE_ACK

如果 RESP 解析复杂度过高，备选方案：
- 在 FENCE 帧的 payload 中携带总请求计数 `total_req_count`
- 新进程只需等后端回复计数 == `total_req_count` 即可

## 6. 完整升级时序

```
┌──────────┐                              ┌──────────┐
│ Old Proc │                              │ New Proc │
└────┬─────┘                              └────┬─────┘
     │  SIGUSR2 received                       │
     │──── fork/exec new binary ──────────────>│
     │                                         │ 启动，启用 SO_REUSEPORT
     │                                         │ bind/listen 自己的端口
     │                                         │ 注册 UDS fd 到 event loop
     │<──── MSG_HANDSHAKE (via UDS) ───────────│
     │──── MSG_HANDSHAKE_ACK ─────────────────>│
     │                                         │
     │  Phase 1: 传递后端 MUX fd               │
     │  dispatchTransferBackends() 到每个 Worker│
     │  Worker 线程中安全执行:                  │
     │    ① 从 epoll 移除 backend_fd           │
     │    ② 刷完 send_buf_                    │
     │    ③ 创建 socketpair                   │
     │    ④ 创建 IPCProxyBackend(socketpair[0])│
     │    ⑤ setStreaming(ipc_proxy, stream_id) │
     │    ⑥ 注册 ipc_fd 到 Worker epoll        │
     │  主线程收集 Worker 结果后发送:           │
     │──── MSG_BACKEND_FD + fd + meta ────────>│  重建 MuxBackendConn
     │──── MSG_IPC_FD + socketpair[1] ────────>│  创建 IPCProxyFrontend
     │                                         │  注册到 Worker event loop
     │                                         │
     │  Phase 2: 定时器驱动 + 逐批搬迁 Client  │
     │  每个 Worker 注册 migrate_timer (1ms)    │
     │  每次 timer 回调:                       │
     │    选一批 stream（最多 BATCH_SIZE 个）    │
     │    对每个: beginMigrate(flush + FENCE)   │
     │  epoll 正常处理:                        │
     │    client I/O → 正常读写                │
     │    IPC I/O → 代理转发                   │
     │    FENCE_ACK → completeMigration        │
     │      → 提取 recv_buf/send_buf           │
     │      → detach fd → sendmsg 给新进程     │
     │                                         │
     │  直到所有 stream 迁移完毕                │
     │                                         │
     │──── MSG_CLIENT_FD + meta + bufs ───────>│  acceptMigratedClient()
     │                                         │  恢复认证状态
     │                                         │  注入 buffer
     │                                         │  rebindStream
     │                                         │  开始本地服务
     │                                         │
     │  Phase 3: 完成                          │
     │──── MSG_TRANSFER_DONE ─────────────────>│
     │<──── MSG_ACK ───────────────────────────│
     │  exit(0)                                │
```

## 7. 后端 MUX 连接传递的元数据

```cpp
struct MuxBackendTransferMeta {
    int fd;                           // 后端 socket fd
    char addr[64];                    // 后端地址
    uint16_t port;                    // 后端端口
    char password[64];                // 认证密码
    MuxConnState state;               // 连接状态（必须是 READY）
    uint64_t next_stream_id;          // 下一个 stream ID
    uint32_t active_stream_count;     // 活跃 stream 数量
    bool mux_enabled;                 // 是否启用 MUX

    // 帧解析状态机（传递半帧状态）
    unsigned char frame_header_buf[MUX_FRAME_HEADER_SIZE];
    size_t header_bytes_read;
    bool parsing_payload;
    uint8_t current_flags;
    uint64_t current_stream_id;
    uint32_t current_payload_len;

    // Stream 映射表大小
    uint32_t stream_map_count;
    // 后跟: StreamMapEntry[stream_map_count]
    //   { uint64_t stream_id; int client_fd; }

    // 变长缓冲区大小
    uint32_t frame_payload_buf_len;   // 半帧 payload 数据
    uint32_t recv_buf_len;            // 未解析的接收数据
    uint32_t send_buf_len;            // 未发送的数据
    // 后跟: frame_payload_buf 数据
    // 后跟: recv_buf 数据
    // 后跟: send_buf 数据
};
```

## 8. 客户端迁移的元数据

```cpp
struct ClientMigrateMeta {
    uint64_t stream_id;        // 对应的后端 stream ID
    uint16_t listen_port;      // 监听端口
    bool authenticated;        // 认证状态
    uint32_t recv_buf_len;     // 接收缓冲区残余长度
    uint32_t send_buf_len;     // 发送缓冲区残余长度
    // 后跟: recv_buf 数据（如有）
    // 后跟: send_buf 数据（如有）
};
```

客户端 fd 通过 `sendmsg(SCM_RIGHTS)` 传递，元数据作为 iov 数据。

## 9. 抽象基类设计

为了让 ClientConnection 对后端替换透明，引入 `IBackendConnection` 接口：

```cpp
class IBackendConnection {
public:
    virtual ~IBackendConnection() = default;
    virtual void sendData(uint64_t stream_id, const char* data, size_t len) = 0;
    virtual void closeStream(uint64_t stream_id) = 0;
    virtual ClientConnection* getStreamClient(uint64_t stream_id) const = 0;
};

class MuxBackendConnection : public IBackendConnection { /* 现有实现 */ };
class IPCProxyBackend      : public IBackendConnection { /* IPC 转发 */ };
```

ClientConnection 持有 `IBackendConnection*`，切换时只需：
```cpp
client->setStreaming(ipc_proxy_backend, same_stream_id);
```

## 10. 迁移调度策略

### 10.1 事件驱动调度（定时器回调）

**关键设计**：迁移调度通过 Worker 线程的定时器驱动，不阻塞 event loop。

```cpp
// 在每个 Worker 中注册定时器
worker->getEventLoop().addTimer(MIGRATE_INTERVAL_MS, [this]() {
    migrateNextBatch();
    return hasMoreStreamsToMigrate(); // 返回 false 则移除定时器
});
```

每次 timer 回调：
1. 从当前 Worker 的 IPCProxyBackend 列表中选一批待迁移的 stream
2. 对每个调用 `beginMigrate()` → 显式 flush recv_buf → 发 FENCE
3. FENCE_ACK 的处理由 IPC fd 的 onReadable 事件驱动
4. 完成后通过 UDS 将 client fd 传给新进程

### 10.2 连接选择

优先迁移"安静"的连接：
- `!hasPendingSend()` — 发送缓冲区为空
- `recv_buf_.empty()` — 无未转发的数据

### 10.3 批量并行迁移

为支持 5000+ 连接场景，支持每次 timer 回调迁移多个 stream：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `migrate_batch_size` | 10 | 每次 timer 回调最多发起的 FENCE 数量 |
| `migrate_interval_ms` | 1 | timer 回调间隔（毫秒） |

5000 连接 ÷ 10/批 ÷ 1000Hz = **0.5 秒**完成所有 FENCE 发送。
加上 FENCE_ACK 等待和 fd 传递，整体预计 **2-5 秒**完成。

### 10.4 超时机制

| 级别 | 超时时间 | 触发动作 |
|------|---------|---------|
| 单 stream FENCE_ACK | 5 秒 | 强制完成迁移（可能丢少量 in-flight 回复） |
| 全局升级 | `max(30s, stream_count * 15ms)` | 剩余连接全部强制迁移，或回滚 |

**全局超时动态计算**：根据连接数自动调整，避免 5000+ 连接场景超时。

### 10.5 超时强制迁移

如果某个 stream 的 FENCE_ACK 超时（后端慢查询/hang），强制迁移：
- 旧进程直接 detach client fd 并传递
- 客户端可能丢失 in-flight 的回复
- Redis 客户端库的 read timeout + 重试机制兜底

## 11. Listen fd 传递策略

Listen 端口的处理是热升级过程中的关键环节，直接影响新建连接的可用性和迁移的收敛性。

### 11.1 SO_REUSEPORT 方案（推荐）

新进程和旧进程同时启用 `SO_REUSEPORT`，允许多进程 bind 同一端口：

1. **新进程启动时**：直接 `bind/listen` 自己的端口（SO_REUSEPORT），立即可以 accept 新连接
2. **旧进程传来 listen fd**：新进程**直接 close**（自己已有）
3. **旧进程退出后**：所有新连接自然流向新进程

**优点**：新进程从 fork/exec 开始就能 accept 新连接，无间断窗口。

### 11.2 各阶段新建连接的归属

| 阶段 | 新连接落在哪里 | 原因 |
|------|--------------|------|
| 新进程 exec 之前 | 旧进程 | 只有旧进程在 listen |
| 新进程启动后 ~ Phase 1 完成前 | **新进程或旧进程（内核负载均衡）** | 两个进程都启用了 `SO_REUSEPORT`，同时 bind 同一端口，内核在两者之间做负载均衡 |
| Phase 1 完成后 ~ 旧进程退出前 | **仅新进程** | 旧进程主动关闭 listen fd（见 11.3） |
| 旧进程退出后 | 新进程 | 只有新进程在 listen |

在双进程共存期间（新进程启动 ~ Phase 1 完成），落在旧进程上的新连接也能正常处理，因为旧进程的 event loop 没有被阻塞（迁移是定时器驱动的），可以正常 accept 和服务。这些连接会在后续的 Phase 2 迁移批次中被搬到新进程。

### 11.3 旧进程主动关闭 listen fd（关键优化）

**在 Phase 1（后端 MUX fd 迁移）完成后，旧进程必须主动关闭所有 listen fd，停止接受新连接。**

这是一个关键优化，原因如下：

1. **保证迁移收敛**：如果旧进程持续 accept 新连接，Phase 2 迁移的同时不断有新连接加入，可能导致"永远搬不完"的问题
2. **加速迁移完成**：Phase 2 只需搬迁存量连接，数量单调递减
3. **语义清晰**：Phase 1 完成后，旧进程的角色从"服务进程"变为"迁移代理"，不应再接受新连接

```cpp
// Phase 1 完成后，旧进程停止接受新连接
void HotUpgradeManager::onPhase1Complete() {
    // 关闭所有 listen fd，不再 accept 新连接
    for (const auto& [fd, port] : listen_fds_) {
        event_loop_.removeEvent(fd);
        ::close(fd);
        LOG_INFO("Hot-upgrade: closed listen fd=%d port=%d, "
                 "stop accepting new connections", fd, port);
    }
    listen_fds_.clear();

    // 从此所有新连接只落在新进程上
    // 开始 Phase 2: 定时器驱动逐批搬迁存量客户端
    startPhase2Migration();
}
```

**时序示意**：

```
旧进程                                    新进程
  │ ── Phase 1: 迁移后端 MUX fd ──────────>│
  │                                        │
  │ Phase 1 完成                            │
  │ ★ close 所有 listen fd ★               │
  │ 从此不再 accept 新连接                  │ ← 所有新连接只落在新进程
  │                                        │
  │ ── Phase 2: 逐批搬迁存量 Client ──────>│
  │ （存量连接数单调递减，保证收敛）         │
  │                                        │
  │ ── Phase 3: exit(0) ─────────────────>│
```

### 11.4 备选方案：直接传递 listen fd

如果不使用 SO_REUSEPORT：
1. 旧进程将 listen fd 通过 `sendmsg(SCM_RIGHTS)` 传给新进程
2. 新进程直接使用传来的 listen fd（不自己 bind）
3. 从接收到 listen fd 的那一刻起接管新连接

**缺点**：在旧进程发送 listen fd 到新进程接收期间，存在短暂的无法 accept 窗口。

## 12. 新进程异步接收

### 12.1 事件驱动接收（关键设计）

新进程的 `receiveFromOldProcess` **不再是同步阻塞循环**，而是：

1. 新进程启动后，将 UDS fd 注册到主线程的 event loop
2. UDS 可读时触发 `onUDSReadable()` 回调，解析一条消息并处理
3. **event loop 同时处理新连接的 accept 和旧进程的 fd 传递**

```
新进程 event loop:
  ┌──────────────────────────────────────┐
  │ epoll_wait()                         │
  │  ├── listen fd → accept 新客户端     │ ← 新连接不受影响
  │  ├── uds fd → onUDSReadable()       │ ← 异步接收旧进程传来的 fd
  │  ├── backend fd → 正常后端 I/O       │
  │  └── ipc fd → IPCProxyFrontend I/O  │
  └──────────────────────────────────────┘
```

### 12.2 消息协议

UDS 上传输的每条消息格式：`[1 byte msg_type] + [sendmsg payload]`

接收端按 msg_type 分派：
- `MSG_BACKEND_FD`：调用 `receiveBackendConnection()` 重建后端连接
- `MSG_IPC_FD`：调用 `receiveIPCFd()` 创建 IPCProxyFrontend
- `MSG_CLIENT_FD`：调用 `receiveClientConnection()` → `acceptMigratedClient()`
- `MSG_TRANSFER_DONE`：回复 ACK，清理 UDS fd

## 13. 错误处理

| 场景 | 处理方式 |
|------|---------|
| IPC socketpair 断开 | 等同后端断连，触发 onConnectionError |
| 后端 Redis 断开 | MuxBackendConn 触发错误，所有 stream 关闭，IPC 通道自然关闭 |
| 新进程崩溃 | 旧进程通过 waitpid 检测，回滚继续服务 |
| 旧进程崩溃 | 新进程检测 IPC 断开，强制接管所有 stream |

## 14. 性能影响分析

| 阶段 | 延迟影响 | 持续时间 |
|------|---------|---------|
| Phase 1: 迁移后端 fd | ~1ms（短暂停止后端读写） | 瞬时 |
| Phase 2: IPC 代理 | 每请求增加 ~1-2μs（socketpair 内核拷贝） | 2-5 秒（5000 连接） |
| Phase 3: Client 迁移 | 单个 Client 在 FENCE→FENCE_ACK 期间不接收新请求 | 通常 < 100ms |

**IPC 代理的额外延迟（~1-2μs）相比 Redis 请求本身的毫秒级延迟可忽略不计。**

### 14.1 5000+ 连接场景分析

| 指标 | 值 | 说明 |
|------|------|------|
| 总迁移时间 | 2-5 秒 | 批量并行迁移，每批 10 个，1ms 间隔 |
| 单连接暂停时间 | < 100ms | 从 FENCE 到 FENCE_ACK |
| 新连接影响 | 无 | SO_REUSEPORT + 异步接收，全程可 accept |
| CPU 影响 | < 5% | 定时器驱动，不占用连续 CPU |
| 内存影响 | ~2x | 双进程运行期间，约 2-5 秒 |

## 15. 新增/修改的文件

| 文件 | 类型 | 说明 |
|------|------|------|
| `include/ipc_proxy_backend.h` | 新增 | IPCProxyBackend 类定义 |
| `include/ipc_proxy_frontend.h` | 新增 | IPCProxyFrontend 类定义 |
| `include/i_backend_connection.h` | 新增 | IBackendConnection 抽象基类 |
| `include/hot_upgrade.h` | 修改 | 新增 MuxBackendTransferMeta、ClientMigrateMeta 等结构 |
| `include/mux_backend_connection.h` | 修改 | 继承 IBackendConnection，新增 rebindStream 等方法 |
| `include/client_connection.h` | 修改 | backend_conn_ 类型改为 IBackendConnection* |
| `src/ipc_proxy_backend.cpp` | 新增 | IPCProxyBackend 实现 |
| `src/ipc_proxy_frontend.cpp` | 新增 | IPCProxyFrontend 实现 |
| `src/hot_upgrade.cpp` | 重写 | 三阶段迁移流程（事件驱动） |
