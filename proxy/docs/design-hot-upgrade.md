# 热升级设计方案

## 1. 概述

热升级允许在不中断客户端连接的情况下替换正在运行的 proxy 二进制。通过 Unix Domain Socket (UDS) 在新旧进程间传递文件描述符和连接状态，实现零停机升级。

## 2. Unix Domain Socket 通信协议

### 2.1 消息类型定义

| 消息类型 | 值 | 描述 |
|---------|-----|------|
| MSG_HANDSHAKE | 0x01 | 新进程向旧进程发送握手 |
| MSG_HANDSHAKE_ACK | 0x02 | 旧进程确认握手 |
| MSG_LISTEN_FD | 0x03 | 传递监听 fd |
| MSG_CLIENT_FD | 0x04 | 传递客户端连接 fd |
| MSG_TRANSFER_DONE | 0x05 | 所有 fd 传递完成 |
| MSG_ACK | 0x06 | 新进程确认接收完成 |
| MSG_ROLLBACK | 0x07 | 回滚通知 |

### 2.2 消息格式

```
+----------+----------+------------+---------+
| type (1) | len (4)  | payload    | fd[]    |
+----------+----------+------------+---------+
```

- `type`: 消息类型 (1 byte)
- `len`: payload 长度 (4 bytes, network byte order)
- `payload`: 元数据 JSON/二进制
- `fd[]`: 通过 `SCM_RIGHTS` 传递的文件描述符数组

### 2.3 fd 批量传递方案

使用 `sendmsg()`/`recvmsg()` 配合 `SCM_RIGHTS` 控制消息批量传递 fd。每次最多传递 64 个 fd（受内核限制）。

```cpp
// sendmsg with SCM_RIGHTS
struct msghdr msg = {};
struct cmsghdr *cmsg;
char cmsgbuf[CMSG_SPACE(sizeof(int) * MAX_FDS_PER_MSG)];
msg.msg_control = cmsgbuf;
msg.msg_controllen = CMSG_SPACE(sizeof(int) * num_fds);
cmsg = CMSG_FIRSTHDR(&msg);
cmsg->cmsg_level = SOL_SOCKET;
cmsg->cmsg_type = SCM_RIGHTS;
cmsg->cmsg_len = CMSG_LEN(sizeof(int) * num_fds);
memcpy(CMSG_DATA(cmsg), fds, sizeof(int) * num_fds);
```

## 3. 传递的状态元数据

### 3.1 监听 fd 元数据

```cpp
struct ListenFdMetadata {
    int fd;                    // 监听 socket fd
    uint16_t listen_port;      // 监听端口
    char backend_addr[64];     // 后端地址
    uint16_t backend_port;     // 后端端口
    char backend_password[64]; // 后端密码
};
```

### 3.2 客户端连接元数据

```cpp
struct ConnectionMetadata {
    int fd;                    // 客户端 socket fd
    uint16_t listen_port;      // 对应的监听端口
    bool authenticated;        // 是否已通过 proxy 认证
    uint32_t stream_id;        // MUX stream ID（0 = 未绑定）
    char backend_addr[64];     // 已绑定的后端地址
    uint16_t backend_port;     // 已绑定的后端端口
    uint32_t recv_buf_len;     // 接收缓冲区残留数据长度
    uint32_t send_buf_len;     // 发送缓冲区残留数据长度
};
```

残留缓冲区数据紧跟在元数据之后传输。

## 4. 新旧进程协调时序

```
┌──────────┐                          ┌──────────┐
│ Old Proc │                          │ New Proc │
└────┬─────┘                          └────┬─────┘
     │                                     │
     │  SIGUSR2 received                   │
     │──── fork/exec new binary ──────────>│
     │                                     │
     │<──── MSG_HANDSHAKE (via UDS) ───────│
     │                                     │
     │──── MSG_HANDSHAKE_ACK ─────────────>│
     │                                     │
     │  Phase 1: Transfer listen fds       │
     │──── MSG_LISTEN_FD + fd + meta ─────>│
     │──── MSG_LISTEN_FD + fd + meta ─────>│
     │  ...                                │
     │                                     │
     │  Phase 2: Stop accepting            │
     │  (close listen fds from epoll)      │
     │                                     │
     │  Phase 3: Transfer client fds       │
     │──── MSG_CLIENT_FD + fd + meta ─────>│
     │──── MSG_CLIENT_FD + fd + meta ─────>│
     │  ... (buffered data follows)        │
     │                                     │
     │──── MSG_TRANSFER_DONE ─────────────>│
     │                                     │
     │  Wait for ACK (with timeout)        │
     │<──── MSG_ACK ───────────────────────│
     │                                     │
     │  exit(0)                            │
     │                                     │
     │                          New proc now serving
```

## 5. Stream ID 回绕处理

MUX stream ID 在传递时不需要特殊处理，因为：
- 旧进程的后端 MUX 连接不传递（无法传递 MUX 状态）
- 新进程为每个传递过来的客户端重新建立后端 MUX 连接和 stream

## 6. 超时回滚机制

- 旧进程在 fork/exec 后启动超时计时器（默认 30 秒，可配置）
- 如果超时未收到 MSG_ACK：
  1. 发送 MSG_ROLLBACK 通知新进程退出
  2. 重新注册所有监听 fd 到 epoll
  3. 恢复正常服务
  4. 日志记录升级失败

## 7. 升级过程中新连接处理

- Phase 1（传递监听 fd 前）：旧进程正常接受新连接
- Phase 2（传递监听 fd 后）：旧进程停止接受，新进程开始接受
- Phase 3（传递客户端 fd）：旧进程的已有客户端依次传递给新进程
- 在 Phase 2-3 之间可能有短暂的新连接由新进程直接处理

## 8. 实现约束

- 后端 MUX 连接不传递（状态过于复杂），新进程为传入的客户端重新建立后端连接
- 传递期间的数据可能有微小延迟，但不会丢失（缓冲区一并传递）
- 最大支持同时传递 10000 个客户端连接
