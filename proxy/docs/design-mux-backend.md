# MUX Backend Connection Management Design

## 1. Overview

Each worker thread maintains a pool of MUX connections to backend Redis instances. Multiple client connections sharing the same backend are multiplexed over shared MUX TCP connections, each getting a unique stream ID.

```
Worker Thread N
├── MuxConnectionPool["127.0.0.1:6379"]
│   ├── MuxBackendConnection #1 (streams: 1, 3, 5, 7, ...)
│   └── MuxBackendConnection #2 (streams: 1025, 1027, ...) [overflow]
├── MuxConnectionPool["127.0.0.1:6380"]
│   └── MuxBackendConnection #1 (streams: 1, 3, 5, ...)
│
├── ClientConnection fd=10 → MuxBC#1 stream=1
├── ClientConnection fd=11 → MuxBC#1 stream=3
├── ClientConnection fd=12 → MuxBC#1 stream=5
└── ClientConnection fd=13 → MuxBC#2 stream=1025
```

## 2. Core Data Structures

### MuxBackendConnection

```cpp
class MuxBackendConnection {
    // Network
    int fd_;                     // Backend TCP socket
    MuxConnState state_;         // CONNECTING → HANDSHAKING → READY → GOAWAY → CLOSED

    // Stream management
    uint32_t next_stream_id_;    // Next odd stream ID to allocate (1, 3, 5, ...)
    uint32_t active_stream_count_;
    uint32_t max_streams_;       // From config: mux.max_streams_per_connection
    std::unordered_map<uint32_t, ClientConnection*> streams_;  // stream_id → client

    // Frame parsing state machine
    unsigned char frame_header_buf_[10];   // 10-byte frame header buffer
    size_t header_bytes_read_;             // Bytes of header read so far
    bool parsing_payload_;                 // true = reading payload, false = reading header
    uint8_t current_flags_;                // Current frame's flags byte
    uint32_t current_stream_id_;           // Current frame's stream ID
    uint32_t current_payload_len_;         // Current frame's payload length
    std::vector<char> frame_payload_buf_;  // Payload reassembly buffer

    // I/O buffers
    std::vector<char> send_buf_;
    size_t send_offset_;
    std::vector<char> recv_buf_;
};
```

### MuxConnectionPool (within WorkerThread)

```cpp
// Per-worker mapping: "addr:port" → vector<MuxBackendConnection*>
std::unordered_map<std::string, std::vector<std::unique_ptr<MuxBackendConnection>>> mux_pools_;
```

### Stream Allocation

When a new ClientConnection needs a backend stream:
1. Look up pool by `"addr:port"` key
2. Find first MuxBackendConnection with `canCreateStream() == true`
3. If none found, create a new MuxBackendConnection
4. Call `createStream(client)` → allocates odd stream ID, sends STREAM_OPEN frame

## 3. MUX Connection Establishment

```
Proxy                              Redis
  │                                  │
  ├──── TCP connect ────────────────→│
  │                                  │
  ├──── HELLO 3 MULTIPLEX ─────────→│
  │                                  │
  │←─── HELLO response (mux:1) ─────┤
  │     [connection now in MUX mode] │
  │                                  │
  ├──── AUTH <backend_password> ────→│  (if password configured)
  │     [wrapped in MUX DATA frame]  │
  │                                  │
  │←─── +OK ────────────────────────┤
  │     [wrapped in MUX DATA frame]  │
  │                                  │
  │     state_ = READY               │
```

### Handshake Detail

The HELLO command is sent as **plain RESP** (before MUX mode is active):
```
*3\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$9\r\nMULTIPLEX\r\n
```

After successful HELLO response, the connection switches to MUX frame mode. All subsequent data (including AUTH) is wrapped in MUX frames.

If the backend has a password, the proxy sends AUTH as the first MUX DATA frame on stream 0 (control stream):
```
[MUX Header: magic=0xAA, flags=0x01(DATA), stream=0, len=N]
*2\r\n$4\r\nAUTH\r\n$<len>\r\n<password>\r\n
```

## 4. Frame Encoding/Decoding

### Frame Header (10 bytes)

```
Offset  Size   Field
0       1      Magic (0xAA)
1       1      Flags (lower 4 bits = type)
2       4      Stream ID (big-endian)
6       4      Payload Length (big-endian)
```

### Encoding

```cpp
static void encodeFrameHeader(unsigned char* buf, uint8_t flags,
                               uint32_t stream_id, uint32_t payload_len) {
    buf[0] = 0xAA;                                    // Magic
    buf[1] = flags;                                    // Flags
    buf[2] = (stream_id >> 24) & 0xFF;                // Stream ID (big-endian)
    buf[3] = (stream_id >> 16) & 0xFF;
    buf[4] = (stream_id >>  8) & 0xFF;
    buf[5] =  stream_id        & 0xFF;
    buf[6] = (payload_len >> 24) & 0xFF;              // Payload Length (big-endian)
    buf[7] = (payload_len >> 16) & 0xFF;
    buf[8] = (payload_len >>  8) & 0xFF;
    buf[9] =  payload_len        & 0xFF;
}
```

### Decoding (State Machine)

The frame parser handles TCP fragmentation:

```
State: READING_HEADER
  - Accumulate bytes into frame_header_buf_[0..9]
  - When 10 bytes collected:
    - Validate magic == 0xAA
    - Extract flags, stream_id, payload_len
    - If payload_len == 0: dispatch frame immediately, stay in READING_HEADER
    - If payload_len > 0: transition to READING_PAYLOAD

State: READING_PAYLOAD
  - Accumulate bytes into frame_payload_buf_
  - When payload_len bytes collected:
    - Dispatch frame (type, stream_id, payload)
    - Clear payload buffer
    - Transition to READING_HEADER
```

## 5. Stream Lifecycle

### Creation (STREAM_OPEN)

```
1. Allocate stream_id = next_stream_id_; next_stream_id_ += 2;
2. Send STREAM_OPEN frame: [Header: type=0x02, stream_id, payload_len=0]
3. Insert into streams_ map: streams_[stream_id] = client_ptr
4. Increment active_stream_count_
```

### Data Transfer (DATA)

**Client → Redis:**
```
1. ClientConnection::forwardToBackend() called
2. MuxBackendConnection::sendData(stream_id, data, len)
3. Encode: [Header: type=0x01, stream_id, payload_len=len] + payload
4. Append to send_buf_, register EPOLLOUT
```

**Redis → Client:**
```
1. MuxBackendConnection::onReadable() → processRecvBuffer()
2. Decode frame → type=DATA
3. Lookup client = streams_[stream_id]
4. client->appendToSendBuffer(payload, len)
```

### Closure (STREAM_CLOSE)

```
1. Send STREAM_CLOSE frame: [Header: type=0x03, stream_id, payload_len=0]
2. Remove from streams_ map
3. Decrement active_stream_count_
4. Clear client's backend binding
```

## 6. Stream ID Exhaustion & GOAWAY

Stream IDs are uint32_t, odd numbers only (client-initiated), so max ~2 billion streams.

### Detection Threshold
```
if (next_stream_id_ > 0xFFFFFFF0) {  // Within ~8 of overflow
    // Trigger GOAWAY
}
```

### GOAWAY Flow
```
1. Send GOAWAY frame on the connection: [Header: type=0x06, stream=0, payload_len=4]
   Payload: last_stream_id (4 bytes, big-endian)
2. Set state_ = GOAWAY
3. No new streams created on this connection
4. Existing streams continue until they close naturally
5. When active_stream_count_ reaches 0, close the TCP connection
6. New client connections get a fresh MuxBackendConnection
```

## 7. Non-MUX Fallback

When `config.mux.enabled = false`:

- Each ClientConnection gets its own direct TCP connection to the backend
- Data is transparently forwarded without frame encoding/decoding
- The `MuxBackendConnection` class handles both modes:
  - MUX mode: frame encode/decode, stream multiplexing
  - Non-MUX mode: direct passthrough, streams_ has exactly one entry (stream_id=0)
  - The backend AUTH is sent as plain RESP (no frame wrapping)

## 8. Connection Recovery

When a MUX backend connection drops (Redis closes it, network error, etc.):

```
1. MuxBackendConnection::onReadable() detects read() == 0 or error
2. For each stream in streams_:
   - Close the corresponding ClientConnection (send nothing, just close fd)
3. Remove MuxBackendConnection from the pool
4. Log error with backend address and number of affected streams
```

No automatic reconnection for existing streams - they are lost with the clients.
New client connections will create new MUX connections as needed.
