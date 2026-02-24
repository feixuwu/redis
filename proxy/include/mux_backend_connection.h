#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

namespace proxy {

// Forward declarations
class ClientConnection;
class WorkerThread;

// MUX frame header constants (matching hiredis_mux.h / mux.h)
constexpr uint8_t  MUX_FRAME_MAGIC          = 0xAA;
constexpr size_t   MUX_FRAME_HEADER_SIZE    = 14;
constexpr uint8_t  MUX_FRAME_DATA           = 0x01;
constexpr uint8_t  MUX_FRAME_STREAM_OPEN    = 0x02;
constexpr uint8_t  MUX_FRAME_STREAM_CLOSE   = 0x03;
constexpr uint8_t  MUX_FRAME_PING           = 0x04;
constexpr uint8_t  MUX_FRAME_PONG           = 0x05;
constexpr uint8_t  MUX_FRAME_GOAWAY         = 0x06;
constexpr uint8_t  MUX_FRAME_TYPE_MASK      = 0x0F;
constexpr uint32_t MUX_MAX_PAYLOAD_SIZE     = 512 * 1024 * 1024;

// MUX connection states
enum class MuxConnState {
    CONNECTING,        // TCP connecting
    HANDSHAKING,       // Sending HELLO 3 MULTIPLEX
    AUTHENTICATING,    // Sending AUTH to backend
    READY,             // Ready for stream operations
    GOAWAY,            // Received/sent GOAWAY, no new streams
    CLOSED             // Connection closed
};

class MuxBackendConnection {
public:
    MuxBackendConnection(const std::string& addr, uint16_t port,
                          const std::string& password, bool mux_enabled,
                          WorkerThread* worker);
    ~MuxBackendConnection();

    // Non-copyable
    MuxBackendConnection(const MuxBackendConnection&) = delete;
    MuxBackendConnection& operator=(const MuxBackendConnection&) = delete;

    // Connection lifecycle
    bool connect();
    void close();

    // Stream management
    uint64_t createStream(ClientConnection* client);
    void closeStream(uint64_t stream_id);
    ClientConnection* getStreamClient(uint64_t stream_id) const;

    // Data sending
    void sendData(uint64_t stream_id, const char* data, size_t len);
    void sendFrame(uint8_t type, uint64_t stream_id, const char* payload, size_t payload_len);

    // I/O handlers
    void onReadable();
    void onWritable();

    // State
    MuxConnState getState() const { return state_; }
    int getFd() const { return fd_; }
    const std::string& getAddr() const { return addr_; }
    uint16_t getPort() const { return port_; }
    uint32_t getActiveStreamCount() const { return active_stream_count_; }
    bool isReady() const { return state_ == MuxConnState::READY; }
    bool canCreateStream() const;

    // Non-MUX mode: direct passthrough
    bool isMuxEnabled() const { return mux_enabled_; }

private:
    std::string addr_;
    uint16_t port_;
    std::string password_;
    bool mux_enabled_;
    WorkerThread* worker_;

    int fd_ = -1;
    MuxConnState state_ = MuxConnState::CONNECTING;

    // Stream ID allocator (odd IDs for client-initiated)
    uint64_t next_stream_id_ = 1;
    uint32_t active_stream_count_ = 0;
    uint32_t max_streams_;

    // Stream mapping: stream_id -> ClientConnection*
    std::unordered_map<uint64_t, ClientConnection*> streams_;

    // Pending clients waiting for connection to become READY
    std::vector<ClientConnection*> pending_clients_;

    // Frame parsing state machine
    unsigned char frame_header_buf_[MUX_FRAME_HEADER_SIZE];
    size_t header_bytes_read_ = 0;
    bool parsing_payload_ = false;
    uint8_t current_flags_ = 0;
    uint64_t current_stream_id_ = 0;
    uint32_t current_payload_len_ = 0;
    std::vector<char> frame_payload_buf_;

    // Write buffer
    std::vector<char> send_buf_;
    size_t send_offset_ = 0;

    // Read buffer
    std::vector<char> recv_buf_;

    // Frame encode/decode
    static void encodeFrameHeader(unsigned char* buf, uint8_t flags,
                                   uint64_t stream_id, uint32_t payload_len);
    bool decodeFrameHeader(const unsigned char* buf, uint8_t& flags,
                           uint64_t& stream_id, uint32_t& payload_len);

    // Process received frames
    void processRecvBuffer();
    void handleFrame(uint8_t type, uint64_t stream_id, const char* payload, size_t len);

    // Handshake and connection lifecycle
    void startHandshake();
    void handleHandshakeReply();
    void handleAuthReply();
    void onConnected();
    void onConnectionError();
    void flushSendBuffer();
    void processNonMuxRecvBuffer();
    void flushPendingStreams();

public:
    // Add a client to pending queue (waiting for connection to be ready)
    void addPendingClient(ClientConnection* client);
};

} // namespace proxy
