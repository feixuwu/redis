#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <functional>

namespace proxy {

class WorkerThread;
class MuxBackendConnection;

// Client connection states
enum class ClientState {
    CONNECTED,       // Just connected, not yet authenticated
    AUTHENTICATED,   // Passed proxy AUTH
    STREAMING,       // Bound to a backend stream, actively forwarding
    CLOSING          // Being closed
};

class ClientConnection {
public:
    ClientConnection(int fd, uint16_t listen_port, WorkerThread* worker);
    ~ClientConnection();

    ClientConnection(const ClientConnection&) = delete;
    ClientConnection& operator=(const ClientConnection&) = delete;

    // Accessors
    int getFd() const { return fd_; }
    uint16_t getListenPort() const { return listen_port_; }
    ClientState getState() const { return state_; }
    bool isAuthenticated() const { return state_ >= ClientState::AUTHENTICATED; }
    uint32_t getStreamId() const { return stream_id_; }
    MuxBackendConnection* getBackendConn() const { return backend_conn_; }
    WorkerThread* getWorker() const { return worker_; }

    // State management
    void setAuthenticated() { state_ = ClientState::AUTHENTICATED; }
    void setStreaming(MuxBackendConnection* conn, uint32_t stream_id);
    void close();

    // I/O operations
    void onReadable();
    void onWritable();
    void sendReply(const std::string& reply);
    void sendError(const std::string& error);

    // Buffers
    void appendToSendBuffer(const char* data, size_t len);
    bool hasPendingSend() const;
    void flushSendBuffer();

private:
    int fd_;
    uint16_t listen_port_;
    ClientState state_ = ClientState::CONNECTED;
    WorkerThread* worker_;

    // Backend binding
    MuxBackendConnection* backend_conn_ = nullptr;
    uint32_t stream_id_ = 0;

    // Read/write buffers
    std::vector<char> recv_buf_;
    std::vector<char> send_buf_;
    size_t send_offset_ = 0;

    // Try to handle AUTH command from recv buffer
    // Returns true if data was consumed (AUTH handled or error sent)
    bool tryHandleAuth();

    // Forward data to backend
    void forwardToBackend();

public:
    // Retry forwarding buffered data (called when backend becomes ready)
    void retryForwardToBackend();

private:
    // Ensure connected to backend stream
    bool ensureBackendStream();

    // Get proxy password from config
    const std::string& getProxyPassword() const;
};

} // namespace proxy

// Global proxy password setter (called from main)
namespace proxy {
void setGlobalProxyPassword(const std::string* pwd);
}
