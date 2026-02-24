#pragma once

#include <string>
#include <vector>
#include <cstddef>

namespace proxy {

// Simple RESP parser for proxy use (AUTH command parsing and admin port)
class RespParser {
public:
    // Parse result
    struct Command {
        std::string name;                    // Command name (uppercase)
        std::vector<std::string> args;       // Command arguments
        size_t bytes_consumed = 0;           // Total bytes consumed from input
        bool complete = false;               // Whether a full command was parsed
    };

    // Try to parse one command from the buffer
    // Returns true if a complete command was parsed
    static bool parse(const char* data, size_t len, Command& cmd);

    // Try to parse inline command (space-separated, ending with \r\n)
    static bool parseInline(const char* data, size_t len, Command& cmd);

    // Try to parse RESP multi-bulk command (*N\r\n$L\r\n...\r\n)
    static bool parseMultiBulk(const char* data, size_t len, Command& cmd);

    // RESP response encoding
    static std::string encodeSimpleString(const std::string& s);
    static std::string encodeError(const std::string& s);
    static std::string encodeBulkString(const std::string& s);
    static std::string encodeInteger(int64_t val);
    static std::string encodeNullBulkString();
    static std::string encodeArray(const std::vector<std::string>& encoded_items);
};

} // namespace proxy
