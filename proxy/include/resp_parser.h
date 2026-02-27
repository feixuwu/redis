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

    // ========== 热升级: RESP 命令/响应计数（用于 in-flight 精确跟踪） ==========

    // 统计 buffer 中完整的 RESP 命令数量（请求侧）
    // 支持 multi-bulk (*N\r\n$L\r\n...) 和 inline 格式
    static uint32_t countCommands(const char* data, size_t len);

    // 统计 buffer 中完整的 RESP 响应数量（回复侧）
    // 支持 Simple String (+), Error (-), Integer (:), Bulk String ($), Array (*) 格式
    static uint32_t countResponses(const char* data, size_t len);

private:
    // 跳过一个完整的 RESP 元素（递归处理 Array），返回消耗的字节数，0 表示不完整
    static size_t skipOneResponse(const char* data, size_t len);
};

} // namespace proxy
