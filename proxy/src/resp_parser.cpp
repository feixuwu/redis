// RESP Parser implementation
#include "resp_parser.h"
#include <algorithm>
#include <cstring>

namespace proxy {

bool RespParser::parse(const char* data, size_t len, Command& cmd) {
    if (len == 0) return false;

    // Try RESP multi-bulk format first
    if (data[0] == '*') {
        return parseMultiBulk(data, len, cmd);
    }

    // Fall back to inline format
    return parseInline(data, len, cmd);
}

bool RespParser::parseInline(const char* data, size_t len, Command& cmd) {
    // Find \r\n
    const char* end = (const char*)memmem(data, len, "\r\n", 2);
    if (!end) return false;

    size_t line_len = end - data;
    cmd.bytes_consumed = line_len + 2;

    // Split by spaces
    std::string line(data, line_len);
    size_t pos = 0;
    bool first = true;

    while (pos < line.size()) {
        // Skip spaces
        while (pos < line.size() && line[pos] == ' ') pos++;
        if (pos >= line.size()) break;

        // Find next space
        size_t end_pos = line.find(' ', pos);
        if (end_pos == std::string::npos) end_pos = line.size();

        std::string token = line.substr(pos, end_pos - pos);
        if (first) {
            cmd.name = token;
            std::transform(cmd.name.begin(), cmd.name.end(), cmd.name.begin(), ::toupper);
            first = false;
        } else {
            cmd.args.push_back(token);
        }
        pos = end_pos;
    }

    cmd.complete = !cmd.name.empty();
    return cmd.complete;
}

bool RespParser::parseMultiBulk(const char* data, size_t len, Command& cmd) {
    if (len < 4 || data[0] != '*') return false;

    size_t pos = 1;

    // Read count
    const char* crlf = (const char*)memmem(data + pos, len - pos, "\r\n", 2);
    if (!crlf) return false;

    int count = 0;
    for (const char* p = data + pos; p < crlf; p++) {
        if (*p < '0' || *p > '9') return false;
        count = count * 10 + (*p - '0');
    }
    pos = (crlf - data) + 2;

    if (count <= 0) return false;

    // Read bulk strings
    bool first = true;
    for (int i = 0; i < count; i++) {
        if (pos >= len || data[pos] != '$') return false;
        pos++;

        // Read length
        crlf = (const char*)memmem(data + pos, len - pos, "\r\n", 2);
        if (!crlf) return false;

        int bulk_len = 0;
        for (const char* p = data + pos; p < crlf; p++) {
            if (*p == '-') continue; // Handle $-1 (null bulk)
            if (*p < '0' || *p > '9') return false;
            bulk_len = bulk_len * 10 + (*p - '0');
        }
        pos = (crlf - data) + 2;

        // Check if we have enough data
        if (pos + bulk_len + 2 > len) return false;

        std::string val(data + pos, bulk_len);
        pos += bulk_len + 2; // Skip \r\n

        if (first) {
            cmd.name = val;
            std::transform(cmd.name.begin(), cmd.name.end(), cmd.name.begin(), ::toupper);
            first = false;
        } else {
            cmd.args.push_back(val);
        }
    }

    cmd.bytes_consumed = pos;
    cmd.complete = true;
    return true;
}

std::string RespParser::encodeSimpleString(const std::string& s) {
    return "+" + s + "\r\n";
}

std::string RespParser::encodeError(const std::string& s) {
    return "-" + s + "\r\n";
}

std::string RespParser::encodeBulkString(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

std::string RespParser::encodeInteger(int64_t val) {
    return ":" + std::to_string(val) + "\r\n";
}

std::string RespParser::encodeNullBulkString() {
    return "$-1\r\n";
}

std::string RespParser::encodeArray(const std::vector<std::string>& encoded_items) {
    std::string result = "*" + std::to_string(encoded_items.size()) + "\r\n";
    for (const auto& item : encoded_items) {
        result += item;
    }
    return result;
}

// ========== 热升级: RESP 命令/响应计数 ==========

uint32_t RespParser::countCommands(const char* data, size_t len) {
    // 统计 buffer 中完整的 RESP 命令数量
    // 每个命令要么是一个 multi-bulk (*N\r\n$L\r\n...)，要么是一行 inline 命令
    uint32_t count = 0;
    size_t pos = 0;

    while (pos < len) {
        if (data[pos] == '*') {
            // Multi-bulk 命令: *N\r\n$L\r\n...\r\n$L\r\n...\r\n
            size_t consumed = skipOneResponse(data + pos, len - pos);
            if (consumed == 0) break; // 不完整
            count++;
            pos += consumed;
        } else {
            // Inline 命令: 以 \r\n 结尾的一行
            const char* crlf = (const char*)memmem(data + pos, len - pos, "\r\n", 2);
            if (!crlf) break; // 不完整
            count++;
            pos = (crlf - data) + 2;
        }
    }

    return count;
}

uint32_t RespParser::countResponses(const char* data, size_t len) {
    // 统计 buffer 中完整的 RESP 响应数量
    uint32_t count = 0;
    size_t pos = 0;

    while (pos < len) {
        size_t consumed = skipOneResponse(data + pos, len - pos);
        if (consumed == 0) break; // 不完整
        count++;
        pos += consumed;
    }

    return count;
}

size_t RespParser::skipOneResponse(const char* data, size_t len) {
    // 跳过一个完整的 RESP 元素，返回消耗的字节数，0 表示不完整
    if (len == 0) return 0;

    switch (data[0]) {
    case '+': // Simple String: +OK\r\n
    case '-': // Error: -ERR ...\r\n
    case ':': // Integer: :123\r\n
    {
        const char* crlf = (const char*)memmem(data, len, "\r\n", 2);
        if (!crlf) return 0;
        return (crlf - data) + 2;
    }
    case '$': // Bulk String: $N\r\n<data>\r\n 或 $-1\r\n
    {
        const char* crlf = (const char*)memmem(data, len, "\r\n", 2);
        if (!crlf) return 0;

        // 解析长度
        int64_t bulk_len = 0;
        bool negative = false;
        for (const char* p = data + 1; p < crlf; p++) {
            if (*p == '-') { negative = true; continue; }
            if (*p < '0' || *p > '9') return 0; // 格式错误
            bulk_len = bulk_len * 10 + (*p - '0');
        }

        if (negative) {
            // $-1\r\n (null bulk string)
            return (crlf - data) + 2;
        }

        size_t header_len = (crlf - data) + 2;
        size_t total = header_len + bulk_len + 2; // +2 for trailing \r\n
        if (total > len) return 0; // 不完整
        return total;
    }
    case '*': // Array: *N\r\n<element>...<element>
    {
        const char* crlf = (const char*)memmem(data, len, "\r\n", 2);
        if (!crlf) return 0;

        // 解析元素个数
        int64_t arr_count = 0;
        bool negative = false;
        for (const char* p = data + 1; p < crlf; p++) {
            if (*p == '-') { negative = true; continue; }
            if (*p < '0' || *p > '9') return 0;
            arr_count = arr_count * 10 + (*p - '0');
        }

        if (negative) {
            // *-1\r\n (null array)
            return (crlf - data) + 2;
        }

        size_t pos = (crlf - data) + 2;
        for (int64_t i = 0; i < arr_count; i++) {
            if (pos >= len) return 0;
            size_t elem_len = skipOneResponse(data + pos, len - pos);
            if (elem_len == 0) return 0; // 某个元素不完整
            pos += elem_len;
        }
        return pos;
    }
    default:
        // 未知类型，尝试按行处理
        {
            const char* crlf = (const char*)memmem(data, len, "\r\n", 2);
            if (!crlf) return 0;
            return (crlf - data) + 2;
        }
    }
}

} // namespace proxy
