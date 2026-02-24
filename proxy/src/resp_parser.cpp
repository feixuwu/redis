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

} // namespace proxy
