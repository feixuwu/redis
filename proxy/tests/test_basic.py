#!/usr/bin/env python3
"""
Basic functional tests for Redis MUX Proxy.

Tests:
  - SET/GET/INCR basic commands through proxy
  - AUTH authentication (correct password, wrong password, no password)
  - Admin port commands: INFO, STATS, BACKEND LIST, CONNECTIONS, LOG LEVEL
  - Connection handling

Usage:
  1. Start a Redis server on port 6379
  2. Start the proxy:
     ./redis-mux-proxy -c proxy.yaml
  3. Run tests:
     python3 test_basic.py
"""

import socket
import time
import sys
import os

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6380       # Proxy listen port for backend
ADMIN_PORT = 9090       # Admin port
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379       # Backend Redis port
PROXY_PASSWORD = ""     # Set if proxy has a password

# -------- Helpers --------

class RedisConn:
    """Simple Redis client using raw sockets."""

    def __init__(self, host, port, timeout=5):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.buf = b""

    def close(self):
        self.sock.close()

    def send_command(self, *args):
        """Send a RESP command."""
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg = str(arg)
            cmd += f"${len(arg)}\r\n{arg}\r\n"
        self.sock.sendall(cmd.encode())

    def send_inline(self, line):
        """Send an inline command."""
        self.sock.sendall((line + "\r\n").encode())

    def read_response(self):
        """Read one RESP response."""
        while True:
            if self.buf:
                resp, rest = self._parse_resp(self.buf)
                if resp is not None:
                    self.buf = rest
                    return resp
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionError("Connection closed")
            self.buf += data

    def _parse_resp(self, data):
        """Parse a single RESP response. Returns (result, remaining_bytes) or (None, data) if incomplete."""
        if not data:
            return None, data

        prefix = chr(data[0])

        if prefix == '+':
            return self._read_line(data)
        elif prefix == '-':
            line, rest = self._read_line(data)
            if line is not None:
                return Exception(line), rest
            return None, data
        elif prefix == ':':
            line, rest = self._read_line(data)
            if line is not None:
                return int(line), rest
            return None, data
        elif prefix == '$':
            line, rest = self._read_line(data)
            if line is None:
                return None, data
            length = int(line)
            if length == -1:
                return None, rest
            if len(rest) < length + 2:
                return None, data
            return rest[:length].decode(), rest[length + 2:]
        elif prefix == '*':
            line, rest = self._read_line(data)
            if line is None:
                return None, data
            count = int(line)
            if count == -1:
                return None, rest
            items = []
            current = rest
            for _ in range(count):
                item, current = self._parse_resp(current)
                if item is None:
                    return None, data
                items.append(item)
            return items, current
        else:
            # Try inline
            return self._read_line(data)

    def _read_line(self, data):
        idx = data.find(b"\r\n")
        if idx == -1:
            return None, data
        line = data[1:idx].decode()  # skip prefix byte
        return line, data[idx + 2:]

    def command(self, *args):
        """Send command and return response."""
        self.send_command(*args)
        return self.read_response()


def assert_eq(actual, expected, msg=""):
    if actual != expected:
        print(f"  FAIL: {msg} expected={expected!r}, got={actual!r}")
        return False
    return True


def assert_ok(resp, msg=""):
    return assert_eq(resp, "OK", msg)


def assert_error(resp, msg=""):
    if not isinstance(resp, Exception):
        print(f"  FAIL: {msg} expected error, got={resp!r}")
        return False
    return True


# -------- Test Cases --------

class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def report(self):
        total = self.passed + self.failed + self.skipped
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{total} passed, {self.failed} failed, {self.skipped} skipped")
        return self.failed == 0


results = TestResults()


def run_test(name, func):
    """Run a test function and track results."""
    print(f"\n--- {name} ---")
    try:
        ok = func()
        if ok:
            print(f"  PASS")
            results.passed += 1
        else:
            print(f"  FAILED")
            results.failed += 1
    except ConnectionRefusedError:
        print(f"  SKIP (connection refused - is proxy running?)")
        results.skipped += 1
    except Exception as e:
        print(f"  ERROR: {e}")
        results.failed += 1


def test_basic_set_get():
    """Test SET/GET through proxy."""
    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        if PROXY_PASSWORD:
            resp = c.command("AUTH", PROXY_PASSWORD)
            if not assert_ok(resp, "AUTH"):
                return False

        resp = c.command("SET", "test_key_1", "hello_proxy")
        if not assert_ok(resp, "SET"):
            return False

        resp = c.command("GET", "test_key_1")
        if not assert_eq(resp, "hello_proxy", "GET"):
            return False

        return True
    finally:
        c.close()


def test_incr():
    """Test INCR command through proxy."""
    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        c.command("SET", "test_counter", "0")
        resp = c.command("INCR", "test_counter")
        if not assert_eq(resp, 1, "INCR"):
            return False

        resp = c.command("INCR", "test_counter")
        if not assert_eq(resp, 2, "INCR second"):
            return False

        return True
    finally:
        c.close()


def test_pipeline():
    """Test pipelined commands through proxy."""
    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        # Send multiple commands at once
        c.send_command("SET", "pipe_key1", "val1")
        c.send_command("SET", "pipe_key2", "val2")
        c.send_command("GET", "pipe_key1")
        c.send_command("GET", "pipe_key2")

        r1 = c.read_response()
        r2 = c.read_response()
        r3 = c.read_response()
        r4 = c.read_response()

        if not assert_ok(r1, "SET pipe1"):
            return False
        if not assert_ok(r2, "SET pipe2"):
            return False
        if not assert_eq(r3, "val1", "GET pipe1"):
            return False
        if not assert_eq(r4, "val2", "GET pipe2"):
            return False

        return True
    finally:
        c.close()


def test_auth_correct_password():
    """Test AUTH with correct proxy password."""
    if not PROXY_PASSWORD:
        print("  SKIP (no proxy password configured)")
        results.skipped += 1
        return True

    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        resp = c.command("AUTH", PROXY_PASSWORD)
        return assert_ok(resp, "AUTH correct password")
    finally:
        c.close()


def test_auth_wrong_password():
    """Test AUTH with wrong proxy password."""
    if not PROXY_PASSWORD:
        print("  SKIP (no proxy password configured)")
        results.skipped += 1
        return True

    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        resp = c.command("AUTH", "wrong_password_12345")
        return assert_error(resp, "AUTH wrong password")
    finally:
        c.close()


def test_auth_no_password_configured():
    """Test AUTH when proxy has no password set."""
    if PROXY_PASSWORD:
        print("  SKIP (proxy password is configured)")
        results.skipped += 1
        return True

    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        resp = c.command("AUTH", "some_password")
        return assert_error(resp, "AUTH no password set")
    finally:
        c.close()


def test_noauth_without_password():
    """Test that commands work without AUTH when no password is set."""
    if PROXY_PASSWORD:
        print("  SKIP (proxy password is configured)")
        results.skipped += 1
        return True

    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        resp = c.command("PING")
        # In non-auth mode, commands should go through
        return True
    finally:
        c.close()


def test_admin_ping():
    """Test admin port PING command."""
    c = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        resp = c.command("PING")
        return assert_eq(resp, "PONG", "PING")
    finally:
        c.close()


def test_admin_proxy_info():
    """Test PROXY INFO command on admin port."""
    c = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        resp = c.command("PROXY", "INFO")
        if resp is None:
            print("  FAIL: empty response")
            return False
        # Response should be a bulk string containing stats
        if "redis_mux_proxy_version" not in str(resp):
            print(f"  FAIL: expected version in response, got: {resp[:100]}")
            return False
        if "uptime_in_seconds" not in str(resp):
            print(f"  FAIL: expected uptime in response")
            return False
        return True
    finally:
        c.close()


def test_admin_proxy_stats():
    """Test PROXY STATS command on admin port."""
    c = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        resp = c.command("PROXY", "STATS")
        # Should return an array
        if not isinstance(resp, list):
            print(f"  FAIL: expected array, got {type(resp)}")
            return False
        return True
    finally:
        c.close()


def test_admin_backend_list():
    """Test PROXY BACKEND LIST command on admin port."""
    c = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        resp = c.command("PROXY", "BACKEND", "LIST")
        if not isinstance(resp, list):
            print(f"  FAIL: expected array, got {type(resp)}")
            return False
        # Should have at least one backend
        if len(resp) < 1:
            print(f"  FAIL: expected at least 1 backend, got {len(resp)}")
            return False
        return True
    finally:
        c.close()


def test_admin_connections():
    """Test PROXY CONNECTIONS command on admin port."""
    c = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        resp = c.command("PROXY", "CONNECTIONS")
        if not isinstance(resp, list):
            print(f"  FAIL: expected array, got {type(resp)}")
            return False
        return True
    finally:
        c.close()


def test_admin_log_level():
    """Test PROXY LOG LEVEL command on admin port."""
    c = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        # Change log level to DEBUG
        resp = c.command("PROXY", "LOG", "LEVEL", "DEBUG")
        if not assert_ok(resp, "LOG LEVEL DEBUG"):
            return False

        # Change back to INFO
        resp = c.command("PROXY", "LOG", "LEVEL", "INFO")
        if not assert_ok(resp, "LOG LEVEL INFO"):
            return False

        return True
    finally:
        c.close()


def test_multiple_clients():
    """Test multiple concurrent clients."""
    clients = []
    try:
        for i in range(5):
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            clients.append(c)

        # Each client sets a different key
        for i, c in enumerate(clients):
            c.command("SET", f"multi_key_{i}", f"value_{i}")

        # Each client reads its own key
        for i, c in enumerate(clients):
            resp = c.command("GET", f"multi_key_{i}")
            if not assert_eq(resp, f"value_{i}", f"client {i} GET"):
                return False

        return True
    finally:
        for c in clients:
            c.close()


def test_connection_close_reopen():
    """Test that connections can be closed and reopened."""
    for i in range(3):
        c = RedisConn(PROXY_HOST, PROXY_PORT)
        try:
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            resp = c.command("SET", "reopen_key", f"val_{i}")
            if not assert_ok(resp, f"SET iteration {i}"):
                return False
        finally:
            c.close()

    # Verify last value
    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)
        resp = c.command("GET", "reopen_key")
        return assert_eq(resp, "val_2", "GET after reopen")
    finally:
        c.close()


# -------- Main --------

def main():
    global PROXY_PORT, ADMIN_PORT, PROXY_PASSWORD

    # Allow command line overrides
    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--admin-port="):
            ADMIN_PORT = int(arg.split("=")[1])
        elif arg.startswith("--password="):
            PROXY_PASSWORD = arg.split("=")[1]

    print(f"Redis MUX Proxy Basic Tests")
    print(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")
    print(f"Admin: {PROXY_HOST}:{ADMIN_PORT}")
    print(f"Password: {'***' if PROXY_PASSWORD else '(none)'}")

    # Basic functionality
    run_test("Basic SET/GET", test_basic_set_get)
    run_test("INCR command", test_incr)
    run_test("Pipeline commands", test_pipeline)

    # Auth tests
    run_test("AUTH correct password", test_auth_correct_password)
    run_test("AUTH wrong password", test_auth_wrong_password)
    run_test("AUTH no password configured", test_auth_no_password_configured)
    run_test("Commands without AUTH (no password)", test_noauth_without_password)

    # Admin port tests
    run_test("Admin PING", test_admin_ping)
    run_test("Admin PROXY INFO", test_admin_proxy_info)
    run_test("Admin PROXY STATS", test_admin_proxy_stats)
    run_test("Admin PROXY BACKEND LIST", test_admin_backend_list)
    run_test("Admin PROXY CONNECTIONS", test_admin_connections)
    run_test("Admin PROXY LOG LEVEL", test_admin_log_level)

    # Concurrency tests
    run_test("Multiple clients", test_multiple_clients)
    run_test("Connection close/reopen", test_connection_close_reopen)

    ok = results.report()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
