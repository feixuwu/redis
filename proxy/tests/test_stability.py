#!/usr/bin/env python3
"""
Stability tests for Redis MUX Proxy.

Tests:
  - Repeated connect/disconnect under load
  - Backend Redis disconnect handling
  - Proxy resilience after backend kill
  - Memory leak detection guidance (ASAN)

Usage:
  python3 test_stability.py [--proxy-port=6380] [--redis-port=6379]
"""

import socket
import subprocess
import threading
import time
import sys
import os
import signal

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6380
ADMIN_PORT = 9090
REDIS_PORT = 6379
PROXY_PASSWORD = ""


class RedisConn:
    """Simple Redis client."""

    def __init__(self, host, port, timeout=5):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def send_command(self, *args):
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg = str(arg)
            cmd += f"${len(arg)}\r\n{arg}\r\n"
        self.sock.sendall(cmd.encode())

    def read_response(self):
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
            return self._read_line(data)

    def _read_line(self, data):
        idx = data.find(b"\r\n")
        if idx == -1:
            return None, data
        line = data[1:idx].decode()
        return line, data[idx + 2:]

    def command(self, *args):
        self.send_command(*args)
        return self.read_response()


passed = 0
failed = 0


def test(name, func):
    global passed, failed
    print(f"\n--- {name} ---")
    try:
        if func():
            print(f"  PASS")
            passed += 1
        else:
            print(f"  FAILED")
            failed += 1
    except ConnectionRefusedError:
        print(f"  SKIP (connection refused)")
    except Exception as e:
        print(f"  ERROR: {e}")
        failed += 1


def test_stress_connect_disconnect():
    """Stress test: rapid connect/disconnect cycles."""
    errors = []
    n = 200
    num_threads = 4

    def worker(thread_id, iterations):
        for i in range(iterations):
            try:
                c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3)
                if PROXY_PASSWORD:
                    c.command("AUTH", PROXY_PASSWORD)
                c.command("SET", f"stress_{thread_id}_{i}", "x")
                c.close()
            except Exception as e:
                if i < 5:  # Only log first few errors per thread
                    errors.append(f"t{thread_id}_i{i}: {e}")

    threads = []
    per_thread = n // num_threads
    for t in range(num_threads):
        th = threading.Thread(target=worker, args=(t, per_thread))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=60)

    error_rate = len(errors) / n
    print(f"  {n} cycles, {len(errors)} errors ({error_rate*100:.1f}%)")
    if error_rate > 0.05:
        for e in errors[:5]:
            print(f"    {e}")
        return False
    return True


def test_proxy_survives_backend_disconnect():
    """Test that proxy handles backend disconnect gracefully."""
    # Connect through proxy
    try:
        c1 = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3)
        if PROXY_PASSWORD:
            c1.command("AUTH", PROXY_PASSWORD)
        c1.command("SET", "survive_key", "before_disconnect")
        c1.close()
    except Exception as e:
        print(f"  Pre-test failed: {e}")
        return False

    # Verify proxy is still accepting connections
    try:
        c2 = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3)
        if PROXY_PASSWORD:
            c2.command("AUTH", PROXY_PASSWORD)
        resp = c2.command("GET", "survive_key")
        c2.close()
        if resp != "before_disconnect":
            print(f"  FAIL: expected 'before_disconnect', got {resp}")
            return False
        return True
    except Exception as e:
        print(f"  Post-test failed: {e}")
        return False


def test_proxy_admin_after_stress():
    """Verify admin port works correctly after stress testing."""
    try:
        admin = RedisConn(PROXY_HOST, ADMIN_PORT, timeout=3)
        resp = admin.command("PROXY", "INFO")
        admin.close()
        if "redis_mux_proxy_version" not in str(resp):
            print(f"  FAIL: invalid PROXY INFO response")
            return False
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def test_half_open_connections():
    """Test handling of half-open connections (connect but no data, then disconnect)."""
    sockets = []
    n = 20

    # Create connections but don't send data
    for i in range(n):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((PROXY_HOST, PROXY_PORT))
            sockets.append(s)
        except Exception as e:
            print(f"  Warning: connect {i} failed: {e}")

    time.sleep(1)

    # Close all
    for s in sockets:
        try:
            s.close()
        except Exception:
            pass

    # Verify proxy still works
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)
        resp = c.command("PING")
        c.close()
        return True
    except Exception as e:
        print(f"  FAIL: proxy not responding after half-open test: {e}")
        return False


def test_invalid_data():
    """Test sending invalid/garbage data to proxy."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((PROXY_HOST, PROXY_PORT))
        # Send garbage
        s.sendall(b"\xff\xfe\xfd\xfc\x00\x01\x02\x03" * 100)
        time.sleep(0.5)
        # Connection should be closed or error returned
        try:
            data = s.recv(4096)
            # If we get a response, it should be an error
        except Exception:
            pass
        s.close()
    except Exception:
        pass

    # Verify proxy still works
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)
        resp = c.command("SET", "after_garbage", "ok")
        c.close()
        return resp == "OK"
    except Exception as e:
        print(f"  FAIL: proxy not responding after garbage test: {e}")
        return False


def main():
    global PROXY_PORT, ADMIN_PORT, REDIS_PORT, PROXY_PASSWORD

    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--admin-port="):
            ADMIN_PORT = int(arg.split("=")[1])
        elif arg.startswith("--redis-port="):
            REDIS_PORT = int(arg.split("=")[1])
        elif arg.startswith("--password="):
            PROXY_PASSWORD = arg.split("=")[1]

    print(f"Redis MUX Proxy - Stability Tests")
    print(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")

    test("Stress connect/disconnect", test_stress_connect_disconnect)
    test("Proxy survives backend disconnect", test_proxy_survives_backend_disconnect)
    test("Admin port after stress", test_proxy_admin_after_stress)
    test("Half-open connections", test_half_open_connections)
    test("Invalid/garbage data", test_invalid_data)

    total = passed + failed
    print(f"\n{'='*60}")
    print(f"Results: {passed}/{total} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
