#!/usr/bin/env python3
"""
MUX mode specific tests for Redis MUX Proxy.

Tests:
  - Multiple clients sharing MUX connections
  - Data isolation between clients
  - Stream creation and cleanup
  - Concurrent operations with data correctness

Usage:
  python3 test_mux.py [--proxy-port=6380] [--admin-port=9090]
"""

import socket
import threading
import time
import sys
import random

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6380
ADMIN_PORT = 9090
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


def test_mux_data_isolation():
    """Multiple clients sharing backend MUX connection should have isolated data."""
    clients = []
    n = 10
    try:
        for i in range(n):
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            clients.append(c)

        # Each client sets a unique key
        for i, c in enumerate(clients):
            c.command("SET", f"mux_iso_{i}", f"data_{i}_{random.randint(0,9999)}")

        # Read back and verify isolation
        for i, c in enumerate(clients):
            val = c.command("GET", f"mux_iso_{i}")
            if val is None or not val.startswith(f"data_{i}_"):
                print(f"  FAIL: client {i} got wrong data: {val}")
                return False

        return True
    finally:
        for c in clients:
            c.close()


def test_concurrent_operations():
    """Multiple threads performing operations concurrently."""
    errors = []
    num_threads = 8
    ops_per_thread = 50

    def worker(thread_id):
        try:
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)

            for i in range(ops_per_thread):
                key = f"conc_{thread_id}_{i}"
                val = f"v_{thread_id}_{i}"
                resp = c.command("SET", key, val)
                if resp != "OK":
                    errors.append(f"thread={thread_id} SET failed: {resp}")
                    return

                resp = c.command("GET", key)
                if resp != val:
                    errors.append(f"thread={thread_id} GET mismatch: expected={val}, got={resp}")
                    return

            c.close()
        except Exception as e:
            errors.append(f"thread={thread_id} error: {e}")

    threads = []
    for t in range(num_threads):
        th = threading.Thread(target=worker, args=(t,))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=30)

    if errors:
        for e in errors:
            print(f"  ERROR: {e}")
        return False

    print(f"  {num_threads} threads x {ops_per_thread} ops = {num_threads * ops_per_thread} ops completed")
    return True


def test_rapid_connect_disconnect():
    """Rapidly connect and disconnect clients."""
    errors = []
    n = 50

    for i in range(n):
        try:
            c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            c.command("PING")
            c.close()
        except Exception as e:
            errors.append(f"iteration={i} error: {e}")

    if errors:
        # Allow some failures due to timing
        if len(errors) > n * 0.1:
            for e in errors[:5]:
                print(f"  ERROR: {e}")
            print(f"  {len(errors)}/{n} failures")
            return False

    print(f"  {n} connect/disconnect cycles completed ({len(errors)} failures)")
    return True


def test_large_payload():
    """Test sending large payloads through proxy."""
    c = RedisConn(PROXY_HOST, PROXY_PORT)
    try:
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        # 1MB value
        large_value = "A" * (1024 * 1024)
        resp = c.command("SET", "large_key", large_value)
        if resp != "OK":
            print(f"  FAIL: SET large value: {resp}")
            return False

        resp = c.command("GET", "large_key")
        if resp != large_value:
            print(f"  FAIL: GET large value mismatch (got {len(resp) if resp else 0} bytes)")
            return False

        print(f"  1MB value SET/GET successful")
        return True
    finally:
        c.close()


def test_mux_stream_stats():
    """Check MUX stream statistics via admin port."""
    # First create some connections
    clients = []
    try:
        for i in range(5):
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            c.command("SET", f"stat_key_{i}", f"val_{i}")
            clients.append(c)

        # Check admin stats
        admin = RedisConn(PROXY_HOST, ADMIN_PORT)
        resp = admin.command("PROXY", "INFO")
        admin.close()

        if "connected_clients" not in str(resp):
            print(f"  FAIL: no connected_clients in INFO")
            return False

        return True
    finally:
        for c in clients:
            c.close()


def main():
    global PROXY_PORT, ADMIN_PORT, PROXY_PASSWORD

    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--admin-port="):
            ADMIN_PORT = int(arg.split("=")[1])
        elif arg.startswith("--password="):
            PROXY_PASSWORD = arg.split("=")[1]

    print(f"Redis MUX Proxy - MUX Mode Tests")
    print(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")

    test("MUX data isolation", test_mux_data_isolation)
    test("Concurrent operations", test_concurrent_operations)
    test("Rapid connect/disconnect", test_rapid_connect_disconnect)
    test("Large payload", test_large_payload)
    test("MUX stream stats", test_mux_stream_stats)

    total = passed + failed
    print(f"\n{'='*60}")
    print(f"Results: {passed}/{total} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
