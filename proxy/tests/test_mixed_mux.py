#!/usr/bin/env python3
"""
Mixed command test for Redis MUX Proxy.

Tests SUBSCRIBE, BLPOP, MULTI/EXEC transactions, and normal commands
running concurrently through the MUX proxy for an extended period.

This verifies that the MUX protocol correctly handles:
1. Long-lived connections (SUBSCRIBE, BLPOP)
2. Multi-command transactions (MULTI/EXEC)
3. Normal short-lived commands (SET/GET/INCR)
4. Data isolation between different streams
5. No response mixing/corruption under concurrent load

Usage:
  python3 test_mixed_mux.py [--proxy-port=6479] [--duration=30]
"""

import socket
import threading
import time
import sys
import random
import traceback

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6479
DURATION = 30  # seconds to run the test
PROXY_PASSWORD = ""

# -------- Simple Redis Connection --------

class RedisConn:
    def __init__(self, host, port, timeout=10):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.buf = b""
        self.lock = threading.Lock()

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

    def read_response(self, timeout=None):
        old_timeout = self.sock.gettimeout()
        if timeout is not None:
            self.sock.settimeout(timeout)
        try:
            while True:
                if self.buf:
                    resp, rest = self._parse_resp(self.buf)
                    if resp is not None:
                        self.buf = rest
                        return resp
                data = self.sock.recv(65536)
                if not data:
                    raise ConnectionError("Connection closed")
                self.buf += data
        finally:
            if timeout is not None:
                self.sock.settimeout(old_timeout)

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
                return "nil", rest
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


# -------- Test Workers --------

class TestStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.pub_sent = 0
        self.sub_received = 0
        self.lpush_sent = 0
        self.blpop_received = 0
        self.tx_completed = 0
        self.tx_errors = 0
        self.normal_ops = 0
        self.normal_errors = 0
        self.errors = []

    def add_error(self, msg):
        with self.lock:
            if len(self.errors) < 50:
                self.errors.append(msg)


stats = TestStats()
stop_event = threading.Event()


def subscriber_worker():
    """Worker that subscribes to a channel and counts received messages."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=DURATION + 10)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        # Subscribe to test channel
        c.send_command("SUBSCRIBE", "mux_test_channel")

        # Read subscribe confirmation
        resp = c.read_response(timeout=5)
        if not isinstance(resp, list) or len(resp) < 3 or resp[0] != "subscribe":
            stats.add_error(f"SUBSCRIBE confirmation failed: {resp}")
            c.close()
            return

        # Read messages until stopped
        while not stop_event.is_set():
            try:
                resp = c.read_response(timeout=1)
                if isinstance(resp, list) and len(resp) >= 3 and resp[0] == "message":
                    with stats.lock:
                        stats.sub_received += 1
            except socket.timeout:
                continue
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"SUB read error: {e}")
                break

        # Unsubscribe before closing
        try:
            c.send_command("UNSUBSCRIBE", "mux_test_channel")
            c.read_response(timeout=2)
        except Exception:
            pass
        c.close()
    except Exception as e:
        stats.add_error(f"Subscriber error: {e}")


def publisher_worker():
    """Worker that publishes messages to the channel."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        msg_id = 0
        while not stop_event.is_set():
            try:
                resp = c.command("PUBLISH", "mux_test_channel", f"msg_{msg_id}")
                if isinstance(resp, int):
                    with stats.lock:
                        stats.pub_sent += 1
                msg_id += 1
                time.sleep(0.05)  # 20 msgs/sec
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"PUB error: {e}")
                # Reconnect
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                    if PROXY_PASSWORD:
                        c.command("AUTH", PROXY_PASSWORD)
                except Exception:
                    break

        c.close()
    except Exception as e:
        stats.add_error(f"Publisher error: {e}")


def blpop_worker():
    """Worker that does BLPOP on a list, receiving items pushed by lpush_worker."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=DURATION + 10)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        while not stop_event.is_set():
            try:
                # BLPOP with 1 second timeout
                c.send_command("BLPOP", "mux_test_list", "1")
                resp = c.read_response(timeout=3)

                if isinstance(resp, list) and len(resp) == 2:
                    # Got an item
                    with stats.lock:
                        stats.blpop_received += 1
                # None/nil means timeout, continue
            except socket.timeout:
                continue
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"BLPOP error: {e}")
                break

        c.close()
    except Exception as e:
        stats.add_error(f"BLPOP worker error: {e}")


def lpush_worker():
    """Worker that pushes items to the list for BLPOP."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        item_id = 0
        while not stop_event.is_set():
            try:
                c.command("LPUSH", "mux_test_list", f"item_{item_id}")
                with stats.lock:
                    stats.lpush_sent += 1
                item_id += 1
                time.sleep(0.05)  # 20 items/sec
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"LPUSH error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                    if PROXY_PASSWORD:
                        c.command("AUTH", PROXY_PASSWORD)
                except Exception:
                    break

        c.close()
    except Exception as e:
        stats.add_error(f"LPUSH worker error: {e}")


def transaction_worker(worker_id):
    """Worker that runs MULTI/EXEC transactions."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        tx_id = 0
        while not stop_event.is_set():
            try:
                key = f"mux_tx_{worker_id}"

                # MULTI
                resp = c.command("MULTI")
                if not isinstance(resp, str) or resp != "OK":
                    stats.add_error(f"TX{worker_id}: MULTI failed: {resp}")
                    with stats.lock:
                        stats.tx_errors += 1
                    continue

                # Queue commands
                c.command("SET", key, f"val_{tx_id}")
                c.command("INCR", f"mux_tx_counter_{worker_id}")
                c.command("GET", key)

                # EXEC
                resp = c.command("EXEC")
                if isinstance(resp, list) and len(resp) == 3:
                    # Verify: SET should return OK, INCR returns int, GET returns value
                    if resp[0] == "OK" and isinstance(resp[1], int) and resp[2] == f"val_{tx_id}":
                        with stats.lock:
                            stats.tx_completed += 1
                    else:
                        stats.add_error(f"TX{worker_id}: EXEC result mismatch: {resp}")
                        with stats.lock:
                            stats.tx_errors += 1
                elif isinstance(resp, Exception):
                    stats.add_error(f"TX{worker_id}: EXEC error: {resp}")
                    with stats.lock:
                        stats.tx_errors += 1
                else:
                    stats.add_error(f"TX{worker_id}: EXEC unexpected: {resp}")
                    with stats.lock:
                        stats.tx_errors += 1

                tx_id += 1
                time.sleep(0.02)  # ~50 tx/sec per worker
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"TX{worker_id} error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                    if PROXY_PASSWORD:
                        c.command("AUTH", PROXY_PASSWORD)
                except Exception:
                    break

        c.close()
    except Exception as e:
        stats.add_error(f"TX worker {worker_id} error: {e}")


def normal_ops_worker(worker_id):
    """Worker that performs normal SET/GET/INCR/DEL operations."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)

        op_id = 0
        while not stop_event.is_set():
            try:
                key = f"mux_normal_{worker_id}_{op_id % 100}"
                val = f"v_{worker_id}_{op_id}"

                # SET
                resp = c.command("SET", key, val)
                if resp != "OK":
                    stats.add_error(f"NORMAL{worker_id}: SET failed: {resp}")
                    with stats.lock:
                        stats.normal_errors += 1
                    continue

                # GET and verify
                resp = c.command("GET", key)
                if resp != val:
                    stats.add_error(f"NORMAL{worker_id}: GET mismatch: expected={val}, got={resp}")
                    with stats.lock:
                        stats.normal_errors += 1
                    continue

                with stats.lock:
                    stats.normal_ops += 1

                op_id += 1
                time.sleep(0.01)  # ~100 ops/sec per worker
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"NORMAL{worker_id} error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                    if PROXY_PASSWORD:
                        c.command("AUTH", PROXY_PASSWORD)
                except Exception:
                    break

        c.close()
    except Exception as e:
        stats.add_error(f"Normal worker {worker_id} error: {e}")


# -------- Main --------

def print_progress():
    """Print live statistics."""
    with stats.lock:
        s = stats
        print(f"\r  SUB: {s.sub_received}/{s.pub_sent} | "
              f"BLPOP: {s.blpop_received}/{s.lpush_sent} | "
              f"TX: {s.tx_completed}(ok)/{s.tx_errors}(err) | "
              f"Normal: {s.normal_ops}(ok)/{s.normal_errors}(err) | "
              f"Errors: {len(s.errors)}", end="", flush=True)


def main():
    global PROXY_PORT, DURATION, PROXY_PASSWORD

    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--duration="):
            DURATION = int(arg.split("=")[1])
        elif arg.startswith("--password="):
            PROXY_PASSWORD = arg.split("=")[1]

    print(f"MUX Proxy Mixed Command Test")
    print(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")
    print(f"Duration: {DURATION} seconds")
    print()

    # Clean up test keys
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        if PROXY_PASSWORD:
            c.command("AUTH", PROXY_PASSWORD)
        c.command("DEL", "mux_test_list")
        c.close()
    except Exception as e:
        print(f"Warning: cleanup failed: {e}")

    # Start all workers
    threads = []
    print("[1/4] Starting SUB/PUB workers...")
    threads.append(threading.Thread(target=subscriber_worker, name="subscriber"))
    time.sleep(0.5)  # Let subscriber connect first
    threads.append(threading.Thread(target=publisher_worker, name="publisher"))

    print("[2/4] Starting BLPOP/LPUSH workers...")
    threads.append(threading.Thread(target=blpop_worker, name="blpop"))
    threads.append(threading.Thread(target=lpush_worker, name="lpush"))

    print("[3/4] Starting transaction workers (x2)...")
    threads.append(threading.Thread(target=transaction_worker, args=(0,), name="tx_0"))
    threads.append(threading.Thread(target=transaction_worker, args=(1,), name="tx_1"))

    print("[4/4] Starting normal ops workers (x3)...")
    threads.append(threading.Thread(target=normal_ops_worker, args=(0,), name="normal_0"))
    threads.append(threading.Thread(target=normal_ops_worker, args=(1,), name="normal_1"))
    threads.append(threading.Thread(target=normal_ops_worker, args=(2,), name="normal_2"))

    for t in threads:
        t.daemon = True
        t.start()

    print(f"\nRunning for {DURATION} seconds...\n")

    # Monitor progress
    start_time = time.time()
    while time.time() - start_time < DURATION:
        time.sleep(1)
        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed:3d}s/{DURATION}s]", end="")
        print_progress()
        print()

    # Stop all workers
    print("\nStopping workers...")
    stop_event.set()

    for t in threads:
        t.join(timeout=10)

    alive = sum(1 for t in threads if t.is_alive())
    if alive > 0:
        print(f"  Warning: {alive} threads still alive after timeout")

    # Print results
    print()
    print("=" * 60)
    print("  RESULTS")
    print("=" * 60)

    all_pass = True

    # PUB/SUB check
    pub_ok = stats.pub_sent > 0 and stats.sub_received > 0
    sub_rate = (stats.sub_received / stats.pub_sent * 100) if stats.pub_sent > 0 else 0
    status = "PASS" if pub_ok else "FAIL"
    print(f"  [{status}] PUB/SUB: published={stats.pub_sent}, received={stats.sub_received} ({sub_rate:.0f}%)")
    if not pub_ok:
        all_pass = False

    # BLPOP check
    blpop_ok = stats.lpush_sent > 0 and stats.blpop_received > 0
    blpop_rate = (stats.blpop_received / stats.lpush_sent * 100) if stats.lpush_sent > 0 else 0
    status = "PASS" if blpop_ok else "FAIL"
    print(f"  [{status}] BLPOP:   pushed={stats.lpush_sent}, popped={stats.blpop_received} ({blpop_rate:.0f}%)")
    if not blpop_ok:
        all_pass = False

    # Transaction check
    tx_ok = stats.tx_completed > 0 and stats.tx_errors == 0
    status = "PASS" if tx_ok else "FAIL"
    print(f"  [{status}] TX:      completed={stats.tx_completed}, errors={stats.tx_errors}")
    if not tx_ok:
        all_pass = False

    # Normal ops check
    normal_ok = stats.normal_ops > 0 and stats.normal_errors == 0
    status = "PASS" if normal_ok else "FAIL"
    print(f"  [{status}] Normal:  ops={stats.normal_ops}, errors={stats.normal_errors}")
    if not normal_ok:
        all_pass = False

    # Error details
    if stats.errors:
        print(f"\n  WARNING: Errors ({len(stats.errors)}):")
        for i, err in enumerate(stats.errors[:10]):
            print(f"    [{i+1}] {err}")
        if len(stats.errors) > 10:
            print(f"    ... and {len(stats.errors) - 10} more")

    print()
    print("=" * 60)
    if all_pass:
        print("  ALL CHECKS PASSED - MUX mode mixed commands working correctly!")
    else:
        print("  SOME CHECKS FAILED")
    print("=" * 60)

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
