#!/usr/bin/env python3
"""
Dynamic Worker Add/Remove Tests for Redis MUX Proxy.

Tests that workers can be dynamically added and removed at runtime
via the admin port, while maintaining data correctness and service continuity.

Usage:
  python3 test_dynamic_workers.py [--proxy-port=6479] [--admin-port=9090]
"""

import socket
import time
import sys
import threading
import traceback

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6479
ADMIN_PORT = 9090
PROXY_PASSWORD = ""


# -------- Helpers --------

class RedisConn:
    """Simple Redis client using raw sockets."""

    def __init__(self, host, port, timeout=5):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except:
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


def assert_eq(actual, expected, msg=""):
    if actual != expected:
        print(f"  FAIL: {msg} expected={expected!r}, got={actual!r}")
        return False
    return True


def assert_ok(resp, msg=""):
    if isinstance(resp, Exception):
        print(f"  FAIL: {msg} got error: {resp}")
        return False
    # Response may be "OK" or "OK ..." (with extra info)
    if not str(resp).startswith("OK"):
        print(f"  FAIL: {msg} expected OK..., got={resp!r}")
        return False
    return True


def assert_error(resp, msg=""):
    if not isinstance(resp, Exception):
        print(f"  FAIL: {msg} expected error, got={resp!r}")
        return False
    return True


def get_worker_count(admin):
    """Get current worker count via admin command."""
    resp = admin.command("PROXY", "WORKER")
    return resp


def add_workers(admin, count):
    """Add workers via admin command."""
    return admin.command("PROXY", "WORKER", "ADD", str(count))


def remove_workers(admin, count):
    """Remove workers via admin command."""
    return admin.command("PROXY", "WORKER", "REMOVE", str(count))


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
        print(f"  SKIP (connection refused)")
        results.skipped += 1
    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()
        results.failed += 1


def test_query_initial_workers():
    """Test querying the initial worker count."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        count = get_worker_count(admin)
        print(f"  Initial worker count: {count}")
        if not isinstance(count, int):
            print(f"  FAIL: expected integer, got {type(count)}")
            return False
        if count < 1:
            print(f"  FAIL: expected at least 1 worker, got {count}")
            return False
        return True
    finally:
        admin.close()


def test_add_workers():
    """Test adding workers dynamically."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        initial = get_worker_count(admin)
        print(f"  Initial workers: {initial}")

        # Add 2 workers
        resp = add_workers(admin, 2)
        if not assert_ok(resp, "ADD 2 workers"):
            return False

        time.sleep(0.3)  # Allow workers to start

        new_count = get_worker_count(admin)
        print(f"  After adding 2: {new_count}")
        if not assert_eq(new_count, initial + 2, "worker count after ADD"):
            return False

        return True
    finally:
        admin.close()


def test_data_correctness_after_add():
    """Test data correctness after adding workers."""
    # Workers have been added in previous test. Verify data operations work.
    errors = 0
    conns = []
    try:
        for i in range(10):
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            conns.append(c)

        # Each connection writes and reads
        for i, c in enumerate(conns):
            key = f"dyn_worker_test_{i}"
            val = f"value_{i}_{time.time()}"
            resp = c.command("SET", key, val)
            if not isinstance(resp, str) or not resp.startswith("OK"):
                print(f"  FAIL: SET {key} returned {resp}")
                errors += 1
                continue
            got = c.command("GET", key)
            if got != val:
                print(f"  FAIL: GET {key} expected {val}, got {got}")
                errors += 1

        print(f"  Verified {len(conns)} connections, errors: {errors}")
        return errors == 0
    finally:
        for c in conns:
            c.close()


def test_concurrent_load_with_workers():
    """Test concurrent load across multiple workers."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        worker_count = get_worker_count(admin)
        print(f"  Current workers: {worker_count}")
    finally:
        admin.close()

    errors = []
    ops_count = [0]
    lock = threading.Lock()

    def client_work(thread_id, num_ops):
        try:
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            for i in range(num_ops):
                key = f"load_t{thread_id}_k{i}"
                val = f"v{thread_id}_{i}"
                resp = c.command("SET", key, val)
                if not isinstance(resp, str) or not resp.startswith("OK"):
                    with lock:
                        errors.append(f"Thread {thread_id}: SET {key} -> {resp}")
                    continue
                got = c.command("GET", key)
                if got != val:
                    with lock:
                        errors.append(f"Thread {thread_id}: GET {key} expected {val}, got {got}")
                with lock:
                    ops_count[0] += 1
            c.close()
        except Exception as e:
            with lock:
                errors.append(f"Thread {thread_id}: exception {e}")

    threads = []
    num_threads = 8
    ops_per_thread = 50
    for i in range(num_threads):
        t = threading.Thread(target=client_work, args=(i, ops_per_thread))
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=30)

    print(f"  Completed {ops_count[0]} ops across {num_threads} threads, errors: {len(errors)}")
    if errors:
        for e in errors[:5]:
            print(f"    {e}")
    return len(errors) == 0


def test_remove_workers():
    """Test removing workers dynamically."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        current = get_worker_count(admin)
        print(f"  Current workers: {current}")

        if current <= 1:
            print("  SKIP: only 1 worker, cannot remove")
            results.skipped += 1
            return True

        # Remove 1 worker
        resp = remove_workers(admin, 1)
        if not assert_ok(resp, "REMOVE 1 worker"):
            return False

        time.sleep(0.3)  # Allow cleanup

        new_count = get_worker_count(admin)
        print(f"  After removing 1: {new_count}")
        if not assert_eq(new_count, current - 1, "worker count after REMOVE"):
            return False

        return True
    finally:
        admin.close()


def test_data_correctness_after_remove():
    """Test data correctness after removing workers."""
    errors = 0
    conns = []
    try:
        for i in range(8):
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            conns.append(c)

        for i, c in enumerate(conns):
            key = f"after_remove_{i}"
            val = f"rv_{i}_{time.time()}"
            resp = c.command("SET", key, val)
            if not isinstance(resp, str) or not resp.startswith("OK"):
                print(f"  FAIL: SET {key} returned {resp}")
                errors += 1
                continue
            got = c.command("GET", key)
            if got != val:
                print(f"  FAIL: GET {key} expected {val}, got {got}")
                errors += 1

        print(f"  Verified {len(conns)} connections, errors: {errors}")
        return errors == 0
    finally:
        for c in conns:
            c.close()


def test_cannot_remove_all_workers():
    """Test that removing all workers is rejected (must keep at least 1)."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        current = get_worker_count(admin)
        print(f"  Current workers: {current}")

        # Try to remove all workers
        resp = remove_workers(admin, current)
        if not assert_error(resp, "REMOVE all workers"):
            return False

        # Verify count unchanged
        after = get_worker_count(admin)
        if not assert_eq(after, current, "worker count unchanged after failed REMOVE"):
            return False

        print(f"  Correctly rejected removing all {current} workers")
        return True
    finally:
        admin.close()


def test_add_workers_under_load():
    """Test adding workers while clients are actively sending requests."""
    stop_flag = threading.Event()
    errors = []
    ops_count = [0]
    lock = threading.Lock()

    def client_loop(thread_id):
        try:
            c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=10)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            i = 0
            while not stop_flag.is_set():
                key = f"add_load_t{thread_id}_k{i}"
                val = f"v{i}"
                resp = c.command("SET", key, val)
                if not isinstance(resp, str) or not resp.startswith("OK"):
                    with lock:
                        errors.append(f"T{thread_id}: SET -> {resp}")
                    continue
                got = c.command("GET", key)
                if got != val:
                    with lock:
                        errors.append(f"T{thread_id}: GET mismatch {val} vs {got}")
                with lock:
                    ops_count[0] += 1
                i += 1
            c.close()
        except Exception as e:
            if not stop_flag.is_set():
                with lock:
                    errors.append(f"T{thread_id}: {e}")

    # Start background load
    threads = []
    for i in range(4):
        t = threading.Thread(target=client_loop, args=(i,))
        t.start()
        threads.append(t)

    time.sleep(0.5)  # Let load build up

    # Now add workers while load is running
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        before = get_worker_count(admin)
        print(f"  Workers before ADD: {before}, ops so far: {ops_count[0]}")

        resp = add_workers(admin, 2)
        if not assert_ok(resp, "ADD 2 under load"):
            stop_flag.set()
            for t in threads:
                t.join(timeout=5)
            return False

        after = get_worker_count(admin)
        print(f"  Workers after ADD: {after}")

        # Let it run a bit more with new workers
        time.sleep(1.0)
    finally:
        admin.close()

    stop_flag.set()
    for t in threads:
        t.join(timeout=5)

    print(f"  Total ops: {ops_count[0]}, errors: {len(errors)}")
    if errors:
        for e in errors[:5]:
            print(f"    {e}")
    return len(errors) == 0


def test_remove_workers_under_load():
    """Test removing workers while clients are actively sending requests."""
    stop_flag = threading.Event()
    errors = []
    ops_count = [0]
    reconnects = [0]
    lock = threading.Lock()

    def client_loop(thread_id):
        c = None
        try:
            c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=10)
            if PROXY_PASSWORD:
                c.command("AUTH", PROXY_PASSWORD)
            i = 0
            while not stop_flag.is_set():
                try:
                    key = f"rm_load_t{thread_id}_k{i}"
                    val = f"v{i}"
                    resp = c.command("SET", key, val)
                    if not isinstance(resp, str) or not resp.startswith("OK"):
                        with lock:
                            errors.append(f"T{thread_id}: SET -> {resp}")
                        continue
                    got = c.command("GET", key)
                    if got != val:
                        with lock:
                            errors.append(f"T{thread_id}: GET mismatch")
                    with lock:
                        ops_count[0] += 1
                    i += 1
                except (ConnectionError, BrokenPipeError, OSError):
                    # Connection may be closed when worker is removed, reconnect
                    with lock:
                        reconnects[0] += 1
                    try:
                        if c:
                            c.close()
                    except:
                        pass
                    time.sleep(0.1)
                    try:
                        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=10)
                        if PROXY_PASSWORD:
                            c.command("AUTH", PROXY_PASSWORD)
                    except:
                        pass
        except Exception as e:
            if not stop_flag.is_set():
                with lock:
                    errors.append(f"T{thread_id}: {e}")
        finally:
            if c:
                try:
                    c.close()
                except:
                    pass

    # Start background load
    threads = []
    for i in range(4):
        t = threading.Thread(target=client_loop, args=(i,))
        t.start()
        threads.append(t)

    time.sleep(0.5)

    # Remove workers while load is running
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        before = get_worker_count(admin)
        print(f"  Workers before REMOVE: {before}, ops so far: {ops_count[0]}")

        if before <= 1:
            print("  SKIP: only 1 worker, adding more first")
            add_workers(admin, 2)
            time.sleep(0.3)
            before = get_worker_count(admin)
            print(f"  Workers after prep ADD: {before}")

        # Remove 1 worker
        resp = remove_workers(admin, 1)
        if not assert_ok(resp, "REMOVE 1 under load"):
            stop_flag.set()
            for t in threads:
                t.join(timeout=5)
            return False

        after = get_worker_count(admin)
        print(f"  Workers after REMOVE: {after}")

        # Let it stabilize
        time.sleep(1.0)
    finally:
        admin.close()

    stop_flag.set()
    for t in threads:
        t.join(timeout=5)

    print(f"  Total ops: {ops_count[0]}, reconnects: {reconnects[0]}, errors: {len(errors)}")
    if errors:
        for e in errors[:5]:
            print(f"    {e}")
    # Allow some reconnects (expected when worker is removed), but no data errors
    return len(errors) == 0


def test_rapid_add_remove_cycles():
    """Stress test: rapidly add and remove workers in cycles."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        initial = get_worker_count(admin)
        print(f"  Initial workers: {initial}")

        cycles = 5
        for cycle in range(cycles):
            # Add 2 workers
            resp = add_workers(admin, 2)
            if not assert_ok(resp, f"cycle {cycle} ADD"):
                return False

            count_after_add = get_worker_count(admin)

            # Quick data check
            c = RedisConn(PROXY_HOST, PROXY_PORT)
            try:
                if PROXY_PASSWORD:
                    c.command("AUTH", PROXY_PASSWORD)
                key = f"cycle_{cycle}"
                c.command("SET", key, f"val_{cycle}")
                got = c.command("GET", key)
                if got != f"val_{cycle}":
                    print(f"  FAIL: cycle {cycle} data mismatch: {got}")
                    return False
            finally:
                c.close()

            # Remove 2 workers
            resp = remove_workers(admin, 2)
            if not assert_ok(resp, f"cycle {cycle} REMOVE"):
                return False

            count_after_remove = get_worker_count(admin)
            print(f"  Cycle {cycle}: {initial} -> +2={count_after_add} -> -2={count_after_remove}")

            if count_after_remove != count_after_add - 2:
                print(f"  FAIL: count mismatch after cycle {cycle}")
                return False

        final = get_worker_count(admin)
        print(f"  Final workers: {final} (expected: {initial})")
        return assert_eq(final, initial, "final worker count")
    finally:
        admin.close()


def test_restore_to_initial():
    """Cleanup: restore worker count to initial value (1 worker)."""
    admin = RedisConn(PROXY_HOST, ADMIN_PORT)
    try:
        current = get_worker_count(admin)
        print(f"  Current workers: {current}")

        if current > 1:
            resp = remove_workers(admin, current - 1)
            if not assert_ok(resp, f"REMOVE {current - 1} to restore"):
                return False

        final = get_worker_count(admin)
        print(f"  Restored to: {final} worker(s)")
        return assert_eq(final, 1, "restored to 1 worker")
    finally:
        admin.close()


# -------- Main --------

def main():
    global PROXY_PORT, ADMIN_PORT, PROXY_PASSWORD

    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--admin-port="):
            ADMIN_PORT = int(arg.split("=")[1])
        elif arg.startswith("--password="):
            PROXY_PASSWORD = arg.split("=")[1]

    print(f"Dynamic Worker Add/Remove Tests")
    print(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")
    print(f"Admin: {PROXY_HOST}:{ADMIN_PORT}")
    print(f"{'='*60}")

    # Test sequence
    run_test("1. Query initial worker count", test_query_initial_workers)
    run_test("2. Add workers", test_add_workers)
    run_test("3. Data correctness after adding workers", test_data_correctness_after_add)
    run_test("4. Concurrent load with multiple workers", test_concurrent_load_with_workers)
    run_test("5. Remove workers", test_remove_workers)
    run_test("6. Data correctness after removing workers", test_data_correctness_after_remove)
    run_test("7. Cannot remove all workers", test_cannot_remove_all_workers)
    run_test("8. Add workers under active load", test_add_workers_under_load)
    run_test("9. Remove workers under active load", test_remove_workers_under_load)
    run_test("10. Rapid add/remove cycles (stress)", test_rapid_add_remove_cycles)
    run_test("11. Restore to initial state", test_restore_to_initial)

    ok = results.report()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
