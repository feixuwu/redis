#!/usr/bin/env python3
"""
Performance Benchmark: 50 Concurrent Connections over 1 MUX Connection

Tests throughput (ops/sec) and latency (avg, p50, p95, p99) for:
  1. SET/GET pipeline (50 connections, pure SET+GET)
  2. Mixed workload (SET/GET + INCR + MGET + MULTI/EXEC)
  3. Large value test (1KB, 4KB values)

All 50 connections share 1 worker = 1 MUX TCP connection.

Usage:
  python3 test_perf_50conn.py [--proxy-port=6479] [--admin-port=9090] [--duration=10]
"""

import socket
import threading
import time
import sys
import statistics

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6479
ADMIN_PORT = 9090
DURATION = 10
NUM_CONNS = 50


class RedisConn:
    """Simple RESP protocol connection."""
    def __init__(self, host, port, timeout=10):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((host, port))
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def send_raw(self, data):
        self.sock.sendall(data)

    def send_command(self, *args):
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            s = str(arg)
            cmd += f"${len(s)}\r\n{s}\r\n"
        self.sock.sendall(cmd.encode())

    def send_command_bytes(self, *args):
        """Send command supporting bytes arguments."""
        parts = [f"*{len(args)}\r\n".encode()]
        for arg in args:
            if isinstance(arg, bytes):
                parts.append(f"${len(arg)}\r\n".encode())
                parts.append(arg)
                parts.append(b"\r\n")
            else:
                s = str(arg)
                parts.append(f"${len(s)}\r\n{s}\r\n".encode())
        self.sock.sendall(b"".join(parts))

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
        if not data or len(data) == 0:
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
            return rest[:length].decode(errors='replace'), rest[length + 2:]
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

    def command_bytes(self, *args):
        self.send_command_bytes(*args)
        return self.read_response()


def get_proxy_info():
    try:
        c = RedisConn(PROXY_HOST, ADMIN_PORT, timeout=3)
        resp = c.command("PROXY", "INFO")
        c.close()
        if isinstance(resp, str):
            info = {}
            for line in resp.split('\n'):
                line = line.strip()
                if ':' in line and not line.startswith('#'):
                    k, v = line.split(':', 1)
                    info[k] = v
            return info
    except Exception:
        pass
    return {}


# ==================== Benchmark Workers ====================

def setget_worker(worker_id, duration, results):
    """Pure SET + GET benchmark, measure per-op latency."""
    latencies = []
    errors = 0
    ops = 0
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=duration + 5)
        end_time = time.monotonic() + duration
        i = 0
        while time.monotonic() < end_time:
            key = f"perf_sg_{worker_id}_{i % 100}"
            val = f"val_{worker_id}_{i}"

            t0 = time.monotonic()
            resp = c.command("SET", key, val)
            if resp != "OK":
                errors += 1
                i += 1
                continue
            resp = c.command("GET", key)
            t1 = time.monotonic()

            if resp != val:
                errors += 1
            else:
                latencies.append((t1 - t0) * 1000)  # ms
                ops += 1
            i += 1
        c.close()
    except Exception as e:
        errors += 1
    results[worker_id] = {"ops": ops, "errors": errors, "latencies": latencies}


def mixed_worker(worker_id, duration, results):
    """Mixed workload: SET/GET, INCR, MULTI/EXEC."""
    latencies = []
    errors = 0
    ops = 0
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=duration + 5)
        end_time = time.monotonic() + duration
        i = 0
        while time.monotonic() < end_time:
            op_type = i % 4
            key = f"perf_mx_{worker_id}_{i % 50}"

            t0 = time.monotonic()
            try:
                if op_type == 0:
                    # SET + GET
                    val = f"mxv_{worker_id}_{i}"
                    c.command("SET", key, val)
                    resp = c.command("GET", key)
                    if resp != val:
                        errors += 1
                        i += 1
                        continue
                elif op_type == 1:
                    # INCR
                    incr_key = f"perf_incr_{worker_id}"
                    resp = c.command("INCR", incr_key)
                    if not isinstance(resp, int):
                        errors += 1
                        i += 1
                        continue
                elif op_type == 2:
                    # MGET (3 keys)
                    keys = [f"perf_mx_{worker_id}_{j}" for j in range(3)]
                    resp = c.command("MGET", *keys)
                    if not isinstance(resp, list) or len(resp) != 3:
                        errors += 1
                        i += 1
                        continue
                elif op_type == 3:
                    # MULTI/EXEC
                    tx_key = f"perf_tx_{worker_id}"
                    c.command("MULTI")
                    c.command("SET", tx_key, f"txv_{i}")
                    c.command("INCR", f"perf_txcnt_{worker_id}")
                    resp = c.command("EXEC")
                    if not isinstance(resp, list) or len(resp) != 2:
                        errors += 1
                        i += 1
                        continue

                t1 = time.monotonic()
                latencies.append((t1 - t0) * 1000)
                ops += 1
            except Exception:
                errors += 1

            i += 1
        c.close()
    except Exception:
        errors += 1
    results[worker_id] = {"ops": ops, "errors": errors, "latencies": latencies}


def largevalue_worker(worker_id, duration, value_size, results):
    """SET/GET with large values."""
    latencies = []
    errors = 0
    ops = 0
    val_payload = "X" * value_size
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=duration + 5)
        end_time = time.monotonic() + duration
        i = 0
        while time.monotonic() < end_time:
            key = f"perf_lv_{worker_id}_{i % 20}"

            t0 = time.monotonic()
            resp = c.command("SET", key, val_payload)
            if resp != "OK":
                errors += 1
                i += 1
                continue
            resp = c.command("GET", key)
            t1 = time.monotonic()

            if resp != val_payload:
                errors += 1
            else:
                latencies.append((t1 - t0) * 1000)
                ops += 1
            i += 1
        c.close()
    except Exception:
        errors += 1
    results[worker_id] = {"ops": ops, "errors": errors, "latencies": latencies}


# ==================== Stats Helpers ====================

def calc_stats(all_latencies, total_ops, total_errors, duration):
    """Calculate and return stats dict."""
    if not all_latencies:
        return None
    all_latencies.sort()
    throughput = total_ops / duration
    avg_lat = statistics.mean(all_latencies)
    p50 = all_latencies[int(len(all_latencies) * 0.50)]
    p95 = all_latencies[int(len(all_latencies) * 0.95)]
    p99 = all_latencies[min(int(len(all_latencies) * 0.99), len(all_latencies) - 1)]
    p999 = all_latencies[min(int(len(all_latencies) * 0.999), len(all_latencies) - 1)]
    max_lat = all_latencies[-1]
    min_lat = all_latencies[0]
    return {
        "throughput": throughput,
        "total_ops": total_ops,
        "total_errors": total_errors,
        "avg": avg_lat,
        "min": min_lat,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "p999": p999,
        "max": max_lat,
    }


def print_stats(title, st):
    if st is None:
        print(f"  {title}: NO DATA")
        return
    print(f"  {title}:")
    print(f"    Throughput:  {st['throughput']:,.0f} ops/sec  (total: {st['total_ops']:,} ops, errors: {st['total_errors']})")
    print(f"    Latency(ms): avg={st['avg']:.2f}  min={st['min']:.2f}  "
          f"p50={st['p50']:.2f}  p95={st['p95']:.2f}  p99={st['p99']:.2f}  "
          f"p999={st['p999']:.2f}  max={st['max']:.2f}")


def run_benchmark(name, worker_fn, num_conns, duration, extra_args=None):
    """Run a benchmark with num_conns concurrent connections."""
    print(f"\n  [{name}] Starting {num_conns} connections for {duration}s...")

    results = {}
    threads = []
    for wid in range(num_conns):
        if extra_args:
            t = threading.Thread(target=worker_fn, args=(wid, duration, *extra_args, results), daemon=True)
        else:
            t = threading.Thread(target=worker_fn, args=(wid, duration, results), daemon=True)
        threads.append(t)

    # Check streams before
    info_before = get_proxy_info()
    streams_before = info_before.get("mux_streams", "?")

    t_start = time.monotonic()
    for t in threads:
        t.start()

    # Sample mid-run stream count
    time.sleep(min(2, duration / 2))
    info_mid = get_proxy_info()
    streams_mid = info_mid.get("mux_streams", "?")
    mux_conns_mid = info_mid.get("mux_connections", "?")

    for t in threads:
        t.join(timeout=duration + 15)

    t_end = time.monotonic()
    actual_duration = t_end - t_start

    # Aggregate
    all_latencies = []
    total_ops = 0
    total_errors = 0
    for wid in range(num_conns):
        r = results.get(wid, {"ops": 0, "errors": 0, "latencies": []})
        total_ops += r["ops"]
        total_errors += r["errors"]
        all_latencies.extend(r["latencies"])

    st = calc_stats(all_latencies, total_ops, total_errors, actual_duration)
    print(f"  [{name}] MUX: conns={mux_conns_mid}, streams={streams_mid} (before={streams_before})")
    print_stats(name, st)
    return st


# ==================== Direct Redis Baseline ====================

def direct_setget_worker(worker_id, duration, redis_port, results):
    """SET+GET directly to Redis (bypass proxy) for baseline comparison."""
    latencies = []
    errors = 0
    ops = 0
    try:
        c = RedisConn(PROXY_HOST, redis_port, timeout=duration + 5)
        end_time = time.monotonic() + duration
        i = 0
        while time.monotonic() < end_time:
            key = f"dperf_sg_{worker_id}_{i % 100}"
            val = f"val_{worker_id}_{i}"

            t0 = time.monotonic()
            resp = c.command("SET", key, val)
            if resp != "OK":
                errors += 1
                i += 1
                continue
            resp = c.command("GET", key)
            t1 = time.monotonic()

            if resp != val:
                errors += 1
            else:
                latencies.append((t1 - t0) * 1000)
                ops += 1
            i += 1
        c.close()
    except Exception:
        errors += 1
    results[worker_id] = {"ops": ops, "errors": errors, "latencies": latencies}


# ==================== Main ====================

def main():
    global PROXY_PORT, ADMIN_PORT, DURATION, NUM_CONNS

    redis_port = 6379
    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--admin-port="):
            ADMIN_PORT = int(arg.split("=")[1])
        elif arg.startswith("--duration="):
            DURATION = int(arg.split("=")[1])
        elif arg.startswith("--conns="):
            NUM_CONNS = int(arg.split("=")[1])
        elif arg.startswith("--redis-port="):
            redis_port = int(arg.split("=")[1])

    print("=" * 70)
    print("  Performance Benchmark: MUX Mode")
    print("=" * 70)

    info = get_proxy_info()
    mux_enabled = info.get("mux_enabled", "unknown")
    workers = info.get("worker_threads", "unknown")
    mux_conns = info.get("mux_connections", "unknown")
    print(f"  Proxy:    {PROXY_HOST}:{PROXY_PORT}")
    print(f"  Admin:    {PROXY_HOST}:{ADMIN_PORT}")
    print(f"  Config:   workers={workers}, mux_enabled={mux_enabled}, mux_connections={mux_conns}")
    print(f"  Clients:  {NUM_CONNS} concurrent connections")
    print(f"  Duration: {DURATION}s per benchmark")

    if mux_enabled != "yes":
        print("  ERROR: MUX is not enabled!")
        sys.exit(1)

    # Warmup
    print(f"\n  Warming up ({NUM_CONNS} connections)...")
    conns = []
    for i in range(NUM_CONNS):
        try:
            c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
            c.command("SET", f"warmup_{i}", "ok")
            conns.append(c)
        except Exception as e:
            print(f"  Warmup connection {i} failed: {e}")
    for c in conns:
        c.close()
    time.sleep(1)
    print(f"  Warmup done. {len(conns)} connections established successfully.")

    results = {}

    # ---- Benchmark 1: Pure SET+GET ----
    st1 = run_benchmark("SET+GET (small values)", setget_worker, NUM_CONNS, DURATION)
    results["setget_small"] = st1
    time.sleep(1)

    # ---- Benchmark 2: Mixed workload ----
    st2 = run_benchmark("Mixed (SET/GET/INCR/MGET/TX)", mixed_worker, NUM_CONNS, DURATION)
    results["mixed"] = st2
    time.sleep(1)

    # ---- Benchmark 3: Large values (1KB) ----
    st3 = run_benchmark("SET+GET (1KB values)", largevalue_worker, NUM_CONNS, DURATION,
                        extra_args=(1024,))
    results["setget_1kb"] = st3
    time.sleep(1)

    # ---- Benchmark 4: Large values (4KB) ----
    st4 = run_benchmark("SET+GET (4KB values)", largevalue_worker, NUM_CONNS, DURATION,
                        extra_args=(4096,))
    results["setget_4kb"] = st4
    time.sleep(1)

    # ---- Benchmark 5: Direct Redis baseline (for comparison) ----
    print(f"\n  [Direct Redis Baseline] Testing direct connection to Redis port {redis_port}...")
    try:
        test_c = RedisConn(PROXY_HOST, redis_port, timeout=3)
        test_c.command("PING")
        test_c.close()
        has_direct = True
    except Exception:
        has_direct = False
        print(f"  Cannot connect to Redis at port {redis_port}, skipping baseline.")

    st_direct = None
    if has_direct:
        # Use 50 direct connections to Redis
        print(f"  [Direct Redis] Starting {NUM_CONNS} direct connections for {DURATION}s...")
        direct_results = {}
        threads = []
        for wid in range(NUM_CONNS):
            t = threading.Thread(target=direct_setget_worker,
                                 args=(wid, DURATION, redis_port, direct_results), daemon=True)
            threads.append(t)

        t_start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=DURATION + 15)
        t_end = time.monotonic()

        all_lat = []
        total_ops = 0
        total_err = 0
        for wid in range(NUM_CONNS):
            r = direct_results.get(wid, {"ops": 0, "errors": 0, "latencies": []})
            total_ops += r["ops"]
            total_err += r["errors"]
            all_lat.extend(r["latencies"])

        st_direct = calc_stats(all_lat, total_ops, total_err, t_end - t_start)
        print_stats("Direct Redis SET+GET", st_direct)
        results["direct_redis"] = st_direct

    # ---- Final Summary ----
    print()
    print("=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print()
    print(f"  {'Benchmark':<35} {'Throughput':>12} {'Avg(ms)':>10} {'P50(ms)':>10} {'P95(ms)':>10} {'P99(ms)':>10} {'Errors':>8}")
    print(f"  {'-'*35} {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8}")

    for name, key in [("SET+GET (small)", "setget_small"),
                       ("Mixed workload", "mixed"),
                       ("SET+GET (1KB)", "setget_1kb"),
                       ("SET+GET (4KB)", "setget_4kb"),
                       ("Direct Redis (baseline)", "direct_redis")]:
        st = results.get(key)
        if st:
            print(f"  {name:<35} {st['throughput']:>10,.0f}/s {st['avg']:>10.2f} "
                  f"{st['p50']:>10.2f} {st['p95']:>10.2f} {st['p99']:>10.2f} {st['total_errors']:>8}")
        else:
            print(f"  {name:<35} {'N/A':>12} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>8}")

    if st_direct and st1:
        overhead = (st1['avg'] - st_direct['avg'])
        overhead_pct = (overhead / st_direct['avg']) * 100 if st_direct['avg'] > 0 else 0
        throughput_ratio = st1['throughput'] / st_direct['throughput'] if st_direct['throughput'] > 0 else 0
        print()
        print(f"  Proxy overhead (SET+GET vs Direct):")
        print(f"    Latency overhead:    +{overhead:.2f}ms ({overhead_pct:+.1f}%)")
        print(f"    Throughput ratio:    {throughput_ratio:.2%} of direct Redis")

    print()
    print("=" * 70)

    # Check for errors
    all_errors = sum(st.get('total_errors', 0) for st in results.values() if st)
    if all_errors == 0:
        print("  ALL BENCHMARKS COMPLETED WITH ZERO ERRORS!")
    else:
        print(f"  WARNING: {all_errors} total errors across all benchmarks")
    print("=" * 70)


if __name__ == "__main__":
    main()
