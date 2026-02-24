#!/usr/bin/env python3
"""
Hot Upgrade Persistent Connection Test

This script creates multiple PERSISTENT TCP connections to the proxy,
continuously sends Redis commands over the SAME connection, and measures
the impact of hot upgrade on latency, QPS, and connection stability.
"""

import socket
import time
import threading
import sys
import os
import subprocess
import statistics

PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6479
ADMIN_PORT = 9090
NUM_CLIENTS = 10
REDIS_CLI = "/home/feixuwu/MyCode/redis/src/redis-cli"

# Global state
stop_event = threading.Event()
upgrade_triggered = threading.Event()
upgrade_complete = threading.Event()
results = {}
results_lock = threading.Lock()


def send_redis_cmd(sock, *args):
    """Send a RESP command and read the response."""
    # Build RESP array
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    sock.sendall(cmd.encode())

    # Read response
    response = b""
    while True:
        data = sock.recv(4096)
        if not data:
            raise ConnectionError("Connection closed by remote")
        response += data
        if response.endswith(b"\r\n"):
            break

    return response.decode().strip()


def persistent_client(client_id):
    """A single persistent client that keeps a connection open and sends commands."""
    counter_key = f"persistent:counter:{client_id}"
    latencies_pre = []
    latencies_during = []
    latencies_post = []
    errors = []
    reconnects = 0
    total_ops = 0
    connection_alive = True

    sock = None
    try:
        # Establish connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect((PROXY_HOST, PROXY_PORT))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # Initialize counter
        send_redis_cmd(sock, "SET", counter_key, "0")

        while not stop_event.is_set():
            try:
                start = time.monotonic()
                resp = send_redis_cmd(sock, "INCR", counter_key)
                end = time.monotonic()

                latency_us = (end - start) * 1_000_000
                total_ops += 1

                if upgrade_complete.is_set():
                    latencies_post.append(latency_us)
                elif upgrade_triggered.is_set():
                    latencies_during.append(latency_us)
                else:
                    latencies_pre.append(latency_us)

            except (ConnectionError, socket.timeout, BrokenPipeError, OSError) as e:
                errors.append((total_ops, str(e), time.time()))
                connection_alive = False

                # Try to reconnect
                try:
                    if sock:
                        sock.close()
                    time.sleep(0.1)
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5.0)
                    sock.connect((PROXY_HOST, PROXY_PORT))
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    reconnects += 1
                    connection_alive = True
                except Exception as e2:
                    errors.append((total_ops, f"Reconnect failed: {e2}", time.time()))
                    time.sleep(0.5)

    except Exception as e:
        errors.append((0, f"Initial connect failed: {e}", time.time()))
    finally:
        if sock:
            try:
                sock.close()
            except:
                pass

    # Read final counter value via separate connection
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3.0)
        s.connect((PROXY_HOST, PROXY_PORT))
        final_val = send_redis_cmd(s, "GET", counter_key)
        s.close()
        # Parse the RESP bulk string response
        if final_val.startswith("$"):
            lines = final_val.split("\r\n")
            if len(lines) >= 2:
                final_val = lines[1]
    except:
        final_val = "UNKNOWN"

    with results_lock:
        results[client_id] = {
            "total_ops": total_ops,
            "errors": len(errors),
            "error_details": errors[:10],
            "reconnects": reconnects,
            "connection_alive": connection_alive,
            "final_counter": final_val,
            "latency_pre": latencies_pre,
            "latency_during": latencies_during,
            "latency_post": latencies_post,
        }


def qps_monitor():
    """Monitor QPS by counting PINGs per second."""
    qps_log = []
    while not stop_event.is_set():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((PROXY_HOST, PROXY_PORT))

            count = 0
            start = time.monotonic()
            while time.monotonic() - start < 2.0 and not stop_event.is_set():
                send_redis_cmd(s, "PING")
                count += 1

            elapsed = time.monotonic() - start
            qps = count / elapsed if elapsed > 0 else 0
            s.close()

            phase = "post_upgrade" if upgrade_complete.is_set() else \
                    "during_upgrade" if upgrade_triggered.is_set() else "pre_upgrade"
            qps_log.append((time.strftime("%H:%M:%S"), phase, int(qps)))

        except Exception as e:
            phase = "post_upgrade" if upgrade_complete.is_set() else \
                    "during_upgrade" if upgrade_triggered.is_set() else "pre_upgrade"
            qps_log.append((time.strftime("%H:%M:%S"), phase, 0))
            time.sleep(0.5)

    with results_lock:
        results["qps_log"] = qps_log


def main():
    print("=" * 60)
    print("  Hot Upgrade Persistent Connection Test")
    print("=" * 60)
    print()

    # Check proxy is running
    proxy_pid = subprocess.check_output(
        ["pgrep", "-f", "redis-mux-proxy"], text=True
    ).strip().split('\n')[0]
    print(f"[INFO] Proxy running with PID={proxy_pid}")

    # Launch persistent clients
    print(f"\n[PHASE 1] Launching {NUM_CLIENTS} persistent clients...")
    threads = []
    for i in range(NUM_CLIENTS):
        t = threading.Thread(target=persistent_client, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    # Launch QPS monitor
    qps_thread = threading.Thread(target=qps_monitor, daemon=True)
    qps_thread.start()

    # Pre-upgrade baseline
    print("[PHASE 1b] Running pre-upgrade baseline for 8 seconds...")
    time.sleep(8)

    # Trigger hot upgrade
    print("\n[PHASE 2] Triggering hot upgrade...")
    upgrade_triggered.set()
    upgrade_start = time.monotonic()

    result = subprocess.run(
        [REDIS_CLI, "-p", str(ADMIN_PORT), "PROXY", "UPGRADE"],
        capture_output=True, text=True, timeout=10
    )
    print(f"  Upgrade command: {result.stdout.strip()}")

    time.sleep(5)

    upgrade_end = time.monotonic()
    upgrade_duration_ms = int((upgrade_end - upgrade_start) * 1000)

    upgrade_complete.set()

    new_pid = subprocess.check_output(
        ["pgrep", "-f", "redis-mux-proxy"], text=True
    ).strip().split('\n')[0]
    print(f"  Old PID: {proxy_pid} -> New PID: {new_pid}")
    print(f"  Upgrade duration: {upgrade_duration_ms}ms")

    # Post-upgrade phase
    print("\n[PHASE 3] Running post-upgrade for 8 seconds...")
    time.sleep(8)

    # Stop all clients
    print("\n[PHASE 4] Stopping clients...")
    stop_event.set()
    for t in threads:
        t.join(timeout=5)
    qps_thread.join(timeout=5)

    # Print results
    print()
    print("=" * 60)
    print("  TEST RESULTS")
    print("=" * 60)

    # QPS timeline
    print("\n--- QPS Timeline ---")
    qps_log = results.get("qps_log", [])
    for ts, phase, qps in qps_log:
        marker = " <<<" if phase == "during_upgrade" else ""
        print(f"  {ts} [{phase:>15s}] {qps:>6d} ops/sec{marker}")

    # Per-client results
    print(f"\n--- Per-Client Results ({NUM_CLIENTS} persistent connections) ---")
    total_ops = 0
    total_errors = 0
    total_reconnects = 0
    data_correct = 0
    all_pre_latencies = []
    all_during_latencies = []
    all_post_latencies = []

    for i in range(NUM_CLIENTS):
        r = results.get(i, {})
        ops = r.get("total_ops", 0)
        errs = r.get("errors", 0)
        recons = r.get("reconnects", 0)
        alive = r.get("connection_alive", False)
        final = r.get("final_counter", "?")

        total_ops += ops
        total_errors += errs
        total_reconnects += recons

        all_pre_latencies.extend(r.get("latency_pre", []))
        all_during_latencies.extend(r.get("latency_during", []))
        all_post_latencies.extend(r.get("latency_post", []))

        # Check data correctness
        expected = str(ops)
        correct = (final == expected)
        if correct:
            data_correct += 1

        status = "✅" if errs == 0 and alive else "⚠️"
        print(f"  Client {i:2d}: {status} ops={ops:>6d} errors={errs} reconnects={recons} "
              f"alive={alive} counter={final} (expected={expected}) "
              f"{'✅' if correct else '❌ MISMATCH'}")

        # Print error details
        for err_ops, err_msg, err_time in r.get("error_details", []):
            print(f"           └─ Error at op#{err_ops}: {err_msg}")

    # Latency statistics
    print("\n--- Latency Statistics (microseconds) ---")

    def print_latency_stats(name, latencies):
        if not latencies:
            print(f"  {name:>20s}: (no data)")
            return
        p50 = statistics.median(latencies)
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        avg = statistics.mean(latencies)
        mx = max(latencies)
        print(f"  {name:>20s}: avg={avg:>8.0f}  p50={p50:>8.0f}  p99={p99:>8.0f}  max={mx:>8.0f}  samples={len(latencies)}")

    print_latency_stats("Pre-upgrade", all_pre_latencies)
    print_latency_stats("During upgrade", all_during_latencies)
    print_latency_stats("Post-upgrade", all_post_latencies)

    # Summary
    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  PID change:           {proxy_pid} -> {new_pid} ({'✅ OK' if proxy_pid != new_pid else '❌ FAILED'})")
    print(f"  Upgrade duration:     {upgrade_duration_ms}ms")
    print(f"  Total operations:     {total_ops}")
    print(f"  Total errors:         {total_errors}")
    print(f"  Total reconnects:     {total_reconnects}")
    print(f"  Data correctness:     {data_correct}/{NUM_CLIENTS}")
    print(f"  Connection keepalive: {NUM_CLIENTS - total_reconnects}/{NUM_CLIENTS} stayed alive")
    print()

    if total_errors == 0 and proxy_pid != new_pid and data_correct == NUM_CLIENTS:
        print("  ✅ HOT UPGRADE TEST PASSED - Zero errors, all connections maintained!")
    elif total_reconnects > 0:
        print(f"  ⚠️  HOT UPGRADE TEST - {total_reconnects} reconnects needed")
        print("     This means some connections were broken during upgrade.")
        print("     Current implementation transfers listen fds but not client fds.")
    else:
        print("  ❌ HOT UPGRADE TEST FAILED")

    print()


if __name__ == "__main__":
    main()
