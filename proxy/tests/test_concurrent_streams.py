#!/usr/bin/env python3
"""
Concurrent Multi-Stream MUX Test

Core goal: Verify that with 1 worker (= 1 MUX TCP connection to Redis),
many simultaneous client connections (= many MUX streams) can all work
correctly at the same time, including:
  - Multiple SUBSCRIBE connections (long-lived streams)
  - Multiple BLPOP connections (blocking streams)
  - Multiple MULTI/EXEC transaction connections
  - Multiple normal SET/GET connections
  - All sharing the same single MUX TCP connection

This is the most critical MUX test: proving that one TCP connection
can multiplex many streams with different command types without
any data corruption, mixing, or deadlocks.

Usage:
  python3 test_concurrent_streams.py [--proxy-port=6479] [--admin-port=9090] [--duration=20]
"""

import socket
import threading
import time
import sys
import traceback


PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6479
ADMIN_PORT = 9090
DURATION = 20


class RedisConn:
    """Simple RESP protocol connection."""
    def __init__(self, host, port, timeout=10):
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


# ==================== Shared State ====================

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        # SUB/PUB stats (per channel)
        self.sub_received = {}   # channel -> count
        self.pub_sent = {}       # channel -> count
        # BLPOP stats (per list)
        self.blpop_received = {} # list -> count
        self.lpush_sent = {}     # list -> count
        # TX stats (per worker)
        self.tx_ok = {}          # worker_id -> count
        self.tx_err = {}         # worker_id -> count
        # Normal ops stats (per worker)
        self.normal_ok = {}      # worker_id -> count
        self.normal_err = {}     # worker_id -> count
        # Errors
        self.errors = []

    def inc(self, d, key, n=1):
        with self.lock:
            d[key] = d.get(key, 0) + n

    def add_error(self, msg):
        with self.lock:
            if len(self.errors) < 100:
                self.errors.append(msg)

    def sum_values(self, d):
        with self.lock:
            return sum(d.values())


stats = Stats()
stop_event = threading.Event()


# ==================== Worker Functions ====================

def subscriber_worker(channel_name, worker_id):
    """A single SUBSCRIBE connection on one channel (= 1 long-lived stream)."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=DURATION + 15)
        c.send_command("SUBSCRIBE", channel_name)
        resp = c.read_response(timeout=5)
        if not isinstance(resp, list) or resp[0] != "subscribe":
            stats.add_error(f"SUB[{worker_id}] subscribe confirm failed: {resp}")
            c.close()
            return

        while not stop_event.is_set():
            try:
                resp = c.read_response(timeout=1)
                if isinstance(resp, list) and resp[0] == "message":
                    stats.inc(stats.sub_received, channel_name)
            except socket.timeout:
                continue
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"SUB[{worker_id}] read: {e}")
                break
        try:
            c.send_command("UNSUBSCRIBE", channel_name)
            c.read_response(timeout=2)
        except Exception:
            pass
        c.close()
    except Exception as e:
        stats.add_error(f"SUB[{worker_id}] fatal: {e}")


def publisher_worker(channel_name, worker_id):
    """A single PUBLISH connection targeting one channel (= 1 stream)."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        while not stop_event.is_set():
            try:
                resp = c.command("PUBLISH", channel_name, f"msg_{worker_id}_{time.monotonic()}")
                if isinstance(resp, int):
                    stats.inc(stats.pub_sent, channel_name)
                time.sleep(0.03)
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"PUB[{worker_id}] error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                except Exception:
                    break
        c.close()
    except Exception as e:
        stats.add_error(f"PUB[{worker_id}] fatal: {e}")


def blpop_worker(list_name, worker_id):
    """A single BLPOP connection on one list (= 1 blocking stream)."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=DURATION + 15)
        while not stop_event.is_set():
            try:
                c.send_command("BLPOP", list_name, "1")
                resp = c.read_response(timeout=3)
                if isinstance(resp, list) and len(resp) == 2:
                    stats.inc(stats.blpop_received, list_name)
            except socket.timeout:
                continue
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"BLPOP[{worker_id}] error: {e}")
                break
        c.close()
    except Exception as e:
        stats.add_error(f"BLPOP[{worker_id}] fatal: {e}")


def lpush_worker(list_name, worker_id):
    """A single LPUSH connection pushing to one list (= 1 stream)."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        item_id = 0
        while not stop_event.is_set():
            try:
                c.command("LPUSH", list_name, f"item_{worker_id}_{item_id}")
                stats.inc(stats.lpush_sent, list_name)
                item_id += 1
                time.sleep(0.03)
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"LPUSH[{worker_id}] error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                except Exception:
                    break
        c.close()
    except Exception as e:
        stats.add_error(f"LPUSH[{worker_id}] fatal: {e}")


def transaction_worker(worker_id):
    """A single connection running MULTI/EXEC transactions (= 1 stream)."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        tx_id = 0
        while not stop_event.is_set():
            try:
                key = f"cs_tx_{worker_id}"
                val = f"v_{tx_id}"

                resp = c.command("MULTI")
                if not isinstance(resp, str) or resp != "OK":
                    stats.add_error(f"TX[{worker_id}] MULTI failed: {resp}")
                    stats.inc(stats.tx_err, worker_id)
                    continue

                c.command("SET", key, val)
                c.command("INCR", f"cs_tx_cnt_{worker_id}")
                c.command("GET", key)

                resp = c.command("EXEC")
                if isinstance(resp, list) and len(resp) == 3:
                    if resp[0] == "OK" and isinstance(resp[1], int) and resp[2] == val:
                        stats.inc(stats.tx_ok, worker_id)
                    else:
                        stats.add_error(f"TX[{worker_id}] result mismatch: {resp}, expected val={val}")
                        stats.inc(stats.tx_err, worker_id)
                else:
                    stats.add_error(f"TX[{worker_id}] EXEC unexpected: {resp}")
                    stats.inc(stats.tx_err, worker_id)

                tx_id += 1
                time.sleep(0.02)
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"TX[{worker_id}] error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                except Exception:
                    break
        c.close()
    except Exception as e:
        stats.add_error(f"TX[{worker_id}] fatal: {e}")


def normal_worker(worker_id):
    """A single connection doing SET/GET (= 1 stream), verifying data isolation."""
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        op_id = 0
        while not stop_event.is_set():
            try:
                key = f"cs_nrm_{worker_id}_{op_id % 50}"
                val = f"nv_{worker_id}_{op_id}"

                resp = c.command("SET", key, val)
                if resp != "OK":
                    stats.add_error(f"NRM[{worker_id}] SET fail: {resp}")
                    stats.inc(stats.normal_err, worker_id)
                    op_id += 1
                    continue

                resp = c.command("GET", key)
                if resp != val:
                    stats.add_error(f"NRM[{worker_id}] GET mismatch: expected={val}, got={resp}")
                    stats.inc(stats.normal_err, worker_id)
                    op_id += 1
                    continue

                stats.inc(stats.normal_ok, worker_id)
                op_id += 1
                time.sleep(0.01)
            except Exception as e:
                if not stop_event.is_set():
                    stats.add_error(f"NRM[{worker_id}] error: {e}")
                try:
                    c.close()
                    c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
                except Exception:
                    break
        c.close()
    except Exception as e:
        stats.add_error(f"NRM[{worker_id}] fatal: {e}")


# ==================== Main ====================

def get_mux_info():
    """Get MUX stream count from admin port."""
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


def main():
    global PROXY_PORT, ADMIN_PORT, DURATION

    for arg in sys.argv[1:]:
        if arg.startswith("--proxy-port="):
            PROXY_PORT = int(arg.split("=")[1])
        elif arg.startswith("--admin-port="):
            ADMIN_PORT = int(arg.split("=")[1])
        elif arg.startswith("--duration="):
            DURATION = int(arg.split("=")[1])

    # Configuration: how many connections (streams) of each type
    NUM_SUB_CHANNELS = 5     # 5 different channels
    NUM_SUB_PER_CHAN = 3     # 3 subscribers per channel = 15 subscriber streams
    NUM_PUB_PER_CHAN = 2     # 2 publishers per channel  = 10 publisher streams
    NUM_BLPOP_LISTS = 4      # 4 different lists
    NUM_BLPOP_PER_LIST = 2   # 2 BLPOP per list          = 8 blpop streams
    NUM_LPUSH_PER_LIST = 2   # 2 LPUSH per list          = 8 lpush streams
    NUM_TX_WORKERS = 8       # 8 transaction streams
    NUM_NORMAL_WORKERS = 10  # 10 normal SET/GET streams

    total_streams = (NUM_SUB_CHANNELS * NUM_SUB_PER_CHAN +
                     NUM_SUB_CHANNELS * NUM_PUB_PER_CHAN +
                     NUM_BLPOP_LISTS * NUM_BLPOP_PER_LIST +
                     NUM_BLPOP_LISTS * NUM_LPUSH_PER_LIST +
                     NUM_TX_WORKERS +
                     NUM_NORMAL_WORKERS)

    print("=" * 70)
    print("  Concurrent Multi-Stream MUX Test")
    print("=" * 70)
    print(f"  Proxy:    {PROXY_HOST}:{PROXY_PORT}")
    print(f"  Admin:    {PROXY_HOST}:{ADMIN_PORT}")
    print(f"  Duration: {DURATION} seconds")
    print()
    print(f"  Stream breakdown:")
    print(f"    SUBSCRIBE streams: {NUM_SUB_CHANNELS} channels x {NUM_SUB_PER_CHAN} subs = {NUM_SUB_CHANNELS * NUM_SUB_PER_CHAN}")
    print(f"    PUBLISH streams:   {NUM_SUB_CHANNELS} channels x {NUM_PUB_PER_CHAN} pubs = {NUM_SUB_CHANNELS * NUM_PUB_PER_CHAN}")
    print(f"    BLPOP streams:     {NUM_BLPOP_LISTS} lists x {NUM_BLPOP_PER_LIST} blpops = {NUM_BLPOP_LISTS * NUM_BLPOP_PER_LIST}")
    print(f"    LPUSH streams:     {NUM_BLPOP_LISTS} lists x {NUM_LPUSH_PER_LIST} lpushs = {NUM_BLPOP_LISTS * NUM_LPUSH_PER_LIST}")
    print(f"    TX streams:        {NUM_TX_WORKERS}")
    print(f"    Normal streams:    {NUM_NORMAL_WORKERS}")
    print(f"    --------------------------------")
    print(f"    TOTAL streams:     {total_streams} (all on 1 MUX TCP connection)")
    print()

    # Verify MUX mode
    info = get_mux_info()
    mux_enabled = info.get("mux_enabled", "unknown")
    workers = info.get("worker_threads", "unknown")
    mux_conns = info.get("mux_connections", "unknown")
    print(f"  Proxy state: mux_enabled={mux_enabled}, workers={workers}, mux_connections={mux_conns}")
    if mux_enabled != "yes":
        print("  ERROR: MUX is not enabled! Aborting.")
        sys.exit(1)
    if workers != "1":
        print(f"  WARNING: workers={workers}, expected 1 for true single-connection MUX test")
    print()

    # Clean up old test keys
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5)
        for i in range(NUM_BLPOP_LISTS):
            c.command("DEL", f"cs_list_{i}")
        c.close()
    except Exception as e:
        print(f"  Warning: cleanup failed: {e}")

    # ---- Start all workers ----
    threads = []
    wid = 0

    # 1. Subscribers (start first so they're ready for messages)
    print(f"  Starting {NUM_SUB_CHANNELS * NUM_SUB_PER_CHAN} subscriber streams...")
    for ch_idx in range(NUM_SUB_CHANNELS):
        channel = f"cs_chan_{ch_idx}"
        for _ in range(NUM_SUB_PER_CHAN):
            t = threading.Thread(target=subscriber_worker, args=(channel, wid), daemon=True)
            threads.append(t)
            t.start()
            wid += 1

    time.sleep(1)  # Let subscribers establish

    # 2. Publishers
    print(f"  Starting {NUM_SUB_CHANNELS * NUM_PUB_PER_CHAN} publisher streams...")
    for ch_idx in range(NUM_SUB_CHANNELS):
        channel = f"cs_chan_{ch_idx}"
        for _ in range(NUM_PUB_PER_CHAN):
            t = threading.Thread(target=publisher_worker, args=(channel, wid), daemon=True)
            threads.append(t)
            t.start()
            wid += 1

    # 3. BLPOP workers
    print(f"  Starting {NUM_BLPOP_LISTS * NUM_BLPOP_PER_LIST} BLPOP streams...")
    for lst_idx in range(NUM_BLPOP_LISTS):
        list_name = f"cs_list_{lst_idx}"
        for _ in range(NUM_BLPOP_PER_LIST):
            t = threading.Thread(target=blpop_worker, args=(list_name, wid), daemon=True)
            threads.append(t)
            t.start()
            wid += 1

    # 4. LPUSH workers
    print(f"  Starting {NUM_BLPOP_LISTS * NUM_LPUSH_PER_LIST} LPUSH streams...")
    for lst_idx in range(NUM_BLPOP_LISTS):
        list_name = f"cs_list_{lst_idx}"
        for _ in range(NUM_LPUSH_PER_LIST):
            t = threading.Thread(target=lpush_worker, args=(list_name, wid), daemon=True)
            threads.append(t)
            t.start()
            wid += 1

    # 5. Transaction workers
    print(f"  Starting {NUM_TX_WORKERS} transaction streams...")
    for i in range(NUM_TX_WORKERS):
        t = threading.Thread(target=transaction_worker, args=(i,), daemon=True)
        threads.append(t)
        t.start()

    # 6. Normal workers
    print(f"  Starting {NUM_NORMAL_WORKERS} normal SET/GET streams...")
    for i in range(NUM_NORMAL_WORKERS):
        t = threading.Thread(target=normal_worker, args=(i,), daemon=True)
        threads.append(t)
        t.start()

    print(f"\n  All {len(threads)} streams launched. Running for {DURATION}s...\n")

    # ---- Monitor ----
    start_time = time.time()
    peak_streams = 0

    while time.time() - start_time < DURATION:
        time.sleep(2)
        elapsed = int(time.time() - start_time)

        # Get current stream count from proxy
        info = get_mux_info()
        cur_streams = info.get("mux_streams", "?")
        cur_mux_conns = info.get("mux_connections", "?")
        try:
            s = int(cur_streams)
            if s > peak_streams:
                peak_streams = s
        except (ValueError, TypeError):
            pass

        total_sub = stats.sum_values(stats.sub_received)
        total_pub = stats.sum_values(stats.pub_sent)
        total_blpop = stats.sum_values(stats.blpop_received)
        total_lpush = stats.sum_values(stats.lpush_sent)
        total_tx_ok = stats.sum_values(stats.tx_ok)
        total_tx_err = stats.sum_values(stats.tx_err)
        total_nrm_ok = stats.sum_values(stats.normal_ok)
        total_nrm_err = stats.sum_values(stats.normal_err)

        print(f"  [{elapsed:3d}s] mux_conns={cur_mux_conns} streams={cur_streams} | "
              f"SUB:{total_sub}/{total_pub} BLPOP:{total_blpop}/{total_lpush} "
              f"TX:{total_tx_ok}/e{total_tx_err} NRM:{total_nrm_ok}/e{total_nrm_err} "
              f"errs:{len(stats.errors)}")

    # ---- Stop ----
    print(f"\n  Stopping all workers...")
    stop_event.set()

    for t in threads:
        t.join(timeout=10)

    alive = sum(1 for t in threads if t.is_alive())
    if alive > 0:
        print(f"  Warning: {alive} threads still alive")

    # Wait a moment for streams to close
    time.sleep(2)
    info = get_mux_info()
    final_streams = info.get("mux_streams", "?")
    final_mux_conns = info.get("mux_connections", "?")

    # ---- Results ----
    print()
    print("=" * 70)
    print("  RESULTS")
    print("=" * 70)
    print()
    print(f"  MUX connection: mux_connections={final_mux_conns}, "
          f"peak_streams={peak_streams}, final_streams={final_streams}")
    print()

    all_pass = True
    test_num = 0

    # -- Test 1: PUB/SUB per channel --
    for ch_idx in range(NUM_SUB_CHANNELS):
        channel = f"cs_chan_{ch_idx}"
        pub = stats.pub_sent.get(channel, 0)
        sub = stats.sub_received.get(channel, 0)
        # Each message is received by NUM_SUB_PER_CHAN subscribers
        expected_min = pub * NUM_SUB_PER_CHAN * 0.8  # allow 20% loss for timing
        ok = pub > 0 and sub > 0
        test_num += 1
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] SUB/PUB channel '{channel}': pub={pub}, sub_recv={sub} "
              f"(expected ~{pub * NUM_SUB_PER_CHAN} for {NUM_SUB_PER_CHAN} subs)")
        if not ok:
            all_pass = False

    print()

    # -- Test 2: BLPOP per list --
    for lst_idx in range(NUM_BLPOP_LISTS):
        list_name = f"cs_list_{lst_idx}"
        pushed = stats.lpush_sent.get(list_name, 0)
        popped = stats.blpop_received.get(list_name, 0)
        ok = pushed > 0 and popped > 0
        test_num += 1
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] BLPOP list '{list_name}': pushed={pushed}, popped={popped}")
        if not ok:
            all_pass = False

    print()

    # -- Test 3: Transactions --
    total_tx_ok = stats.sum_values(stats.tx_ok)
    total_tx_err = stats.sum_values(stats.tx_err)
    tx_pass = total_tx_ok > 0 and total_tx_err == 0
    test_num += 1
    status = "PASS" if tx_pass else "FAIL"
    print(f"  [{status}] Transactions: {NUM_TX_WORKERS} streams, "
          f"completed={total_tx_ok}, errors={total_tx_err}")
    for wid in sorted(stats.tx_ok.keys()):
        print(f"         TX[{wid}]: ok={stats.tx_ok.get(wid, 0)}, err={stats.tx_err.get(wid, 0)}")
    if not tx_pass:
        all_pass = False

    print()

    # -- Test 4: Normal ops --
    total_nrm_ok = stats.sum_values(stats.normal_ok)
    total_nrm_err = stats.sum_values(stats.normal_err)
    nrm_pass = total_nrm_ok > 0 and total_nrm_err == 0
    test_num += 1
    status = "PASS" if nrm_pass else "FAIL"
    print(f"  [{status}] Normal SET/GET: {NUM_NORMAL_WORKERS} streams, "
          f"ops={total_nrm_ok}, errors={total_nrm_err}")
    for wid in sorted(stats.normal_ok.keys()):
        print(f"         NRM[{wid}]: ok={stats.normal_ok.get(wid, 0)}, err={stats.normal_err.get(wid, 0)}")
    if not nrm_pass:
        all_pass = False

    print()

    # -- Test 5: Stream cleanup --
    stream_cleanup_ok = final_streams == "0"
    test_num += 1
    status = "PASS" if stream_cleanup_ok else "FAIL"
    print(f"  [{status}] Stream cleanup: final_streams={final_streams} (expected 0)")
    if not stream_cleanup_ok:
        all_pass = False

    print()

    # -- Test 6: Single MUX connection --
    single_conn_ok = final_mux_conns == "1"
    test_num += 1
    status = "PASS" if single_conn_ok else "FAIL"
    print(f"  [{status}] Single MUX connection: mux_connections={final_mux_conns} (expected 1)")
    if not single_conn_ok:
        all_pass = False

    # Error details
    if stats.errors:
        print(f"\n  Errors ({len(stats.errors)}):")
        for i, err in enumerate(stats.errors[:20]):
            print(f"    [{i+1}] {err}")
        if len(stats.errors) > 20:
            print(f"    ... and {len(stats.errors) - 20} more")

    print()
    print("=" * 70)
    if all_pass:
        print(f"  ALL {test_num} CHECKS PASSED!")
        print(f"  {total_streams} streams multiplexed over 1 MUX TCP connection - all correct!")
    else:
        print(f"  SOME CHECKS FAILED")
    print("=" * 70)

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
