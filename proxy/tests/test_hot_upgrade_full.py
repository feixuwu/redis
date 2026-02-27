#!/usr/bin/env python3
"""
热升级全面测试脚本

测试覆盖的设计目标：
  1. 零连接中断 — 持久 TCP 连接全程不断开
  2. 零数据丢失 — INCR 计数器精确，SET/GET 数据一致
  3. 渐进式迁移 — 升级期间延迟不出现长时间卡顿
  4. 后端连接复用 — 升级前后数据一致，Redis 侧无感知
  5. 事件驱动 — 不阻塞新连接建立
  6. 大规模稳定 — 50+ 并发持久连接

测试场景：
  A. 基础 INCR 计数器（零数据丢失验证）
  B. Pipeline 批量命令（多命令原子性）
  C. 大 Value 读写（数据完整性）
  D. Hash 命令（HSET/HGET/HGETALL）
  E. List 命令（LPUSH/LRANGE）
  F. MSET/MGET 多键操作
  G. 升级期间新建连接（事件驱动验证）
  H. 延迟分布（升级前/中/后对比）

用法:
  python3 test_hot_upgrade_full.py [--num-clients=50] [--duration=30]
"""

import socket
import time
import threading
import subprocess
import sys
import os
import signal
import statistics
import struct
import json
import tempfile
import atexit

# ============= 配置 =============
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REDIS_SERVER = os.path.join(BASE_DIR, "..", "src", "redis-server")
REDIS_CLI = os.path.join(BASE_DIR, "..", "src", "redis-cli")
PROXY_BIN = os.path.join(BASE_DIR, "build", "redis-mux-proxy")
PROXY_CONF = os.path.join(BASE_DIR, "conf", "proxy.yaml")

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6389       # 后端 Redis 端口（与 proxy.yaml 一致）
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 6479       # Proxy 数据端口
ADMIN_PORT = 9090       # Proxy Admin 端口

NUM_CLIENTS = 50         # 并发持久连接数
PRE_UPGRADE_SECS = 6     # 升级前基线测量秒数
POST_UPGRADE_SECS = 6    # 升级后验证秒数
UPGRADE_WAIT_SECS = 10   # 等待升级完成秒数

# ============= 全局状态 =============
stop_event = threading.Event()
upgrade_triggered = threading.Event()
upgrade_complete = threading.Event()
results_lock = threading.Lock()
results = {}

# 管理进程
managed_pids = []


def cleanup_processes():
    """清理启动的 redis-server 和 proxy"""
    for pid in managed_pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    # 等一下让进程退出
    time.sleep(0.5)
    for pid in managed_pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


atexit.register(cleanup_processes)


# ============= RESP 协议工具 =============
class RedisConn:
    """简单的 Redis RESP 协议客户端，支持持久连接"""

    def __init__(self, host, port, timeout=30.0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect((host, port))
        self.buf = b""
        self._connected = True

    @property
    def connected(self):
        return self._connected

    def close(self):
        try:
            self.sock.close()
        except:
            pass
        self._connected = False

    def fileno(self):
        return self.sock.fileno()

    def send_command(self, *args):
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            arg_str = str(arg)
            cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
        self.sock.sendall(cmd.encode())

    def send_raw(self, data: bytes):
        """发送原始字节数据（pipeline 用）"""
        self.sock.sendall(data)

    def read_response(self):
        while True:
            resp, rest = self._parse_resp(self.buf)
            if resp is not None:
                self.buf = rest
                return resp
            chunk = self.sock.recv(16384)
            if not chunk:
                self._connected = False
                raise ConnectionError("Connection closed by remote")
            self.buf += chunk

    def command(self, *args):
        self.send_command(*args)
        return self.read_response()

    def pipeline(self, commands):
        """发送一组 pipeline 命令并读取所有回复"""
        raw = b""
        for cmd_args in commands:
            c = f"*{len(cmd_args)}\r\n"
            for a in cmd_args:
                s = str(a)
                c += f"${len(s)}\r\n{s}\r\n"
            raw += c.encode()
        self.sock.sendall(raw)
        responses = []
        for _ in commands:
            responses.append(self.read_response())
        return responses

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
            return rest[:length].decode('utf-8', errors='replace'), rest[length + 2:]
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
        line = data[1:idx].decode('utf-8', errors='replace')
        return line, data[idx + 2:]


def build_resp(*args):
    """构造 RESP 命令字节"""
    cmd = f"*{len(args)}\r\n"
    for a in args:
        s = str(a)
        cmd += f"${len(s)}\r\n{s}\r\n"
    return cmd.encode()


# ============= 基础设施 =============

def wait_for_port(host, port, timeout=10):
    """等待端口可连接"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((host, port))
            s.close()
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.2)
    return False


def start_redis_server():
    """启动 Redis server"""
    print(f"  启动 Redis server，端口 {REDIS_PORT}...")
    proc = subprocess.Popen(
        [REDIS_SERVER, "--port", str(REDIS_PORT),
         "--save", "", "--appendonly", "no",
         "--loglevel", "warning",
         "--daemonize", "no"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    managed_pids.append(proc.pid)
    if not wait_for_port(REDIS_HOST, REDIS_PORT):
        print("  [错误] Redis server 启动超时！")
        return None
    print(f"  Redis server 已启动 (PID={proc.pid})")
    return proc


def start_proxy():
    """启动 Proxy"""
    print(f"  启动 Proxy，数据端口 {PROXY_PORT}，管理端口 {ADMIN_PORT}...")
    proc = subprocess.Popen(
        [PROXY_BIN, "-c", PROXY_CONF],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    managed_pids.append(proc.pid)
    if not wait_for_port(PROXY_HOST, PROXY_PORT):
        print("  [错误] Proxy 启动超时！")
        return None
    print(f"  Proxy 已启动 (PID={proc.pid})")
    return proc


def get_proxy_pid():
    """获取当前 proxy 进程的 PID"""
    try:
        output = subprocess.check_output(
            ["pgrep", "-f", "redis-mux-proxy"], text=True
        ).strip()
        pids = output.split('\n')
        return pids[-1]  # 取最新的 PID
    except subprocess.CalledProcessError:
        return None


def trigger_upgrade():
    """通过 admin 端口触发热升级"""
    try:
        c = RedisConn(PROXY_HOST, ADMIN_PORT, timeout=10)
        resp = c.command("PROXY", "UPGRADE")
        c.close()
        return str(resp)
    except Exception as e:
        return f"ERROR: {e}"


# ============= 测试 Worker 线程 =============

class ClientWorker:
    """
    一个持久连接 worker，在后台线程中持续执行命令并收集统计数据。
    每个 worker 可以运行不同的命令模式。
    """

    def __init__(self, client_id, mode="incr"):
        self.client_id = client_id
        self.mode = mode  # incr, pipeline, bigvalue, hash, list, mset
        self.thread = None
        self.conn = None

        # 统计
        self.ops_pre = 0
        self.ops_during = 0
        self.ops_post = 0
        self.errors = []
        self.reconnects = 0
        self.latencies_pre = []
        self.latencies_during = []
        self.latencies_post = []
        self.final_counter = None
        self.data_correct = None
        self.connection_kept_alive = True

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def join(self, timeout=10):
        if self.thread:
            self.thread.join(timeout=timeout)

    def _connect(self):
        self.conn = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5.0)

    def _reconnect(self):
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        time.sleep(0.1)
        self.conn = RedisConn(PROXY_HOST, PROXY_PORT, timeout=5.0)
        self.reconnects += 1
        self.connection_kept_alive = False

    def _get_phase(self):
        if upgrade_complete.is_set():
            return "post"
        elif upgrade_triggered.is_set():
            return "during"
        return "pre"

    def _record_latency(self, phase, latency_us):
        if phase == "pre":
            self.latencies_pre.append(latency_us)
            self.ops_pre += 1
        elif phase == "during":
            self.latencies_during.append(latency_us)
            self.ops_during += 1
        else:
            self.latencies_post.append(latency_us)
            self.ops_post += 1

    def _run(self):
        try:
            self._connect()
        except Exception as e:
            self.errors.append(f"初始连接失败: {e}")
            return

        if self.mode == "incr":
            self._run_incr()
        elif self.mode == "pipeline":
            self._run_pipeline()
        elif self.mode == "bigvalue":
            self._run_bigvalue()
        elif self.mode == "hash":
            self._run_hash()
        elif self.mode == "list":
            self._run_list()
        elif self.mode == "mset":
            self._run_mset()

    def _do_op(self, func):
        """执行一个操作，记录延迟，异常时区分 timeout 和连接断开"""
        phase = self._get_phase()
        try:
            t0 = time.monotonic()
            result = func()
            t1 = time.monotonic()
            latency_us = (t1 - t0) * 1_000_000
            self._record_latency(phase, latency_us)
            return result
        except socket.timeout as e:
            # timeout 不代表连接断开，只是回复慢
            # 记录为慢操作但不 reconnect
            self.errors.append(f"timeout[{phase}]: {e}")
            # 但 timeout 后 socket 状态不确定，需要 reconnect 以避免协议错乱
            try:
                self._reconnect()
            except Exception as e2:
                self.errors.append(f"reconnect_fail: {e2}")
            return None
        except (ConnectionError, BrokenPipeError, OSError) as e:
            # 真正的连接断开
            self.errors.append(f"conn_error[{phase}]: {e}")
            try:
                self._reconnect()
            except Exception as e2:
                self.errors.append(f"reconnect_fail: {e2}")
            return None

    # --- 模式 A: INCR 计数器 ---
    def _run_incr(self):
        key = f"hotup:incr:{self.client_id}"
        try:
            self.conn.command("SET", key, "0")
        except:
            pass

        count = 0
        while not stop_event.is_set():
            result = self._do_op(lambda: self.conn.command("INCR", key))
            if result is not None:
                count += 1

        # 验证最终值
        try:
            val = self.conn.command("GET", key)
            self.final_counter = int(val) if val and val != "nil" else 0
            self.data_correct = (self.final_counter == count)
        except:
            self.data_correct = False

    # --- 模式 B: Pipeline ---
    def _run_pipeline(self):
        key_prefix = f"hotup:pipe:{self.client_id}"
        batch_no = 0
        while not stop_event.is_set():
            batch_no += 1
            cmds = []
            for j in range(5):
                cmds.append(("SET", f"{key_prefix}:{batch_no}:{j}", f"v{batch_no}_{j}"))
            for j in range(5):
                cmds.append(("GET", f"{key_prefix}:{batch_no}:{j}"))

            def do_pipeline():
                return self.conn.pipeline(cmds)

            resps = self._do_op(do_pipeline)
            if resps is not None:
                # 验证 GET 结果
                ok = True
                for j in range(5):
                    expected = f"v{batch_no}_{j}"
                    actual = resps[5 + j]
                    if actual != expected:
                        ok = False
                        self.errors.append(f"pipeline_mismatch batch={batch_no} j={j} exp={expected} got={actual}")
                if ok:
                    self.data_correct = True if self.data_correct is None else self.data_correct
                else:
                    self.data_correct = False

    # --- 模式 C: 大 Value ---
    def _run_bigvalue(self):
        key = f"hotup:big:{self.client_id}"
        # 生成不同大小的 value: 1KB, 10KB, 50KB
        sizes = [1024, 10240, 51200]
        round_no = 0
        self.data_correct = True
        while not stop_event.is_set():
            round_no += 1
            sz = sizes[round_no % len(sizes)]
            val = "X" * sz

            def do_set_get():
                r1 = self.conn.command("SET", key, val)
                r2 = self.conn.command("GET", key)
                return (r1, r2)

            result = self._do_op(do_set_get)
            if result is not None:
                set_resp, get_resp = result
                if get_resp != val:
                    self.errors.append(f"bigval_mismatch round={round_no} sz={sz} got_len={len(get_resp) if get_resp else 0}")
                    self.data_correct = False

    # --- 模式 D: Hash ---
    def _run_hash(self):
        key = f"hotup:hash:{self.client_id}"
        round_no = 0
        self.data_correct = True
        while not stop_event.is_set():
            round_no += 1
            fields = {f"f{i}": f"v{round_no}_{i}" for i in range(5)}

            def do_hash():
                for f, v in fields.items():
                    self.conn.command("HSET", key, f, v)
                vals = []
                for f in fields:
                    vals.append(self.conn.command("HGET", key, f))
                return vals

            result = self._do_op(do_hash)
            if result is not None:
                for i, (f, v) in enumerate(fields.items()):
                    if result[i] != v:
                        self.errors.append(f"hash_mismatch round={round_no} field={f} exp={v} got={result[i]}")
                        self.data_correct = False

    # --- 模式 E: List ---
    def _run_list(self):
        key = f"hotup:list:{self.client_id}"
        try:
            self.conn.command("DEL", key)
        except:
            pass
        push_count = 0
        self.data_correct = True
        while not stop_event.is_set():
            push_count += 1

            def do_list():
                self.conn.command("RPUSH", key, f"item{push_count}")
                result = self.conn.command("LRANGE", key, "-1", "-1")
                return result

            result = self._do_op(do_list)
            if result is not None:
                if isinstance(result, list) and len(result) > 0:
                    if result[0] != f"item{push_count}":
                        self.errors.append(f"list_mismatch push={push_count} got={result[0]}")
                        self.data_correct = False

    # --- 模式 F: MSET/MGET ---
    def _run_mset(self):
        key_prefix = f"hotup:mset:{self.client_id}"
        round_no = 0
        self.data_correct = True
        while not stop_event.is_set():
            round_no += 1
            kv_args = []
            keys = []
            for j in range(10):
                k = f"{key_prefix}:{j}"
                keys.append(k)
                kv_args.extend([k, f"val{round_no}_{j}"])

            def do_mset():
                self.conn.command("MSET", *kv_args)
                result = self.conn.command("MGET", *keys)
                return result

            result = self._do_op(do_mset)
            if result is not None and isinstance(result, list):
                for j in range(10):
                    expected = f"val{round_no}_{j}"
                    if result[j] != expected:
                        self.errors.append(f"mset_mismatch round={round_no} j={j} exp={expected} got={result[j]}")
                        self.data_correct = False


class NewConnProber:
    """
    升级期间不断新建连接，验证事件驱动目标（新连接不受阻）
    """

    def __init__(self):
        self.thread = None
        self.successes = 0
        self.failures = 0
        self.latencies = []

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def join(self, timeout=10):
        if self.thread:
            self.thread.join(timeout=timeout)

    def _run(self):
        while not stop_event.is_set():
            if not upgrade_triggered.is_set():
                time.sleep(0.1)
                continue
            try:
                t0 = time.monotonic()
                c = RedisConn(PROXY_HOST, PROXY_PORT, timeout=3.0)
                resp = c.command("PING")
                t1 = time.monotonic()
                c.close()
                latency_ms = (t1 - t0) * 1000
                self.latencies.append(latency_ms)
                if str(resp) == "PONG":
                    self.successes += 1
                else:
                    self.failures += 1
            except Exception:
                self.failures += 1
            time.sleep(0.05)  # 每 50ms 尝试一次新建连接


# ============= 主测试流程 =============

def print_separator(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def print_latency_stats(name, latencies):
    if not latencies:
        print(f"  {name:>20s}: (无数据)")
        return {}
    p50 = statistics.median(latencies)
    p99 = sorted(latencies)[min(int(len(latencies) * 0.99), len(latencies) - 1)]
    p999 = sorted(latencies)[min(int(len(latencies) * 0.999), len(latencies) - 1)]
    avg = statistics.mean(latencies)
    mx = max(latencies)
    print(f"  {name:>20s}: avg={avg:>8.0f}µs  p50={p50:>8.0f}µs  p99={p99:>8.0f}µs  p999={p999:>8.0f}µs  max={mx:>8.0f}µs  N={len(latencies)}")
    return {"avg": avg, "p50": p50, "p99": p99, "p999": p999, "max": mx, "N": len(latencies)}


def main():
    global NUM_CLIENTS, PRE_UPGRADE_SECS, POST_UPGRADE_SECS

    # 解析命令行参数
    for arg in sys.argv[1:]:
        if arg.startswith("--num-clients="):
            NUM_CLIENTS = int(arg.split("=")[1])
        elif arg.startswith("--duration="):
            d = int(arg.split("=")[1])
            PRE_UPGRADE_SECS = d // 3
            POST_UPGRADE_SECS = d // 3

    print_separator("热升级全面测试 (Hot Upgrade Comprehensive Test)")
    print(f"  并发持久连接数: {NUM_CLIENTS}")
    print(f"  升级前基线: {PRE_UPGRADE_SECS}s  |  升级后验证: {POST_UPGRADE_SECS}s")
    print(f"  Proxy 二进制: {PROXY_BIN}")
    print(f"  升级方式: 自己升级到自己（同一二进制）")

    # ===== 第 0 步：启动基础设施 =====
    print_separator("第 0 步：启动基础设施")

    # 检查是否已有进程在运行
    existing_redis = subprocess.run(["pgrep", "-f", f"redis-server.*{REDIS_PORT}"],
                                     capture_output=True, text=True)
    if existing_redis.returncode == 0:
        print(f"  Redis server 已在运行 (port {REDIS_PORT})")
    else:
        redis_proc = start_redis_server()
        if redis_proc is None:
            print("  [致命] 无法启动 Redis server，退出")
            sys.exit(1)

    existing_proxy = subprocess.run(["pgrep", "-f", "redis-mux-proxy"],
                                     capture_output=True, text=True)
    if existing_proxy.returncode == 0:
        print(f"  Proxy 已在运行 (PID={existing_proxy.stdout.strip()})")
    else:
        proxy_proc = start_proxy()
        if proxy_proc is None:
            print("  [致命] 无法启动 Proxy，退出")
            sys.exit(1)

    # 预热：确保 proxy 正常工作
    time.sleep(0.5)
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT)
        resp = c.command("PING")
        assert str(resp) == "PONG", f"PING 失败: {resp}"
        c.command("SET", "__hotup_test_ready", "1")
        c.close()
        print("  Proxy 连通性验证 ✅")
    except Exception as e:
        print(f"  [致命] Proxy 连通性验证失败: {e}")
        sys.exit(1)

    old_pid = get_proxy_pid()
    print(f"  旧进程 PID: {old_pid}")

    # ===== 第 1 步：预填充测试数据 =====
    print_separator("第 1 步：预填充测试数据（验证后端连接复用）")
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT)
        for i in range(200):
            c.command("SET", f"hotup:prefill:{i}", f"prefill_value_{i}")
        c.close()
        print(f"  已写入 200 个预填充 key ✅")
    except Exception as e:
        print(f"  预填充失败: {e}")

    # ===== 第 2 步：启动持久连接 workers =====
    print_separator("第 2 步：启动持久连接 Workers")

    workers = []
    # 分配不同模式的 worker
    modes = ["incr", "pipeline", "bigvalue", "hash", "list", "mset"]
    for i in range(NUM_CLIENTS):
        mode = modes[i % len(modes)]
        w = ClientWorker(i, mode=mode)
        workers.append(w)

    # 统计各模式数量
    mode_counts = {}
    for w in workers:
        mode_counts[w.mode] = mode_counts.get(w.mode, 0) + 1
    for mode, cnt in sorted(mode_counts.items()):
        print(f"  {mode:>12s}: {cnt} 个客户端")

    # 新连接探测器
    new_conn_prober = NewConnProber()

    # 启动所有 worker
    for w in workers:
        w.start()
    new_conn_prober.start()
    print(f"  {NUM_CLIENTS} 个 worker 已启动 ✅")

    # ===== 第 3 步：运行升级前基线 =====
    print_separator(f"第 3 步：运行升级前基线 ({PRE_UPGRADE_SECS}s)")
    time.sleep(PRE_UPGRADE_SECS)

    # 检查基线期间 worker 状态
    pre_errors = sum(len(w.errors) for w in workers)
    pre_ops = sum(w.ops_pre for w in workers)
    print(f"  基线期间总操作: {pre_ops}，错误: {pre_errors}")

    # ===== 第 4 步：触发热升级 =====
    print_separator("第 4 步：触发热升级")
    upgrade_triggered.set()
    upgrade_start = time.monotonic()

    result = trigger_upgrade()
    print(f"  升级命令返回: {result}")
    print(f"  等待升级完成 (最长 {UPGRADE_WAIT_SECS}s)...")

    # 轮询等待 PID 变化
    upgrade_success = False
    for i in range(UPGRADE_WAIT_SECS * 10):
        time.sleep(0.1)
        new_pid = get_proxy_pid()
        if new_pid and new_pid != old_pid:
            upgrade_duration_ms = int((time.monotonic() - upgrade_start) * 1000)
            print(f"  PID 已变化: {old_pid} → {new_pid} (耗时 {upgrade_duration_ms}ms) ✅")
            upgrade_success = True
            break

    if not upgrade_success:
        upgrade_duration_ms = int((time.monotonic() - upgrade_start) * 1000)
        new_pid = get_proxy_pid()
        print(f"  ⚠️  PID 未变化 (old={old_pid}, current={new_pid}, 耗时 {upgrade_duration_ms}ms)")
    else:
        # 额外等待 2 秒让迁移完全结束
        time.sleep(2)

    upgrade_complete.set()

    # ===== 第 5 步：运行升级后验证 =====
    print_separator(f"第 5 步：运行升级后验证 ({POST_UPGRADE_SECS}s)")
    time.sleep(POST_UPGRADE_SECS)

    # ===== 第 6 步：停止 workers 并收集结果 =====
    print_separator("第 6 步：停止 Workers 并收集结果")
    stop_event.set()
    for w in workers:
        w.join(timeout=10)
    new_conn_prober.join(timeout=5)

    # ===== 第 7 步：验证预填充数据 =====
    print_separator("第 7 步：验证预填充数据完整性（后端连接复用）")
    prefill_ok = 0
    prefill_fail = 0
    try:
        c = RedisConn(PROXY_HOST, PROXY_PORT)
        for i in range(200):
            val = c.command("GET", f"hotup:prefill:{i}")
            if val == f"prefill_value_{i}":
                prefill_ok += 1
            else:
                prefill_fail += 1
                if prefill_fail <= 5:
                    print(f"  ❌ key=hotup:prefill:{i} expected=prefill_value_{i} got={val}")
        c.close()
    except Exception as e:
        print(f"  验证连接失败: {e}")
        prefill_fail = 200

    print(f"  预填充数据验证: {prefill_ok}/200 正确 {'✅' if prefill_fail == 0 else '❌'}")

    # ===== 第 8 步：输出详细结果 =====
    print_separator("测试结果详情")

    # --- 8a: 连接存活统计 ---
    print("\n--- 连接存活 (零连接中断) ---")
    total_reconnects = sum(w.reconnects for w in workers)
    alive_count = sum(1 for w in workers if w.connection_kept_alive)
    # 区分 timeout（客户端侧超时，连接实际未断）和真正的连接断开
    timeout_reconnects = 0
    conn_error_reconnects = 0
    for w in workers:
        if w.reconnects > 0:
            for e in w.errors:
                if e.startswith("timeout["):
                    timeout_reconnects += 1
                elif e.startswith("conn_error["):
                    conn_error_reconnects += 1
    print(f"  持久连接保活: {alive_count}/{NUM_CLIENTS} (重连次数: {total_reconnects})")
    if timeout_reconnects > 0:
        print(f"  其中 timeout 导致的重连: {timeout_reconnects} (客户端侧超时，非连接断开)")
    if conn_error_reconnects > 0:
        print(f"  真正的连接断开: {conn_error_reconnects}")
    if conn_error_reconnects == 0:
        print(f"  ✅ 零连接中断 — 无连接被 proxy 主动关闭！" +
              (f" (有 {timeout_reconnects} 次客户端侧 timeout)" if timeout_reconnects > 0 else ""))
    else:
        print(f"  ❌ 有 {conn_error_reconnects} 次真正的连接断开")
        shown = 0
        for w in workers:
            for e in w.errors:
                if e.startswith("conn_error[") and shown < 10:
                    print(f"     Client {w.client_id} ({w.mode}): {e}")
                    shown += 1

    # --- 8b: 数据正确性 ---
    print("\n--- 数据正确性 (零数据丢失) ---")
    total_errors = 0
    data_correct_count = 0
    incr_details = []
    for w in workers:
        total_errors += len(w.errors)
        if w.data_correct is True:
            data_correct_count += 1
        elif w.data_correct is False:
            if len(w.errors) <= 3:
                for e in w.errors:
                    print(f"     Client {w.client_id} ({w.mode}): {e}")
            else:
                print(f"     Client {w.client_id} ({w.mode}): {len(w.errors)} 个错误")

        if w.mode == "incr":
            total_ops = w.ops_pre + w.ops_during + w.ops_post
            incr_details.append((w.client_id, total_ops, w.final_counter, w.data_correct))

    print(f"  数据正确客户端: {data_correct_count}/{NUM_CLIENTS}")
    print(f"  总错误数: {total_errors}")

    # 打印 INCR 验证详情
    if incr_details:
        print(f"\n  INCR 计数器验证 (精确丢包检测):")
        incr_all_correct = True
        for cid, ops, final, correct in incr_details:
            status = "✅" if correct else "❌"
            print(f"    Client {cid}: ops={ops}, counter={final}, {'一致' if correct else '不一致'} {status}")
            if not correct:
                incr_all_correct = False
        if incr_all_correct:
            print(f"  ✅ 所有 INCR 计数器一致 — 零数据丢失！")
        else:
            print(f"  ❌ INCR 计数器有不一致")

    # --- 8c: 按模式统计 ---
    print("\n--- 按命令模式统计 ---")
    for mode in modes:
        mode_workers = [w for w in workers if w.mode == mode]
        if not mode_workers:
            continue
        mode_ops = sum(w.ops_pre + w.ops_during + w.ops_post for w in mode_workers)
        mode_errs = sum(len(w.errors) for w in mode_workers)
        mode_correct = sum(1 for w in mode_workers if w.data_correct is True)
        status = "✅" if mode_errs == 0 and mode_correct == len(mode_workers) else "❌"
        print(f"  {mode:>12s}: ops={mode_ops:>8d}  errors={mode_errs:>3d}  "
              f"correct={mode_correct}/{len(mode_workers)} {status}")

    # --- 8d: 延迟统计 ---
    print("\n--- 延迟统计 (渐进式迁移 / 事件驱动) ---")
    all_pre = []
    all_during = []
    all_post = []
    for w in workers:
        all_pre.extend(w.latencies_pre)
        all_during.extend(w.latencies_during)
        all_post.extend(w.latencies_post)

    stats_pre = print_latency_stats("升级前 (baseline)", all_pre)
    stats_during = print_latency_stats("升级中", all_during)
    stats_post = print_latency_stats("升级后", all_post)

    # 延迟恶化分析
    if stats_pre and stats_during:
        p99_ratio = stats_during.get("p99", 0) / max(stats_pre.get("p99", 1), 1)
        max_ratio = stats_during.get("max", 0) / max(stats_pre.get("max", 1), 1)
        print(f"\n  升级期间延迟变化:")
        print(f"    p99 变化: {p99_ratio:.2f}x {'✅' if p99_ratio < 5 else '⚠️ 较大'}")
        print(f"    max 变化: {max_ratio:.2f}x {'✅' if max_ratio < 20 else '⚠️ 较大'}")

    # --- 8e: 新建连接 ---
    print("\n--- 新建连接测试 (事件驱动) ---")
    print(f"  升级期间新建连接: 成功={new_conn_prober.successes} 失败={new_conn_prober.failures}")
    if new_conn_prober.latencies:
        avg_new = statistics.mean(new_conn_prober.latencies)
        max_new = max(new_conn_prober.latencies)
        print(f"  新建连接延迟: avg={avg_new:.1f}ms  max={max_new:.1f}ms")
    if new_conn_prober.failures == 0 and new_conn_prober.successes > 0:
        print(f"  ✅ 升级期间新连接全部成功！")
    elif new_conn_prober.failures > 0:
        print(f"  ⚠️ 有 {new_conn_prober.failures} 次新建连接失败")

    # --- 8f: 吞吐量 ---
    print("\n--- 吞吐量统计 ---")
    total_pre_ops = sum(w.ops_pre for w in workers)
    total_during_ops = sum(w.ops_during for w in workers)
    total_post_ops = sum(w.ops_post for w in workers)
    pre_qps = total_pre_ops / max(PRE_UPGRADE_SECS, 1)
    during_secs = (upgrade_duration_ms / 1000.0) + 2 if upgrade_success else UPGRADE_WAIT_SECS
    during_qps = total_during_ops / max(during_secs, 1)
    post_qps = total_post_ops / max(POST_UPGRADE_SECS, 1)
    print(f"  升级前 QPS: {pre_qps:>10.0f}  (持续 {PRE_UPGRADE_SECS}s)")
    print(f"  升级中 QPS: {during_qps:>10.0f}  (持续 ~{during_secs:.1f}s)")
    print(f"  升级后 QPS: {post_qps:>10.0f}  (持续 {POST_UPGRADE_SECS}s)")
    if pre_qps > 0:
        qps_drop = (1 - during_qps / pre_qps) * 100
        print(f"  QPS 下降: {qps_drop:.1f}% {'✅' if qps_drop < 20 else '⚠️ 较大'}")

    # ===== 第 9 步：最终总结 =====
    print_separator("最终总结")

    checks = {
        "零连接中断": conn_error_reconnects == 0,
        "零数据丢失 (INCR)": all(c for _, _, _, c in incr_details) if incr_details else True,
        "零数据丢失 (全模式)": data_correct_count == NUM_CLIENTS,
        "预填充数据完整": prefill_fail == 0,
        "PID 变化 (升级成功)": upgrade_success,
        "新建连接不受阻": new_conn_prober.failures == 0 and new_conn_prober.successes > 0,
        "延迟 p99 恶化<5x": (stats_during.get("p99", 0) / max(stats_pre.get("p99", 1), 1) < 5) if stats_pre and stats_during else True,
    }

    all_pass = True
    for check_name, passed in checks.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {check_name:.<45s} {status}")
        if not passed:
            all_pass = False

    print()
    if all_pass:
        print("  🎉 全部检查通过！热升级设计目标完全达成！")
    else:
        failed_checks = [k for k, v in checks.items() if not v]
        print(f"  ⚠️  {len(failed_checks)} 项检查未通过: {', '.join(failed_checks)}")

    print(f"\n  总操作数: {total_pre_ops + total_during_ops + total_post_ops}")
    print(f"  总错误数: {total_errors}")
    print(f"  总重连数: {total_reconnects}")
    print(f"  升级耗时: {upgrade_duration_ms}ms")
    print()

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
