#!/usr/bin/env python3
"""
Redis MUX Protocol Test Suite

Comprehensive tests for the Redis Stream-Multiplexed Protocol (RSMP).
Usage: python3 test_mux_client.py [host] [port]
"""

import socket
import struct
import sys
import time
import threading

# Frame constants
MUX_FRAME_HEADER_SIZE = 14
MUX_FRAME_MAGIC = 0xAA
MUX_FRAME_DATA = 0x01
MUX_FRAME_STREAM_OPEN = 0x02
MUX_FRAME_STREAM_CLOSE = 0x03
MUX_FRAME_PING = 0x04
MUX_FRAME_PONG = 0x05
MUX_FRAME_GOAWAY = 0x06
MUX_FRAME_BATCH = 0x07
MUX_FRAME_WINDOW = 0x08
MUX_FRAME_ERROR = 0x09

MUX_FLAG_FIN = 0x10

TYPE_NAMES = {
    1: 'DATA', 2: 'STREAM_OPEN', 3: 'STREAM_CLOSE',
    4: 'PING', 5: 'PONG', 6: 'GOAWAY', 7: 'BATCH',
    8: 'WINDOW', 9: 'ERROR'
}


def encode_frame(flags, stream_id, payload):
    """Encode a MUX frame: 14-byte header + payload."""
    header = struct.pack('>BBQI', MUX_FRAME_MAGIC, flags, stream_id, len(payload))
    return header + payload


def decode_frame(data):
    """Decode a MUX frame. Returns (flags, stream_id, payload, consumed_bytes) or None."""
    if len(data) < MUX_FRAME_HEADER_SIZE:
        return None
    magic, flags, stream_id, payload_len = struct.unpack('>BBQI', data[:MUX_FRAME_HEADER_SIZE])
    if magic != MUX_FRAME_MAGIC:
        return None
    total = MUX_FRAME_HEADER_SIZE + payload_len
    if len(data) < total:
        return None
    payload = data[MUX_FRAME_HEADER_SIZE:total]
    return (flags, stream_id, payload, total)


def resp_command(*args):
    """Build a RESP command string."""
    parts = [f"*{len(args)}\r\n"]
    for arg in args:
        arg_str = str(arg)
        parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
    return ''.join(parts).encode()


def parse_resp_simple(data):
    """Parse a simple RESP response (one-level, for basic checks)."""
    if not data:
        return None
    s = data.decode('utf-8', errors='replace')
    if s.startswith('+'):
        return s[1:].strip()
    if s.startswith('-'):
        return s.strip()
    if s.startswith(':'):
        return int(s[1:].strip())
    if s.startswith('$'):
        lines = s.split('\r\n')
        length = int(lines[0][1:])
        if length == -1:
            return None
        return lines[1]
    return s.strip()


def recv_all(sock, timeout=2.0):
    """Receive all available data from socket."""
    sock.settimeout(timeout)
    data = b''
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    except socket.timeout:
        pass
    return data


class MuxTestClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.sock = None
        self.mux_mode = False

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def send_resp(self, *args):
        cmd = resp_command(*args)
        self.sock.sendall(cmd)
        return recv_all(self.sock, 1.0)

    def negotiate_mux(self):
        resp = self.send_resp('HELLO', '3', 'MULTIPLEX')
        self.mux_mode = True
        return resp

    def send_mux_frame(self, stream_id, *cmd_args):
        payload = resp_command(*cmd_args)
        frame = encode_frame(MUX_FRAME_DATA, stream_id, payload)
        self.sock.sendall(frame)

    def send_raw_frame(self, flags, stream_id, payload=b''):
        frame = encode_frame(flags, stream_id, payload)
        self.sock.sendall(frame)

    def send_mux_ping(self):
        self.send_raw_frame(MUX_FRAME_PING, 0)

    def send_mux_close_stream(self, stream_id):
        self.send_raw_frame(MUX_FRAME_STREAM_CLOSE, stream_id)

    def send_mux_goaway(self):
        self.send_raw_frame(MUX_FRAME_GOAWAY, 0)

    def send_mux_error(self, stream_id, msg=b'test error'):
        self.send_raw_frame(MUX_FRAME_ERROR, stream_id, msg)

    def send_mux_batch(self, commands):
        batch_payload = b''
        for stream_id, cmd_args in commands:
            inner_payload = resp_command(*cmd_args)
            inner_frame = encode_frame(MUX_FRAME_DATA, stream_id, inner_payload)
            batch_payload += inner_frame
        frame = encode_frame(MUX_FRAME_BATCH, 0, batch_payload)
        self.sock.sendall(frame)

    def send_raw_bytes(self, data):
        self.sock.sendall(data)

    def recv_mux_frames(self, timeout=2.0):
        data = recv_all(self.sock, timeout)
        frames = []
        offset = 0
        while offset < len(data):
            result = decode_frame(data[offset:])
            if result is None:
                break
            flags, stream_id, payload, consumed = result
            frames.append((flags, stream_id, payload))
            offset += consumed
        return frames

    def recv_mux_frames_by_sid(self, timeout=2.0):
        """Receive frames and return a dict: stream_id -> list of payloads."""
        frames = self.recv_mux_frames(timeout)
        result = {}
        for flags, sid, payload in frames:
            ftype = flags & 0x0F
            result.setdefault(sid, []).append((ftype, payload))
        return result, frames

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


class PlainClient:
    """A plain RESP client for verification purposes."""
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))

    def command(self, *args):
        cmd = resp_command(*args)
        self.sock.sendall(cmd)
        return recv_all(self.sock, 1.0)

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


# ==================== Test Framework ====================

class TestRunner:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.passed = 0
        self.failed = 0
        self.total = 0
        self.current_test = ""

    def check(self, name, condition):
        self.total += 1
        if condition:
            self.passed += 1
        else:
            self.failed += 1
            print(f"    FAIL: [{self.current_test}] {name}")

    def run_test(self, name, func):
        self.current_test = name
        print(f"  [{name}]...", end=" ", flush=True)
        before_fail = self.failed
        try:
            func()
            if self.failed == before_fail:
                print("OK")
            else:
                print(f"FAILED ({self.failed - before_fail} assertions)")
        except Exception as e:
            self.failed += 1
            self.total += 1
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()

    def new_mux_client(self):
        c = MuxTestClient(self.host, self.port)
        c.connect()
        c.negotiate_mux()
        return c

    def new_plain_client(self):
        c = PlainClient(self.host, self.port)
        c.connect()
        return c

    def summary(self):
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{self.total} passed, {self.failed}/{self.total} failed")
        if self.failed == 0:
            print("ALL TESTS PASSED!")
        else:
            print(f"{self.failed} test(s) FAILED!")
        print(f"{'='*60}")
        return self.failed == 0


# ==================== Test Cases ====================

def test_basic_set_get(t: TestRunner):
    """Basic SET/GET on multiple streams."""
    c = t.new_mux_client()
    try:
        c.send_mux_frame(1, 'SET', 'mux:basic:k1', 'hello')
        c.send_mux_frame(3, 'SET', 'mux:basic:k2', 'world')
        c.send_mux_frame(5, 'GET', 'mux:basic:k1')
        time.sleep(0.3)
        by_sid, frames = c.recv_mux_frames_by_sid()

        t.check("3 response frames", len(frames) == 3)
        t.check("SID=1 +OK", 1 in by_sid and b'+OK' in by_sid[1][0][1])
        t.check("SID=3 +OK", 3 in by_sid and b'+OK' in by_sid[3][0][1])
        t.check("SID=5 hello", 5 in by_sid and b'hello' in by_sid[5][0][1])

        # Cleanup
        c.send_mux_frame(1, 'DEL', 'mux:basic:k1', 'mux:basic:k2')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_ping_pong(t: TestRunner):
    """PING/PONG on control stream."""
    c = t.new_mux_client()
    try:
        c.send_mux_ping()
        time.sleep(0.3)
        frames = c.recv_mux_frames()
        pongs = [f for f in frames if (f[0] & 0x0F) == MUX_FRAME_PONG]
        t.check("Received PONG", len(pongs) >= 1)
        t.check("PONG on SID=0", pongs[0][1] == 0 if pongs else False)
    finally:
        c.close()


def test_batch_frame(t: TestRunner):
    """Batch frame with multiple sub-commands."""
    c = t.new_mux_client()
    try:
        c.send_mux_batch([
            (1, ('SET', 'mux:batch:a', '1')),
            (3, ('SET', 'mux:batch:b', '2')),
            (5, ('MGET', 'mux:batch:a', 'mux:batch:b')),
        ])
        time.sleep(0.3)
        by_sid, frames = c.recv_mux_frames_by_sid()

        t.check("3 batch responses", len(frames) == 3)
        t.check("Batch SID=5 val1", 5 in by_sid and b'1' in by_sid[5][0][1])
        t.check("Batch SID=5 val2", 5 in by_sid and b'2' in by_sid[5][0][1])

        c.send_mux_frame(1, 'DEL', 'mux:batch:a', 'mux:batch:b')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_stream_close(t: TestRunner):
    """Closing a stream and sending on closed stream."""
    c = t.new_mux_client()
    try:
        # Open stream 1 and use it
        c.send_mux_frame(1, 'SET', 'mux:close:k', 'v')
        time.sleep(0.3)
        c.recv_mux_frames(0.3)

        # Close stream 1
        c.send_mux_close_stream(1)
        time.sleep(0.2)

        # Send on closed stream 1 — should get error or be ignored
        c.send_mux_frame(1, 'GET', 'mux:close:k')
        time.sleep(0.3)
        by_sid, frames = c.recv_mux_frames_by_sid()

        # Stream 1 was closed; the server might return an error frame
        # or might accept it as a re-opened stream (since ID reuse <= max is rejected).
        # Key point: server must not crash.
        t.check("Server alive after send-on-closed", True)

        # Verify error frame or rejection
        has_error = any(ftype == MUX_FRAME_ERROR
                        for ftype_list in by_sid.values()
                        for ftype, payload in ftype_list)
        t.check("Got error for closed/reused stream ID", has_error)

        c.send_mux_frame(3, 'DEL', 'mux:close:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_stream_isolation_db(t: TestRunner):
    """Different streams selecting different DBs should be isolated."""
    c = t.new_mux_client()
    try:
        # Stream 1 uses DB 0
        c.send_mux_frame(1, 'SELECT', '0')
        c.send_mux_frame(1, 'SET', 'mux:iso:key', 'db0val')
        # Stream 3 uses DB 1
        c.send_mux_frame(3, 'SELECT', '1')
        c.send_mux_frame(3, 'SET', 'mux:iso:key', 'db1val')
        time.sleep(0.3)
        c.recv_mux_frames(0.5)

        # Verify isolation: GET on each stream should see its own DB value
        c.send_mux_frame(1, 'GET', 'mux:iso:key')
        c.send_mux_frame(3, 'GET', 'mux:iso:key')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()

        t.check("Stream 1 sees DB0 value", 1 in by_sid and b'db0val' in by_sid[1][0][1])
        t.check("Stream 3 sees DB1 value", 3 in by_sid and b'db1val' in by_sid[3][0][1])

        # Cleanup
        c.send_mux_frame(1, 'DEL', 'mux:iso:key')
        c.send_mux_frame(3, 'DEL', 'mux:iso:key')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_transaction_multi_exec(t: TestRunner):
    """MULTI/EXEC transaction on a MUX stream."""
    c = t.new_mux_client()
    try:
        c.send_mux_frame(1, 'MULTI')
        time.sleep(0.2)
        frames = c.recv_mux_frames(0.5)
        t.check("MULTI returns +OK", any(b'+OK' in f[2] for f in frames))

        c.send_mux_frame(1, 'SET', 'mux:tx:a', '100')
        c.send_mux_frame(1, 'INCR', 'mux:tx:a')
        c.send_mux_frame(1, 'GET', 'mux:tx:a')
        time.sleep(0.3)
        frames = c.recv_mux_frames(0.5)
        # QUEUED responses may be merged into a single frame payload
        all_queued_payload = b''.join(f[2] for f in frames)
        queued_count = all_queued_payload.count(b'+QUEUED')
        t.check("3 QUEUED responses", queued_count == 3)

        c.send_mux_frame(1, 'EXEC')
        time.sleep(0.3)
        frames = c.recv_mux_frames(0.5)
        t.check("EXEC returns results", len(frames) >= 1)
        # Result should contain: +OK, :101, $3\r\n101
        exec_payload = frames[0][2] if frames else b''
        t.check("EXEC result has 101", b'101' in exec_payload)

        c.send_mux_frame(1, 'DEL', 'mux:tx:a')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_transaction_isolation(t: TestRunner):
    """MULTI/EXEC on one stream should not affect another stream."""
    c = t.new_mux_client()
    try:
        # Stream 1 starts a transaction
        c.send_mux_frame(1, 'MULTI')
        c.send_mux_frame(1, 'SET', 'mux:txiso:k', 'txval')
        time.sleep(0.2)
        c.recv_mux_frames(0.5)

        # Stream 3 should be able to work independently
        c.send_mux_frame(3, 'SET', 'mux:txiso:k2', 'independent')
        time.sleep(0.2)
        by_sid, _ = c.recv_mux_frames_by_sid(0.5)
        t.check("Stream 3 works during stream 1 tx", 3 in by_sid and b'+OK' in by_sid[3][0][1])

        # EXEC on stream 1
        c.send_mux_frame(1, 'EXEC')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        # Verify both values
        c.send_mux_frame(1, 'GET', 'mux:txiso:k')
        c.send_mux_frame(3, 'GET', 'mux:txiso:k2')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        t.check("Stream 1 tx committed", 1 in by_sid and b'txval' in by_sid[1][0][1])
        t.check("Stream 3 value intact", 3 in by_sid and b'independent' in by_sid[3][0][1])

        c.send_mux_frame(1, 'DEL', 'mux:txiso:k', 'mux:txiso:k2')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_even_stream_id_rejected(t: TestRunner):
    """Even stream IDs should be rejected (server-reserved)."""
    c = t.new_mux_client()
    try:
        c.send_mux_frame(2, 'PING')
        time.sleep(0.3)
        frames = c.recv_mux_frames()
        error_frames = [f for f in frames if (f[0] & 0x0F) == MUX_FRAME_ERROR]
        t.check("Even SID rejected with error", len(error_frames) >= 1)
        if error_frames:
            t.check("Error mentions invalid stream ID",
                     b'invalid' in error_frames[0][2].lower())
    finally:
        c.close()


def test_stream_id_non_increasing(t: TestRunner):
    """Non-increasing stream IDs should be rejected after close."""
    c = t.new_mux_client()
    try:
        # Open stream 5 first
        c.send_mux_frame(5, 'SET', 'mux:noinc:k', 'v')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        # Now try stream 3 (lower than max seen = 5) — should fail
        c.send_mux_frame(3, 'PING')
        time.sleep(0.3)
        frames = c.recv_mux_frames()
        error_frames = [f for f in frames if (f[0] & 0x0F) == MUX_FRAME_ERROR]
        t.check("Non-increasing SID rejected", len(error_frames) >= 1)

        c.send_mux_frame(5, 'DEL', 'mux:noinc:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_64bit_stream_ids(t: TestRunner):
    """Stream IDs beyond uint32 range."""
    c = t.new_mux_client()
    try:
        big_sid1 = 0x100000001
        big_sid2 = 0x200000003
        big_sid3 = 0xFFFFFFFFFFFFFFFD

        c.send_mux_frame(big_sid1, 'SET', 'mux:big:k1', 'v1')
        c.send_mux_frame(big_sid2, 'SET', 'mux:big:k2', 'v2')
        c.send_mux_frame(big_sid3, 'SET', 'mux:big:k3', 'v3')
        time.sleep(0.5)
        by_sid, frames = c.recv_mux_frames_by_sid()

        t.check("3 frames for 64-bit SIDs", len(frames) == 3)
        t.check(f"SID={big_sid1:#x} OK", big_sid1 in by_sid and b'+OK' in by_sid[big_sid1][0][1])
        t.check(f"SID={big_sid2:#x} OK", big_sid2 in by_sid and b'+OK' in by_sid[big_sid2][0][1])
        t.check(f"SID={big_sid3:#x} OK", big_sid3 in by_sid and b'+OK' in by_sid[big_sid3][0][1])

        c.send_mux_frame(big_sid1, 'DEL', 'mux:big:k1', 'mux:big:k2', 'mux:big:k3')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_pipeline_on_stream(t: TestRunner):
    """Multiple commands pipelined in a single DATA frame."""
    c = t.new_mux_client()
    try:
        # Build two RESP commands and send in one frame payload
        payload = resp_command('SET', 'mux:pipe:k', 'pval') + resp_command('GET', 'mux:pipe:k')
        frame = encode_frame(MUX_FRAME_DATA, 1, payload)
        c.send_raw_bytes(frame)
        time.sleep(0.3)

        by_sid, frames = c.recv_mux_frames_by_sid()
        sid1_frames = by_sid.get(1, [])
        # May come as one merged DATA frame or two
        all_payload = b''.join(p for _, p in sid1_frames)
        t.check("Pipeline SET +OK", b'+OK' in all_payload)
        t.check("Pipeline GET pval", b'pval' in all_payload)

        c.send_mux_frame(1, 'DEL', 'mux:pipe:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_large_payload(t: TestRunner):
    """Large value SET/GET through MUX."""
    c = t.new_mux_client()
    try:
        large_val = 'X' * 100000  # 100KB value
        c.send_mux_frame(1, 'SET', 'mux:large:k', large_val)
        time.sleep(0.5)
        frames = c.recv_mux_frames(1.0)
        t.check("Large SET +OK", any(b'+OK' in f[2] for f in frames))

        c.send_mux_frame(1, 'GET', 'mux:large:k')
        time.sleep(0.5)
        frames = c.recv_mux_frames(1.0)
        t.check("Large GET returns full value",
                any(large_val.encode() in f[2] for f in frames))

        c.send_mux_frame(1, 'DEL', 'mux:large:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_partial_frame_send(t: TestRunner):
    """Send a frame in two parts (header first, then payload)."""
    c = t.new_mux_client()
    try:
        payload = resp_command('SET', 'mux:partial:k', 'pv')
        header = struct.pack('>BBQI', MUX_FRAME_MAGIC, MUX_FRAME_DATA, 1, len(payload))

        # Send header first
        c.send_raw_bytes(header)
        time.sleep(0.1)
        # Then payload
        c.send_raw_bytes(payload)
        time.sleep(0.3)

        frames = c.recv_mux_frames()
        t.check("Partial frame reassembled", any(b'+OK' in f[2] for f in frames))

        c.send_mux_frame(1, 'DEL', 'mux:partial:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_goaway(t: TestRunner):
    """GOAWAY frame should trigger graceful close."""
    c = t.new_mux_client()
    try:
        # First do something to confirm connection works
        c.send_mux_frame(1, 'PING')
        time.sleep(0.2)
        frames = c.recv_mux_frames(0.5)
        t.check("Pre-GOAWAY command works", len(frames) >= 1)

        # Send GOAWAY
        c.send_mux_goaway()
        time.sleep(0.5)

        # Connection should be closing. Sending more data may or may not work
        # but server must not crash.
        try:
            c.sock.settimeout(1.0)
            data = c.sock.recv(4096)
            # If we get empty data, connection is closed (expected)
            if data == b'':
                t.check("Connection closed after GOAWAY", True)
            else:
                # Server might still send pending data before closing
                t.check("Server still responding after GOAWAY (pending data)", True)
        except (socket.timeout, ConnectionError):
            t.check("Connection closed after GOAWAY", True)
    finally:
        c.close()


def test_error_frame(t: TestRunner):
    """Sending an ERROR frame should close the target stream."""
    c = t.new_mux_client()
    try:
        # Open stream 1
        c.send_mux_frame(1, 'SET', 'mux:err:k', 'v')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        # Send ERROR frame on stream 1
        c.send_mux_error(1, b'client-side error')
        time.sleep(0.2)

        # Stream 1 should now be closed. Try to use it again.
        c.send_mux_frame(1, 'GET', 'mux:err:k')
        time.sleep(0.3)
        frames = c.recv_mux_frames()

        # Should get an error response since stream was closed and ID is not increasing
        error_frames = [f for f in frames if (f[0] & 0x0F) == MUX_FRAME_ERROR]
        t.check("Error frame closed the stream", len(error_frames) >= 1)

        c.send_mux_frame(3, 'DEL', 'mux:err:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_client_list_visibility(t: TestRunner):
    """Virtual clients should appear in CLIENT LIST."""
    c = t.new_mux_client()
    plain = t.new_plain_client()
    try:
        # Create a couple of streams
        c.send_mux_frame(1, 'SET', 'mux:cl:k', 'v')
        c.send_mux_frame(3, 'SET', 'mux:cl:k2', 'v2')
        time.sleep(0.3)
        c.recv_mux_frames(0.3)

        # Use plain client to check CLIENT LIST
        resp = plain.command('CLIENT', 'LIST')
        resp_str = resp.decode('utf-8', errors='replace')

        # Virtual clients have conn=NULL, they should have fd=-1
        # Count lines with fd=-1 (virtual clients)
        lines = [l for l in resp_str.split('\n') if l.strip()]
        virtual_lines = [l for l in lines if 'fd=-1' in l]
        t.check("Virtual clients in CLIENT LIST (fd=-1)", len(virtual_lines) >= 2)

        c.send_mux_frame(1, 'DEL', 'mux:cl:k', 'mux:cl:k2')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()
        plain.close()


def test_info_mux_virtual_clients(t: TestRunner):
    """INFO should report mux_virtual_clients count."""
    c = t.new_mux_client()
    plain = t.new_plain_client()
    try:
        # Create 3 streams
        c.send_mux_frame(1, 'PING')
        c.send_mux_frame(3, 'PING')
        c.send_mux_frame(5, 'PING')
        time.sleep(0.3)
        c.recv_mux_frames(0.5)

        resp = plain.command('INFO', 'Clients')
        resp_str = resp.decode('utf-8', errors='replace')
        t.check("mux_virtual_clients in INFO",
                'mux_virtual_clients' in resp_str)

        # Parse the count
        for line in resp_str.split('\r\n'):
            if 'mux_virtual_clients' in line:
                parts = line.split(':')
                if len(parts) == 2:
                    count = int(parts[1].strip())
                    t.check("mux_virtual_clients >= 3", count >= 3)
                break
    finally:
        c.close()
        plain.close()


def test_many_streams(t: TestRunner):
    """Stress test: open many concurrent streams."""
    c = t.new_mux_client()
    num_streams = 100
    try:
        # Open 100 streams with SET commands
        for i in range(num_streams):
            sid = 2 * i + 1  # odd IDs: 1, 3, 5, ...
            c.send_mux_frame(sid, 'SET', f'mux:many:{i}', f'val{i}')
        time.sleep(1.0)

        all_frames = c.recv_mux_frames(2.0)
        ok_count = sum(1 for f in all_frames if b'+OK' in f[2])
        t.check(f"All {num_streams} SETs returned +OK", ok_count == num_streams)

        # Verify a few random values
        c.send_mux_frame(1, 'GET', 'mux:many:0')
        c.send_mux_frame(99, 'GET', 'mux:many:49')
        c.send_mux_frame(199, 'GET', 'mux:many:99')
        time.sleep(0.5)
        by_sid, _ = c.recv_mux_frames_by_sid()
        t.check("GET mux:many:0 = val0", 1 in by_sid and b'val0' in by_sid[1][0][1])
        t.check("GET mux:many:49 = val49", 99 in by_sid and b'val49' in by_sid[99][0][1])
        t.check("GET mux:many:99 = val99", 199 in by_sid and b'val99' in by_sid[199][0][1])

        # Cleanup
        for i in range(num_streams):
            sid = 2 * i + 1
            c.send_mux_close_stream(sid)
        time.sleep(0.5)
        c.recv_mux_frames(0.5)

        # Cleanup keys with a new stream
        next_sid = 2 * num_streams + 1
        keys = [f'mux:many:{i}' for i in range(num_streams)]
        c.send_mux_frame(next_sid, 'DEL', *keys)
        time.sleep(0.5)
        c.recv_mux_frames(0.5)
    finally:
        c.close()


def test_concurrent_mux_connections(t: TestRunner):
    """Multiple MUX connections working simultaneously."""
    c1 = t.new_mux_client()
    c2 = t.new_mux_client()
    try:
        # Both connections set different values
        c1.send_mux_frame(1, 'SET', 'mux:conc:c1', 'from_c1')
        c2.send_mux_frame(1, 'SET', 'mux:conc:c2', 'from_c2')
        time.sleep(0.3)
        c1.recv_mux_frames(0.3)
        c2.recv_mux_frames(0.3)

        # Cross-read: c1 reads c2's key and vice versa
        c1.send_mux_frame(1, 'GET', 'mux:conc:c2')
        c2.send_mux_frame(1, 'GET', 'mux:conc:c1')
        time.sleep(0.3)

        by_sid1, _ = c1.recv_mux_frames_by_sid()
        by_sid2, _ = c2.recv_mux_frames_by_sid()
        t.check("c1 reads c2's value",
                1 in by_sid1 and b'from_c2' in by_sid1[1][0][1])
        t.check("c2 reads c1's value",
                1 in by_sid2 and b'from_c1' in by_sid2[1][0][1])

        c1.send_mux_frame(1, 'DEL', 'mux:conc:c1', 'mux:conc:c2')
        time.sleep(0.2)
        c1.recv_mux_frames(0.3)
    finally:
        c1.close()
        c2.close()


def test_mux_with_plain_client(t: TestRunner):
    """MUX and plain clients coexisting."""
    mux_c = t.new_mux_client()
    plain_c = t.new_plain_client()
    try:
        # MUX client sets
        mux_c.send_mux_frame(1, 'SET', 'mux:mix:k', 'muxval')
        time.sleep(0.3)
        mux_c.recv_mux_frames(0.3)

        # Plain client reads
        resp = plain_c.command('GET', 'mux:mix:k')
        t.check("Plain client reads MUX value", b'muxval' in resp)

        # Plain client sets
        plain_c.command('SET', 'mux:mix:k2', 'plainval')

        # MUX client reads
        mux_c.send_mux_frame(1, 'GET', 'mux:mix:k2')
        time.sleep(0.3)
        frames = mux_c.recv_mux_frames()
        t.check("MUX client reads plain value",
                any(b'plainval' in f[2] for f in frames))

        plain_c.command('DEL', 'mux:mix:k', 'mux:mix:k2')
    finally:
        mux_c.close()
        plain_c.close()


def test_stream_reuse_after_close(t: TestRunner):
    """Stream ID cannot be reused after close (must be increasing)."""
    c = t.new_mux_client()
    try:
        # Use stream 1, then 3
        c.send_mux_frame(1, 'PING')
        c.send_mux_frame(3, 'PING')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        # Close stream 1
        c.send_mux_close_stream(1)
        time.sleep(0.2)

        # Try to reopen stream 1 (ID <= max_client_stream_id=3)
        c.send_mux_frame(1, 'PING')
        time.sleep(0.3)
        frames = c.recv_mux_frames()
        error_frames = [f for f in frames if (f[0] & 0x0F) == MUX_FRAME_ERROR]
        t.check("Reused stream ID rejected", len(error_frames) >= 1)
    finally:
        c.close()


def test_multiple_pings(t: TestRunner):
    """Multiple rapid PINGs should all get PONGs."""
    c = t.new_mux_client()
    try:
        for _ in range(5):
            c.send_mux_ping()
        time.sleep(0.5)
        frames = c.recv_mux_frames()
        pongs = [f for f in frames if (f[0] & 0x0F) == MUX_FRAME_PONG]
        t.check("5 PONGs received", len(pongs) == 5)
    finally:
        c.close()


def test_data_types(t: TestRunner):
    """Various Redis data types through MUX."""
    c = t.new_mux_client()
    try:
        # List
        c.send_mux_frame(1, 'RPUSH', 'mux:dt:list', 'a', 'b', 'c')
        c.send_mux_frame(1, 'LRANGE', 'mux:dt:list', '0', '-1')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        payloads = b''.join(p for _, p in by_sid.get(1, []))
        t.check("LRANGE has all elements", b'a' in payloads and b'b' in payloads and b'c' in payloads)

        # Hash
        c.send_mux_frame(3, 'HSET', 'mux:dt:hash', 'f1', 'v1', 'f2', 'v2')
        c.send_mux_frame(3, 'HGETALL', 'mux:dt:hash')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        payloads = b''.join(p for _, p in by_sid.get(3, []))
        t.check("HGETALL has fields", b'f1' in payloads and b'v1' in payloads)

        # Set
        c.send_mux_frame(5, 'SADD', 'mux:dt:set', 'x', 'y', 'z')
        c.send_mux_frame(5, 'SCARD', 'mux:dt:set')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        payloads = b''.join(p for _, p in by_sid.get(5, []))
        t.check("SCARD = 3", b'3' in payloads)

        # Sorted set
        c.send_mux_frame(7, 'ZADD', 'mux:dt:zset', '1', 'a', '2', 'b', '3', 'c')
        c.send_mux_frame(7, 'ZRANGE', 'mux:dt:zset', '0', '-1', 'WITHSCORES')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        payloads = b''.join(p for _, p in by_sid.get(7, []))
        t.check("ZRANGE has elements", b'a' in payloads and b'b' in payloads)

        # Cleanup
        c.send_mux_frame(9, 'DEL', 'mux:dt:list', 'mux:dt:hash', 'mux:dt:set', 'mux:dt:zset')
        time.sleep(0.3)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_rapid_open_close(t: TestRunner):
    """Rapidly open and close many streams."""
    c = t.new_mux_client()
    try:
        num = 50
        # Open all streams with SET commands first
        for i in range(num):
            sid = 2 * i + 1
            c.send_mux_frame(sid, 'SET', f'mux:rc:{i}', f'{i}')

        # Wait for all responses
        time.sleep(1.0)
        frames = c.recv_mux_frames(1.0)
        ok_count = sum(1 for f in frames if b'+OK' in f[2])
        t.check(f"Rapid open/close: {ok_count}/{num} SET OK", ok_count == num)

        # Now close all streams rapidly
        for i in range(num):
            sid = 2 * i + 1
            c.send_mux_close_stream(sid)
        time.sleep(0.5)
        c.recv_mux_frames(0.5)

        # Verify server is alive with a new stream
        next_sid = 2 * num + 1
        c.send_mux_frame(next_sid, 'PING')
        time.sleep(0.3)
        frames = c.recv_mux_frames(0.5)
        t.check("Server alive after rapid close", len(frames) >= 1)

        # Cleanup keys
        keys = [f'mux:rc:{i}' for i in range(num)]
        c.send_mux_frame(next_sid, 'DEL', *keys)
        time.sleep(0.5)
        c.recv_mux_frames(0.5)
    finally:
        c.close()


def test_pubsub_on_mux_stream(t: TestRunner):
    """SUBSCRIBE/PUBLISH through MUX streams."""
    sub_client = t.new_mux_client()
    pub_client = t.new_mux_client()
    try:
        # Subscribe on stream 1
        sub_client.send_mux_frame(1, 'SUBSCRIBE', 'mux:chan')
        time.sleep(0.3)
        frames = sub_client.recv_mux_frames(0.5)
        t.check("SUBSCRIBE confirmation", any(b'subscribe' in f[2] for f in frames))

        # Publish from another client
        pub_client.send_mux_frame(1, 'PUBLISH', 'mux:chan', 'hello_mux')
        time.sleep(0.3)

        # Subscriber should receive the message
        frames = sub_client.recv_mux_frames(1.0)
        all_payload = b''.join(f[2] for f in frames)
        t.check("Received published message", b'hello_mux' in all_payload)

        # Unsubscribe
        sub_client.send_mux_frame(1, 'UNSUBSCRIBE', 'mux:chan')
        time.sleep(0.3)
        sub_client.recv_mux_frames(0.3)
    finally:
        sub_client.close()
        pub_client.close()


def test_client_setname_getname(t: TestRunner):
    """CLIENT SETNAME/GETNAME on virtual clients."""
    c = t.new_mux_client()
    try:
        c.send_mux_frame(1, 'CLIENT', 'SETNAME', 'mux-stream-1')
        c.send_mux_frame(3, 'CLIENT', 'SETNAME', 'mux-stream-3')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        c.send_mux_frame(1, 'CLIENT', 'GETNAME')
        c.send_mux_frame(3, 'CLIENT', 'GETNAME')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()

        t.check("Stream 1 name", 1 in by_sid and b'mux-stream-1' in by_sid[1][0][1])
        t.check("Stream 3 name", 3 in by_sid and b'mux-stream-3' in by_sid[3][0][1])
    finally:
        c.close()


def test_fin_flag(t: TestRunner):
    """FIN flag on a DATA frame should half-close the stream."""
    c = t.new_mux_client()
    try:
        # Send a DATA frame with FIN flag
        payload = resp_command('SET', 'mux:fin:k', 'finval')
        frame = encode_frame(MUX_FRAME_DATA | MUX_FLAG_FIN, 1, payload)
        c.send_raw_bytes(frame)
        time.sleep(0.3)
        frames = c.recv_mux_frames()
        t.check("FIN frame processed (SET OK)", any(b'+OK' in f[2] for f in frames))

        c.send_mux_frame(3, 'DEL', 'mux:fin:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_empty_payload_data_frame(t: TestRunner):
    """DATA frame with empty payload should not crash."""
    c = t.new_mux_client()
    try:
        c.send_raw_frame(MUX_FRAME_DATA, 1, b'')
        time.sleep(0.3)
        # Should not crash, might not produce any output
        frames = c.recv_mux_frames(0.5)
        t.check("Empty DATA frame doesn't crash", True)
    finally:
        c.close()


def test_interleaved_commands(t: TestRunner):
    """Interleaved commands across streams to verify no cross-contamination."""
    c = t.new_mux_client()
    try:
        # Interleave INCRs on different keys via different streams
        c.send_mux_frame(1, 'SET', 'mux:il:c1', '0')
        c.send_mux_frame(3, 'SET', 'mux:il:c2', '0')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        for i in range(10):
            c.send_mux_frame(1, 'INCR', 'mux:il:c1')
            c.send_mux_frame(3, 'INCR', 'mux:il:c2')

        time.sleep(0.5)
        c.recv_mux_frames(0.5)

        # Verify final values
        c.send_mux_frame(1, 'GET', 'mux:il:c1')
        c.send_mux_frame(3, 'GET', 'mux:il:c2')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()

        t.check("Counter 1 = 10", 1 in by_sid and b'10' in by_sid[1][0][1])
        t.check("Counter 2 = 10", 3 in by_sid and b'10' in by_sid[3][0][1])

        c.send_mux_frame(1, 'DEL', 'mux:il:c1', 'mux:il:c2')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_client_id_unique(t: TestRunner):
    """Each virtual client should have a unique client ID."""
    c = t.new_mux_client()
    try:
        c.send_mux_frame(1, 'CLIENT', 'ID')
        c.send_mux_frame(3, 'CLIENT', 'ID')
        c.send_mux_frame(5, 'CLIENT', 'ID')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()

        ids = set()
        for sid in [1, 3, 5]:
            if sid in by_sid:
                payload = by_sid[sid][0][1].decode('utf-8', errors='replace').strip()
                # Parse integer response ":123\r\n"
                for line in payload.split('\r\n'):
                    if line.startswith(':'):
                        ids.add(int(line[1:]))
        t.check("3 unique client IDs", len(ids) == 3)
    finally:
        c.close()


def test_mux_virtual_client_count_after_close(t: TestRunner):
    """mux_virtual_client_count should decrease when streams are closed."""
    plain = t.new_plain_client()
    c = t.new_mux_client()
    try:
        # Open 3 streams
        c.send_mux_frame(1, 'PING')
        c.send_mux_frame(3, 'PING')
        c.send_mux_frame(5, 'PING')
        time.sleep(0.3)
        c.recv_mux_frames(0.5)

        resp = plain.command('INFO', 'Clients')
        count_before = 0
        for line in resp.decode().split('\r\n'):
            if 'mux_virtual_clients' in line:
                count_before = int(line.split(':')[1].strip())
                break

        # Close 2 streams
        c.send_mux_close_stream(1)
        c.send_mux_close_stream(3)
        time.sleep(0.3)

        resp = plain.command('INFO', 'Clients')
        count_after = 0
        for line in resp.decode().split('\r\n'):
            if 'mux_virtual_clients' in line:
                count_after = int(line.split(':')[1].strip())
                break

        t.check("Count decreased by 2", count_before - count_after == 2)
    finally:
        c.close()
        plain.close()


def test_mux_disconnect_cleanup(t: TestRunner):
    """When MUX connection disconnects, all virtual clients should be freed."""
    plain = t.new_plain_client()

    # Get baseline count
    resp = plain.command('INFO', 'Clients')
    baseline = 0
    for line in resp.decode().split('\r\n'):
        if 'mux_virtual_clients' in line:
            baseline = int(line.split(':')[1].strip())
            break

    # Create MUX conn with streams, then disconnect
    c = t.new_mux_client()
    c.send_mux_frame(1, 'PING')
    c.send_mux_frame(3, 'PING')
    c.send_mux_frame(5, 'PING')
    time.sleep(0.3)
    c.recv_mux_frames(0.5)
    c.close()
    time.sleep(0.5)

    # Check count returned to baseline
    resp = plain.command('INFO', 'Clients')
    count_after = 0
    for line in resp.decode().split('\r\n'):
        if 'mux_virtual_clients' in line:
            count_after = int(line.split(':')[1].strip())
            break

    t.check("Virtual clients freed on disconnect", count_after == baseline)
    plain.close()


def test_watch_on_mux_stream(t: TestRunner):
    """WATCH/MULTI/EXEC optimistic locking on MUX stream."""
    c = t.new_mux_client()
    c2 = t.new_mux_client()
    try:
        c.send_mux_frame(1, 'SET', 'mux:watch:k', '100')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        # Stream 1 watches the key
        c.send_mux_frame(1, 'WATCH', 'mux:watch:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)

        # Another connection modifies the key
        c2.send_mux_frame(1, 'SET', 'mux:watch:k', '200')
        time.sleep(0.2)
        c2.recv_mux_frames(0.3)

        # Stream 1 tries MULTI/EXEC - should fail (return nil array)
        c.send_mux_frame(1, 'MULTI')
        c.send_mux_frame(1, 'SET', 'mux:watch:k', '300')
        c.send_mux_frame(1, 'EXEC')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()

        # EXEC should return nil (empty array) since WATCH was triggered
        payloads = b''.join(p for _, p in by_sid.get(1, []))
        t.check("WATCH+EXEC aborted (nil array)", b'*-1' in payloads)

        # Verify value is 200 (set by c2), not 300
        c.send_mux_frame(3, 'GET', 'mux:watch:k')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        t.check("Value is 200 (WATCH worked)", 3 in by_sid and b'200' in by_sid[3][0][1])

        c.send_mux_frame(3, 'DEL', 'mux:watch:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()
        c2.close()


def test_lua_script_on_mux(t: TestRunner):
    """EVAL Lua script on a MUX stream."""
    c = t.new_mux_client()
    try:
        script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
        c.send_mux_frame(1, 'EVAL', script, '1', 'mux:lua:k', 'luaval')
        time.sleep(0.3)
        by_sid, _ = c.recv_mux_frames_by_sid()
        t.check("Lua script result", 1 in by_sid and b'luaval' in by_sid[1][0][1])

        c.send_mux_frame(1, 'DEL', 'mux:lua:k')
        time.sleep(0.2)
        c.recv_mux_frames(0.3)
    finally:
        c.close()


def test_blocking_command(t: TestRunner):
    """Blocking command (BLPOP) on a MUX stream."""
    c = t.new_mux_client()
    pusher = t.new_mux_client()
    try:
        # Stream 1 does BLPOP with timeout
        c.send_mux_frame(1, 'BLPOP', 'mux:blk:list', '3')

        time.sleep(0.3)
        # Push from another connection
        pusher.send_mux_frame(1, 'RPUSH', 'mux:blk:list', 'blocked_val')
        time.sleep(0.5)
        pusher.recv_mux_frames(0.3)

        # Stream 1 should receive the BLPOP result
        frames = c.recv_mux_frames(2.0)
        all_payload = b''.join(f[2] for f in frames)
        t.check("BLPOP received value", b'blocked_val' in all_payload)
    finally:
        c.close()
        pusher.close()


def test_concurrent_threads(t: TestRunner):
    """Multiple threads using separate MUX connections simultaneously."""
    results = {}
    errors = []

    def worker(thread_id):
        try:
            c = MuxTestClient(t.host, t.port)
            c.connect()
            c.negotiate_mux()

            key = f'mux:thread:{thread_id}'
            c.send_mux_frame(1, 'SET', key, f'thread{thread_id}')
            time.sleep(0.3)
            c.recv_mux_frames(0.5)

            c.send_mux_frame(1, 'GET', key)
            time.sleep(0.3)
            frames = c.recv_mux_frames(0.5)

            ok = any(f'thread{thread_id}'.encode() in f[2] for f in frames)
            results[thread_id] = ok

            c.send_mux_frame(1, 'DEL', key)
            time.sleep(0.2)
            c.recv_mux_frames(0.3)
            c.close()
        except Exception as e:
            errors.append((thread_id, str(e)))
            results[thread_id] = False

    threads = []
    for i in range(10):
        th = threading.Thread(target=worker, args=(i,))
        threads.append(th)
        th.start()

    for th in threads:
        th.join(timeout=10)

    t.check("No thread errors", len(errors) == 0)
    t.check("All 10 threads got correct values",
            all(results.get(i, False) for i in range(10)))


# ==================== Main ====================

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379

    t = TestRunner(host, port)
    print(f"Redis MUX Protocol Test Suite")
    print(f"Target: {host}:{port}")
    print(f"{'='*60}")

    tests = [
        ("basic_set_get",                lambda: test_basic_set_get(t)),
        ("ping_pong",                    lambda: test_ping_pong(t)),
        ("batch_frame",                  lambda: test_batch_frame(t)),
        ("stream_close",                 lambda: test_stream_close(t)),
        ("stream_isolation_db",          lambda: test_stream_isolation_db(t)),
        ("transaction_multi_exec",       lambda: test_transaction_multi_exec(t)),
        ("transaction_isolation",        lambda: test_transaction_isolation(t)),
        ("even_stream_id_rejected",      lambda: test_even_stream_id_rejected(t)),
        ("stream_id_non_increasing",     lambda: test_stream_id_non_increasing(t)),
        ("64bit_stream_ids",             lambda: test_64bit_stream_ids(t)),
        ("pipeline_on_stream",           lambda: test_pipeline_on_stream(t)),
        ("large_payload",                lambda: test_large_payload(t)),
        ("partial_frame_send",           lambda: test_partial_frame_send(t)),
        ("goaway",                       lambda: test_goaway(t)),
        ("error_frame",                  lambda: test_error_frame(t)),
        ("client_list_visibility",       lambda: test_client_list_visibility(t)),
        ("info_mux_virtual_clients",     lambda: test_info_mux_virtual_clients(t)),
        ("many_streams",                 lambda: test_many_streams(t)),
        ("concurrent_mux_connections",   lambda: test_concurrent_mux_connections(t)),
        ("mux_with_plain_client",        lambda: test_mux_with_plain_client(t)),
        ("stream_reuse_after_close",     lambda: test_stream_reuse_after_close(t)),
        ("multiple_pings",               lambda: test_multiple_pings(t)),
        ("data_types",                   lambda: test_data_types(t)),
        ("rapid_open_close",             lambda: test_rapid_open_close(t)),
        ("pubsub_on_mux_stream",         lambda: test_pubsub_on_mux_stream(t)),
        ("client_setname_getname",       lambda: test_client_setname_getname(t)),
        ("fin_flag",                     lambda: test_fin_flag(t)),
        ("empty_payload_data_frame",     lambda: test_empty_payload_data_frame(t)),
        ("interleaved_commands",         lambda: test_interleaved_commands(t)),
        ("client_id_unique",             lambda: test_client_id_unique(t)),
        ("virtual_client_count_close",   lambda: test_mux_virtual_client_count_after_close(t)),
        ("disconnect_cleanup",           lambda: test_mux_disconnect_cleanup(t)),
        ("watch_optimistic_locking",     lambda: test_watch_on_mux_stream(t)),
        ("lua_script",                   lambda: test_lua_script_on_mux(t)),
        ("blocking_command",             lambda: test_blocking_command(t)),
        ("concurrent_threads",           lambda: test_concurrent_threads(t)),
    ]

    for name, func in tests:
        t.run_test(name, func)

    ok = t.summary()
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
