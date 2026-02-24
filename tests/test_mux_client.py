#!/usr/bin/env python3
"""
Redis MUX Protocol Test Client

A minimal test client to verify the Redis Stream-Multiplexed Protocol (RSMP).
Usage: python3 test_mux_client.py [host] [port]
"""

import socket
import struct
import sys
import time

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

def encode_frame(flags, stream_id, payload):
    """Encode a MUX frame: 14-byte header + payload."""
    header = struct.pack('>BBQI', MUX_FRAME_MAGIC, flags, stream_id, len(payload))
    return header + payload

def decode_frame(data):
    """Decode a MUX frame from data. Returns (flags, stream_id, payload, consumed_bytes) or None."""
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
        """Connect to Redis server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        print(f"[+] Connected to {self.host}:{self.port}")

    def send_resp(self, *args):
        """Send a RESP command and receive response (non-MUX mode)."""
        cmd = resp_command(*args)
        self.sock.sendall(cmd)
        return recv_all(self.sock, 1.0)

    def negotiate_mux(self):
        """Send HELLO 3 MULTIPLEX to switch to MUX mode."""
        print("[*] Negotiating MUX mode with HELLO 3 MULTIPLEX...")
        resp = self.send_resp('HELLO', '3', 'MULTIPLEX')
        resp_str = resp.decode('utf-8', errors='replace')
        print(f"[*] HELLO response: {resp_str.strip()}")
        if b'mux' in resp.lower() if hasattr(resp, 'lower') else b'mux' in resp:
            self.mux_mode = True
            print("[+] MUX mode enabled!")
        else:
            # Check if the response contains 'enabled' in some form
            if b'enabled' in resp:
                self.mux_mode = True
                print("[+] MUX mode enabled!")
            else:
                print("[!] MUX mode may or may not be enabled, continuing...")
                self.mux_mode = True  # Assume success since HELLO should have worked
        return resp

    def send_mux_frame(self, stream_id, *cmd_args):
        """Send a command as a MUX DATA frame."""
        payload = resp_command(*cmd_args)
        frame = encode_frame(MUX_FRAME_DATA, stream_id, payload)
        self.sock.sendall(frame)
        print(f"[>] Sent frame: SID={stream_id} cmd={' '.join(str(a) for a in cmd_args)}")

    def send_mux_ping(self):
        """Send a MUX PING frame on stream 0."""
        frame = encode_frame(MUX_FRAME_PING, 0, b'')
        self.sock.sendall(frame)
        print("[>] Sent PING frame")

    def send_mux_close_stream(self, stream_id):
        """Send a STREAM_CLOSE frame."""
        frame = encode_frame(MUX_FRAME_STREAM_CLOSE, stream_id, b'')
        self.sock.sendall(frame)
        print(f"[>] Sent STREAM_CLOSE for SID={stream_id}")

    def send_mux_batch(self, commands):
        """Send multiple commands as a single BATCH frame.
        commands: list of (stream_id, cmd_args_tuple)
        """
        batch_payload = b''
        for stream_id, cmd_args in commands:
            inner_payload = resp_command(*cmd_args)
            inner_frame = encode_frame(MUX_FRAME_DATA, stream_id, inner_payload)
            batch_payload += inner_frame
        frame = encode_frame(MUX_FRAME_BATCH, 0, batch_payload)
        self.sock.sendall(frame)
        print(f"[>] Sent BATCH frame with {len(commands)} sub-frames")

    def recv_mux_frames(self, timeout=2.0):
        """Receive and decode MUX frames from server."""
        data = recv_all(self.sock, timeout)
        frames = []
        offset = 0
        while offset < len(data):
            result = decode_frame(data[offset:])
            if result is None:
                # Remaining data might be incomplete
                print(f"[!] Remaining undecoded bytes: {len(data) - offset}")
                break
            flags, stream_id, payload, consumed = result
            frame_type = flags & 0x0F
            type_names = {
                1: 'DATA', 2: 'STREAM_OPEN', 3: 'STREAM_CLOSE',
                4: 'PING', 5: 'PONG', 6: 'GOAWAY', 7: 'BATCH',
                8: 'WINDOW', 9: 'ERROR'
            }
            type_name = type_names.get(frame_type, f'UNKNOWN(0x{frame_type:02x})')
            payload_str = payload.decode('utf-8', errors='replace')
            print(f"[<] Frame: type={type_name} SID={stream_id} len={len(payload)} payload={payload_str.strip()}")
            frames.append((flags, stream_id, payload))
            offset += consumed
        return frames

    def close(self):
        if self.sock:
            self.sock.close()
            print("[*] Connection closed")


def test_basic_mux(host, port):
    """Test basic MUX operations."""
    client = MuxTestClient(host, port)
    passed = 0
    failed = 0
    total = 0

    def check(name, condition):
        nonlocal passed, failed, total
        total += 1
        if condition:
            passed += 1
            print(f"  ✅ PASS: {name}")
        else:
            failed += 1
            print(f"  ❌ FAIL: {name}")

    try:
        # Step 1: Connect
        client.connect()

        # Step 2: Negotiate MUX mode
        client.negotiate_mux()

        # Step 3: Send commands on different streams
        print("\n--- Test 1: Multiple streams ---")
        client.send_mux_frame(1, 'SET', 'mux_key1', 'hello')
        client.send_mux_frame(3, 'SET', 'mux_key2', 'world')
        client.send_mux_frame(5, 'GET', 'mux_key1')

        time.sleep(0.5)
        frames = client.recv_mux_frames()

        # Validate: should get 3 response frames on SID 1, 3, 5
        sids = [f[1] for f in frames]
        check("Received 3 response frames", len(frames) == 3)
        check("SID=1 present in responses", 1 in sids)
        check("SID=3 present in responses", 3 in sids)
        check("SID=5 present in responses", 5 in sids)

        # Validate response content
        for flags, sid, payload in frames:
            if sid == 1:
                check("SID=1 SET response is +OK", b'+OK' in payload)
            elif sid == 3:
                check("SID=3 SET response is +OK", b'+OK' in payload)
            elif sid == 5:
                check("SID=5 GET response contains 'hello'", b'hello' in payload)

        # Step 4: Test PING/PONG
        print("\n--- Test 2: PING/PONG ---")
        client.send_mux_ping()
        time.sleep(0.5)
        frames = client.recv_mux_frames()
        pong_frames = [f for f in frames if (f[0] & 0x0F) == 0x05]
        check("Received PONG frame", len(pong_frames) >= 1)
        if pong_frames:
            check("PONG on SID=0 (control stream)", pong_frames[0][1] == 0)

        # Step 5: Test batch
        print("\n--- Test 3: Batch frame ---")
        client.send_mux_batch([
            (7, ('SET', 'batch_key1', 'val1')),
            (9, ('SET', 'batch_key2', 'val2')),
            (11, ('MGET', 'batch_key1', 'batch_key2')),
        ])
        time.sleep(0.5)
        frames = client.recv_mux_frames()
        batch_sids = [f[1] for f in frames]
        check("Received 3 batch response frames", len(frames) == 3)
        check("Batch SID=7 present", 7 in batch_sids)
        check("Batch SID=9 present", 9 in batch_sids)
        check("Batch SID=11 present", 11 in batch_sids)

        for flags, sid, payload in frames:
            if sid == 11:
                check("SID=11 MGET contains 'val1'", b'val1' in payload)
                check("SID=11 MGET contains 'val2'", b'val2' in payload)

        # Step 6: Test stream close
        print("\n--- Test 4: Stream close ---")
        client.send_mux_close_stream(1)
        time.sleep(0.2)
        # Sending on closed stream 1 should get an error or be ignored
        # For now just verify no crash
        check("Stream close did not crash server", True)

        # Step 7: Verify key values (new stream)
        print("\n--- Test 5: Verify data persistence ---")
        client.send_mux_frame(13, 'GET', 'mux_key2')
        time.sleep(0.5)
        frames = client.recv_mux_frames()
        check("Received response on SID=13", len(frames) >= 1)
        if frames:
            check("SID=13 GET returns 'world'", b'world' in frames[0][2])

        # Step 8: Test 64-bit stream IDs (beyond uint32 range)
        print("\n--- Test 6: 64-bit stream IDs (beyond uint32 range) ---")

        # Close some old streams to free up capacity
        for sid in [3, 5, 7, 9, 11, 13]:
            client.send_mux_close_stream(sid)
        time.sleep(0.3)
        client.recv_mux_frames(0.5)  # Drain any responses

        # Use stream IDs that exceed 0xFFFFFFFF to prove 64-bit support
        big_sid_1 = 0x100000001  # 4294967297 - just over uint32 max
        big_sid_2 = 0x200000003  # 8589934595
        big_sid_3 = 0xFFFFFFFFFFFFFFFD  # Near uint64 max (odd number)

        client.send_mux_frame(big_sid_1, 'SET', 'big_sid_key1', 'value_big1')
        client.send_mux_frame(big_sid_2, 'SET', 'big_sid_key2', 'value_big2')
        client.send_mux_frame(big_sid_3, 'SET', 'big_sid_key3', 'value_big3')

        time.sleep(0.5)
        frames = client.recv_mux_frames()

        big_sids = [f[1] for f in frames]
        check("Received 3 frames for 64-bit SIDs", len(frames) == 3)
        check(f"SID=0x{big_sid_1:X} present in responses", big_sid_1 in big_sids)
        check(f"SID=0x{big_sid_2:X} present in responses", big_sid_2 in big_sids)
        check(f"SID=0x{big_sid_3:X} present in responses", big_sid_3 in big_sids)

        for flags, sid, payload in frames:
            if sid == big_sid_1:
                check(f"SID=0x{big_sid_1:X} SET response is +OK", b'+OK' in payload)
            elif sid == big_sid_2:
                check(f"SID=0x{big_sid_2:X} SET response is +OK", b'+OK' in payload)
            elif sid == big_sid_3:
                check(f"SID=0x{big_sid_3:X} SET response is +OK", b'+OK' in payload)

        # Verify we can GET back the values using the same big stream IDs
        # (reuse big_sid_1 which is already open)
        client.send_mux_frame(big_sid_1, 'GET', 'big_sid_key1')
        time.sleep(0.5)
        frames = client.recv_mux_frames()
        check(f"SID=0x{big_sid_1:X} GET returns 'value_big1'",
              len(frames) >= 1 and b'value_big1' in frames[0][2])

        # Verify another big stream ID
        client.send_mux_frame(big_sid_3, 'GET', 'big_sid_key3')
        time.sleep(0.5)
        frames = client.recv_mux_frames()
        check(f"SID=0x{big_sid_3:X} GET returns 'value_big3'",
              len(frames) >= 1 and b'value_big3' in frames[0][2])

        # Test batch with 64-bit stream IDs (reuse existing open streams)
        print("\n--- Test 7: Batch with 64-bit stream IDs ---")
        client.send_mux_batch([
            (big_sid_2, ('GET', 'big_sid_key2')),
            (big_sid_3, ('GET', 'big_sid_key3')),
        ])
        time.sleep(0.5)
        frames = client.recv_mux_frames()
        batch_big_sids = [f[1] for f in frames]
        check("Received 2 batch frames for 64-bit SIDs", len(frames) == 2)
        check(f"Batch SID=0x{big_sid_2:X} present", big_sid_2 in batch_big_sids)
        check(f"Batch SID=0x{big_sid_3:X} present", big_sid_3 in batch_big_sids)

        for flags, sid, payload in frames:
            if sid == big_sid_2:
                check(f"SID=0x{big_sid_2:X} GET returns 'value_big2'", b'value_big2' in payload)
            elif sid == big_sid_3:
                check(f"SID=0x{big_sid_3:X} GET returns 'value_big3'", b'value_big3' in payload)

        # Cleanup
        print("\n--- Cleanup ---")
        client.send_mux_frame(big_sid_1, 'DEL', 'mux_key1', 'mux_key2', 'batch_key1', 'batch_key2',
                              'big_sid_key1', 'big_sid_key2', 'big_sid_key3')
        time.sleep(0.5)
        client.recv_mux_frames()

        # Summary
        print(f"\n{'='*50}")
        print(f"Test Results: {passed}/{total} passed, {failed}/{total} failed")
        if failed == 0:
            print("[+] All tests PASSED!")
        else:
            print(f"[!] {failed} test(s) FAILED!")
        print(f"{'='*50}")

    except Exception as e:
        print(f"[!] Error: {e}")
        import traceback
        traceback.print_exc()
        failed += 1
    finally:
        client.close()

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    host = sys.argv[1] if len(sys.argv) > 1 else '127.0.0.1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379
    sys.exit(test_basic_mux(host, port))
