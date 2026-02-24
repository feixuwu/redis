#!/bin/bash
# ASAN Stability Test for MUX Proxy
# Runs various stress tests and checks ASAN output for memory issues

set -e

BENCH=/home/feixuwu/MyCode/redis/src/redis-benchmark
CLI=/home/feixuwu/MyCode/redis/src/redis-cli
PROXY_HOST=127.0.0.1
PROXY_PORT=6479
REDIS_PORT=6379
ADMIN_PORT=9090
CONNS=50
REQUESTS=100000
OUTPUT=/tmp/asan_stability_result.txt
ASAN_LOG_PREFIX=/tmp/asan_proxy

echo "============================================================" > $OUTPUT
echo "  ASAN Stability Test for MUX Proxy" >> $OUTPUT
echo "  $(date)" >> $OUTPUT
echo "============================================================" >> $OUTPUT
echo "" >> $OUTPUT

# Proxy info
echo "--- Proxy Info ---" >> $OUTPUT
$CLI -p $ADMIN_PORT PROXY INFO 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Phase 1: redis-benchmark stress (no pipeline)
echo "=== Phase 1: redis-benchmark (c=$CONNS, n=$REQUESTS, no pipeline) ===" >> $OUTPUT
echo "[Phase 1] Running redis-benchmark stress (no pipeline)..."
$BENCH -h $PROXY_HOST -p $PROXY_PORT -c $CONNS -n $REQUESTS \
  -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,mset \
  --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Phase 2: redis-benchmark stress (pipeline=10)
echo "=== Phase 2: redis-benchmark (c=$CONNS, n=$REQUESTS, pipeline=10) ===" >> $OUTPUT
echo "[Phase 2] Running redis-benchmark stress (pipeline=10)..."
$BENCH -h $PROXY_HOST -p $PROXY_PORT -c $CONNS -n $REQUESTS -P 10 \
  -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,mset \
  --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Phase 3: Mixed concurrent test (SUB/PUB + BLPOP + TX + Normal)
echo "=== Phase 3: Mixed concurrent test (60s) ===" >> $OUTPUT
echo "[Phase 3] Running mixed concurrent test (60s)..."
python3 /home/feixuwu/MyCode/redis/proxy/tests/test_mixed_mux.py \
  --proxy-port=$PROXY_PORT --duration=60 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Phase 4: Rapid connect/disconnect stress (tests connection lifecycle)
echo "=== Phase 4: Rapid connect/disconnect (5 rounds x $CONNS conn) ===" >> $OUTPUT
echo "[Phase 4] Running rapid connect/disconnect stress..."
for round in 1 2 3 4 5; do
    echo "  Round $round..." >> $OUTPUT
    $BENCH -h $PROXY_HOST -p $PROXY_PORT -c $CONNS -n 10000 -t set,get --csv 2>&1 >> $OUTPUT
    sleep 1
done
echo "" >> $OUTPUT

# Phase 5: Large payload test
echo "=== Phase 5: Large payload test (d=1024, c=$CONNS, n=50000) ===" >> $OUTPUT
echo "[Phase 5] Running large payload test..."
$BENCH -h $PROXY_HOST -p $PROXY_PORT -c $CONNS -n 50000 -d 1024 -t set,get --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Phase 6: Very large payload test
echo "=== Phase 6: Very large payload test (d=16384, c=10, n=10000) ===" >> $OUTPUT
echo "[Phase 6] Running very large payload test..."
$BENCH -h $PROXY_HOST -p $PROXY_PORT -c 10 -n 10000 -d 16384 -t set,get --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Phase 7: Second round of mixed concurrent test (catch delayed issues)
echo "=== Phase 7: Mixed concurrent test round 2 (30s) ===" >> $OUTPUT
echo "[Phase 7] Running mixed concurrent test round 2 (30s)..."
python3 /home/feixuwu/MyCode/redis/proxy/tests/test_mixed_mux.py \
  --proxy-port=$PROXY_PORT --duration=30 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Final proxy info
echo "--- Proxy Info After All Tests ---" >> $OUTPUT
$CLI -p $ADMIN_PORT PROXY INFO 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Gracefully stop proxy to trigger ASAN leak check
echo "=== Stopping proxy for ASAN leak detection ===" >> $OUTPUT
echo "[Final] Stopping proxy for ASAN leak detection..."
PROXY_PID=$(pgrep -f redis-mux-proxy 2>/dev/null | head -1)
if [ -n "$PROXY_PID" ]; then
    echo "  Sending SIGTERM to PID $PROXY_PID" >> $OUTPUT
    kill -TERM $PROXY_PID 2>/dev/null || true
    # Wait for proxy to exit (ASAN needs time to dump leak info)
    for i in $(seq 1 30); do
        if ! kill -0 $PROXY_PID 2>/dev/null; then
            echo "  Proxy exited after ${i}s" >> $OUTPUT
            break
        fi
        sleep 1
    done
    # Force kill if still alive
    if kill -0 $PROXY_PID 2>/dev/null; then
        echo "  WARNING: Force killing proxy" >> $OUTPUT
        kill -9 $PROXY_PID 2>/dev/null || true
    fi
else
    echo "  WARNING: No proxy process found" >> $OUTPUT
fi

sleep 2

# Check ASAN output
echo "" >> $OUTPUT
echo "=== ASAN Log Analysis ===" >> $OUTPUT
echo "[Final] Checking ASAN logs..."

ASAN_ERRORS=0

# Check ASAN log files
for logfile in ${ASAN_LOG_PREFIX}.*; do
    if [ -f "$logfile" ]; then
        echo "--- File: $logfile ---" >> $OUTPUT
        cat "$logfile" >> $OUTPUT 2>&1
        echo "" >> $OUTPUT
        # Check for actual errors (not just summary)
        if grep -qE "ERROR:|SUMMARY:.*[1-9]" "$logfile" 2>/dev/null; then
            ASAN_ERRORS=$((ASAN_ERRORS + 1))
        fi
    fi
done

# Also check proxy stdout/stderr log
if [ -f /tmp/proxy_asan_stdout.log ]; then
    echo "--- Proxy stdout/stderr log (last 50 lines) ---" >> $OUTPUT
    tail -50 /tmp/proxy_asan_stdout.log >> $OUTPUT 2>&1
    echo "" >> $OUTPUT
    if grep -qE "ERROR:.*Sanitizer|SUMMARY:.*[1-9]" /tmp/proxy_asan_stdout.log 2>/dev/null; then
        ASAN_ERRORS=$((ASAN_ERRORS + 1))
    fi
fi

if [ ! -f ${ASAN_LOG_PREFIX}.* ] 2>/dev/null && [ ! -s /tmp/proxy_asan_stdout.log ]; then
    echo "  No ASAN log files found (this could mean no errors were detected)" >> $OUTPUT
fi

echo "" >> $OUTPUT
echo "============================================================" >> $OUTPUT
if [ $ASAN_ERRORS -eq 0 ]; then
    echo "  ASAN STABILITY TEST PASSED - No memory errors detected" >> $OUTPUT
else
    echo "  ASAN STABILITY TEST FAILED - $ASAN_ERRORS ASAN error(s) found" >> $OUTPUT
fi
echo "============================================================" >> $OUTPUT

echo ""
echo "Test complete. Results saved to $OUTPUT"
echo "ASAN logs: ls -la ${ASAN_LOG_PREFIX}.*"
cat $OUTPUT
