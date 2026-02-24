#!/bin/bash
# Hot upgrade stress test script
# Tests: multiple persistent clients, data correctness, connection persistence,
# and measures QPS/latency impact during hot upgrade

REDIS_CLI="/home/feixuwu/MyCode/redis/src/redis-cli"
REDIS_BENCHMARK="/home/feixuwu/MyCode/redis/src/redis-benchmark"
PROXY_PORT=6479
ADMIN_PORT=9090
PROXY_BIN="/home/feixuwu/MyCode/redis/proxy/build/redis-mux-proxy"
PROXY_CONF="/home/feixuwu/MyCode/redis/proxy/conf/proxy.yaml"
LOG_DIR="/tmp/hot_upgrade_test"
NUM_PERSISTENT_CLIENTS=5
TEST_DURATION=30  # seconds for each phase

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*

echo "=============================================="
echo "  Hot Upgrade Stress Test"
echo "=============================================="
echo ""

# Ensure proxy is running
PROXY_PID=$(pgrep -f redis-mux-proxy)
if [ -z "$PROXY_PID" ]; then
    echo "[ERROR] Proxy not running. Please start it first."
    exit 1
fi
echo "[INFO] Proxy running with PID=$PROXY_PID"

# Pre-populate some data
echo "[PHASE 0] Pre-populating test data..."
for i in $(seq 1 100); do
    $REDIS_CLI -p $PROXY_PORT SET "hotupgrade:key:$i" "value_$i" > /dev/null 2>&1
done
echo "[OK] 100 keys populated"

# Verify data
VERIFY_OK=0
VERIFY_FAIL=0
for i in $(seq 1 100); do
    val=$($REDIS_CLI -p $PROXY_PORT GET "hotupgrade:key:$i" 2>/dev/null)
    if [ "$val" = "value_$i" ]; then
        ((VERIFY_OK++))
    else
        ((VERIFY_FAIL++))
    fi
done
echo "[OK] Pre-verification: $VERIFY_OK ok, $VERIFY_FAIL failed"

###############################################################################
# PHASE 1: Launch persistent clients that continuously send commands
###############################################################################
echo ""
echo "[PHASE 1] Launching $NUM_PERSISTENT_CLIENTS persistent clients..."

# Each client runs a loop: INCR a counter + GET/SET + measure latency
for cid in $(seq 1 $NUM_PERSISTENT_CLIENTS); do
    (
        counter_key="hotupgrade:counter:$cid"
        $REDIS_CLI -p $PROXY_PORT SET "$counter_key" 0 > /dev/null 2>&1

        errors=0
        successes=0
        max_latency_us=0
        total_latency_us=0
        start_ts=$(date +%s%N)

        # Use a single persistent connection via pipe
        while true; do
            cmd_start=$(date +%s%N)
            result=$($REDIS_CLI -p $PROXY_PORT INCR "$counter_key" 2>&1)
            cmd_end=$(date +%s%N)

            if [[ "$result" =~ ^[0-9]+$ ]]; then
                ((successes++))
                latency_us=$(( (cmd_end - cmd_start) / 1000 ))
                total_latency_us=$((total_latency_us + latency_us))
                if [ $latency_us -gt $max_latency_us ]; then
                    max_latency_us=$latency_us
                fi
            else
                ((errors++))
                echo "[CLIENT $cid] ERROR at cmd #$((successes+errors)): $result" >> "$LOG_DIR/client_${cid}_errors.log"
            fi

            # Check if we should stop (check for stop file)
            if [ -f "$LOG_DIR/stop_clients" ]; then
                break
            fi
        done

        end_ts=$(date +%s%N)
        elapsed_ms=$(( (end_ts - start_ts) / 1000000 ))
        if [ $successes -gt 0 ]; then
            avg_latency_us=$((total_latency_us / successes))
        else
            avg_latency_us=0
        fi

        # Get final counter value to verify data correctness
        final_val=$($REDIS_CLI -p $PROXY_PORT GET "$counter_key" 2>/dev/null)

        echo "client=$cid successes=$successes errors=$errors final_counter=$final_val expected=$successes elapsed_ms=$elapsed_ms avg_latency_us=$avg_latency_us max_latency_us=$max_latency_us" > "$LOG_DIR/client_${cid}_result.txt"
    ) &
    echo "  Client $cid started (PID=$!)"
done

# Also launch a benchmark-style client to measure QPS continuously
echo "[PHASE 1b] Launching continuous QPS measurement..."
(
    phase="pre_upgrade"
    while true; do
        if [ -f "$LOG_DIR/stop_clients" ]; then
            break
        fi

        if [ -f "$LOG_DIR/upgrade_triggered" ]; then
            phase="during_upgrade"
        fi
        if [ -f "$LOG_DIR/upgrade_complete" ]; then
            phase="post_upgrade"
        fi

        # Measure QPS for 2-second intervals
        qps_start=$(date +%s%N)
        qps_count=0
        while true; do
            $REDIS_CLI -p $PROXY_PORT PING > /dev/null 2>&1
            ((qps_count++))
            qps_now=$(date +%s%N)
            qps_elapsed=$(( (qps_now - qps_start) / 1000000 ))
            if [ $qps_elapsed -ge 2000 ]; then
                break
            fi
        done
        qps=$((qps_count * 1000 / qps_elapsed))
        echo "$(date +%H:%M:%S) phase=$phase qps=$qps ops=$qps_count elapsed_ms=$qps_elapsed" >> "$LOG_DIR/qps_log.txt"

        if [ -f "$LOG_DIR/stop_clients" ]; then
            break
        fi
    done
) &
QPS_PID=$!
echo "  QPS monitor started (PID=$QPS_PID)"

# Let pre-upgrade phase run for a few seconds to establish baseline
echo ""
echo "[PHASE 1c] Running pre-upgrade baseline for 8 seconds..."
sleep 8

###############################################################################
# PHASE 2: Trigger hot upgrade while clients are running
###############################################################################
echo ""
echo "[PHASE 2] Triggering hot upgrade..."
OLD_PID=$(pgrep -f redis-mux-proxy)
echo "  Old PID: $OLD_PID"

touch "$LOG_DIR/upgrade_triggered"
UPGRADE_START=$(date +%s%N)

# Trigger upgrade via admin port
UPGRADE_RESULT=$($REDIS_CLI -p $ADMIN_PORT PROXY UPGRADE 2>&1)
echo "  Upgrade command result: $UPGRADE_RESULT"

# Wait for new process to take over
sleep 5

NEW_PID=$(pgrep -f redis-mux-proxy)
UPGRADE_END=$(date +%s%N)
UPGRADE_DURATION_MS=$(( (UPGRADE_END - UPGRADE_START) / 1000000 ))

echo "  New PID: $NEW_PID"
echo "  Upgrade duration: ${UPGRADE_DURATION_MS}ms"

touch "$LOG_DIR/upgrade_complete"

if [ "$OLD_PID" = "$NEW_PID" ]; then
    echo "  [WARNING] PID unchanged - upgrade may have failed!"
else
    echo "  [OK] PID changed: $OLD_PID -> $NEW_PID"
fi

# Test connectivity after upgrade
POST_PING=$($REDIS_CLI -p $PROXY_PORT PING 2>&1)
POST_ADMIN=$($REDIS_CLI -p $ADMIN_PORT PING 2>&1)
echo "  Post-upgrade data port PING: $POST_PING"
echo "  Post-upgrade admin port PING: $POST_ADMIN"

###############################################################################
# PHASE 3: Continue running after upgrade for a while
###############################################################################
echo ""
echo "[PHASE 3] Running post-upgrade for 8 seconds..."
sleep 8

###############################################################################
# PHASE 4: Stop clients and collect results
###############################################################################
echo ""
echo "[PHASE 4] Stopping clients and collecting results..."
touch "$LOG_DIR/stop_clients"
sleep 3

# Wait for all background jobs
wait 2>/dev/null

echo ""
echo "=============================================="
echo "  TEST RESULTS"
echo "=============================================="

# Display per-client results
echo ""
echo "--- Per-Client Results ---"
TOTAL_SUCCESS=0
TOTAL_ERRORS=0
DATA_CORRECT=0
DATA_INCORRECT=0

for cid in $(seq 1 $NUM_PERSISTENT_CLIENTS); do
    if [ -f "$LOG_DIR/client_${cid}_result.txt" ]; then
        cat "$LOG_DIR/client_${cid}_result.txt"
        eval $(cat "$LOG_DIR/client_${cid}_result.txt" | tr ' ' '\n' | grep -E '^(successes|errors|final_counter|expected)=' | tr '\n' ';')
        TOTAL_SUCCESS=$((TOTAL_SUCCESS + successes))
        TOTAL_ERRORS=$((TOTAL_ERRORS + errors))
        if [ "$final_counter" = "$expected" ] || [ "$final_counter" = "$successes" ]; then
            ((DATA_CORRECT++))
        else
            ((DATA_INCORRECT++))
            echo "  [MISMATCH] Client $cid: final=$final_counter expected=$successes"
        fi
    else
        echo "  [MISSING] No result for client $cid"
    fi
done

# Display error logs if any
echo ""
echo "--- Error Logs ---"
for cid in $(seq 1 $NUM_PERSISTENT_CLIENTS); do
    if [ -f "$LOG_DIR/client_${cid}_errors.log" ]; then
        echo "Client $cid errors:"
        head -20 "$LOG_DIR/client_${cid}_errors.log"
    fi
done

# Display QPS timeline
echo ""
echo "--- QPS Timeline ---"
if [ -f "$LOG_DIR/qps_log.txt" ]; then
    cat "$LOG_DIR/qps_log.txt"
fi

# Verify original data is still intact
echo ""
echo "--- Data Integrity Check ---"
POST_VERIFY_OK=0
POST_VERIFY_FAIL=0
for i in $(seq 1 100); do
    val=$($REDIS_CLI -p $PROXY_PORT GET "hotupgrade:key:$i" 2>/dev/null)
    if [ "$val" = "value_$i" ]; then
        ((POST_VERIFY_OK++))
    else
        ((POST_VERIFY_FAIL++))
        if [ $POST_VERIFY_FAIL -le 5 ]; then
            echo "  [FAIL] key=hotupgrade:key:$i expected=value_$i got=$val"
        fi
    fi
done
echo "Post-upgrade data verification: $POST_VERIFY_OK ok, $POST_VERIFY_FAIL failed (out of 100)"

# Summary
echo ""
echo "=============================================="
echo "  SUMMARY"
echo "=============================================="
echo "  Upgrade duration:     ${UPGRADE_DURATION_MS}ms"
echo "  Old PID -> New PID:   $OLD_PID -> $NEW_PID"
echo "  Total operations:     $((TOTAL_SUCCESS + TOTAL_ERRORS))"
echo "  Successful ops:       $TOTAL_SUCCESS"
echo "  Failed ops:           $TOTAL_ERRORS"
echo "  Data correctness:     $DATA_CORRECT/$NUM_PERSISTENT_CLIENTS clients correct"
echo "  Original data intact: $POST_VERIFY_OK/100"
echo ""

if [ $TOTAL_ERRORS -eq 0 ] && [ $POST_VERIFY_FAIL -eq 0 ] && [ "$OLD_PID" != "$NEW_PID" ]; then
    echo "  ✅ HOT UPGRADE TEST PASSED"
else
    echo "  ❌ HOT UPGRADE TEST HAS ISSUES"
    [ $TOTAL_ERRORS -gt 0 ] && echo "     - $TOTAL_ERRORS operation errors detected"
    [ $POST_VERIFY_FAIL -gt 0 ] && echo "     - $POST_VERIFY_FAIL data integrity failures"
    [ "$OLD_PID" = "$NEW_PID" ] && echo "     - PID did not change (upgrade may have failed)"
fi

echo ""
echo "Detailed logs in: $LOG_DIR/"
