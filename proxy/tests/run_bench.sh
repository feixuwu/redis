#!/bin/bash
# Performance benchmark for MUX proxy using redis-benchmark
# 1 worker, MUX enabled, 50 concurrent connections

BENCH=/home/feixuwu/MyCode/redis/src/redis-benchmark
CLI=/home/feixuwu/MyCode/redis/src/redis-cli
PROXY_HOST=127.0.0.1
PROXY_PORT=6479
REDIS_PORT=6379
ADMIN_PORT=9090
CONNS=50
REQUESTS=100000
OUTPUT=/tmp/bench_result.txt

echo "============================================================" > $OUTPUT
echo "  Redis MUX Proxy Benchmark (50 connections, 1 worker)" >> $OUTPUT
echo "============================================================" >> $OUTPUT
echo "" >> $OUTPUT

# Proxy info
echo "--- Proxy Info ---" >> $OUTPUT
$CLI -p $ADMIN_PORT PROXY INFO 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Test 1: Proxy benchmark (CSV format for easy reading)
echo "=== Proxy Benchmark (c=$CONNS, n=$REQUESTS) ===" >> $OUTPUT
$BENCH -h $PROXY_HOST -p $PROXY_PORT -c $CONNS -n $REQUESTS \
  -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,mset \
  --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Test 2: Proxy benchmark with pipeline=10
echo "=== Proxy Benchmark with Pipeline=10 ===" >> $OUTPUT
$BENCH -h $PROXY_HOST -p $PROXY_PORT -c $CONNS -n $REQUESTS -P 10 \
  -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,mset \
  --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Test 3: Direct Redis benchmark (baseline)
echo "=== Direct Redis Benchmark (baseline, c=$CONNS, n=$REQUESTS) ===" >> $OUTPUT
$BENCH -h $PROXY_HOST -p $REDIS_PORT -c $CONNS -n $REQUESTS \
  -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,mset \
  --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Test 4: Direct Redis with pipeline=10
echo "=== Direct Redis Benchmark with Pipeline=10 ===" >> $OUTPUT
$BENCH -h $PROXY_HOST -p $REDIS_PORT -c $CONNS -n $REQUESTS -P 10 \
  -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,mset \
  --csv 2>&1 >> $OUTPUT
echo "" >> $OUTPUT

# Proxy info after test
echo "--- Proxy Info After Test ---" >> $OUTPUT
$CLI -p $ADMIN_PORT PROXY INFO 2>&1 >> $OUTPUT

echo "============================================================" >> $OUTPUT
echo "  BENCHMARK COMPLETE" >> $OUTPUT
echo "============================================================" >> $OUTPUT

echo "DONE"
