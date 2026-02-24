#!/bin/bash
# Non-MUX proxy benchmark - run each test individually and capture to file
BENCH=/home/feixuwu/MyCode/redis/src/redis-benchmark
HOST=127.0.0.1
PORT=6479
C=50
N=100000
OUT=/home/feixuwu/MyCode/redis/proxy/tests/bench_no_mux.txt

> $OUT

echo "=== Non-MUX Proxy, c=$C, n=$N ===" >> $OUT
for cmd in set get incr lpush rpush lpop rpop sadd hset spop mset; do
  echo -n "Testing $cmd ... " >> $OUT
  RESULT=$($BENCH -h $HOST -p $PORT -c $C -n $N -t $cmd --csv 2>&1 | grep -v "^\"test\"")
  echo "$RESULT" >> $OUT
done

echo "" >> $OUT
echo "=== Non-MUX Proxy Pipeline=10, c=$C, n=$N ===" >> $OUT
for cmd in set get incr lpush rpush lpop rpop sadd hset spop mset; do
  echo -n "Testing ${cmd}_P10 ... " >> $OUT
  RESULT=$($BENCH -h $HOST -p $PORT -c $C -n $N -P 10 -t $cmd --csv 2>&1 | grep -v "^\"test\"")
  echo "$RESULT" >> $OUT
done

echo "" >> $OUT
echo "=== ALL DONE ===" >> $OUT
