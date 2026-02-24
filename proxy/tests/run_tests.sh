#!/bin/bash
# Run all Redis MUX Proxy tests
# Usage: ./run_tests.sh [--asan] [--proxy-port=6380] [--redis-port=6379]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROXY_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROXY_DIR/build"
PROXY_BIN="$BUILD_DIR/redis-mux-proxy"
REDIS_BIN="${REDIS_BIN:-redis-server}"

PROXY_PORT=6380
ADMIN_PORT=9090
REDIS_PORT=6379
PASSWORD=""
ASAN=0

for arg in "$@"; do
    case "$arg" in
        --asan) ASAN=1 ;;
        --proxy-port=*) PROXY_PORT="${arg#*=}" ;;
        --redis-port=*) REDIS_PORT="${arg#*=}" ;;
        --admin-port=*) ADMIN_PORT="${arg#*=}" ;;
        --password=*) PASSWORD="${arg#*=}" ;;
    esac
done

echo "=============================="
echo "Redis MUX Proxy Test Runner"
echo "=============================="

# Build
echo "[1/5] Building proxy..."
mkdir -p "$BUILD_DIR"
pushd "$BUILD_DIR" > /dev/null
if [ $ASAN -eq 1 ]; then
    echo "  ASAN enabled"
    cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON
else
    cmake .. -DCMAKE_BUILD_TYPE=Debug
fi
make -j$(nproc)
popd > /dev/null
echo "  Build OK"

# Check if Redis is running
if ! command -v redis-cli &> /dev/null; then
    echo "WARNING: redis-cli not found in PATH"
fi

echo ""
echo "[2/5] Starting test infrastructure..."
echo "  Make sure Redis is running on port $REDIS_PORT"
echo "  Make sure proxy is running on port $PROXY_PORT with admin on $ADMIN_PORT"
echo ""

# Build test args
TEST_ARGS="--proxy-port=$PROXY_PORT --admin-port=$ADMIN_PORT"
if [ -n "$PASSWORD" ]; then
    TEST_ARGS="$TEST_ARGS --password=$PASSWORD"
fi

# Run basic tests
echo "[3/5] Running basic functional tests..."
python3 "$SCRIPT_DIR/test_basic.py" $TEST_ARGS
echo ""

# Run MUX tests
echo "[4/5] Running MUX mode tests..."
python3 "$SCRIPT_DIR/test_mux.py" $TEST_ARGS
echo ""

# Run stability tests
echo "[5/5] Running stability tests..."
python3 "$SCRIPT_DIR/test_stability.py" $TEST_ARGS
echo ""

echo "=============================="
echo "All test suites completed!"
echo "=============================="
