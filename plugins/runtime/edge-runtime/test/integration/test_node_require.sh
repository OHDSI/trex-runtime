#!/bin/bash
# Test script to verify Node.js require functionality works in trexas extension

set -e

EXTENSION_PATH="${1:-/home/ph/code/trex/plugins/runtime/edge-runtime/ext/trexas/build/release/trexas.duckdb_extension}"
PORT="${2:-19876}"

echo "=== Trexas Node.js Require Integration Test ==="
echo "Extension: $EXTENSION_PATH"
echo "Test port: $PORT"

# Check extension exists
if [ ! -f "$EXTENSION_PATH" ]; then
    echo "ERROR: Extension not found at $EXTENSION_PATH"
    exit 1
fi

# Test with the actual d2e service path
echo ""
echo "=== Testing with real d2e service ==="
D2E_PATH="/home/ph/code/d2e/services/trex/core/server"
D2E_EVENT="/home/ph/code/d2e/services/trex/core/event"

if [ ! -d "$D2E_PATH" ]; then
    echo "ERROR: d2e service not found at $D2E_PATH"
    exit 1
fi

# Start server in background
echo "Starting server..."
duckdb -unsigned -c "
LOAD '$EXTENSION_PATH';
SELECT trex_start_server('127.0.0.1', $PORT, '$D2E_PATH', '$D2E_EVENT');
" &
DUCKDB_PID=$!

# Wait for server to start
sleep 3

# Check if server is running
if ! kill -0 $DUCKDB_PID 2>/dev/null; then
    echo "FAIL: Server crashed during startup"
    wait $DUCKDB_PID 2>/dev/null || true
    exit 1
fi

echo "Server started, making HTTP request..."

# Make an HTTP request to trigger the application code
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/" 2>&1 || echo "CURL_FAILED")

echo "Response: $RESPONSE"

# Kill the server
kill $DUCKDB_PID 2>/dev/null || true
wait $DUCKDB_PID 2>/dev/null || true

if [ "$RESPONSE" = "CURL_FAILED" ]; then
    echo "FAIL: Could not connect to server (likely crashed)"
    exit 1
elif [ "$RESPONSE" = "000" ]; then
    echo "FAIL: Server not responding"
    exit 1
else
    echo "PASS: Server responded with HTTP $RESPONSE"
fi

echo ""
echo "=== Test completed ==="
