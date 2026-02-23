#!/bin/bash
# Integration test for PostGraphile GraphQL endpoint in core/server

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PORT="${TEST_PORT:-8001}"
DATABASE_URL="postgres://postgres:mypass@localhost:65432/testdb"
TREX_BIN="${PROJECT_DIR}/target/debug/trex"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose-core.yml"
TREX_PID=""
PASSED=0
FAILED=0

cleanup() {
  echo ""
  echo "=== Cleanup ==="
  if [ -n "$TREX_PID" ] && kill -0 "$TREX_PID" 2>/dev/null; then
    echo "Stopping trex (PID $TREX_PID)..."
    kill "$TREX_PID" 2>/dev/null || true
    wait "$TREX_PID" 2>/dev/null || true
  fi
  echo "Stopping postgres..."
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}
trap cleanup EXIT

assert_status() {
  local test_name="$1"
  local expected="$2"
  local actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "  PASS: $test_name (HTTP $actual)"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL: $test_name (expected HTTP $expected, got $actual)"
    FAILED=$((FAILED + 1))
  fi
}

assert_contains() {
  local test_name="$1"
  local needle="$2"
  local haystack="$3"
  if echo "$haystack" | grep -q "$needle"; then
    echo "  PASS: $test_name"
    PASSED=$((PASSED + 1))
  else
    echo "  FAIL: $test_name (response does not contain '$needle')"
    echo "  Response: $haystack"
    FAILED=$((FAILED + 1))
  fi
}

echo "=== PostGraphile GraphQL Integration Test ==="
echo "Project: $PROJECT_DIR"
echo "Port: $PORT"

# Check trex binary exists
if [ ! -f "$TREX_BIN" ]; then
  echo "ERROR: trex binary not found at $TREX_BIN"
  echo "Run: cd ext/trexas && make debug"
  exit 1
fi

# 1. Start postgres
echo ""
echo "=== Starting PostgreSQL ==="
docker compose -f "$COMPOSE_FILE" up -d postgres

echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
  if docker compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  if [ "$i" = "30" ]; then
    echo "ERROR: PostgreSQL failed to start within 30 seconds"
    exit 1
  fi
  sleep 1
done

# 2. Start trex
echo ""
echo "=== Starting Trex ==="
DATABASE_URL="$DATABASE_URL" "$TREX_BIN" start \
  -p "$PORT" \
  --main-service "$PROJECT_DIR/core/server" \
  --event-worker "$PROJECT_DIR/core/event" &
TREX_PID=$!

echo "Waiting for trex to be ready (PID $TREX_PID)..."
for i in $(seq 1 30); do
  if curl -sf "http://localhost:$PORT/_internal/health" >/dev/null 2>&1; then
    echo "Trex is ready."
    break
  fi
  if ! kill -0 "$TREX_PID" 2>/dev/null; then
    echo "ERROR: Trex process exited unexpectedly"
    exit 1
  fi
  if [ "$i" = "30" ]; then
    echo "ERROR: Trex failed to start within 30 seconds"
    exit 1
  fi
  sleep 1
done

# 3. Run tests
echo ""
echo "=== Running Tests ==="

# Test: health endpoint
echo ""
echo "--- Health Check ---"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$PORT/_internal/health")
assert_status "GET /_internal/health" "200" "$STATUS"

BODY=$(curl -s "http://localhost:$PORT/_internal/health")
assert_contains "Health response body" '"ok"' "$BODY"

# Test: GraphQL introspection
echo ""
echo "--- GraphQL Introspection ---"
INTROSPECTION=$(curl -s -w "\n%{http_code}" -X POST "http://localhost:$PORT/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ __schema { queryType { name } } }"}')
INTRO_BODY=$(echo "$INTROSPECTION" | head -n -1)
INTRO_STATUS=$(echo "$INTROSPECTION" | tail -n 1)

assert_status "POST /graphql introspection" "200" "$INTRO_STATUS"
assert_contains "Introspection returns queryType" "queryType" "$INTRO_BODY"

# Test: Query seeded data
echo ""
echo "--- Query Seeded Data ---"
QUERY_RESULT=$(curl -s -w "\n%{http_code}" -X POST "http://localhost:$PORT/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ allPeople { nodes { firstName lastName } } }"}')
QUERY_BODY=$(echo "$QUERY_RESULT" | head -n -1)
QUERY_STATUS=$(echo "$QUERY_RESULT" | tail -n 1)

assert_status "POST /graphql allPeople query" "200" "$QUERY_STATUS"
assert_contains "Query returns Alice" "Alice" "$QUERY_BODY"
assert_contains "Query returns Bob" "Bob" "$QUERY_BODY"
assert_contains "Query returns Carol" "Carol" "$QUERY_BODY"

# Test: GraphiQL IDE
echo ""
echo "--- GraphiQL IDE ---"
GRAPHIQL=$(curl -s -w "\n%{http_code}" "http://localhost:$PORT/graphiql")
GRAPHIQL_STATUS=$(echo "$GRAPHIQL" | tail -n 1)

assert_status "GET /graphiql" "200" "$GRAPHIQL_STATUS"

# Summary
echo ""
echo "=== Results ==="
echo "Passed: $PASSED"
echo "Failed: $FAILED"

if [ "$FAILED" -gt 0 ]; then
  exit 1
fi

echo "All tests passed!"
