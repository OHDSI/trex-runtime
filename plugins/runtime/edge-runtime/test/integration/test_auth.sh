#!/bin/bash
# Integration test for Better Auth e2e flows (email/password, sessions, password reset)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PORT="${TEST_PORT:-8001}"
DATABASE_URL="postgres://postgres:mypass@localhost:65432/testdb"
TREX_BIN="${PROJECT_DIR}/target/debug/trex"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose-core.yml"
TREX_PID=""

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

echo "=== Auth E2E Integration Tests ==="
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
DATABASE_URL="$DATABASE_URL" \
BETTER_AUTH_SECRET="test-secret-at-least-32-characters-long" \
BETTER_AUTH_URL="http://localhost:$PORT" \
  "$TREX_BIN" start \
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

# 3. Install test dependencies
echo ""
echo "=== Installing test dependencies ==="
cd "$SCRIPT_DIR/auth"
npm install --silent

# 4. Run tests
echo ""
echo "=== Running Auth E2E Tests ==="
TEST_SERVER_URL="http://localhost:$PORT" \
DATABASE_URL="$DATABASE_URL" \
  npx vitest run

echo ""
echo "All auth tests passed!"
