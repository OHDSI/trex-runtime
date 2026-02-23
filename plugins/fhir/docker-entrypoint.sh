#!/bin/bash
set -e

FHIR_HOST="${FHIR_HOST:-0.0.0.0}"
FHIR_PORT="${FHIR_PORT:-8080}"
TREXSQL_PATH="${TREXSQL_PATH:-/data/fhir.db}"

echo "[fhir] Starting FHIR R4 server on ${FHIR_HOST}:${FHIR_PORT}"
echo "[fhir] trexsql path: ${TREXSQL_PATH}"

# Start trexsql with the FHIR extension loaded
exec duckdb "${TREXSQL_PATH}" -c "
INSTALL '/app/fhir.trex';
LOAD fhir;
SELECT fhir_start('${FHIR_HOST}', ${FHIR_PORT});
-- Keep the process alive
SELECT 'FHIR server started successfully';
" &

# Wait for the server to start
sleep 2

# Keep container running
wait
