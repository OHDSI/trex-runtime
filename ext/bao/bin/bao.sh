#!/bin/bash
set -e

# Default values
TREXAS_HOST="${TREXAS_HOST:-0.0.0.0}"
TREXAS_PORT="${TREXAS_PORT:-9876}"
PGWIRE_HOST="${PGWIRE_HOST:-0.0.0.0}"
PGWIRE_PORT="${PGWIRE_PORT:-5433}"
MAIN_PATH="${MAIN_PATH:-$(pwd)/main}"
EVENT_WORKER_PATH="${EVENT_WORKER_PATH:-}"
TLS_CERT="${TLS_CERT:-}"
TLS_KEY="${TLS_KEY:-}"
TLS_PORT="${TLS_PORT:-9443}"
ENABLE_INSPECTOR="${ENABLE_INSPECTOR:-false}"
INSPECTOR_TYPE="${INSPECTOR_TYPE:-inspect}"
INSPECTOR_HOST="${INSPECTOR_HOST:-0.0.0.0}"
INSPECTOR_PORT="${INSPECTOR_PORT:-9229}"
ALLOW_MAIN_INSPECTOR="${ALLOW_MAIN_INSPECTOR:-false}"

show_help() {
    cat << EOF
Usage: bao [options]

Options:
  --trexas-host <host>        Trexas server host (default: 0.0.0.0)
  --trexas-port <port>        Trexas server port (default: 9876)
  --pgwire-host <host>        PgWire server host (default: 0.0.0.0)
  --pgwire-port <port>        PgWire server port (default: 5433)
  --enable-inspector          Enable Trexas inspector
  --inspector-type <type>     Inspector type: inspect, inspect-brk, inspect-wait (default: inspect)
  --inspector-host <host>     Inspector host (default: 0.0.0.0)
  --inspector-port <port>     Inspector port (default: 9229)
  --allow-main-inspector      Allow inspector in main worker (default: false)
  --tls-cert <path>           Path to TLS certificate file (enables HTTPS)
  --tls-key <path>            Path to TLS private key file (required with --tls-cert)
  --tls-port <port>           TLS port (default: 9443)
  --main-path <path>          Path to main service directory (default: ./main)
  --event-worker-path <path>  Path to event worker directory
  -h, --help                  Show this help message
EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --trexas-host) TREXAS_HOST="$2"; shift 2 ;;
        --trexas-port) TREXAS_PORT="$2"; shift 2 ;;
        --pgwire-host) PGWIRE_HOST="$2"; shift 2 ;;
        --pgwire-port) PGWIRE_PORT="$2"; shift 2 ;;
        --main-path) MAIN_PATH="$2"; shift 2 ;;
        --event-worker-path) EVENT_WORKER_PATH="$2"; shift 2 ;;
        --tls-cert) TLS_CERT="$2"; shift 2 ;;
        --tls-key) TLS_KEY="$2"; shift 2 ;;
        --tls-port) TLS_PORT="$2"; shift 2 ;;
        --enable-inspector) ENABLE_INSPECTOR="true"; shift ;;
        --inspector-type) INSPECTOR_TYPE="$2"; shift 2 ;;
        --inspector-host) INSPECTOR_HOST="$2"; shift 2 ;;
        --inspector-port) INSPECTOR_PORT="$2"; shift 2 ;;
        --allow-main-inspector) ALLOW_MAIN_INSPECTOR="true"; shift ;;
        -h|--help) show_help ;;
        --) shift; break ;;
        *) echo "Unknown option: $1"; show_help ;;
    esac
done

echo "🦕 Starting TREX"

# Find duckdb CLI
DUCKDB_CMD="${DUCKDB_CMD:-duckdb}"
if ! command -v "$DUCKDB_CMD" &> /dev/null; then
    echo "Error: duckdb CLI not found. Please install it or set DUCKDB_CMD."
    exit 1
fi

# Check for required environment variable
if [[ -z "$TREX_SQL_PASSWORD" ]]; then
    echo "Error: TREX_SQL_PASSWORD environment variable is not set"
    exit 1
fi

# Function to check if AVX is supported (for llama extension)
has_avx_support() {
    if [[ -f /proc/cpuinfo ]]; then
        grep -q '\bavx\b' /proc/cpuinfo
        return $?
    fi
    return 1
}

# Build SQL commands to load extensions
build_load_extensions_sql() {
    local trex_path="${TREX_EXTENSIONS_PATH:-/usr/src/extensions}"
    local sql=""

    if [[ -d "$trex_path" ]]; then
        # Look for .duckdb_extension files directly in the path
        for ext_file in "$trex_path"/*.duckdb_extension; do
            if [[ -f "$ext_file" ]]; then
                local ext_name=$(basename "$ext_file" .duckdb_extension)

                # Skip llama if no AVX support
                if [[ "$ext_name" == "llama" ]] && ! has_avx_support; then
                    echo "Skipping llama extension (no AVX support)" >&2
                    continue
                fi

                echo "Loading extension: $ext_name" >&2
                sql+="LOAD '${ext_file}';"
            fi
        done

        # Also check subdirectories (for node_modules/@trex style layout)
        for dir in "$trex_path"/*/; do
            if [[ -d "$dir" ]]; then
                for ext_file in "$dir"*.duckdb_extension; do
                    if [[ -f "$ext_file" ]]; then
                        local ext_name=$(basename "$ext_file" .duckdb_extension)

                        # Skip llama if no AVX support
                        if [[ "$ext_name" == "llama" ]] && ! has_avx_support; then
                            echo "Skipping llama extension (no AVX support)" >&2
                            continue
                        fi

                        echo "Loading extension: $ext_name" >&2
                        sql+="LOAD '${ext_file}';"
                    fi
                done
            fi
        done
    fi

    echo "$sql"
}

# Build trexas config JSON
build_trexas_config() {
    local config="{\"host\":\"${TREXAS_HOST}\",\"port\":${TREXAS_PORT},\"main_service_path\":\"${MAIN_PATH}\""

    if [[ -n "$EVENT_WORKER_PATH" ]]; then
        config+=",\"event_worker_path\":\"${EVENT_WORKER_PATH}\""
    fi

    if [[ -n "$TLS_CERT" ]]; then
        config+=",\"tls_cert_path\":\"${TLS_CERT}\""
    fi

    if [[ -n "$TLS_KEY" ]]; then
        config+=",\"tls_key_path\":\"${TLS_KEY}\""
    fi

    if [[ -n "$TLS_CERT" ]]; then
        config+=",\"tls_port\":${TLS_PORT}"
    fi

    if [[ "$ENABLE_INSPECTOR" == "true" ]]; then
        config+=",\"inspector\":\"${INSPECTOR_TYPE}:${INSPECTOR_HOST}:${INSPECTOR_PORT}\""
    fi

    if [[ "$ALLOW_MAIN_INSPECTOR" == "true" ]]; then
        config+=",\"allow_main_inspector\":true"
    fi

    config+="}"
    echo "$config"
}

# Build the SQL script
LOAD_EXTENSIONS_SQL=$(build_load_extensions_sql)
TREXAS_CONFIG=$(build_trexas_config)

# Create a temporary SQL file for initialization
INIT_SQL_FILE=$(mktemp /tmp/bao_init.XXXXXX.sql)

cat > "$INIT_SQL_FILE" << EOF
${LOAD_EXTENSIONS_SQL}
SELECT start_pgwire_server('${PGWIRE_HOST}', ${PGWIRE_PORT}, '${TREX_SQL_PASSWORD}', '') as pgwire_result;
SELECT trex_start_server_with_config('${TREXAS_CONFIG}') as trexas_result;
EOF

echo ""
echo "🚀 Starting servers..."

# Set up signal handlers
DUCKDB_PID=""
cleanup() {
    echo ""
    echo "Shutting down..."
    if [[ -n "$DUCKDB_PID" ]]; then
        kill "$DUCKDB_PID" 2>/dev/null || true
    fi
    rm -f "$INIT_SQL_FILE"
    exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# Print startup info
echo ""
echo "✅ Servers starting..."

if [[ -n "$TLS_CERT" ]]; then
    echo "Trexas listening on https://${TREXAS_HOST}:${TREXAS_PORT}"
else
    echo "Trexas listening on http://${TREXAS_HOST}:${TREXAS_PORT}"
fi

if [[ "$ENABLE_INSPECTOR" == "true" ]]; then
    echo "  (inspector: ${INSPECTOR_TYPE}:${INSPECTOR_HOST}:${INSPECTOR_PORT})"
fi

if [[ -n "$EVENT_WORKER_PATH" ]]; then
    echo "  (with event worker)"
else
    echo "  (without event worker)"
fi

echo "PgWire listening on ${PGWIRE_HOST}:${PGWIRE_PORT}"
echo ""
echo "Press Ctrl+C to stop"

# Run DuckDB with the init script
# The servers run in background threads spawned by the extensions
# We use -readonly and :memory: to keep DuckDB from exiting
# The -init runs our SQL, then DuckDB waits for more input from stdin
# We keep stdin open by reading from a blocking read
"$DUCKDB_CMD" -unsigned -init "$INIT_SQL_FILE" -no-stdin &
DUCKDB_PID=$!

# Wait for servers to start
sleep 2

# Check if DuckDB is still running (servers might have crashed)
if ! kill -0 "$DUCKDB_PID" 2>/dev/null; then
    echo "Error: DuckDB process exited unexpectedly"
    wait $DUCKDB_PID
    exit 1
fi

# Keep the script alive - the servers run in DuckDB's threads
# Wait for DuckDB process (which should run until killed)
wait $DUCKDB_PID
