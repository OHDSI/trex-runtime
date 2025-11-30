#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SRC_FILE="$PROJECT_DIR/c/bao.c"
OUT_FILE="$SCRIPT_DIR/trex"
LIB_DIR="$PROJECT_DIR/lib"

DUCKDB_VERSION="${DUCKDB_VERSION:-1.4.0}"

if ! command -v gcc &> /dev/null; then
    echo "Error: gcc is required to build trex"
    exit 1
fi

detect_platform() {
    case "$(uname -s)" in
        Linux*)
            case "$(uname -m)" in
                x86_64) echo "linux-amd64" ;;
                aarch64) echo "linux-aarch64" ;;
                *) echo "unsupported"; return 1 ;;
            esac
            ;;
        *) echo "unsupported"; return 1 ;;
    esac
}

download_duckdb() {
    local platform="$1"

    echo "Downloading DuckDB v${DUCKDB_VERSION} for ${platform}..."

    mkdir -p "$LIB_DIR"

    local zip_file="/tmp/libduckdb-${platform}.zip"
    local url="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-${platform}.zip"

    curl -L -o "$zip_file" "$url"
    unzip -o "$zip_file" -d "$LIB_DIR"
    rm "$zip_file"

    rm -f "$LIB_DIR/libduckdb_static.a"
    rm -f "$LIB_DIR/duckdb.hpp"
    rm -f "$LIB_DIR/libduckdb.so.1.4.0"  

    if [[ -f "$LIB_DIR/libduckdb.so" ]]; then
        ln -sf libduckdb.so "$LIB_DIR/libduckdb.so.1.4"
    elif [[ -f "$LIB_DIR/libduckdb.dylib" ]]; then
        ln -sf libduckdb.dylib "$LIB_DIR/libduckdb.1.4.dylib"
    fi

    echo "DuckDB downloaded to $LIB_DIR"
}

if [[ ! -f "$LIB_DIR/libduckdb.so" ]] && [[ ! -f "$LIB_DIR/libduckdb.dylib" ]]; then
    PLATFORM=$(detect_platform)
    if [[ "$PLATFORM" == "unsupported" ]]; then
        echo "Error: Unsupported platform $(uname -s) $(uname -m)"
        exit 1
    fi
    download_duckdb "$PLATFORM"
fi

if [[ -f "$OUT_FILE" ]] && [[ -f "$LIB_DIR/libduckdb.so" || -f "$LIB_DIR/libduckdb.dylib" ]]; then
    echo "Binary already built, skipping compilation"
    exit 0
fi

if [[ ! -f "$LIB_DIR/duckdb.h" ]]; then
    echo "Error: duckdb.h not found in $LIB_DIR"
    exit 1
fi

echo "Compiling $SRC_FILE..."
echo "  Include: $LIB_DIR"
echo "  Library: $LIB_DIR"

gcc -o "$OUT_FILE" "$SRC_FILE" \
    -I"$LIB_DIR" -L"$LIB_DIR" -lduckdb \
    -Wl,-rpath,"\$ORIGIN/../lib" -O2

echo "Built: $OUT_FILE"
file "$OUT_FILE"
ls -lh "$OUT_FILE"

rm -f "$LIB_DIR/duckdb.h"
