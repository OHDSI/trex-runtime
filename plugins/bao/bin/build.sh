#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAVA_DIR="$PROJECT_DIR/java"
LIB_DIR="$PROJECT_DIR/lib"
JAR_FILE="$LIB_DIR/trexsql.jar"

mkdir -p "$LIB_DIR"

# Check if JAR already exists
if [[ -f "$JAR_FILE" ]]; then
    echo "JAR already exists: $JAR_FILE"
    exit 0
fi

# Build from source
if [[ ! -d "$JAVA_DIR" ]]; then
    echo "Error: java/ directory not found at $JAVA_DIR"
    exit 1
fi

if ! command -v lein &> /dev/null; then
    echo "Error: Leiningen is required to build trexsql.jar"
    echo "Install from: https://leiningen.org/"
    exit 1
fi

# Ensure native libraries are in resources for JNA classpath loading
RESOURCE_DIR="$JAVA_DIR/resources/linux-x86-64"
REPO_ROOT="$(cd "$PROJECT_DIR/../.." && pwd)"
NATIVE_LIB="$REPO_ROOT/target/release/libtrexsql_engine.so"

if [[ -f "$RESOURCE_DIR/libtrexsql_engine.so" ]]; then
    echo "Native libraries already in resources (placed by CI)"
elif [[ -f "$NATIVE_LIB" ]]; then
    mkdir -p "$RESOURCE_DIR"
    cp "$NATIVE_LIB" "$RESOURCE_DIR/"
    echo "Bundled: $NATIVE_LIB"
    # Bundle libduckdb.so (trexsql_engine's runtime dependency)
    for DUCKDB_LIB in /usr/local/lib/libduckdb.so /usr/lib/libduckdb.so /usr/lib/libtrexsql.so; do
        if [[ -f "$DUCKDB_LIB" ]]; then
            cp "$DUCKDB_LIB" "$RESOURCE_DIR/libduckdb.so"
            echo "Bundled: $DUCKDB_LIB -> libduckdb.so"
            break
        fi
    done
else
    echo "Error: libtrexsql_engine.so not found"
    echo "Build it first with: cargo build --release"
    exit 1
fi

echo "Building trexsql.jar from source..."
cd "$JAVA_DIR"
lein uberjar

# Find and copy the standalone jar
STANDALONE_JAR=$(ls target/trexsql-*-standalone.jar 2>/dev/null | head -1)
if [[ -n "$STANDALONE_JAR" ]]; then
    cp "$STANDALONE_JAR" "$JAR_FILE"
    echo "Built: $JAR_FILE"
    ls -lh "$JAR_FILE"
else
    echo "Error: Build succeeded but standalone JAR not found"
    exit 1
fi
