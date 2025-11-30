#!/bin/bash

# Test script for HANA DuckDB extension
# Uses HANA_CON environment variable for connection string
# Expected format: hdb://USERNAME:PASSWORD@HOST:PORT

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "HANA DuckDB Extension Test Script"
echo "========================================"
echo ""

# Check if HANA_CON is set
if [ -z "$HANA_CON" ]; then
    echo -e "${RED}Error: HANA_CON environment variable is not set${NC}"
    echo "Please set it with: export HANA_CON=hdb://USERNAME:PASSWORD@HOST:PORT"
    exit 1
fi

echo -e "${GREEN}✓ HANA_CON is set${NC}"
echo ""

# Find DuckDB CLI (try multiple common locations)
DUCKDB_CLI=""
if command -v duckdb &> /dev/null; then
    DUCKDB_CLI="duckdb"
elif [ -f "./duckdb/build/release/duckdb" ]; then
    DUCKDB_CLI="./duckdb/build/release/duckdb"
elif [ -f "./build/release/duckdb" ]; then
    DUCKDB_CLI="./build/release/duckdb"
else
    echo -e "${RED}Error: DuckDB CLI not found${NC}"
    echo "Please build DuckDB or ensure it's in your PATH"
    exit 1
fi

echo -e "${GREEN}✓ Using DuckDB CLI: $DUCKDB_CLI${NC}"
echo ""

# Find the extension file
EXTENSION_FILE=""
if [ -f "./build/release/extension/hana_scan/hana_scan.duckdb_extension" ]; then
    EXTENSION_FILE="./build/release/extension/hana_scan/hana_scan.duckdb_extension"
elif [ -f "./build/debug/extension/hana_scan/hana_scan.duckdb_extension" ]; then
    EXTENSION_FILE="./build/debug/extension/hana_scan/hana_scan.duckdb_extension"
    echo -e "${YELLOW}Note: Using debug build${NC}"
else
    echo -e "${RED}Error: Extension file not found${NC}"
    echo "Please build the extension first with: make debug or make release"
    exit 1
fi

echo -e "${GREEN}✓ Found extension: $EXTENSION_FILE${NC}"
echo ""

# Create a temporary SQL file for testing
TEST_SQL=$(mktemp)
trap "rm -f $TEST_SQL" EXIT

cat > "$TEST_SQL" << 'EOF'
-- Load the HANA extension
LOAD 'BUILD_PATH/extension/hana_scan/hana_scan.duckdb_extension';

-- Test 1: Execute a simple query
SELECT '=== Test 1: Simple Query ===' as test;
SELECT hana_execute('CONNECTION_STRING', 'SELECT 1 FROM DUMMY') as result;

-- Test 2: Create a test table
SELECT '=== Test 2: Create Table ===' as test;
SELECT hana_execute('CONNECTION_STRING', 'CREATE COLUMN TABLE TEST_DUCKDB_HANA (id INTEGER, name VARCHAR(100))') as result;

-- Test 3: Insert data
SELECT '=== Test 3: Insert Data ===' as test;
SELECT hana_execute('CONNECTION_STRING', 'INSERT INTO TEST_DUCKDB_HANA VALUES (1, ''Test1'')') as result;
SELECT hana_execute('CONNECTION_STRING', 'INSERT INTO TEST_DUCKDB_HANA VALUES (2, ''Test2'')') as result;

-- Test 4: Query the data (using hana_scan if available)
SELECT '=== Test 4: Query Data ===' as test;
-- Note: If hana_scan function is implemented, you can use it here
-- SELECT * FROM hana_scan('CONNECTION_STRING', 'SELECT * FROM TEST_DUCKDB_HANA');

-- Test 5: Clean up
SELECT '=== Test 5: Cleanup ===' as test;
SELECT hana_execute('CONNECTION_STRING', 'DROP TABLE TEST_DUCKDB_HANA') as result;

SELECT '=== All Tests Completed ===' as test;
EOF

# Replace placeholders in the SQL file
BUILD_TYPE="release"
if [[ "$EXTENSION_FILE" == *"debug"* ]]; then
    BUILD_TYPE="debug"
fi

sed -i "s|BUILD_PATH|./build/$BUILD_TYPE|g" "$TEST_SQL"
sed -i "s|CONNECTION_STRING|$HANA_CON|g" "$TEST_SQL"

echo "========================================"
echo "Running Tests..."
echo "========================================"
echo ""

# Run the tests (with -unsigned flag to allow loading unsigned extensions)
if $DUCKDB_CLI -unsigned < "$TEST_SQL"; then
    echo ""
    echo "========================================"
    echo -e "${GREEN}✓ All tests completed successfully!${NC}"
    echo "========================================"
    exit 0
else
    echo ""
    echo "========================================"
    echo -e "${RED}✗ Tests failed!${NC}"
    echo "========================================"
    exit 1
fi
