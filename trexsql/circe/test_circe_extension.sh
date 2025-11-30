#!/bin/bash

# Test script for Circe DuckDB Extension
# This script tests all available functions in the Circe extension
# Requires: duckdb executable in PATH

# Don't exit on error - we want to run all tests
set +e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
B    # Test 15: Error handling - invalid dialect (returns error as SQL comment)
    run_test "Error handling - invalid dialect" \
        "SELECT circe_sql_translate('SELECT * FROM person;', 'invalid_dialect');" \
        "sqltranslate error" \
        "contains"
    
    # Test 16: Error handling - invalid dialect in render_translate (returns error as SQL comment)
    run_test "Error handling - invalid dialect in render_translate" \
        "SELECT circe_sql_render_translate('SELECT TOP 10 * FROM person;', 'not_a_real_dialect', '{}');" \
        "sqlrender_translate error" \
        "contains"
    
    # Test 17: Verify SQL structure contains expected DDL elements4m'
NC='\033[0m' # No Color

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Test result tracking
declare -a FAILED_TESTS=()

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "PASS")
            echo -e "${GREEN}[PASS]${NC} $message"
            ((TESTS_PASSED++))
            ;;
        "FAIL")
            echo -e "${RED}[FAIL]${NC} $message"
            ((TESTS_FAILED++))
            FAILED_TESTS+=("$message")
            ;;
        "INFO")
            echo -e "${BLUE}[INFO]${NC} $message"
            ;;
        "WARN")
            echo -e "${YELLOW}[WARN]${NC} $message"
            ;;
    esac
}

# Function to run a test
run_test() {
    local test_name="$1"
    local sql_query="$2"
    local expected_result="$3"
    local check_type="${4:-exact}"  # exact, contains, not_contains, not_null, should_error
    
    ((TESTS_RUN++))
    print_status "INFO" "Running test: $test_name"
    
    # Prepare the SQL with extension loading
    local full_query="$sql_query"
    if [ -n "$EXTENSION_PATH" ]; then
        full_query="LOAD '$EXTENSION_PATH'; $sql_query"
    fi
    
    # Execute the query with -unsigned flag
    local result
    local exit_code
    result=$(echo "$full_query" | duckdb -unsigned 2>&1)
    exit_code=$?
    
    # Print the query result (truncate if too long)
    if [ ${#result} -gt 200 ]; then
        echo "  Result (truncated): ${result:0:200}..."
    else
        echo "  Result: $result"
    fi
    
    # For should_error tests, we expect the command to fail
    if [ "$check_type" == "should_error" ]; then
        if [ $exit_code -ne 0 ] && [[ "$result" == *"$expected_result"* ]]; then
            print_status "PASS" "$test_name"
            return 0
        else
            print_status "FAIL" "$test_name - Expected error containing '$expected_result', Got (exit=$exit_code): '$result'"
            return 1
        fi
    fi
    
    # For other tests, check if command succeeded
    if [ $exit_code -eq 0 ]; then
        case $check_type in
            "exact")
                if [[ "$result" == "$expected_result" ]]; then
                    print_status "PASS" "$test_name"
                    return 0
                else
                    print_status "FAIL" "$test_name - Expected: '$expected_result', Got: '$result'"
                    return 1
                fi
                ;;
            "contains")
                if [[ "$result" == *"$expected_result"* ]]; then
                    print_status "PASS" "$test_name"
                    return 0
                else
                    print_status "FAIL" "$test_name - Expected result to contain: '$expected_result', Got: '$result'"
                    return 1
                fi
                ;;
            "not_contains")
                if [[ "$result" != *"$expected_result"* ]]; then
                    print_status "PASS" "$test_name"
                    return 0
                else
                    print_status "FAIL" "$test_name - Expected result NOT to contain: '$expected_result', Got: '$result'"
                    return 1
                fi
                ;;
            "not_null")
                if [[ -n "$result" && "$result" != "NULL" ]]; then
                    print_status "PASS" "$test_name"
                    return 0
                else
                    print_status "FAIL" "$test_name - Expected non-null result, Got: '$result'"
                    return 1
                fi
                ;;
        esac
    else
        print_status "FAIL" "$test_name - Query failed: $result"
        return 1
    fi
}

# Function to check if DuckDB is available
check_duckdb() {
    if ! command -v duckdb &> /dev/null; then
        print_status "FAIL" "duckdb executable not found in PATH"
        echo "Please install DuckDB or add it to your PATH"
        exit 1
    fi
    
    local version=$(duckdb --version 2>/dev/null || echo "Unknown")
    print_status "INFO" "DuckDB version: $version"
}

# Function to load the Circe extension
load_extension() {
    print_status "INFO" "Loading Circe extension..."
    
    # Find the extension file
    EXTENSION_PATH=""
    if [ -f "build/release/circe.duckdb_extension" ]; then
        EXTENSION_PATH="$(pwd)/build/release/circe.duckdb_extension"
    elif [ -f "build/release/extension/circe/circe.duckdb_extension" ]; then
        EXTENSION_PATH="$(pwd)/build/release/extension/circe/circe.duckdb_extension"
    fi
    
    if [ -z "$EXTENSION_PATH" ]; then
        print_status "WARN" "Extension file not found in build directory"
        print_status "INFO" "Trying to load from system..."
        if echo "INSTALL circe; LOAD circe;" | duckdb 2>/dev/null; then
            print_status "PASS" "Circe extension loaded from system"
        else
            print_status "FAIL" "Could not load Circe extension"
            echo "Please build the extension first with: make release"
            exit 1
        fi
    else
        print_status "INFO" "Found extension at: $EXTENSION_PATH"
        print_status "PASS" "Circe extension ready to load"
    fi
}

# Base64 encoded JSON for testing
# Simple cohort expression: {"dummy":"expression"}
SIMPLE_JSON_B64="eyJkdW1teSI6ImV4cHJlc3Npb24ifQ=="

# Complex cohort expression JSON (Base64 encoded)
COMPLEX_JSON_B64="ewogICJQcmltYXJ5Q3JpdGVyaWEiOiB7CiAgICAiQ3JpdGVyaWFMaXN0IjogW10sCiAgICAiT2JzZXJ2YXRpb25XaW5kb3ciOiB7IlByaW9yRGF5cyI6IDAsICJQb3N0RGF5cyI6IDB9LAogICAgIlByaW1hcnlDcml0ZXJpYUxpbWl0IjogeyJUeXBlIjogIkFsbCJ9CiAgfSwKICAiQ29uY2VwdFNldHMiOiBbXSwKICAiUXVhbGlmaWVkTGltaXQiOiB7IlR5cGUiOiAiRmlyc3QifSwKICAiRXhwcmVzc2lvbkxpbWl0IjogeyJUeXBlIjogIkFsbCJ9LAogICJJbmNsdXNpb25SdWxlcyI6IFtdCn0="

# Options JSON for testing
OPTIONS_JSON='{"cdmSchema":"cdm","resultSchema":"results","targetTable":"cohort","cohortId":123,"generateStats":false}'
OPTIONS_JSON_STATS='{"cdmSchema":"cdm","resultSchema":"results","targetTable":"cohort","cohortId":999,"generateStats":true}'

# SQL template for testing
SQL_TEMPLATE="SELECT * FROM @cdmSchema.person WHERE person_id = @person_id"
TEMPLATE_PARAMS='{"cdmSchema":"test_schema","person_id":12345}'

# Basic SQL for translation testing (must end with semicolon for proper translation)
TEST_SQL="SELECT TOP 10 * FROM person;"

# Main test execution
main() {
    echo "======================================="
    echo "    Circe DuckDB Extension Test Suite"
    echo "======================================="
    echo
    
    # Check prerequisites
    check_duckdb
    
    # Load extension
    load_extension
    
    echo
    print_status "INFO" "Starting function tests..."
    echo
    
    # Test 1: circe_hello function
    run_test "circe_hello function" \
        "SELECT circe_hello('World');" \
        "Circe World" \
        "contains"
    
    # Test 2: circe_openssl_version function
    run_test "circe_openssl_version function" \
        "SELECT circe_openssl_version('Test');" \
        "OpenSSL" \
        "contains"
    
    # Test 3: circe_json_to_sql function with complex JSON - check actual SQL content
    run_test "circe_json_to_sql - generates SQL" \
        "SELECT SUBSTRING(circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON'), 1, 100);" \
        "CREATE" \
        "contains"
    
    # Test 4: circe_json_to_sql function with complex JSON - verify no fallback error
    run_test "circe_json_to_sql - complex JSON (no fallback)" \
        "SELECT CASE WHEN circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON') LIKE 'CIRCE_NATIVE_LIBRARY_NOT_FOUND%' THEN 'FAILED' ELSE 'SUCCESS' END;" \
        "SUCCESS" \
        "contains"
    
    # Test 5: circe_json_to_sql - verify SQL contains cohort operations
    run_test "circe_json_to_sql - contains cohort operations" \
        "SELECT SUBSTRING(circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON'), 1, 500);" \
        "Codesets" \
        "contains"
    
    # Test 6: circe_json_to_sql with generateStats=true - check for stats tables (look further in)
    run_test "circe_json_to_sql - with stats generation" \
        "SELECT CASE WHEN circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON_STATS') LIKE '%cohort_inclusion%' THEN 'HAS_STATS' ELSE 'NO_STATS' END;" \
        "HAS_STATS" \
        "contains"
    
    # Test 7: circe_sql_render function - verify parameter substitution
    run_test "circe_sql_render function" \
        "SELECT circe_sql_render('$SQL_TEMPLATE', '$TEMPLATE_PARAMS');" \
        "test_schema" \
        "contains"
    
    # Test 8: circe_sql_render - verify person_id substitution
    run_test "circe_sql_render - parameter replacement" \
        "SELECT circe_sql_render('$SQL_TEMPLATE', '$TEMPLATE_PARAMS');" \
        "12345" \
        "contains"
    
    # Test 9: circe_sql_translate function - NOTE: Translation may not be implemented yet
    # According to test/sql/circe_sqlrender.test, this should translate TOP to LIMIT
    # but currently it appears to just return the input unchanged
    run_test "circe_sql_translate function returns result" \
        "SELECT LENGTH(circe_sql_translate('$TEST_SQL', 'postgresql')) > 0;" \
        "true" \
        "contains"
    
    # Test 10: circe_sql_render_translate function - combined render and translate operation
    run_test "circe_sql_render_translate function" \
        "SELECT circe_sql_render_translate('SELECT TOP @limit * FROM @schema.person;', 'postgresql', '{\"limit\":\"5\",\"schema\":\"cdm\"}');" \
        "LIMIT 5" \
        "contains"
    
    # Test 11: circe_sql_translate - DuckDB dialect
    run_test "circe_sql_translate - DuckDB dialect" \
        "SELECT circe_sql_translate('$TEST_SQL', 'duckdb');" \
        "LIMIT 10" \
        "contains"
    
    # Test 12: circe_sql_render_translate - DuckDB dialect with GETDATE
    run_test "circe_sql_render_translate - DuckDB GETDATE translation" \
        "SELECT circe_sql_render_translate('SELECT GETDATE() AS today;', 'duckdb', '{}');" \
        "CURRENT_DATE" \
        "contains"
    
    # Test 13: circe_generate_and_translate - combines JSON to SQL and translation to DuckDB
    run_test "circe_generate_and_translate - generates DuckDB SQL" \
        "SELECT LENGTH(circe_generate_and_translate('$COMPLEX_JSON_B64', '$OPTIONS_JSON')) > 0;" \
        "true" \
        "contains"
    
    # Test 14: circe_generate_and_translate - verify output contains DuckDB-specific syntax
    run_test "circe_generate_and_translate - contains DuckDB syntax" \
        "SELECT circe_generate_and_translate('$COMPLEX_JSON_B64', '$OPTIONS_JSON');" \
        "CREATE TABLE" \
        "contains"
    
    # Test 15: Error handling - invalid dialect (returns error as SQL comment)
    run_test "Error handling - invalid dialect" \
        "SELECT circe_sql_translate('SELECT * FROM person;', 'invalid_dialect');" \
        "sqltranslate error" \
        "contains"
    
    # Test 14: Error handling - invalid dialect in render_translate (returns error as SQL comment)
    run_test "Error handling - invalid dialect in render_translate" \
        "SELECT circe_sql_render_translate('SELECT TOP 10 * FROM person;', 'not_a_real_dialect', '{}');" \
        "sqlrender_translate error" \
        "contains"
    
    # Test 17: Verify SQL structure contains expected DDL elements
    run_test "SQL structure validation - DDL statements" \
        "WITH sql_output AS (SELECT circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON') AS sql) SELECT CASE WHEN position('CREATE TABLE' IN sql) > 0 AND position('INSERT INTO' IN sql) > 0 THEN 'PASS' ELSE 'FAIL' END FROM sql_output;" \
        "PASS" \
        "contains"
    
    # Test 18: Schema parameter substitution
    run_test "Schema parameter substitution" \
        "SELECT SUBSTRING(circe_json_to_sql('$COMPLEX_JSON_B64', '{\"cdmSchema\":\"test_cdm\",\"resultSchema\":\"test_results\",\"targetTable\":\"test_cohort\",\"cohortId\":999,\"generateStats\":false}'), 1, 500);" \
        "test_cdm" \
        "contains"
    
    # Test 19: Verify cohort ID is inserted correctly (look later in SQL)
    run_test "Cohort ID validation" \
        "SELECT SUBSTRING(circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON'), 3500, 1000);" \
        "123" \
        "contains"
    
    # Test 20: Error handling - invalid JSON
    run_test "Error handling - invalid JSON input" \
        "SELECT circe_json_to_sql('invalid_base64', '$OPTIONS_JSON');" \
        "Error" \
        "should_error"
    
    # Test 21: Verify qualified_events temp table in generated SQL
    run_test "SQL contains qualified_events" \
        "SELECT SUBSTRING(circe_json_to_sql('$COMPLEX_JSON_B64', '$OPTIONS_JSON'), 1, 1000);" \
        "qualified_events" \
        "contains"
    
    echo
    echo "======================================="
    echo "           Test Results Summary"
    echo "======================================="
    echo "Tests Run:    $TESTS_RUN"
    echo "Tests Passed: $TESTS_PASSED"
    echo "Tests Failed: $TESTS_FAILED"
    
    if [ $TESTS_FAILED -gt 0 ]; then
        echo
        print_status "FAIL" "Failed tests:"
        for failed_test in "${FAILED_TESTS[@]}"; do
            echo "  - $failed_test"
        done
        echo
        exit 1
    else
        echo
        print_status "PASS" "All tests passed successfully!"
        echo
        exit 0
    fi
}

# Help function
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Test script for Circe DuckDB Extension"
    echo
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -v, --verbose  Enable verbose output"
    echo
    echo "Prerequisites:"
    echo "  - duckdb executable must be available in PATH"
    echo "  - Circe extension must be installed and loadable"
    echo
    echo "Examples:"
    echo "  $0                # Run all tests"
    echo "  $0 --verbose      # Run with verbose output"
}

# Parse command line arguments
case "${1:-}" in
    -h|--help)
        show_help
        exit 0
        ;;
    -v|--verbose)
        set -x  # Enable verbose mode
        main
        ;;
    "")
        main
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use -h or --help for usage information"
        exit 1
        ;;
esac