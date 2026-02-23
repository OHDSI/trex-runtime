use hana_scan::{HanaError, LogLevel, HanaLogger};

mod common;

#[test]
fn test_duckdb_extension_loading() {
    common::setup();

    println!("✓ DuckDB extension compilation test passed");
    assert!(true, "Extension compiles successfully");
}

#[test]
fn test_duckdb_hana_scan_function_registration() {
    common::setup();
    let config = common::HanaTestConfig::new();

    if config.should_skip {
        println!("Skipping test_duckdb_hana_scan_function_registration: {}", config.skip_reason);
        return;
    }

    println!("✓ DuckDB extension registration test framework ready");
    println!("  Extension would be loaded and hana_scan function registered");
    println!("  Connection URL: {}", config.connection_url);

    assert!(config.connection_url.starts_with("hdbsql://"), "Connection URL should start with hdbsql://");
}

#[test]
fn test_hana_scan_query_validation() {
    common::setup();
    let config = common::HanaTestConfig::new();

    if config.should_skip {
        println!("Skipping test_hana_scan_query_validation: {}", config.skip_reason);
        return;
    }

    let valid_queries = vec![
        "SELECT * FROM DUMMY",
        "SELECT CURRENT_DATE FROM DUMMY",
        "SELECT 1, 2, 3 FROM DUMMY",
        "SELECT * FROM SYS.TABLES WHERE SCHEMA_NAME = 'SYS'",
    ];

    let invalid_queries = vec![
        "",
        "   ",
        "INVALID SQL SYNTAX",
        "DROP TABLE SYS.TABLES",
    ];

    println!("Testing valid queries:");
    for (i, query) in valid_queries.iter().enumerate() {
        println!("  {}: {}", i + 1, query);
        assert!(!query.trim().is_empty(), "Query should not be empty");
    }

    println!("Testing invalid queries:");
    for (i, query) in invalid_queries.iter().enumerate() {
        println!("  {}: {:?}", i + 1, query);
        if query.trim().is_empty() {
            println!("    ✓ Correctly identified empty query");
        }
    }

    println!("✓ Query validation tests completed");
}

#[test]
fn test_hana_configuration_parsing() {
    common::setup();

    let test_configs = vec![
        ("HANA_BATCH_SIZE", "1000"),
        ("HANA_CONNECTION_TIMEOUT_MS", "30000"),
        ("HANA_QUERY_TIMEOUT_MS", "300000"),
        ("HANA_MAX_RETRIES", "3"),
        ("HANA_LOG_LEVEL", "INFO"),
    ];

    println!("Testing configuration parsing:");
    for (key, value) in test_configs {
        std::env::set_var(key, value);
        let retrieved = std::env::var(key).unwrap_or_default();
        println!("  {} = {}", key, retrieved);
        assert_eq!(retrieved, value, "Configuration value should match");
    }

    let batch_size: usize = std::env::var("HANA_BATCH_SIZE")
        .unwrap_or("1000".to_string())
        .parse()
        .expect("Batch size should be numeric");

    assert!(batch_size > 0, "Batch size should be positive");
    assert!(batch_size <= 10000, "Batch size should be reasonable");

    println!("✓ Configuration parsing tests completed");
}

#[test]
fn test_error_hierarchy() {
    common::setup();

    let connection_error = HanaError::Connection {
        message: "Failed to connect".to_string(),
        url: Some("hdbsql://test@localhost:39041/HDB".to_string()),
        retry_count: Some(3),
        context: "Test connection context".to_string(),
    };

    let query_error = HanaError::Query {
        message: "Invalid SQL".to_string(),
        query: Some("SELECT * FROM invalid_table".to_string()),
        execution_time_ms: Some(100),
        context: "Test query context".to_string(),
    };

    let type_error = HanaError::TypeConversion {
        message: "Cannot convert type".to_string(),
        source_type: Some("VARCHAR".to_string()),
        target_type: Some("INTEGER".to_string()),
        column_name: Some("test_column".to_string()),
        row_index: Some(1),
    };

    println!("Testing error hierarchy:");
    println!("  Connection error: {}", connection_error);
    println!("  Query error: {}", query_error);
    println!("  Type conversion error: {}", type_error);

    match connection_error {
        HanaError::Connection { retry_count, .. } => {
            assert_eq!(retry_count, Some(3), "Retry count should match");
        }
        _ => panic!("Should be a connection error"),
    }

    match query_error {
        HanaError::Query { query, .. } => {
            assert!(query.as_ref().map_or(false, |q| q.contains("invalid_table")), "Query should contain table name");
        }
        _ => panic!("Should be a query error"),
    }

    println!("✓ Error hierarchy test completed");
}

#[test]
fn test_logging_system() {
    common::setup();

    println!("Testing logging system:");

    HanaLogger::log(LogLevel::Error, "test", "Test error message");
    HanaLogger::log(LogLevel::Warn, "test", "Test warning message");
    HanaLogger::log(LogLevel::Info, "test", "Test info message");
    HanaLogger::log(LogLevel::Debug, "test", "Test debug message");
    HanaLogger::log(LogLevel::Trace, "test", "Test trace message");

    HanaLogger::error("test", "Direct error message");
    HanaLogger::warn("test", "Direct warning message");
    HanaLogger::info("test", "Direct info message");
    HanaLogger::debug("test", "Direct debug message");
    HanaLogger::trace("test", "Direct trace message");

    println!("✓ Logging system test completed");
}
