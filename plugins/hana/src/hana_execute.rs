use duckdb::{
    core::{DataChunkHandle, LogicalTypeId, Inserter},
    vtab::arrow::WritableVector,
    vscalar::{VScalar, ScalarFunctionSignature},
};
use std::error::Error;
use std::panic::{self, AssertUnwindSafe};
use crate::{HanaConnection, HanaError};

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while let Some(c) = chars.next() {
        if in_single_quote {
            current_statement.push(c);
            if c == '\'' {
                if chars.peek() == Some(&'\'') {
                    current_statement.push(chars.next().unwrap());
                } else {
                    in_single_quote = false;
                }
            }
            continue;
        }
        if in_double_quote {
            current_statement.push(c);
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    current_statement.push(chars.next().unwrap());
                } else {
                    in_double_quote = false;
                }
            }
            continue;
        }
        match c {
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                for c2 in chars.by_ref() {
                    if c2 == '\n' {
                        current_statement.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut depth = 1u32;
                while depth > 0 {
                    match chars.next() {
                        Some('*') if chars.peek() == Some(&'/') => {
                            chars.next();
                            depth -= 1;
                        }
                        Some('/') if chars.peek() == Some(&'*') => {
                            chars.next();
                            depth += 1;
                        }
                        None => break,
                        _ => {}
                    }
                }
            }
            '\'' => {
                current_statement.push(c);
                in_single_quote = true;
            }
            '"' => {
                current_statement.push(c);
                in_double_quote = true;
            }
            ';' => {
                let trimmed = current_statement.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current_statement.clear();
            }
            _ => {
                current_statement.push(c);
            }
        }
    }

    let trimmed = current_statement.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

pub struct HanaExecuteScalar;

impl VScalar for HanaExecuteScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let connection_string_vector = input.flat_vector(0);
        let sql_statement_vector = input.flat_vector(1);
        
        let connection_string_slice = connection_string_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let sql_statement_slice = sql_statement_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        
        let connection_string = {
            let mut binding = connection_string_slice[0];
            duckdb::types::DuckString::new(&mut binding).as_str().to_string()
        };
        
        let sql_statement = {
            let mut binding = sql_statement_slice[0];
            duckdb::types::DuckString::new(&mut binding).as_str().to_string()
        };
        
        let statements_executed = execute_hana_statement(&connection_string, &sql_statement)?;
        let result = format!("{} statement(s) executed", statements_executed);

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &result);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into()
        )]
    }
}

fn execute_hana_statement(connection_string: &str, sql_statement: &str) -> Result<usize, Box<dyn Error>> {
    let connection = match panic::catch_unwind(AssertUnwindSafe(|| {
        HanaConnection::new(connection_string.to_string())
    })) {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => return Err(Box::new(HanaError::connection(
            &format!("Connection failed: {}", e),
            None,
            None,
            "execute_hana_statement"
        ))),
        Err(panic_err) => {
            let panic_msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_err.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic during HANA connection".to_string()
            };
            return Err(Box::new(HanaError::connection(
                &format!("Connection panicked: {}", panic_msg),
                None,
                None,
                "execute_hana_statement"
            )));
        }
    };

    let statements = split_sql_statements(sql_statement);

    if statements.is_empty() {
        return Ok(0);
    }

    let mut total_affected = 0usize;

    for (idx, stmt) in statements.iter().enumerate() {
        match connection.prepare(stmt) {
            Ok(mut prepared) => {
                match prepared.execute(&()) {
                    Ok(_) => {
                        total_affected += 1;
                    }
                    Err(e) => return Err(Box::new(HanaError::query(
                        &format!("Failed to execute statement {} of {}: {}", idx + 1, statements.len(), e),
                        Some(stmt),
                        None,
                        "execute_hana_statement"
                    )))
                }
            }
            Err(e) => return Err(Box::new(HanaError::query(
                &format!("Failed to prepare statement {} of {}: {}", idx + 1, statements.len(), e),
                Some(stmt),
                None,
                "execute_hana_statement"
            )))
        }
    }

    Ok(total_affected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_single_statement() {
        let sql = "SELECT * FROM users";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT * FROM users"]);
    }

    #[test]
    fn test_split_single_statement_with_trailing_semicolon() {
        let sql = "SELECT * FROM users;";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT * FROM users"]);
    }

    #[test]
    fn test_split_multiple_statements() {
        let sql = "SELECT * FROM users; INSERT INTO logs (msg) VALUES ('test'); DELETE FROM temp";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            "SELECT * FROM users",
            "INSERT INTO logs (msg) VALUES ('test')",
            "DELETE FROM temp"
        ]);
    }

    #[test]
    fn test_semicolon_in_single_quoted_string() {
        let sql = "SELECT * FROM users WHERE name = 'hello;world'; SELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            "SELECT * FROM users WHERE name = 'hello;world'",
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_semicolon_in_double_quoted_identifier() {
        let sql = r#"SELECT * FROM "table;name"; SELECT 1"#;
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            r#"SELECT * FROM "table;name""#,
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_escaped_single_quotes() {
        let sql = "INSERT INTO t (col) VALUES ('it''s a ; test'); SELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            "INSERT INTO t (col) VALUES ('it''s a ; test')",
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_escaped_double_quotes() {
        let sql = r#"SELECT * FROM "say ""hello;world"""; SELECT 1"#;
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            r##"SELECT * FROM "say ""hello;world""""##,
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_mixed_quotes() {
        let sql = r#"SELECT 'semicolon "in" single;quotes' FROM "double;quotes"; SELECT 1"#;
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            r#"SELECT 'semicolon "in" single;quotes' FROM "double;quotes""#,
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_empty_statements_are_skipped() {
        let sql = "SELECT 1;; ; SELECT 2";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_whitespace_only_statements_are_skipped() {
        let sql = "SELECT 1;   \n\t  ; SELECT 2";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_empty_input() {
        let sql = "";
        let result = split_sql_statements(sql);
        assert!(result.is_empty());
    }

    #[test]
    fn test_whitespace_only_input() {
        let sql = "   \n\t  ";
        let result = split_sql_statements(sql);
        assert!(result.is_empty());
    }

    #[test]
    fn test_multiline_statements() {
        let sql = "CREATE TABLE foo (\n  id INT,\n  name VARCHAR(100)\n);\nINSERT INTO foo VALUES (1, 'test')";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            "CREATE TABLE foo (\n  id INT,\n  name VARCHAR(100)\n)",
            "INSERT INTO foo VALUES (1, 'test')"
        ]);
    }

    #[test]
    fn test_line_comment_stripped() {
        let sql = "-- this is a comment\nSELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1"]);
    }

    #[test]
    fn test_line_comment_after_statement() {
        let sql = "SELECT 1; -- trailing comment\nSELECT 2";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_line_comment_between_statements() {
        let sql = "SELECT 1;\n-- middle comment\nSELECT 2";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_block_comment_stripped() {
        let sql = "/* block comment */ SELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1"]);
    }

    #[test]
    fn test_block_comment_between_statements() {
        let sql = "SELECT 1; /* comment */ SELECT 2";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_nested_block_comments() {
        let sql = "/* outer /* inner */ still comment */ SELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec!["SELECT 1"]);
    }

    #[test]
    fn test_comment_like_in_single_quotes_preserved() {
        let sql = "SELECT '-- not a comment' FROM t; SELECT 1";
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            "SELECT '-- not a comment' FROM t",
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_comment_like_in_double_quotes_preserved() {
        let sql = r#"SELECT * FROM "/* not a comment */"; SELECT 1"#;
        let result = split_sql_statements(sql);
        assert_eq!(result, vec![
            r#"SELECT * FROM "/* not a comment */""#,
            "SELECT 1"
        ]);
    }

    #[test]
    fn test_only_comments() {
        let sql = "-- just a comment\n/* another comment */";
        let result = split_sql_statements(sql);
        assert!(result.is_empty());
    }
}
