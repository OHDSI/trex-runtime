use crate::error::TransformationResult;
use regex::Regex;

pub struct PostProcessor;

impl PostProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process(&self, sql: &str) -> TransformationResult<String> {
        let mut result = sql.to_string();

        // sqlparser outputs "FULL JOIN" but HANA requires "FULL OUTER JOIN"
        result = self.fix_full_outer_join(&result)?;

        // HANA doesn't support USING btree/gin/etc on CREATE INDEX
        result = self.fix_index_using_clause(&result)?;

        Ok(result)
    }

    fn fix_full_outer_join(&self, sql: &str) -> TransformationResult<String> {
        let regex = Regex::new(r"\bFULL\s+JOIN\b").map_err(|e| {
            crate::error::TransformationError::ParseError {
                message: format!("Regex error: {}", e),
                line: 0,
                column: 0,
            }
        })?;

        Ok(regex.replace_all(sql, "FULL OUTER JOIN").to_string())
    }

    fn fix_index_using_clause(&self, sql: &str) -> TransformationResult<String> {
        let regex = Regex::new(
            r"\bUSING\s+(?:btree|gin|hash|gist|spgist|brin|BTREE|GIN|HASH|GIST|SPGIST|BRIN)\b",
        )
        .map_err(|e| crate::error::TransformationError::ParseError {
            message: format!("Regex error: {}", e),
            line: 0,
            column: 0,
        })?;

        let result = regex.replace_all(sql, "").to_string();

        let cleanup_regex =
            Regex::new(r"\s+").map_err(|e| crate::error::TransformationError::ParseError {
                message: format!("Regex error: {}", e),
                line: 0,
                column: 0,
            })?;

        Ok(cleanup_regex.replace_all(&result, " ").trim().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fix_full_outer_join() {
        let processor = PostProcessor::new();

        let sql = "SELECT * FROM users AS u FULL JOIN profiles AS p ON u.id = p.user_id;";
        let result = processor.fix_full_outer_join(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM users AS u FULL OUTER JOIN profiles AS p ON u.id = p.user_id;"
        );
    }

    #[test]
    fn test_fix_index_using_clause() {
        let processor = PostProcessor::new();

        let sql = "CREATE INDEX idx_users_email ON users USING BTREE (email);";
        let result = processor.fix_index_using_clause(sql).unwrap();
        assert_eq!(result, "CREATE INDEX idx_users_email ON users (email);");

        let sql2 = "CREATE INDEX idx_users_data ON users USING gin (data);";
        let result2 = processor.fix_index_using_clause(sql2).unwrap();
        assert_eq!(result2, "CREATE INDEX idx_users_data ON users (data);");
    }

    #[test]
    fn test_full_process() {
        let processor = PostProcessor::new();

        let sql = "SELECT * FROM users AS u FULL JOIN profiles AS p ON u.id = p.user_id; CREATE INDEX idx_users_email ON users USING BTREE (email);";
        let result = processor.process(sql).unwrap();
        assert!(result.contains("FULL OUTER JOIN"));
        assert!(!result.contains("USING BTREE"));
    }
}
