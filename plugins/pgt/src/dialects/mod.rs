pub mod hana;
pub mod duckdb;

use crate::config::TransformationConfig;
use crate::error::{TransformationError, TransformationResult};
use sqlparser::ast::Statement;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dialect {
    Hana,
    DuckDb,
}

impl Dialect {
    pub fn all() -> &'static [Dialect] {
        &[Dialect::Hana, Dialect::DuckDb]
    }

    pub fn name(&self) -> &'static str {
        match self {
            Dialect::Hana => "hana",
            Dialect::DuckDb => "duckdb",
        }
    }

    pub fn from_str(s: &str) -> Result<Dialect, String> {
        match s.to_lowercase().as_str() {
            "hana" | "sap-hana" | "sap_hana" => Ok(Dialect::Hana),
            "duckdb" | "duck-db" | "duck_db" => Ok(Dialect::DuckDb),
            _ => Err(format!("Unsupported dialect: {}. Supported dialects: {}", 
                s, 
                Dialect::all().iter()
                    .map(|d| d.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}

impl Default for Dialect {
    fn default() -> Self {
        Dialect::Hana
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

pub trait DialectTransformationEngine {
    fn dialect(&self) -> Dialect;
    fn transform_statement(&self, stmt: Statement) -> TransformationResult<Statement>;
    fn transform_statements(&self, statements: &[Statement]) -> TransformationResult<Vec<Statement>>;
    fn apply_post_processing_rules(&self, sql: &str) -> TransformationResult<String>;
    fn validate_statement_for_hana(&self, stmt: &Statement) -> TransformationResult<Vec<String>>;
    fn name(&self) -> &'static str;
}

pub struct DialectEngineFactory;

impl DialectEngineFactory {
    pub fn create_engine(dialect: Dialect, config: &TransformationConfig) -> Result<Box<dyn DialectTransformationEngine>, TransformationError> {
        match dialect {
            Dialect::Hana => Ok(Box::new(hana::HanaTransformationEngine::new(config))),
            Dialect::DuckDb => Ok(Box::new(duckdb::DuckDbTransformationEngine::new(config))),
        }
    }

    pub fn supported_dialects() -> &'static [Dialect] {
        Dialect::all()
    }
}
