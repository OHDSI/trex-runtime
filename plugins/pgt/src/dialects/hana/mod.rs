pub mod data_types;
pub mod expressions;
pub mod functions;
pub mod post_processor;
pub mod statements;

use crate::config::TransformationConfig;
use crate::error::{TransformationError, TransformationResult, TransformationWarning};
use crate::rules::TransformationRules;
use sqlparser::ast::Statement;
use std::time::Duration;

pub struct DetailedTransformationResult {
    pub sql: Option<String>,
    pub errors: Vec<TransformationError>,
    pub warnings: Vec<TransformationWarning>,
    pub metadata: TransformationMetadata,
}

#[derive(Debug, Clone)]
pub struct TransformationMetadata {
    pub input_statements: usize,
    pub transformed_statements: usize,
    pub skipped_statements: usize,
    pub transformation_time: Duration,
}

pub trait Transformer {
    fn name(&self) -> &'static str;
    fn transform(&self, stmt: &mut Statement) -> TransformationResult<bool>;
    fn supports_statement_type(&self, stmt: &Statement) -> bool;

    /// Lower numbers execute first
    fn priority(&self) -> u8 {
        100
    }

    fn collect_warnings(&self) -> Vec<TransformationWarning> {
        Vec::new()
    }
}

pub struct TransformationEngine {
    transformers: Vec<Box<dyn Transformer>>,
    config: TransformationConfig,
    rules: TransformationRules,
}

impl TransformationEngine {
    pub fn new(config: &TransformationConfig) -> Self {
        let mut transformers: Vec<Box<dyn Transformer>> = vec![
            Box::new(data_types::DataTypeTransformer::new(config)),
            Box::new(functions::FunctionTransformer::new(config)),
            Box::new(statements::StatementTransformer::new(config)),
            Box::new(expressions::ExpressionTransformer::new(config)),
        ];

        transformers.sort_by_key(|t| t.priority());

        Self {
            transformers,
            config: config.clone(),
            rules: TransformationRules::new(config.rules.clone()),
        }
    }

    pub fn transform_statement(&self, mut stmt: Statement) -> TransformationResult<Statement> {
        let mut warnings = Vec::new();
        let mut any_changes = false;

        for transformer in &self.transformers {
            if transformer.supports_statement_type(&stmt) {
                match transformer.transform(&mut stmt) {
                    Ok(changed) => {
                        if changed {
                            any_changes = true;
                        }
                    }
                    Err(e) => {
                        log::warn!("Transformer '{}' failed: {}", transformer.name(), e);
                        warnings.push(TransformationWarning::high(&format!(
                            "Transformer '{}' failed: {}",
                            transformer.name(),
                            e
                        )));
                    }
                }
            }
        }

        if !warnings.is_empty() {
            log::info!("Transformation completed with {} warnings", warnings.len());
        }

        Ok(stmt)
    }

    pub fn transform_statements(
        &self,
        statements: &[Statement],
    ) -> TransformationResult<Vec<Statement>> {
        let start_time = std::time::Instant::now();

        self.rules.validate_hana_compatibility(statements)?;

        let mut transformed_statements = Vec::new();
        let mut errors = Vec::new();

        for (index, stmt) in statements.iter().enumerate() {
            match self.transform_statement(stmt.clone()) {
                Ok(transformed_stmt) => {
                    transformed_statements.push(transformed_stmt);
                }
                Err(e) => {
                    if self.config.rules.enable_strict_mode {
                        return Err(e);
                    } else {
                        errors.push(e);
                        log::warn!("Statement {} failed: using original", index);
                        transformed_statements.push(stmt.clone());
                    }
                }
            }
        }

        if !errors.is_empty() {
            return Err(TransformationError::partial_transformation(
                transformed_statements.len() - errors.len(),
                errors.len(),
                None,
                errors,
            ));
        }

        Ok(transformed_statements)
    }

    pub fn apply_post_processing_rules(&self, sql: &str) -> TransformationResult<String> {
        self.rules.apply_transformation_rules(sql)
    }

    pub fn validate_statement_for_hana(
        &self,
        stmt: &Statement,
    ) -> TransformationResult<Vec<String>> {
        self.rules
            .validate_hana_compatibility(&[stmt.clone()])
            .map(|_| Vec::new())
    }
}

pub struct HanaTransformationEngine {
    engine: TransformationEngine,
}

impl HanaTransformationEngine {
    pub fn new(config: &TransformationConfig) -> Self {
        Self {
            engine: TransformationEngine::new(config),
        }
    }
}

impl super::DialectTransformationEngine for HanaTransformationEngine {
    fn dialect(&self) -> super::Dialect {
        super::Dialect::Hana
    }

    fn transform_statement(&self, stmt: Statement) -> TransformationResult<Statement> {
        self.engine.transform_statement(stmt)
    }

    fn transform_statements(&self, statements: &[Statement]) -> TransformationResult<Vec<Statement>> {
        self.engine.transform_statements(statements)
    }

    fn apply_post_processing_rules(&self, sql: &str) -> TransformationResult<String> {
        self.engine.apply_post_processing_rules(sql)
    }

    fn validate_statement_for_hana(&self, stmt: &Statement) -> TransformationResult<Vec<String>> {
        self.engine.validate_statement_for_hana(stmt)
    }

    fn name(&self) -> &'static str {
        "HANA Transformation Engine"
    }
}

pub trait AstVisitor<T> {
    fn visit(&mut self, node: &mut T) -> TransformationResult<bool>;
}

pub trait TransformationStats {
    fn transformations_applied(&self) -> usize;
    fn warnings_generated(&self) -> usize;
    fn processing_time(&self) -> Duration;
}
