use sqlparser::ast::Statement;

pub struct SqlValidator;

impl SqlValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate_statement(&self, _stmt: &Statement) -> Result<(), String> {
        Ok(())
    }

    pub fn has_unsupported_features(&self, _stmt: &Statement) -> bool {
        false
    }

    pub fn validate_hana_syntax(
        &self,
        _sql: &str,
    ) -> Result<ValidationResult, crate::error::TransformationError> {
        Ok(ValidationResult::new())
    }
}

impl Default for SqlValidator {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}
