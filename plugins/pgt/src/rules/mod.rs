pub mod mappings;
pub mod patterns;

use crate::config::RulesConfig;
use crate::error::{TransformationError, TransformationResult};
use sqlparser::ast::Statement;
use std::collections::HashMap;

pub struct TransformationRules {
    data_type_rules: HashMap<String, String>,
    function_rules: HashMap<String, String>,
    pattern_rules: Vec<PatternRule>,
    validation_rules: Vec<ValidationRule>,
    config: RulesConfig,
}

#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub name: String,
    pub description: String,
    pub check: fn(&Statement) -> Result<(), String>,
    pub suggestion: String,
}

#[derive(Debug, Clone)]
pub struct PostTransformationRule {
    pub name: String,
    pub pattern: String,
    pub replacement: String,
    pub enabled: bool,
}

impl TransformationRules {
    pub fn new(config: RulesConfig) -> Self {
        let mut rules = Self {
            data_type_rules: HashMap::new(),
            function_rules: HashMap::new(),
            pattern_rules: Vec::new(),
            validation_rules: Vec::new(),
            config,
        };

        rules.initialize_validation_rules();

        rules
    }

    fn initialize_validation_rules(&mut self) {
        self.validation_rules.push(ValidationRule {
            name: "single_identity_column".to_string(),
            description: "HANA allows only one IDENTITY column per table".to_string(),
            check: Self::check_single_identity_column,
            suggestion: "Use sequences for additional columns".to_string(),
        });

        self.validation_rules.push(ValidationRule {
            name: "no_pg_extensions".to_string(),
            description: "PostgreSQL extensions not supported".to_string(),
            check: Self::check_no_extensions,
            suggestion: "Remove PostgreSQL extensions".to_string(),
        });

        self.validation_rules.push(ValidationRule {
            name: "hana_reserved_words".to_string(),
            description: "Avoid HANA reserved words".to_string(),
            check: Self::check_reserved_words,
            suggestion: "Quote or rename identifiers".to_string(),
        });
    }

    pub fn validate_hana_compatibility(
        &self,
        statements: &[Statement],
    ) -> TransformationResult<()> {
        if !self.config.validate_hana_compatibility {
            return Ok(());
        }

        let mut violations = Vec::new();
        let mut suggestions = Vec::new();

        for statement in statements {
            for rule in &self.validation_rules {
                if let Err(violation) = (rule.check)(statement) {
                    violations.push(format!("{}: {}", rule.name, violation));
                    suggestions.push(rule.suggestion.clone());
                }
            }
        }

        if !violations.is_empty() && self.config.enable_strict_mode {
            return Err(TransformationError::ValidationError {
                hana_rule_violations: violations,
                suggestions,
            });
        }

        Ok(())
    }

    fn check_single_identity_column(stmt: &Statement) -> Result<(), String> {
        use sqlparser::ast::{ColumnOption, GeneratedAs, Statement};

        if let Statement::CreateTable(create_table) = stmt {
            let identity_count = create_table
                .columns
                .iter()
                .filter(|col| {
                    col.options.iter().any(|opt| {
                        matches!(
                            opt.option,
                            ColumnOption::Generated {
                                generated_as: GeneratedAs::Always | GeneratedAs::ByDefault,
                                ..
                            }
                        )
                    })
                })
                .count();

            if identity_count > 1 {
                return Err(format!(
                    "Table '{}' has {} IDENTITY columns, but HANA allows only one",
                    create_table.name, identity_count
                ));
            }
        }

        Ok(())
    }

    fn check_no_extensions(stmt: &Statement) -> Result<(), String> {
        let stmt_str = stmt.to_string().to_uppercase();
        if stmt_str.contains("CREATE EXTENSION") {
            return Err("PostgreSQL extensions are not supported in HANA".to_string());
        }
        Ok(())
    }

    fn check_reserved_words(stmt: &Statement) -> Result<(), String> {
        let reserved_words = get_hana_reserved_words();
        let stmt_str = stmt.to_string().to_uppercase();

        for word in &reserved_words {
            if stmt_str.contains(&format!(" {} ", word)) || stmt_str.contains(&format!("({}", word))
            {
                return Err(format!("'{}' is HANA reserved", word));
            }
        }

        Ok(())
    }

    pub fn apply_transformation_rules(&self, sql: &str) -> TransformationResult<String> {
        let mut result = sql.to_string();

        if *self
            .config
            .transformation_rules
            .get("remove_pg_extensions")
            .unwrap_or(&false)
        {
            result = self.remove_postgresql_extensions(&result);
        }

        if *self
            .config
            .transformation_rules
            .get("preserve_comments")
            .unwrap_or(&true)
        {
        }

        if *self
            .config
            .transformation_rules
            .get("convert_arrays_to_json")
            .unwrap_or(&false)
        {
            result = self.convert_arrays_to_json(&result);
        }

        Ok(result)
    }

    fn remove_postgresql_extensions(&self, sql: &str) -> String {
        use regex::Regex;

        let extension_regex = Regex::new(r"(?i)CREATE\s+EXTENSION[^;]*;").unwrap();
        extension_regex
            .replace_all(sql, |caps: &regex::Captures| format!("-- {}", &caps[0]))
            .to_string()
    }

    fn convert_arrays_to_json(&self, sql: &str) -> String {
        use regex::Regex;

        let array_regex = Regex::new(r"(?i)TEXT\[\]").unwrap();
        let mut result = array_regex.replace_all(sql, "NCLOB").to_string();

        let array_literal_regex = Regex::new(r"ARRAY\[([^\]]+)\]").unwrap();
        result = array_literal_regex
            .replace_all(&result, r#"'[$1]'"#)
            .to_string();

        result
    }

    pub fn add_data_type_rule(&mut self, from: String, to: String) {
        self.data_type_rules.insert(from, to);
    }

    pub fn add_function_rule(&mut self, from: String, to: String) {
        self.function_rules.insert(from, to);
    }

    pub fn add_pattern_rule(&mut self, rule: PatternRule) {
        self.pattern_rules.push(rule);
    }

    pub fn get_data_type_mapping(&self, data_type: &str) -> Option<&String> {
        self.data_type_rules.get(data_type)
    }

    pub fn get_function_mapping(&self, function: &str) -> Option<&String> {
        self.function_rules.get(function)
    }

    pub fn get_pattern_rules(&self) -> &[PatternRule] {
        &self.pattern_rules
    }
}

impl Default for TransformationRules {
    fn default() -> Self {
        Self::new(RulesConfig::default())
    }
}

fn get_hana_reserved_words() -> Vec<String> {
    vec![
        "OBJECT".to_string(),
        "SYSTEM".to_string(),
        "VIEW".to_string(),
        "TABLE".to_string(),
        "INDEX".to_string(),
        "SCHEMA".to_string(),
        "USER".to_string(),
        "GROUP".to_string(),
        "ROLE".to_string(),
        "PROCEDURE".to_string(),
        "FUNCTION".to_string(),
        "TRIGGER".to_string(),
        "SEQUENCE".to_string(),
        "TYPE".to_string(),
        "DOMAIN".to_string(),
        "CONSTRAINT".to_string(),
        "PRIMARY".to_string(),
        "FOREIGN".to_string(),
        "UNIQUE".to_string(),
        "CHECK".to_string(),
        "DEFAULT".to_string(),
        "IDENTITY".to_string(),
        "GENERATED".to_string(),
        "ALWAYS".to_string(),
        "ORDER".to_string(),
        "GROUP".to_string(),
        "HAVING".to_string(),
        "WHERE".to_string(),
        "SELECT".to_string(),
        "FROM".to_string(),
        "INSERT".to_string(),
        "UPDATE".to_string(),
        "DELETE".to_string(),
        "CREATE".to_string(),
        "ALTER".to_string(),
        "DROP".to_string(),
        "GRANT".to_string(),
        "REVOKE".to_string(),
    ]
}

pub struct PatternRule {
    pub name: String,
    pub description: String,
    pub pattern: String,
    pub replacement: String,
    pub conditions: Vec<RuleCondition>,
}

pub enum RuleCondition {
    StatementType(String),
    ContextContains(String),
    NotInContext(String),
}

impl PatternRule {
    pub fn new(name: &str, description: &str, pattern: &str, replacement: &str) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            pattern: pattern.to_string(),
            replacement: replacement.to_string(),
            conditions: Vec::new(),
        }
    }

    pub fn with_condition(mut self, condition: RuleCondition) -> Self {
        self.conditions.push(condition);
        self
    }
}
