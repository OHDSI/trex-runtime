use super::Transformer;
use crate::config::TransformationConfig;
use crate::error::TransformationResult;
use sqlparser::ast::{ColumnDef, DataType, Statement};
use std::collections::HashMap;

pub struct DataTypeTransformer {
    mappings: HashMap<String, String>,
    preserve_precision: bool,
}

impl DataTypeTransformer {
    pub fn new(config: &TransformationConfig) -> Self {
        let mut mappings = config.data_types.custom_mappings.clone();

        for (pg_type, hana_type) in get_default_mappings() {
            mappings.entry(pg_type).or_insert(hana_type);
        }

        Self {
            mappings,
            preserve_precision: config.data_types.preserve_precision,
        }
    }

    pub fn transform_data_type(&self, data_type: &mut DataType) -> TransformationResult<bool> {
        let mut changed = false;

        match data_type {
            DataType::Custom(object_name, type_values) => {
                let type_name = object_name.to_string().to_uppercase();

                if let Some(hana_type) = self.mappings.get(&type_name) {
                    if let Ok(new_type) = parse_hana_type(hana_type) {
                        *data_type = new_type;
                        changed = true;
                    }
                }
            }
            DataType::Varchar(length) => {
                *data_type = DataType::Nvarchar(length.clone());
                changed = true;
            }
            DataType::Char(length) => {
                *data_type = DataType::Nvarchar(length.clone());
                changed = true;
            }
            DataType::Text => {
                *data_type = DataType::Clob(None);
                changed = true;
            }
            DataType::JSON => {
                *data_type = DataType::Clob(None);
                changed = true;
            }
            DataType::Boolean => {}
            DataType::Integer(display) => {}
            DataType::BigInt(display) => {}
            DataType::Timestamp(precision, timezone) => {
                if *timezone == sqlparser::ast::TimezoneInfo::WithTimeZone {
                    *data_type =
                        DataType::Timestamp(precision.clone(), sqlparser::ast::TimezoneInfo::None);
                    changed = true;
                }
            }
            DataType::Array(element_type) => {
                *data_type = DataType::Clob(None);
                changed = true;
            }
            _ => {}
        }

        Ok(changed)
    }
}

impl Transformer for DataTypeTransformer {
    fn name(&self) -> &'static str {
        "DataTypeTransformer"
    }

    fn priority(&self) -> u8 {
        10
    }

    fn supports_statement_type(&self, stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::AlterTable { .. }
                | Statement::CreateIndex { .. }
        )
    }

    fn transform(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let mut changed = false;

        match stmt {
            Statement::CreateTable(create_table) => {
                for column in &mut create_table.columns {
                    if self.transform_column_data_type(column)? {
                        changed = true;
                    }
                }
            }
            Statement::AlterTable { operations, .. } => {
                for operation in operations {
                    match operation {
                        sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                            if self.transform_column_data_type(column_def)? {
                                changed = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        Ok(changed)
    }
}

impl DataTypeTransformer {
    fn transform_column_data_type(&self, column: &mut ColumnDef) -> TransformationResult<bool> {
        let mut changed = false;

        if self.transform_data_type(&mut column.data_type)? {
            changed = true;
        }

        let mut is_serial_column = false;
        let mut is_bigserial_column = false;

        for option in &column.options {
            if let sqlparser::ast::ColumnOption::Default(expr) = &option.option {
                if let sqlparser::ast::Expr::Function(func) = expr {
                    if func.name.to_string().to_lowercase() == "nextval" {
                        if matches!(column.data_type, DataType::Integer(_)) {
                            is_serial_column = true;
                        } else if matches!(column.data_type, DataType::BigInt(_)) {
                            is_bigserial_column = true;
                        }
                        break;
                    }
                }
            }
        }

        if is_serial_column {
            column.options.retain(|opt| {
                !matches!(opt.option, sqlparser::ast::ColumnOption::Default(ref expr)
                    if matches!(expr, sqlparser::ast::Expr::Function(ref func)
                        if func.name.to_string().to_lowercase() == "nextval"))
            });

            column.options.push(sqlparser::ast::ColumnOptionDef {
                name: None,
                option: sqlparser::ast::ColumnOption::Generated {
                    generated_as: sqlparser::ast::GeneratedAs::ByDefault,
                    sequence_options: None,
                    generation_expr: None,
                    generation_expr_mode: None,
                    generated_keyword: true,
                },
            });
            changed = true;
        } else if is_bigserial_column {
            column.options.retain(|opt| {
                !matches!(opt.option, sqlparser::ast::ColumnOption::Default(ref expr)
                    if matches!(expr, sqlparser::ast::Expr::Function(ref func)
                        if func.name.to_string().to_lowercase() == "nextval"))
            });

            column.options.push(sqlparser::ast::ColumnOptionDef {
                name: None,
                option: sqlparser::ast::ColumnOption::Generated {
                    generated_as: sqlparser::ast::GeneratedAs::ByDefault,
                    sequence_options: None,
                    generation_expr: None,
                    generation_expr_mode: None,
                    generated_keyword: true,
                },
            });
            changed = true;
        }

        Ok(changed)
    }
}

fn get_default_mappings() -> HashMap<String, String> {
    let mut mappings = HashMap::new();

    mappings.insert("SERIAL".to_string(), "INTEGER".to_string());
    mappings.insert("BIGSERIAL".to_string(), "BIGINT".to_string());
    mappings.insert("TEXT".to_string(), "NCLOB".to_string());
    mappings.insert("JSON".to_string(), "NCLOB".to_string());
    mappings.insert("JSONB".to_string(), "NCLOB".to_string());
    mappings.insert("UUID".to_string(), "NVARCHAR(36)".to_string());
    mappings.insert("INET".to_string(), "NVARCHAR(45)".to_string());
    mappings.insert("MACADDR".to_string(), "NVARCHAR(17)".to_string());
    mappings.insert("BYTEA".to_string(), "BLOB".to_string());

    mappings
}

fn parse_hana_type(type_str: &str) -> Result<DataType, String> {
    let type_str = type_str.to_uppercase();

    match type_str.as_str() {
        "NCLOB" => Ok(DataType::Clob(None)),
        "BLOB" => Ok(DataType::Blob(None)),
        "INTEGER" => Ok(DataType::Integer(None)),
        "BIGINT" => Ok(DataType::BigInt(None)),
        s if s.starts_with("NVARCHAR(") && s.ends_with(')') => {
            let len_str = &s[9..s.len() - 1];
            if let Ok(length) = len_str.parse::<u64>() {
                Ok(DataType::Nvarchar(Some(
                    sqlparser::ast::CharacterLength::IntegerLength { length, unit: None },
                )))
            } else {
                Err(format!("Invalid NVARCHAR length: {}", len_str))
            }
        }
        s if s.starts_with("NCHAR(") && s.ends_with(')') => {
            let len_str = &s[6..s.len() - 1];
            if let Ok(length) = len_str.parse::<u64>() {
                Ok(DataType::Nvarchar(Some(
                    sqlparser::ast::CharacterLength::IntegerLength { length, unit: None },
                )))
            } else {
                Err(format!("Invalid NCHAR length: {}", len_str))
            }
        }
        _ => Err(format!("Unsupported HANA type: {}", type_str)),
    }
}
