use super::Transformer;
use crate::config::TransformationConfig;
use crate::error::TransformationResult;
use sqlparser::ast::{Expr, Function, Ident, ObjectName, Statement};
use std::collections::HashMap;

pub struct FunctionTransformer {
    simple_mappings: HashMap<String, String>,
    preserve_case: bool,
}

impl FunctionTransformer {
    pub fn new(config: &TransformationConfig) -> Self {
        let mut simple_mappings = config.functions.custom_mappings.clone();

        for (pg_func, hana_func) in get_default_function_mappings() {
            simple_mappings.entry(pg_func).or_insert(hana_func);
        }

        Self {
            simple_mappings,
            preserve_case: config.functions.preserve_case,
        }
    }

    fn transform_expression(&self, expr: &mut Expr) -> TransformationResult<bool> {
        let mut changed = false;

        match expr {
            Expr::Function(func) => {
                if self.transform_function(func)? {
                    changed = true;
                }
            }
            Expr::BinaryOp { left, op, right } => {
                if self.transform_expression(left)? {
                    changed = true;
                }
                if self.transform_expression(right)? {
                    changed = true;
                }

                if matches!(op, sqlparser::ast::BinaryOperator::StringConcat) {
                }
            }
            Expr::Nested(inner) => {
                if self.transform_expression(inner)? {
                    changed = true;
                }
            }
            Expr::Subquery(query) => {
                if self.transform_query_functions(&mut query.body)? {
                    changed = true;
                }
            }
            _ => {}
        }

        Ok(changed)
    }

    fn transform_function(&self, func: &mut Function) -> TransformationResult<bool> {
        let func_name = func.name.to_string().to_uppercase();
        let mut changed = false;

        if let Some(hana_name) = self.simple_mappings.get(&func_name) {
            func.name = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                Ident::new(hana_name),
            )]);
            changed = true;
        } else {
            changed = self.transform_complex_function(func)?;
        }

        match &mut func.args {
            sqlparser::ast::FunctionArguments::List(arg_list) => {
                for arg in &mut arg_list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) = arg
                    {
                        if self.transform_expression(expr)? {
                            changed = true;
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(changed)
    }

    fn transform_complex_function(&self, func: &mut Function) -> TransformationResult<bool> {
        let func_name = func.name.to_string().to_uppercase();

        match func_name.as_str() {
            "CONCAT" => self.transform_concat_function(func),
            "POSITION" => self.transform_position_function(func),
            "SUBSTRING" => self.transform_substring_function(func),
            "EXTRACT" => self.validate_extract_function(func),
            "RANDOM" => {
                func.name = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                    Ident::new("RAND"),
                )]);
                Ok(true)
            }
            "NEXTVAL" => self.transform_nextval_function(func),
            _ => Ok(false),
        }
    }

    fn transform_position_function(&self, func: &mut Function) -> TransformationResult<bool> {
        func.name = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("LOCATE"),
        )]);

        Ok(true)
    }

    fn transform_substring_function(&self, func: &mut Function) -> TransformationResult<bool> {
        Ok(false)
    }

    fn validate_extract_function(&self, func: &mut Function) -> TransformationResult<bool> {
        Ok(false)
    }

    fn transform_concat_function(&self, func: &mut Function) -> TransformationResult<bool> {
        if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut func.args {
            if arg_list.args.len() > 2 {
                log::warn!("CONCAT with >2 args - consider || operator");
                return Ok(false);
            }
        }

        Ok(false)
    }

    fn transform_nextval_function(&self, func: &mut Function) -> TransformationResult<bool> {
        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
            if arg_list.args.len() == 1 {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(value_with_span)),
                ) = &arg_list.args[0]
                {
                    if let sqlparser::ast::Value::SingleQuotedString(seq_name) =
                        &value_with_span.value
                    {
                        log::warn!("NEXTVAL requires manual conversion to HANA sequence syntax");
                    }
                }
            }
        }

        Ok(false)
    }

    fn transform_query_functions(
        &self,
        query: &mut sqlparser::ast::SetExpr,
    ) -> TransformationResult<bool> {
        let mut changed = false;

        match query {
            sqlparser::ast::SetExpr::Select(select) => {
                for item in &mut select.projection {
                    if let sqlparser::ast::SelectItem::UnnamedExpr(expr) = item {
                        if self.transform_expression(expr)? {
                            changed = true;
                        }
                    } else if let sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item {
                        if self.transform_expression(expr)? {
                            changed = true;
                        }
                    }
                }

                if let Some(ref mut where_clause) = select.selection {
                    if self.transform_expression(where_clause)? {
                        changed = true;
                    }
                }

                if let sqlparser::ast::GroupByExpr::Expressions(expressions, _) =
                    &mut select.group_by
                {
                    for expr in expressions {
                        if self.transform_expression(expr)? {
                            changed = true;
                        }
                    }
                }

                if let Some(ref mut having) = select.having {
                    if self.transform_expression(having)? {
                        changed = true;
                    }
                }
            }
            sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
                if self.transform_query_functions(left)? {
                    changed = true;
                }
                if self.transform_query_functions(right)? {
                    changed = true;
                }
            }
            _ => {}
        }

        Ok(changed)
    }
}

impl Transformer for FunctionTransformer {
    fn name(&self) -> &'static str {
        "FunctionTransformer"
    }

    fn priority(&self) -> u8 {
        30
    }

    fn supports_statement_type(&self, stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Query(_)
                | Statement::Insert(_)
                | Statement::Update { .. }
                | Statement::Delete(_)
                | Statement::CreateTable(_)
                | Statement::CreateView { .. }
        )
    }

    fn transform(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let mut changed = false;

        match stmt {
            Statement::Query(query) => {
                if self.transform_query_functions(&mut query.body)? {
                    changed = true;
                }
            }
            Statement::Insert(insert) => {
                if let Some(source) = &mut insert.source {
                    if self.transform_query_functions(&mut source.body)? {
                        changed = true;
                    }
                }
            }
            Statement::Update {
                selection,
                assignments,
                ..
            } => {
                for assignment in assignments {
                    if self.transform_expression(&mut assignment.value)? {
                        changed = true;
                    }
                }

                if let Some(ref mut where_clause) = selection {
                    if self.transform_expression(where_clause)? {
                        changed = true;
                    }
                }
            }
            Statement::Delete(delete) => {
                if let Some(ref mut where_clause) = delete.selection {
                    if self.transform_expression(where_clause)? {
                        changed = true;
                    }
                }
            }
            _ => {}
        }

        Ok(changed)
    }
}

fn get_default_function_mappings() -> HashMap<String, String> {
    let mut mappings = HashMap::new();

    mappings.insert("RANDOM".to_string(), "RAND".to_string());
    mappings.insert(
        "CURRENT_TIMESTAMP()".to_string(),
        "CURRENT_TIMESTAMP".to_string(),
    );
    mappings.insert("CURRENT_TIME()".to_string(), "CURRENT_TIME".to_string());
    mappings.insert("CURRENT_DATE()".to_string(), "CURRENT_DATE".to_string());

    mappings.insert("LENGTH".to_string(), "LENGTH".to_string());
    mappings.insert("UPPER".to_string(), "UPPER".to_string());
    mappings.insert("LOWER".to_string(), "LOWER".to_string());
    mappings.insert("TRIM".to_string(), "TRIM".to_string());

    mappings.insert("ABS".to_string(), "ABS".to_string());
    mappings.insert("ROUND".to_string(), "ROUND".to_string());
    mappings.insert("CEIL".to_string(), "CEIL".to_string());
    mappings.insert("FLOOR".to_string(), "FLOOR".to_string());

    mappings.insert("COUNT".to_string(), "COUNT".to_string());
    mappings.insert("SUM".to_string(), "SUM".to_string());
    mappings.insert("AVG".to_string(), "AVG".to_string());
    mappings.insert("MIN".to_string(), "MIN".to_string());
    mappings.insert("MAX".to_string(), "MAX".to_string());

    mappings
}
