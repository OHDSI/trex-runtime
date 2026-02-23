use super::Transformer;
use crate::config::TransformationConfig;
use crate::error::TransformationResult;
use sqlparser::ast::{DataType, Expr, Query, SelectItem, SetExpr, Statement};

pub struct StatementTransformer {
    config: TransformationConfig,
}

impl StatementTransformer {
    pub fn new(config: &TransformationConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    fn transform_limit_offset(&self, query: &mut Query) -> TransformationResult<bool> {
        let mut changed = false;

        if query.limit_clause.is_some() {
            changed = true;
        }

        Ok(changed)
    }

    fn transform_window_functions(&self, query: &mut Query) -> TransformationResult<bool> {
        let mut changed = false;

        if let SetExpr::Select(ref mut select) = query.body.as_mut() {
            for item in &mut select.projection {
                if let SelectItem::ExprWithAlias { expr, .. } = item {
                    if self.transform_window_function_expr(expr)? {
                        changed = true;
                    }
                } else if let SelectItem::UnnamedExpr(expr) = item {
                    if self.transform_window_function_expr(expr)? {
                        changed = true;
                    }
                }
            }
        }

        Ok(changed)
    }

    fn transform_window_function_expr(&self, expr: &mut Expr) -> TransformationResult<bool> {
        let mut changed = false;

        match expr {
            Expr::Function(func) => {
                if let Some(ref mut over) = func.over {
                    let func_name = func.name.to_string().to_uppercase();
                    match func_name.as_str() {
                        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => {}
                        "LAG" | "LEAD" => {}
                        "FIRST_VALUE" | "LAST_VALUE" => {}
                        _ => {}
                    }
                }
            }
            Expr::Nested(inner_expr) => {
                if self.transform_window_function_expr(inner_expr)? {
                    changed = true;
                }
            }
            _ => {}
        }

        Ok(changed)
    }

    fn transform_create_table(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let mut changed = false;

        if let Statement::CreateTable(create_table) = stmt {
            if let Some(query) = &create_table.query {
                log::warn!("CREATE TABLE AS SELECT may need manual transformation");
                changed = true;
            }

            for column in &mut create_table.columns {
                for option in &mut column.options {
                    if let sqlparser::ast::ColumnOption::Default(expr) = &mut option.option {
                        if self.transform_default_expression(expr)? {
                            changed = true;
                        }
                    }
                }
            }

            for constraint in &mut create_table.constraints {
                if self.transform_table_constraint(constraint)? {
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn transform_default_expression(&self, expr: &mut Expr) -> TransformationResult<bool> {
        let mut changed = false;

        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                if func_name == "nextval" {
                    log::warn!("nextval() in DEFAULT - convert to IDENTITY");
                } else {
                    let func_name_upper = func.name.to_string().to_uppercase();
                    match func_name_upper.as_str() {
                        "NOW" => {
                            func.name = sqlparser::ast::ObjectName(vec![
                                sqlparser::ast::ObjectNamePart::Identifier(
                                    sqlparser::ast::Ident::new("CURRENT_TIMESTAMP"),
                                ),
                            ]);
                            func.args = sqlparser::ast::FunctionArguments::None;
                            changed = true;
                        }
                        "RANDOM" => {
                            func.name = sqlparser::ast::ObjectName(vec![
                                sqlparser::ast::ObjectNamePart::Identifier(
                                    sqlparser::ast::Ident::new("RAND"),
                                ),
                            ]);
                            changed = true;
                        }
                        _ => {}
                    }
                }
            }
            Expr::Nested(inner_expr) => {
                if self.transform_default_expression(inner_expr)? {
                    changed = true;
                }
            }
            _ => {}
        }

        Ok(changed)
    }

    fn transform_data_type(&self, _data_type: &mut DataType) -> TransformationResult<bool> {
        Ok(false)
    }

    fn transform_table_constraint(
        &self,
        constraint: &mut sqlparser::ast::TableConstraint,
    ) -> TransformationResult<bool> {
        match constraint {
            sqlparser::ast::TableConstraint::Check { .. } => Ok(false),
            sqlparser::ast::TableConstraint::ForeignKey { .. } => Ok(false),
            sqlparser::ast::TableConstraint::Unique { .. } => Ok(false),
            sqlparser::ast::TableConstraint::PrimaryKey { .. } => Ok(false),
            _ => Ok(false),
        }
    }

    fn transform_insert(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let mut changed = false;

        if let Statement::Insert(insert) = stmt {
            if insert.on.is_some() {
                log::warn!("ON CONFLICT requires manual conversion to HANA UPSERT");
            }

            if let Some(ref returning) = insert.returning {
                if !returning.is_empty() {
                    log::warn!("RETURNING not supported - use OUTPUT clause");
                }
            }

            if let Some(ref mut source_query) = insert.source {
                if self.transform_limit_offset(source_query)? {
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn transform_update(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let changed = false;

        if let Statement::Update {
            from, returning, ..
        } = stmt
        {
            if let Some(ref from_clause) = from {
                log::warn!("UPDATE ... FROM may need adjustment");
            }

            if let Some(ref returning) = returning {
                if !returning.is_empty() {
                    log::warn!("RETURNING in UPDATE not supported");
                }
            }
        }

        Ok(changed)
    }

    fn transform_delete(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let changed = false;

        if let Statement::Delete(delete) = stmt {
            if let Some(ref using) = delete.using {
                if !using.is_empty() {
                    log::warn!("DELETE ... USING may need adjustment");
                }
            }

            if let Some(ref returning) = delete.returning {
                if !returning.is_empty() {
                    log::warn!("RETURNING in DELETE not supported");
                }
            }
        }

        Ok(changed)
    }
}

impl Transformer for StatementTransformer {
    fn name(&self) -> &'static str {
        "StatementTransformer"
    }

    fn priority(&self) -> u8 {
        50
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
                if self.transform_limit_offset(query)? {
                    changed = true;
                }
                if self.transform_window_functions(query)? {
                    changed = true;
                }
            }
            Statement::CreateTable(_) => {
                if self.transform_create_table(stmt)? {
                    changed = true;
                }
            }
            Statement::Insert(_) => {
                if self.transform_insert(stmt)? {
                    changed = true;
                }
            }
            Statement::Update { .. } => {
                if self.transform_update(stmt)? {
                    changed = true;
                }
            }
            Statement::Delete(_) => {
                if self.transform_delete(stmt)? {
                    changed = true;
                }
            }
            _ => {}
        }

        Ok(changed)
    }
}
