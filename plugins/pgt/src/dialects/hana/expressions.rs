use super::Transformer;
use crate::config::TransformationConfig;
use crate::error::TransformationResult;
use sqlparser::ast::{
    BinaryOperator, CastKind, Delete, Expr, Function, Ident, Statement, UnaryOperator,
};

pub struct ExpressionTransformer {
    config: TransformationConfig,
}

impl ExpressionTransformer {
    pub fn new(config: &TransformationConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    fn transform_expression(&self, expr: &mut Expr) -> TransformationResult<bool> {
        let mut changed = false;

        match expr {
            Expr::BinaryOp { left, op, right } => {
                if self.transform_expression(left)? {
                    changed = true;
                }
                if self.transform_expression(right)? {
                    changed = true;
                }

                if self.transform_binary_operator(op)? {
                    changed = true;
                }
            }
            Expr::UnaryOp {
                op,
                expr: inner_expr,
            } => {
                if self.transform_expression(inner_expr)? {
                    changed = true;
                }

                if self.transform_unary_operator(op)? {
                    changed = true;
                }
            }
            Expr::Nested(inner_expr) => {
                if self.transform_expression(inner_expr)? {
                    changed = true;
                }
            }
            Expr::Cast {
                expr: inner_expr,
                data_type,
                kind,
                ..
            } => {
                if matches!(kind, CastKind::DoubleColon) {
                    *kind = CastKind::Cast;
                    changed = true;
                }

                if self.transform_expression(inner_expr)? {
                    changed = true;
                }

                let data_type_transformer =
                    crate::dialects::hana::data_types::DataTypeTransformer::new(&self.config);
                if data_type_transformer.transform_data_type(data_type)? {
                    changed = true;
                }
            }
            Expr::IsNull(inner_expr) | Expr::IsNotNull(inner_expr) => {
                if self.transform_expression(inner_expr)? {
                    changed = true;
                }
            }
            Expr::Case {
                operand,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    if self.transform_expression(operand)? {
                        changed = true;
                    }
                }

                if let Some(else_result) = else_result {
                    if self.transform_expression(else_result)? {
                        changed = true;
                    }
                }
            }
            Expr::InList {
                expr: inner_expr,
                list,
                negated,
            } => {
                if self.transform_expression(inner_expr)? {
                    changed = true;
                }

                for item in list {
                    if self.transform_expression(item)? {
                        changed = true;
                    }
                }
            }
            Expr::Between {
                expr: inner_expr,
                negated,
                low,
                high,
            } => {
                if self.transform_expression(inner_expr)? {
                    changed = true;
                }
                if self.transform_expression(low)? {
                    changed = true;
                }
                if self.transform_expression(high)? {
                    changed = true;
                }
            }
            Expr::Like {
                expr: inner_expr,
                pattern,
                negated,
                escape_char,
                ..
            } => {
                if self.transform_expression(inner_expr)? {
                    changed = true;
                }
                if self.transform_expression(pattern)? {
                    changed = true;
                }
            }
            Expr::ILike {
                expr: _inner_expr,
                pattern: _,
                negated: _,
                escape_char: _,
                ..
            } => {}
            Expr::Subquery(query) => {
                if self.transform_query_expressions(&mut query.body)? {
                    changed = true;
                }
            }
            Expr::Exists { subquery, negated } => {
                if self.transform_query_expressions(&mut subquery.body)? {
                    changed = true;
                }
            }
            Expr::TypedString {
                data_type, value, ..
            } => {
                if self.transform_typed_string_to_cast(expr)? {
                    changed = true;
                }
            }
            Expr::Function(function) => {
                let function_name = function.name.to_string().to_uppercase();

                match function_name.as_str() {
                    "NEXTVAL" => {
                        if let Some(new_expr) = self.build_hana_nextval_expr(function)? {
                            *expr = new_expr;
                            changed = true;
                        }
                    }
                    "CURRVAL" => {
                        if let Some(new_expr) = self.build_hana_currval_expr(function)? {
                            *expr = new_expr;
                            changed = true;
                        }
                    }
                    _ => {
                        if let sqlparser::ast::FunctionArguments::List(ref mut arg_list) =
                            function.args
                        {
                            for arg in &mut arg_list.args {
                                if let sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(ref mut arg_expr),
                                ) = arg
                                {
                                    if self.transform_expression(arg_expr)? {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(changed)
    }

    fn transform_binary_operator(&self, op: &mut BinaryOperator) -> TransformationResult<bool> {
        match op {
            BinaryOperator::StringConcat => Ok(false),
            BinaryOperator::PGRegexMatch => {
                log::warn!("Regex operator ~ requires manual conversion");
                Ok(false)
            }
            BinaryOperator::PGRegexIMatch => {
                log::warn!("Regex operator ~* requires manual conversion");
                Ok(false)
            }
            BinaryOperator::PGRegexNotMatch => {
                log::warn!("Regex operator !~ requires manual conversion");
                Ok(false)
            }
            BinaryOperator::PGRegexNotIMatch => {
                log::warn!("Regex operator !~* requires manual conversion");
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    fn transform_unary_operator(&self, op: &mut UnaryOperator) -> TransformationResult<bool> {
        match op {
            UnaryOperator::Not | UnaryOperator::Plus | UnaryOperator::Minus => Ok(false),
            _ => Ok(false),
        }
    }

    fn transform_ilike_to_like(&self, expr: &mut Expr) -> TransformationResult<bool> {
        Ok(false)
    }

    fn transform_query_expressions(
        &self,
        query: &mut sqlparser::ast::SetExpr,
    ) -> TransformationResult<bool> {
        let mut changed = false;

        match query {
            sqlparser::ast::SetExpr::Select(select) => {
                for item in &mut select.projection {
                    match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                            if self.transform_expression(expr)? {
                                changed = true;
                            }
                        }
                        sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                            if self.transform_expression(expr)? {
                                changed = true;
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(ref mut selection) = select.selection {
                    if self.transform_expression(selection)? {
                        changed = true;
                    }
                }

                if let Some(ref mut having) = select.having {
                    if self.transform_expression(having)? {
                        changed = true;
                    }
                }
            }
            sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
                if self.transform_query_expressions(left)? {
                    changed = true;
                }
                if self.transform_query_expressions(right)? {
                    changed = true;
                }
            }
            sqlparser::ast::SetExpr::Values(values) => {
                for row in &mut values.rows {
                    for expr in row {
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

    fn transform_typed_string_to_cast(&self, expr: &mut Expr) -> TransformationResult<bool> {
        if let Expr::TypedString { data_type, value } = expr {
            let cast_expr = Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(Expr::Value(value.clone())),
                data_type: data_type.clone(),
                format: None,
            };

            *expr = cast_expr;
            return Ok(true);
        }

        Ok(false)
    }

    fn transform_statement(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        let mut changed = false;

        match stmt {
            Statement::Query(query) => {
                if self.transform_query_expressions(&mut query.body)? {
                    changed = true;
                }
            }
            Statement::Insert(insert_stmt) => {
                if let Some(ref mut source) = insert_stmt.source {
                    if self.transform_query_expressions(&mut source.body)? {
                        changed = true;
                    }
                }
            }
            Statement::Update {
                assignments,
                selection,
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
            Statement::Delete(Delete { selection, .. }) => {
                if let Some(ref mut where_clause) = selection {
                    if self.transform_expression(where_clause)? {
                        changed = true;
                    }
                }
            }
            Statement::CreateView { query, .. } => {
                if self.transform_query_expressions(&mut query.body)? {
                    changed = true;
                }
            }
            _ => {}
        }

        Ok(changed)
    }

    fn build_hana_nextval_expr(&self, function: &Function) -> TransformationResult<Option<Expr>> {
        if let sqlparser::ast::FunctionArguments::List(ref arg_list) = function.args {
            if arg_list.args.len() == 1 {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(
                        sqlparser::ast::ValueWithSpan {
                            value: sqlparser::ast::Value::SingleQuotedString(ref seq_name),
                            span: _,
                        },
                    )),
                ) = &arg_list.args[0]
                {
                    let new_expr =
                        Expr::CompoundIdentifier(vec![Ident::new(seq_name), Ident::new("NEXTVAL")]);
                    return Ok(Some(new_expr));
                }
            }
        }
        Ok(None)
    }

    fn build_hana_currval_expr(&self, function: &Function) -> TransformationResult<Option<Expr>> {
        if let sqlparser::ast::FunctionArguments::List(ref arg_list) = function.args {
            if arg_list.args.len() == 1 {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Value(
                        sqlparser::ast::ValueWithSpan {
                            value: sqlparser::ast::Value::SingleQuotedString(ref seq_name),
                            span: _,
                        },
                    )),
                ) = &arg_list.args[0]
                {
                    let new_expr =
                        Expr::CompoundIdentifier(vec![Ident::new(seq_name), Ident::new("CURRVAL")]);
                    return Ok(Some(new_expr));
                }
            }
        }
        Ok(None)
    }
}

impl Transformer for ExpressionTransformer {
    fn name(&self) -> &'static str {
        "ExpressionTransformer"
    }

    fn priority(&self) -> u8 {
        40
    }

    fn supports_statement_type(&self, stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Query(_)
                | Statement::Insert { .. }
                | Statement::Update { .. }
                | Statement::Delete { .. }
                | Statement::CreateView { .. }
                | Statement::CreateTable { .. }
        )
    }

    fn transform(&self, stmt: &mut Statement) -> TransformationResult<bool> {
        self.transform_statement(stmt)
    }
}
