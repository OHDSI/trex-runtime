//! SQL aggregation decomposition for distributed queries.
//!
//! Decomposes COUNT/SUM/MIN/MAX/AVG into per-node partial queries and a
//! merge query. AVG is split into SUM+COUNT on nodes, then SUM/SUM on merge.

use sqlparser::ast::{
    helpers::attached_token::AttachedToken, BinaryOperator, Expr, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, GroupByExpr, Ident, ObjectName, OrderBy, Query,
    Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Span, Token, TokenWithSpan};

#[derive(Debug, Clone, PartialEq)]
pub struct DecomposedQuery {
    pub node_sql: String,
    pub merge_sql: String,
    pub has_aggregations: bool,
}

enum ProjectionItem {
    PassThrough(SelectItem),
    SimpleAggregate {
        func_name: String,
        arg_expr: Option<Expr>,
        is_count_star: bool,
        user_alias: Option<Ident>,
    },
    Avg {
        arg_expr: Expr,
        user_alias: Option<Ident>,
    },
}

/// Decompose a SQL query for distributed execution. Falls back to passthrough
/// for unparseable or unsupported queries.
pub fn decompose_query(sql: &str) -> Result<DecomposedQuery, String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse error: {e}"))?;

    if statements.len() != 1 {
        return fallback(sql);
    }

    let query = match &statements[0] {
        sqlparser::ast::Statement::Query(q) => q,
        _ => return fallback(sql),
    };

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return fallback(sql),
    };

    let mut items: Vec<ProjectionItem> = Vec::new();
    let mut has_aggregates = false;

    for proj in &select.projection {
        match classify_projection(proj) {
            Ok(item) => {
                match &item {
                    ProjectionItem::SimpleAggregate { .. } | ProjectionItem::Avg { .. } => {
                        has_aggregates = true;
                    }
                    _ => {}
                }
                items.push(item);
            }
            Err(_) => return fallback(sql),
        }
    }

    if !has_aggregates {
        return decompose_non_aggregate(query, select);
    }

    let mut node_projection: Vec<SelectItem> = Vec::new();
    let mut merge_projection: Vec<SelectItem> = Vec::new();
    let mut counter: u32 = 0; // disambiguate when the same agg appears multiple times

    for item in &items {
        match item {
            ProjectionItem::PassThrough(si) => {
                node_projection.push(si.clone());
                merge_projection.push(si.clone());
            }
            ProjectionItem::SimpleAggregate {
                func_name,
                arg_expr,
                is_count_star,
                user_alias,
            } => {
                counter += 1;
                let alias_name = node_alias_for(func_name, arg_expr.as_ref(), *is_count_star, counter);

                let node_func = if *is_count_star {
                    make_count_star()
                } else {
                    make_func(func_name, arg_expr.clone().unwrap())
                };
                node_projection.push(SelectItem::ExprWithAlias {
                    expr: Expr::Function(node_func),
                    alias: Ident::new(&alias_name),
                });

                let merge_func_name = merge_func_for(func_name);
                let merge_expr =
                    Expr::Function(make_func(&merge_func_name, Expr::Identifier(Ident::new(&alias_name))));

                let merge_alias = final_alias(user_alias.as_ref(), func_name, arg_expr.as_ref(), *is_count_star);
                merge_projection.push(SelectItem::ExprWithAlias {
                    expr: merge_expr,
                    alias: Ident::new(&merge_alias),
                });
            }
            ProjectionItem::Avg { arg_expr, user_alias } => {
                counter += 1;
                let col_label = col_label_for(arg_expr);
                let sum_alias = format!("_sum_{}{}", col_label, counter);
                let count_alias = format!("_count_{}{}", col_label, counter);

                node_projection.push(SelectItem::ExprWithAlias {
                    expr: Expr::Function(make_func("SUM", arg_expr.clone())),
                    alias: Ident::new(&sum_alias),
                });
                node_projection.push(SelectItem::ExprWithAlias {
                    expr: Expr::Function(make_func("COUNT", arg_expr.clone())),
                    alias: Ident::new(&count_alias),
                });

                let merge_expr = Expr::BinaryOp {
                    left: Box::new(Expr::Function(make_func(
                        "SUM",
                        Expr::Identifier(Ident::new(&sum_alias)),
                    ))),
                    op: BinaryOperator::Divide,
                    right: Box::new(Expr::Function(sqlparser::ast::Function {
                        name: ObjectName(vec![Ident::new("NULLIF")]),
                        uses_odbc_syntax: false,
                        parameters: FunctionArguments::None,
                        args: FunctionArguments::List(FunctionArgumentList {
                            duplicate_treatment: None,
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Function(make_func("SUM", Expr::Identifier(Ident::new(&count_alias))))
                                )),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Value(sqlparser::ast::Value::Number("0".to_string(), false))
                                )),
                            ],
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: None,
                        within_group: vec![],
                    })),
                };

                let merge_alias = match user_alias {
                    Some(a) => a.value.clone(),
                    None => format!("avg_{}", col_label),
                };
                merge_projection.push(SelectItem::ExprWithAlias {
                    expr: merge_expr,
                    alias: Ident::new(&merge_alias),
                });
            }
        }
    }

    let group_by_exprs = extract_group_by_exprs(&select.group_by);
    let node_select = build_select(node_projection, &select.from, &group_by_exprs, &select.selection, &select.having);
    let node_query = build_query(node_select, None, None, None);
    let node_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(node_query)));

    let merge_select = build_select(
        merge_projection,
        &[merged_table_source()],
        &group_by_exprs,
        &None,
        &None,
    );
    let merge_query = build_query(merge_select, query.order_by.clone(), query.limit.clone(), query.offset.clone());
    let merge_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(merge_query)));

    Ok(DecomposedQuery {
        node_sql,
        merge_sql,
        has_aggregations: true,
    })
}

/// Non-aggregate: node_sql = original sans ORDER BY/LIMIT; merge adds them back.
fn decompose_non_aggregate(query: &Query, select: &Select) -> Result<DecomposedQuery, String> {
    let node_select = select.clone();
    let node_query = build_query(node_select, None, None, None);
    let node_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(node_query)));

    let has_order_or_limit =
        query.order_by.is_some() || query.limit.is_some() || query.offset.is_some();

    if !has_order_or_limit {
        return Ok(DecomposedQuery {
            node_sql: node_sql.clone(),
            merge_sql: format!("SELECT * FROM _merged"),
            has_aggregations: false,
        });
    }

    let merge_select = build_select(
        vec![SelectItem::Wildcard(
            sqlparser::ast::WildcardAdditionalOptions {
                wildcard_token: AttachedToken(TokenWithSpan {
                    token: Token::Mul,
                    span: Span::empty(),
                }),
                opt_ilike: None,
                opt_exclude: None,
                opt_except: None,
                opt_replace: None,
                opt_rename: None,
            },
        )],
        &[merged_table_source()],
        &[],
        &None,
        &None,
    );
    let merge_query = build_query(merge_select, query.order_by.clone(), query.limit.clone(), query.offset.clone());
    let merge_sql = format!("{}", sqlparser::ast::Statement::Query(Box::new(merge_query)));

    Ok(DecomposedQuery {
        node_sql,
        merge_sql,
        has_aggregations: false,
    })
}

fn classify_projection(item: &SelectItem) -> Result<ProjectionItem, String> {
    match item {
        SelectItem::UnnamedExpr(expr) => classify_expr(expr, None),
        SelectItem::ExprWithAlias { expr, alias } => classify_expr(expr, Some(alias.clone())),
        other => Ok(ProjectionItem::PassThrough(other.clone())),
    }
}

fn classify_expr(expr: &Expr, alias: Option<Ident>) -> Result<ProjectionItem, String> {
    match expr {
        Expr::Function(f) => {
            let func_name = f.name.to_string().to_uppercase();
            match func_name.as_str() {
                "COUNT" | "SUM" | "MIN" | "MAX" => {
                    let (arg_expr, is_count_star) = extract_single_arg(f)?;
                    Ok(ProjectionItem::SimpleAggregate {
                        func_name,
                        arg_expr,
                        is_count_star,
                        user_alias: alias,
                    })
                }
                "AVG" => {
                    let (arg_expr, is_star) = extract_single_arg(f)?;
                    if is_star {
                        return Err("AVG(*) is not valid SQL".to_string());
                    }
                    Ok(ProjectionItem::Avg {
                        arg_expr: arg_expr.ok_or("AVG requires an argument")?,
                        user_alias: alias,
                    })
                }
                _ => {
                    let item = match alias {
                        Some(a) => SelectItem::ExprWithAlias {
                            expr: expr.clone(),
                            alias: a,
                        },
                        None => SelectItem::UnnamedExpr(expr.clone()),
                    };
                    Ok(ProjectionItem::PassThrough(item))
                }
            }
        }
        _ => {
            let item = match alias {
                Some(a) => SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias: a,
                },
                None => SelectItem::UnnamedExpr(expr.clone()),
            };
            Ok(ProjectionItem::PassThrough(item))
        }
    }
}

/// Returns `(Some(expr), false)` for normal args, `(None, true)` for `COUNT(*)`.
fn extract_single_arg(f: &sqlparser::ast::Function) -> Result<(Option<Expr>, bool), String> {
    match &f.args {
        FunctionArguments::List(arg_list) => {
            if arg_list.args.len() != 1 {
                return Err(format!(
                    "{} with {} args is not supported for decomposition",
                    f.name,
                    arg_list.args.len()
                ));
            }
            match &arg_list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok((None, true)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok((Some(e.clone()), false)),
                other => Err(format!("unsupported function argument: {:?}", other)),
            }
        }
        FunctionArguments::None => Err("function has no arguments".to_string()),
        FunctionArguments::Subquery(_) => Err("subquery arguments not supported".to_string()),
    }
}

fn node_alias_for(func_name: &str, arg: Option<&Expr>, is_count_star: bool, counter: u32) -> String {
    if is_count_star {
        return format!("_count{}", counter);
    }
    let col = arg.map(|e| col_label_for(e)).unwrap_or_else(|| "x".to_string());
    format!("_{}_{}{}", func_name.to_lowercase(), col, counter)
}

fn col_label_for(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(id) => sanitize_label(&id.value),
        Expr::CompoundIdentifier(ids) => {
            ids.iter().map(|id| sanitize_label(&id.value)).collect::<Vec<_>>().join("_")
        }
        _ => {
            let s = format!("{}", expr);
            sanitize_label(&s)
        }
    }
}

fn sanitize_label(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_lowercase()
}

fn merge_func_for(node_func: &str) -> String {
    match node_func {
        "COUNT" => "SUM".to_string(), // partial counts must be summed
        other => other.to_string(),
    }
}

fn final_alias(
    user_alias: Option<&Ident>,
    func_name: &str,
    arg: Option<&Expr>,
    is_count_star: bool,
) -> String {
    if let Some(a) = user_alias {
        return a.value.clone();
    }
    if is_count_star {
        return "count_star".to_string();
    }
    let col = arg.map(|e| col_label_for(e)).unwrap_or_else(|| "x".to_string());
    format!("{}_{}", func_name.to_lowercase(), col)
}

fn make_func(name: &str, arg: Expr) -> sqlparser::ast::Function {
    sqlparser::ast::Function {
        name: ObjectName(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

fn make_count_star() -> sqlparser::ast::Function {
    sqlparser::ast::Function {
        name: ObjectName(vec![Ident::new("COUNT")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

fn build_select(
    projection: Vec<SelectItem>,
    from: &[TableWithJoins],
    group_by_exprs: &[Expr],
    selection: &Option<Expr>,
    having: &Option<Expr>,
) -> Select {
    let group_by = if group_by_exprs.is_empty() {
        GroupByExpr::Expressions(vec![], vec![])
    } else {
        GroupByExpr::Expressions(group_by_exprs.to_vec(), vec![])
    };

    Select {
        select_token: AttachedToken(TokenWithSpan {
            token: Token::make_keyword("SELECT"),
            span: Span::empty(),
        }),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection,
        into: None,
        from: from.to_vec(),
        lateral_views: vec![],
        prewhere: None,
        selection: selection.clone(),
        group_by,
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: having.clone(),
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: None,
    }
}

fn build_query(
    select: Select,
    order_by: Option<OrderBy>,
    limit: Option<Expr>,
    offset: Option<sqlparser::ast::Offset>,
) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(select))),
        order_by,
        limit,
        limit_by: vec![],
        offset,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
    }
}

fn extract_group_by_exprs(group_by: &GroupByExpr) -> Vec<Expr> {
    match group_by {
        GroupByExpr::Expressions(exprs, _modifiers) => exprs.clone(),
        GroupByExpr::All(_) => vec![],
    }
}

fn merged_table_source() -> TableWithJoins {
    TableWithJoins {
        relation: TableFactor::Table {
            name: ObjectName(vec![Ident::new("_merged")]),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            with_ordinality: false,
            partitions: vec![],
            json_path: None,
        },
        joins: vec![],
    }
}

fn fallback(sql: &str) -> Result<DecomposedQuery, String> {
    Ok(DecomposedQuery {
        node_sql: sql.to_string(),
        merge_sql: format!("SELECT * FROM _merged"),
        has_aggregations: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_no_aggregates() {
        let sql = "SELECT id, name FROM users";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        assert_eq!(result.node_sql, "SELECT id, name FROM users");
        assert_eq!(result.merge_sql, "SELECT * FROM _merged");
    }

    #[test]
    fn test_select_with_order_by_and_limit() {
        let sql = "SELECT id, name FROM users ORDER BY name LIMIT 10";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        assert!(!result.node_sql.to_uppercase().contains("ORDER BY"));
        assert!(!result.node_sql.to_uppercase().contains("LIMIT"));
        assert!(result.merge_sql.to_uppercase().contains("ORDER BY"));
        assert!(result.merge_sql.to_uppercase().contains("LIMIT"));
    }

    #[test]
    fn test_count_star() {
        let sql = "SELECT COUNT(*) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("COUNT(*)"));
        assert!(result.node_sql.to_uppercase().contains(" AS "));
        assert!(result.merge_sql.to_uppercase().contains("SUM("));
    }

    #[test]
    fn test_count_column() {
        let sql = "SELECT COUNT(id) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("COUNT(ID)"));
        assert!(result.merge_sql.to_uppercase().contains("SUM("));
    }

    #[test]
    fn test_sum() {
        let sql = "SELECT SUM(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("SUM(PRICE)"));
        assert!(result.merge_sql.to_uppercase().contains("SUM("));
    }

    #[test]
    fn test_min_max() {
        let sql = "SELECT MIN(price), MAX(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("MIN(PRICE)"));
        assert!(result.node_sql.to_uppercase().contains("MAX(PRICE)"));
        assert!(result.merge_sql.to_uppercase().contains("MIN("));
        assert!(result.merge_sql.to_uppercase().contains("MAX("));
    }

    #[test]
    fn test_avg_decomposition() {
        let sql = "SELECT AVG(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        let node_upper = result.node_sql.to_uppercase();
        assert!(node_upper.contains("SUM(PRICE)"));
        assert!(node_upper.contains("COUNT(PRICE)"));
        assert!(!node_upper.contains("AVG"));
        assert!(result.merge_sql.contains("/"));
    }

    #[test]
    fn test_aggregate_with_group_by() {
        let sql = "SELECT region, SUM(price) FROM orders GROUP BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("GROUP BY"));
        assert!(result.merge_sql.to_uppercase().contains("GROUP BY"));
        assert!(result.node_sql.to_uppercase().contains("REGION"));
        assert!(result.merge_sql.to_uppercase().contains("REGION"));
    }

    #[test]
    fn test_aggregate_with_order_by() {
        let sql = "SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(!result.node_sql.to_uppercase().contains("ORDER BY"));
        assert!(result.merge_sql.to_uppercase().contains("ORDER BY"));
    }

    #[test]
    fn test_full_example_from_spec() {
        let sql = "SELECT region, AVG(price), COUNT(*) FROM orders GROUP BY region ORDER BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);

        let node_upper = result.node_sql.to_uppercase();
        assert!(node_upper.contains("REGION"));
        assert!(node_upper.contains("SUM(PRICE)"));
        assert!(node_upper.contains("COUNT(PRICE)"));
        assert!(node_upper.contains("COUNT(*)"));
        assert!(node_upper.contains("GROUP BY"));
        assert!(!node_upper.contains("ORDER BY"));

        let merge_upper = result.merge_sql.to_uppercase();
        assert!(merge_upper.contains("REGION"));
        assert!(merge_upper.contains("/"));
        assert!(merge_upper.contains("SUM("));
        assert!(merge_upper.contains("GROUP BY"));
        assert!(merge_upper.contains("ORDER BY"));
        assert!(merge_upper.contains("_MERGED"));
    }

    #[test]
    fn test_user_alias_preserved() {
        let sql = "SELECT SUM(price) AS total FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.merge_sql.to_uppercase().contains("AS TOTAL"));
    }

    #[test]
    fn test_avg_user_alias_preserved() {
        let sql = "SELECT AVG(price) AS avg_price FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.merge_sql.to_uppercase().contains("AS AVG_PRICE"));
    }

    #[test]
    fn test_non_select_falls_back() {
        let sql = "INSERT INTO t VALUES (1, 2)";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        assert_eq!(result.node_sql, sql);
    }

    #[test]
    fn test_invalid_sql_returns_error() {
        let sql = "NOT VALID SQL AT ALL %%%";
        let result = decompose_query(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_aggregates_same_type() {
        let sql = "SELECT SUM(a), SUM(b) FROM t";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        let node_upper = result.node_sql.to_uppercase();
        assert!(node_upper.contains("SUM(A)"));
        assert!(node_upper.contains("SUM(B)"));
    }

    #[test]
    fn test_where_clause_preserved_in_node() {
        let sql = "SELECT COUNT(*) FROM orders WHERE status = 'active'";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        assert!(result.node_sql.to_uppercase().contains("WHERE"));
        assert!(!result.merge_sql.to_uppercase().contains("WHERE"));
    }

    #[test]
    fn test_limit_and_offset() {
        let sql = "SELECT id FROM t ORDER BY id LIMIT 10 OFFSET 5";
        let result = decompose_query(sql).unwrap();
        assert!(!result.has_aggregations);
        assert!(!result.node_sql.to_uppercase().contains("LIMIT"));
        assert!(!result.node_sql.to_uppercase().contains("OFFSET"));
        assert!(result.merge_sql.to_uppercase().contains("LIMIT"));
        assert!(result.merge_sql.to_uppercase().contains("OFFSET"));
    }

    #[test]
    fn test_merge_references_merged_table() {
        let sql = "SELECT SUM(x) FROM t";
        let result = decompose_query(sql).unwrap();
        assert!(result.merge_sql.contains("_merged"));
    }

    #[test]
    fn test_node_preserves_from_clause() {
        let sql = "SELECT SUM(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.node_sql.to_uppercase().contains("FROM ORDERS"));
    }

    #[test]
    fn test_avg_empty_table_no_division_by_zero() {
        let sql = "SELECT AVG(price) FROM orders";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);
        // The merge SQL should use NULLIF to prevent division by zero
        assert!(
            result.merge_sql.to_uppercase().contains("NULLIF"),
            "AVG merge should use NULLIF to prevent division by zero: {}",
            result.merge_sql,
        );
    }

    #[test]
    fn test_mixed_aggregate_and_non_aggregate() {
        let sql = "SELECT region, COUNT(*), MIN(price), MAX(price) FROM orders GROUP BY region";
        let result = decompose_query(sql).unwrap();
        assert!(result.has_aggregations);

        let node_upper = result.node_sql.to_uppercase();
        assert!(node_upper.contains("REGION"));
        assert!(node_upper.contains("COUNT(*)"));
        assert!(node_upper.contains("MIN(PRICE)"));
        assert!(node_upper.contains("MAX(PRICE)"));
        assert!(node_upper.contains("GROUP BY"));

        let merge_upper = result.merge_sql.to_uppercase();
        assert!(merge_upper.contains("REGION"));
        assert!(merge_upper.contains("SUM(")); // COUNT(*) becomes SUM(_count)
        assert!(merge_upper.contains("MIN("));
        assert!(merge_upper.contains("MAX("));
        assert!(merge_upper.contains("GROUP BY"));
    }
}
