use sqlparser::ast::{
    Expr, FunctionArguments, ObjectNamePart, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, With,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::error::Error;

fn ident_value(part: &ObjectNamePart) -> Option<&str> {
    part.as_ident().map(|i| i.value.as_str())
}

pub fn extract_dependencies(sql: &str) -> Result<HashSet<String>, Box<dyn Error>> {
    let dialect = DuckDbDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    let mut refs = HashSet::new();
    for stmt in &statements {
        extract_from_statement(stmt, &mut refs, &HashSet::new());
    }
    Ok(refs)
}

fn extract_from_statement(
    stmt: &Statement,
    refs: &mut HashSet<String>,
    cte_names: &HashSet<String>,
) {
    if let Statement::Query(query) = stmt {
        extract_from_query(query, refs, cte_names);
    }
}

fn extract_from_query(
    query: &Query,
    refs: &mut HashSet<String>,
    parent_ctes: &HashSet<String>,
) {
    let mut cte_names = parent_ctes.clone();

    if let Some(with) = &query.with {
        extract_from_with(with, refs, &mut cte_names);
    }

    extract_from_set_expr(&query.body, refs, &cte_names);
}

fn extract_from_with(
    with: &With,
    refs: &mut HashSet<String>,
    cte_names: &mut HashSet<String>,
) {
    for cte in &with.cte_tables {
        cte_names.insert(cte.alias.name.value.clone());
        extract_from_query(&cte.query, refs, cte_names);
    }
}

fn extract_from_set_expr(
    set_expr: &SetExpr,
    refs: &mut HashSet<String>,
    cte_names: &HashSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => extract_from_select(select, refs, cte_names),
        SetExpr::Query(query) => extract_from_query(query, refs, cte_names),
        SetExpr::SetOperation { left, right, .. } => {
            extract_from_set_expr(left, refs, cte_names);
            extract_from_set_expr(right, refs, cte_names);
        }
        _ => {}
    }
}

fn extract_from_select(
    select: &Select,
    refs: &mut HashSet<String>,
    cte_names: &HashSet<String>,
) {
    for table_with_joins in &select.from {
        extract_from_table_with_joins(table_with_joins, refs, cte_names);
    }

    if let Some(selection) = &select.selection {
        extract_from_expr(selection, refs, cte_names);
    }

    for item in &select.projection {
        if let SelectItem::ExprWithAlias { expr, .. } | SelectItem::UnnamedExpr(expr) = item {
            extract_from_expr(expr, refs, cte_names);
        }
    }
}

fn extract_from_table_with_joins(
    twj: &TableWithJoins,
    refs: &mut HashSet<String>,
    cte_names: &HashSet<String>,
) {
    extract_from_table_factor(&twj.relation, refs, cte_names);
    for join in &twj.joins {
        extract_from_table_factor(&join.relation, refs, cte_names);
    }
}

fn extract_from_table_factor(
    factor: &TableFactor,
    refs: &mut HashSet<String>,
    cte_names: &HashSet<String>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                if let Some(table_name) = ident_value(&name.0[0]) {
                    if !cte_names.contains(table_name) {
                        refs.insert(table_name.to_string());
                    }
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            extract_from_query(subquery, refs, cte_names);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            extract_from_table_with_joins(table_with_joins, refs, cte_names);
        }
        _ => {}
    }
}

fn extract_from_expr(
    expr: &Expr,
    refs: &mut HashSet<String>,
    cte_names: &HashSet<String>,
) {
    match expr {
        Expr::Subquery(query) => extract_from_query(query, refs, cte_names),
        Expr::InSubquery { subquery, expr, .. } => {
            extract_from_query(subquery, refs, cte_names);
            extract_from_expr(expr, refs, cte_names);
        }
        Expr::Exists { subquery, .. } => {
            extract_from_query(subquery, refs, cte_names);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_from_expr(left, refs, cte_names);
            extract_from_expr(right, refs, cte_names);
        }
        Expr::UnaryOp { expr, .. } => {
            extract_from_expr(expr, refs, cte_names);
        }
        Expr::Nested(expr) => {
            extract_from_expr(expr, refs, cte_names);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            extract_from_expr(expr, refs, cte_names);
        }
        Expr::Between { expr, low, high, .. } => {
            extract_from_expr(expr, refs, cte_names);
            extract_from_expr(low, refs, cte_names);
            extract_from_expr(high, refs, cte_names);
        }
        Expr::Case { operand, conditions, else_result, .. } => {
            if let Some(op) = operand {
                extract_from_expr(op, refs, cte_names);
            }
            for case_when in conditions {
                extract_from_expr(&case_when.condition, refs, cte_names);
                extract_from_expr(&case_when.result, refs, cte_names);
            }
            if let Some(el) = else_result {
                extract_from_expr(el, refs, cte_names);
            }
        }
        Expr::Function(func) => {
            if let FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(arg_expr)
                        | sqlparser::ast::FunctionArg::Named { arg: arg_expr, .. } => {
                            if let sqlparser::ast::FunctionArgExpr::Expr(e) = arg_expr {
                                extract_from_expr(e, refs, cte_names);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
}

pub fn rewrite_table_references_dual(
    sql: &str,
    known_names: &HashSet<String>,
    source_names: &HashSet<String>,
    dest_schema: &str,
    source_schema: &str,
) -> Result<String, Box<dyn Error>> {
    let dialect = DuckDbDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    let mut cte_names = HashSet::new();
    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    cte_names.insert(cte.alias.name.value.clone());
                }
            }
            rewrite_set_expr_dual(&mut query.body, known_names, source_names, dest_schema, source_schema, &cte_names);
        }
    }

    let rewritten: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    Ok(rewritten.join(";\n"))
}

pub fn rewrite_table_references(
    sql: &str,
    known_names: &HashSet<String>,
    schema: &str,
) -> Result<String, Box<dyn Error>> {
    let dialect = DuckDbDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    let mut cte_names = HashSet::new();
    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    cte_names.insert(cte.alias.name.value.clone());
                }
            }
            rewrite_set_expr(&mut query.body, known_names, schema, &cte_names);
        }
    }

    let rewritten: Vec<String> = statements.iter().map(|s| s.to_string()).collect();
    Ok(rewritten.join(";\n"))
}

fn rewrite_set_expr(
    set_expr: &mut SetExpr,
    known_names: &HashSet<String>,
    schema: &str,
    cte_names: &HashSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                rewrite_table_with_joins(twj, known_names, schema, cte_names);
            }
        }
        SetExpr::Query(query) => {
            rewrite_set_expr(&mut query.body, known_names, schema, cte_names);
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left, known_names, schema, cte_names);
            rewrite_set_expr(right, known_names, schema, cte_names);
        }
        _ => {}
    }
}

fn rewrite_table_with_joins(
    twj: &mut TableWithJoins,
    known_names: &HashSet<String>,
    schema: &str,
    cte_names: &HashSet<String>,
) {
    rewrite_table_factor(&mut twj.relation, known_names, schema, cte_names);
    for join in &mut twj.joins {
        rewrite_table_factor(&mut join.relation, known_names, schema, cte_names);
    }
}

fn rewrite_table_factor(
    factor: &mut TableFactor,
    known_names: &HashSet<String>,
    schema: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                let table_name = match ident_value(&name.0[0]) {
                    Some(v) => v.to_string(),
                    None => return,
                };
                if known_names.contains(&table_name) && !cte_names.contains(&table_name) {
                    let schema_ident = sqlparser::ast::Ident::with_quote('"', schema);
                    let table_ident = name.0[0].clone();
                    name.0 = vec![
                        ObjectNamePart::Identifier(schema_ident),
                        table_ident,
                    ];
                }
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_set_expr(&mut subquery.body, known_names, schema, cte_names);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            rewrite_table_with_joins(table_with_joins, known_names, schema, cte_names);
        }
        _ => {}
    }
}

fn rewrite_set_expr_dual(
    set_expr: &mut SetExpr,
    known_names: &HashSet<String>,
    source_names: &HashSet<String>,
    dest_schema: &str,
    source_schema: &str,
    cte_names: &HashSet<String>,
) {
    match set_expr {
        SetExpr::Select(select) => {
            for twj in &mut select.from {
                rewrite_table_with_joins_dual(twj, known_names, source_names, dest_schema, source_schema, cte_names);
            }
        }
        SetExpr::Query(query) => {
            rewrite_set_expr_dual(&mut query.body, known_names, source_names, dest_schema, source_schema, cte_names);
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr_dual(left, known_names, source_names, dest_schema, source_schema, cte_names);
            rewrite_set_expr_dual(right, known_names, source_names, dest_schema, source_schema, cte_names);
        }
        _ => {}
    }
}

fn rewrite_table_with_joins_dual(
    twj: &mut TableWithJoins,
    known_names: &HashSet<String>,
    source_names: &HashSet<String>,
    dest_schema: &str,
    source_schema: &str,
    cte_names: &HashSet<String>,
) {
    rewrite_table_factor_dual(&mut twj.relation, known_names, source_names, dest_schema, source_schema, cte_names);
    for join in &mut twj.joins {
        rewrite_table_factor_dual(&mut join.relation, known_names, source_names, dest_schema, source_schema, cte_names);
    }
}

fn rewrite_table_factor_dual(
    factor: &mut TableFactor,
    known_names: &HashSet<String>,
    source_names: &HashSet<String>,
    dest_schema: &str,
    source_schema: &str,
    cte_names: &HashSet<String>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            if name.0.len() == 1 {
                let table_name = match ident_value(&name.0[0]) {
                    Some(v) => v.to_string(),
                    None => return,
                };
                if cte_names.contains(&table_name) {
                    return;
                }
                let schema = if source_names.contains(&table_name) {
                    source_schema
                } else if known_names.contains(&table_name) {
                    dest_schema
                } else {
                    return;
                };
                let schema_ident = sqlparser::ast::Ident::with_quote('"', schema);
                let table_ident = name.0[0].clone();
                name.0 = vec![
                    ObjectNamePart::Identifier(schema_ident),
                    table_ident,
                ];
            }
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_set_expr_dual(&mut subquery.body, known_names, source_names, dest_schema, source_schema, cte_names);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            rewrite_table_with_joins_dual(table_with_joins, known_names, source_names, dest_schema, source_schema, cte_names);
        }
        _ => {}
    }
}
