use std::collections::HashMap;

use crate::cql::elm_types::*;

pub struct CompilationContext {
    pub schema_name: String,
    pub aliases: HashMap<String, (String, String)>,
    pub expression_ctes: HashMap<String, String>,
    pub valueset_urls: HashMap<String, String>,
    pub code_defs: HashMap<String, (String, String)>,
    pub ctes: Vec<(String, String)>,
    pub patient_context_alias: Option<String>,
    next_alias: usize,
}

impl CompilationContext {
    pub fn new(schema_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            aliases: HashMap::new(),
            expression_ctes: HashMap::new(),
            valueset_urls: HashMap::new(),
            code_defs: HashMap::new(),
            ctes: Vec::new(),
            patient_context_alias: None,
            next_alias: 0,
        }
    }

    fn next_alias(&mut self) -> String {
        self.next_alias += 1;
        format!("_t{}", self.next_alias)
    }
}

pub fn compile_library(library: &ElmLibrary, schema_name: &str) -> Result<String, String> {
    let mut ctx = CompilationContext::new(schema_name);

    if let Some(vs_defs) = &library.valueSets {
        for vs in &vs_defs.defs {
            ctx.valueset_urls.insert(vs.name.clone(), vs.id.clone());
        }
    }

    if let Some(code_defs) = &library.codes {
        for code in &code_defs.defs {
            let system_name = code
                .code_system
                .as_ref()
                .map(|cs| cs.name.clone())
                .unwrap_or_default();
            ctx.code_defs
                .insert(code.name.clone(), (system_name, code.id.clone()));
        }
    }

    let statements = library
        .statements
        .as_ref()
        .ok_or("Library has no statements")?;

    let mut result_expressions = Vec::new();

    for def in &statements.defs {
        if def.name == "Patient" {
            continue;
        }

        let sql = compile_expression(&def.expression, &mut ctx)?;
        let cte_name = format!("\"{}\"", def.name.replace('"', "\"\""));
        ctx.expression_ctes
            .insert(def.name.clone(), cte_name.clone());
        ctx.ctes.push((cte_name.clone(), sql));
        result_expressions.push(cte_name);
    }

    if ctx.ctes.is_empty() {
        return Err("No compilable expressions in library".to_string());
    }

    let cte_clauses: Vec<String> = ctx
        .ctes
        .iter()
        .map(|(name, sql)| format!("{} AS (\n  {}\n)", name, sql))
        .collect();

    let last_cte = result_expressions
        .last()
        .cloned()
        .unwrap_or_else(|| ctx.ctes.last().unwrap().0.clone());

    Ok(format!(
        "WITH\n{}\nSELECT * FROM {}",
        cte_clauses.join(",\n"),
        last_cte
    ))
}

pub fn compile_expression(
    expr: &ElmExpression,
    ctx: &mut CompilationContext,
) -> Result<String, String> {
    match expr {
        ElmExpression::Retrieve {
            data_type,
            codes,
            code_property,
            ..
        } => {
            let resource_type = extract_resource_type(data_type);

            if let Some(ref alias) = ctx.patient_context_alias {
                if resource_type == "Patient" {
                    return Ok(format!("(SELECT {}.*)", alias));
                }
            }

            let table_name = resource_type.to_lowercase();
            let qualified = format!(
                "\"{}\".\"{}\"",
                ctx.schema_name, table_name
            );

            let mut query = format!("SELECT * FROM {} WHERE NOT _is_deleted", qualified);

            if let Some(ref alias) = ctx.patient_context_alias {
                query.push_str(&format!(
                    " AND json_extract_string(_raw, '$.subject.reference') = CONCAT('Patient/', json_extract_string({}.\"_raw\", '$.id'))",
                    alias
                ));
            }

            if let Some(codes_expr) = codes {
                let code_sql = compile_expression(codes_expr, ctx)?;
                let prop = code_property.as_deref().unwrap_or("code");
                Ok(format!(
                    "{} AND json_extract_string(_raw, '$.{}.coding[0].code') IN ({})",
                    query, prop, code_sql
                ))
            } else {
                Ok(query)
            }
        }

        ElmExpression::Property { source, path, scope, .. } => {
            if let Some(scope_name) = scope {
                Ok(format!(
                    "json_extract_string({}.\"_raw\", '$.{}')",
                    scope_name, path
                ))
            } else if let Some(src) = source {
                let src_sql = compile_expression(src, ctx)?;
                Ok(format!(
                    "json_extract_string(({})._raw, '$.{}')",
                    src_sql, path
                ))
            } else if let Some(ref alias) = ctx.patient_context_alias {
                Ok(format!("json_extract_string({}.\"_raw\", '$.{}')", alias, path))
            } else {
                Ok(format!("json_extract_string(_raw, '$.{}')", path))
            }
        }

        ElmExpression::FunctionRef { name, operand, .. } => {
            let args: Vec<String> = operand
                .iter()
                .map(|op| compile_expression(op, ctx))
                .collect::<Result<_, _>>()?;

            match name.as_str() {
                "ToInteger" | "ToInt" => Ok(format!("CAST({} AS INTEGER)", args.first().unwrap_or(&"NULL".to_string()))),
                "ToDecimal" => Ok(format!("CAST({} AS DOUBLE)", args.first().unwrap_or(&"NULL".to_string()))),
                "ToString" => Ok(format!("CAST({} AS VARCHAR)", args.first().unwrap_or(&"NULL".to_string()))),
                "AgeInYears" => {
                    let raw_ref = if let Some(ref alias) = ctx.patient_context_alias {
                        format!("{}.\"_raw\"", alias)
                    } else {
                        "_raw".to_string()
                    };
                    Ok(format!("DATE_DIFF('year', CAST(json_extract_string({}, '$.birthDate') AS DATE), CURRENT_DATE)", raw_ref))
                }
                "AgeInMonths" => {
                    let raw_ref = if let Some(ref alias) = ctx.patient_context_alias {
                        format!("{}.\"_raw\"", alias)
                    } else {
                        "_raw".to_string()
                    };
                    Ok(format!("DATE_DIFF('month', CAST(json_extract_string({}, '$.birthDate') AS DATE), CURRENT_DATE)", raw_ref))
                }
                _ => Ok(format!("{}({})", name, args.join(", "))),
            }
        }

        ElmExpression::Query {
            source,
            where_clause,
            return_clause,
            relationship,
            ..
        } => {
            if source.is_empty() {
                return Err("Query has no source".to_string());
            }

            let src = &source[0];
            let src_sql = compile_expression(&src.expression, ctx)?;
            let alias = &src.alias;

            ctx.aliases
                .insert(alias.clone(), ("unknown".to_string(), alias.clone()));

            let mut sql = format!("SELECT ");

            if let Some(ret) = return_clause {
                let ret_sql = compile_expression(&ret.expression, ctx)?;
                sql.push_str(&ret_sql);
            } else {
                sql.push_str(&format!("{}.*", alias));
            }

            sql.push_str(&format!(" FROM ({}) AS {}", src_sql, alias));

            let mut has_where = false;
            if let Some(rels) = relationship {
                for rel in rels {
                    match rel {
                        ElmRelationship::With {
                            alias: rel_alias,
                            expression: rel_expr,
                            such_that,
                        } => {
                            let rel_sql = compile_expression(rel_expr, ctx)?;
                            sql.push_str(&format!(
                                " INNER JOIN ({}) AS {} ON ",
                                rel_sql, rel_alias
                            ));
                            if let Some(st) = such_that {
                                let st_sql = compile_expression(st, ctx)?;
                                sql.push_str(&st_sql);
                            } else {
                                sql.push_str("TRUE");
                            }
                        }
                        ElmRelationship::Without {
                            alias: rel_alias,
                            expression: rel_expr,
                            such_that,
                        } => {
                            let rel_sql = compile_expression(rel_expr, ctx)?;
                            sql.push_str(&format!(
                                " LEFT JOIN ({}) AS {} ON ",
                                rel_sql, rel_alias
                            ));
                            if let Some(st) = such_that {
                                let st_sql = compile_expression(st, ctx)?;
                                sql.push_str(&st_sql);
                            } else {
                                sql.push_str("TRUE");
                            }
                            sql.push_str(&format!(" WHERE {}.\"_id\" IS NULL", rel_alias));
                            has_where = true;
                        }
                    }
                }
            }

            if let Some(wh) = where_clause {
                let where_sql = compile_expression(wh, ctx)?;
                if has_where {
                    sql.push_str(&format!(" AND ({})", where_sql));
                } else {
                    sql.push_str(&format!(" WHERE {}", where_sql));
                }
            }

            Ok(sql)
        }

        ElmExpression::AliasRef { name } => Ok(name.clone()),

        ElmExpression::Literal { value_type, value } => {
            let val = value.as_deref().unwrap_or("NULL");
            if val == "NULL" {
                return Ok("NULL".to_string());
            }

            let type_str = value_type.as_deref().unwrap_or("");
            if type_str.contains("Boolean") {
                Ok(val.to_string())
            } else if type_str.contains("Integer") || type_str.contains("Long") {
                Ok(val.to_string())
            } else if type_str.contains("Decimal") {
                Ok(val.to_string())
            } else if type_str.contains("Date") || type_str.contains("DateTime") {
                Ok(format!("'{}'", val.replace('\'', "''")))
            } else {
                Ok(format!("'{}'", val.replace('\'', "''")))
            }
        }

        ElmExpression::Null { .. } => Ok("NULL".to_string()),

        ElmExpression::ListExpr { element } => {
            let items: Vec<String> = element
                .iter()
                .map(|e| compile_expression(e, ctx))
                .collect::<Result<_, _>>()?;
            Ok(format!("[{}]", items.join(", ")))
        }

        ElmExpression::Tuple { element } => {
            let fields: Vec<String> = element
                .iter()
                .map(|e| {
                    let val = compile_expression(&e.value, ctx)?;
                    Ok(format!("'{}': {}", e.name, val))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(format!("{{{}}}", fields.join(", ")))
        }

        ElmExpression::Equal { operand } => {
            binary_op(&operand[0], &operand[1], "=", ctx)
        }
        ElmExpression::NotEqual { operand } => {
            binary_op(&operand[0], &operand[1], "!=", ctx)
        }
        ElmExpression::Less { operand } => {
            binary_op(&operand[0], &operand[1], "<", ctx)
        }
        ElmExpression::LessOrEqual { operand } => {
            binary_op(&operand[0], &operand[1], "<=", ctx)
        }
        ElmExpression::Greater { operand } => {
            binary_op(&operand[0], &operand[1], ">", ctx)
        }
        ElmExpression::GreaterOrEqual { operand } => {
            binary_op(&operand[0], &operand[1], ">=", ctx)
        }
        ElmExpression::Equivalent { operand } => {
            let left = compile_expression(&operand[0], ctx)?;
            let right = compile_expression(&operand[1], ctx)?;
            Ok(format!(
                "(({l} = {r}) OR ({l} IS NULL AND {r} IS NULL))",
                l = left,
                r = right
            ))
        }

        ElmExpression::And { operand } => {
            binary_op(&operand[0], &operand[1], "AND", ctx)
        }
        ElmExpression::Or { operand } => {
            binary_op(&operand[0], &operand[1], "OR", ctx)
        }
        ElmExpression::Not { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("NOT ({})", inner))
        }
        ElmExpression::IsTrue { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("({} IS TRUE)", inner))
        }
        ElmExpression::IsFalse { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("({} IS FALSE)", inner))
        }

        ElmExpression::Add { operand } => {
            binary_op(&operand[0], &operand[1], "+", ctx)
        }
        ElmExpression::Subtract { operand } => {
            binary_op(&operand[0], &operand[1], "-", ctx)
        }
        ElmExpression::Multiply { operand } => {
            binary_op(&operand[0], &operand[1], "*", ctx)
        }
        ElmExpression::Divide { operand } => {
            binary_op(&operand[0], &operand[1], "/", ctx)
        }
        ElmExpression::Modulo { operand } => {
            binary_op(&operand[0], &operand[1], "%", ctx)
        }
        ElmExpression::Negate { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("-({})", inner))
        }

        ElmExpression::Concatenate { operand } => {
            let parts: Vec<String> = operand
                .iter()
                .map(|op| compile_expression(op, ctx))
                .collect::<Result<_, _>>()?;
            Ok(parts.join(" || "))
        }

        ElmExpression::As { operand, as_type, .. } => {
            let inner = compile_expression(operand, ctx)?;
            if let Some(t) = as_type {
                let sql_type = elm_type_to_sql(t);
                Ok(format!("CAST({} AS {})", inner, sql_type))
            } else {
                Ok(inner)
            }
        }
        ElmExpression::Is { operand, is_type, .. } => {
            let inner = compile_expression(operand, ctx)?;
            if let Some(t) = is_type {
                let resource_type = extract_resource_type(t);
                Ok(format!(
                    "(json_extract_string(({})._raw, '$.resourceType') = '{}')",
                    inner, resource_type
                ))
            } else {
                Ok(format!("({} IS NOT NULL)", inner))
            }
        }
        ElmExpression::ToBoolean { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("CAST({} AS BOOLEAN)", inner))
        }
        ElmExpression::ToInteger { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("CAST({} AS INTEGER)", inner))
        }
        ElmExpression::ToDecimal { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("CAST({} AS DOUBLE)", inner))
        }
        ElmExpression::ToString { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("CAST({} AS VARCHAR)", inner))
        }
        ElmExpression::ToDateTime { operand } | ElmExpression::ToDate { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("CAST({} AS TIMESTAMP)", inner))
        }

        ElmExpression::Exists { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("EXISTS ({})", inner))
        }
        ElmExpression::IsNull { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("({} IS NULL)", inner))
        }
        ElmExpression::Coalesce { operand } => {
            let parts: Vec<String> = operand
                .iter()
                .map(|op| compile_expression(op, ctx))
                .collect::<Result<_, _>>()?;
            Ok(format!("COALESCE({})", parts.join(", ")))
        }
        ElmExpression::IfExpr {
            condition,
            then,
            else_clause,
        } => {
            let cond = compile_expression(condition, ctx)?;
            let then_sql = compile_expression(then, ctx)?;
            let else_sql = compile_expression(else_clause, ctx)?;
            Ok(format!("CASE WHEN {} THEN {} ELSE {} END", cond, then_sql, else_sql))
        }
        ElmExpression::Case {
            comparand,
            case_items,
            else_clause,
        } => {
            let mut sql = String::from("CASE");
            if let Some(comp) = comparand {
                let comp_sql = compile_expression(comp, ctx)?;
                sql.push_str(&format!(" {}", comp_sql));
            }
            for item in case_items {
                let when_sql = compile_expression(&item.when, ctx)?;
                let then_sql = compile_expression(&item.then, ctx)?;
                sql.push_str(&format!(" WHEN {} THEN {}", when_sql, then_sql));
            }
            let else_sql = compile_expression(else_clause, ctx)?;
            sql.push_str(&format!(" ELSE {} END", else_sql));
            Ok(sql)
        }

        ElmExpression::ExpressionRef { name, .. } => {
            if let Some(expr_sql) = ctx.expression_ctes.get(name) {
                if ctx.patient_context_alias.is_some() {
                    Ok(expr_sql.clone())
                } else {
                    Ok(format!("SELECT * FROM {}", expr_sql))
                }
            } else {
                Ok(format!("\"{}\"", name))
            }
        }
        ElmExpression::ParameterRef { name, .. } => {
            Ok(format!("@{}", name))
        }
        ElmExpression::ValueSetRef { name, .. } => {
            if let Some(url) = ctx.valueset_urls.get(name) {
                Ok(format!("'{}'", url.replace('\'', "''")))
            } else {
                Ok(format!("'{}'", name.replace('\'', "''")))
            }
        }
        ElmExpression::CodeRef { name, .. } => {
            if let Some((_system, code)) = ctx.code_defs.get(name) {
                Ok(format!("'{}'", code.replace('\'', "''")))
            } else {
                Ok(format!("'{}'", name.replace('\'', "''")))
            }
        }
        ElmExpression::CodeSystemRef { name, .. } => {
            Ok(format!("'{}'", name.replace('\'', "''")))
        }

        ElmExpression::InValueSet { code, valueset } => {
            let code_sql = compile_expression(code, ctx)?;
            let vs_sql = compile_expression(valueset, ctx)?;
            Ok(format!(
                "EXISTS (SELECT 1 FROM \"{}\".\"_valueset_expansion\" WHERE valueset_url = {} AND code = {})",
                ctx.schema_name, vs_sql, code_sql
            ))
        }
        ElmExpression::In { operand } => {
            let left = compile_expression(&operand[0], ctx)?;
            let right = compile_expression(&operand[1], ctx)?;
            Ok(format!("{} IN ({})", left, right))
        }
        ElmExpression::Contains { operand } => {
            let left = compile_expression(&operand[0], ctx)?;
            let right = compile_expression(&operand[1], ctx)?;
            Ok(format!("list_contains({}, {})", left, right))
        }

        ElmExpression::Count { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!("(SELECT COUNT(*)::VARCHAR FROM ({}))", inner))
        }
        ElmExpression::Sum { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!("(SELECT SUM(*) FROM ({}))", inner))
        }
        ElmExpression::Min { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!("(SELECT MIN(*) FROM ({}))", inner))
        }
        ElmExpression::Max { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!("(SELECT MAX(*) FROM ({}))", inner))
        }
        ElmExpression::Avg { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!("(SELECT AVG(*) FROM ({}))", inner))
        }
        ElmExpression::First { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!("(SELECT * FROM ({}) LIMIT 1)", inner))
        }
        ElmExpression::Last { source } => {
            let inner = compile_expression(source, ctx)?;
            Ok(format!(
                "(SELECT * FROM ({}) ORDER BY rowid DESC LIMIT 1)",
                inner
            ))
        }
        ElmExpression::Distinct { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("list_distinct({})", inner))
        }
        ElmExpression::Flatten { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("flatten({})", inner))
        }
        ElmExpression::SingletonFrom { operand } => {
            let inner = compile_expression(operand, ctx)?;
            Ok(format!("(SELECT * FROM ({}) LIMIT 1)", inner))
        }

        ElmExpression::Interval { low, high, .. } => {
            let low_sql = low
                .as_ref()
                .map(|l| compile_expression(l, ctx))
                .transpose()?
                .unwrap_or_else(|| "NULL".to_string());
            let high_sql = high
                .as_ref()
                .map(|h| compile_expression(h, ctx))
                .transpose()?
                .unwrap_or_else(|| "NULL".to_string());
            Ok(format!(
                "STRUCT_PACK(low := {}, high := {})",
                low_sql, high_sql
            ))
        }

        ElmExpression::Now {} => Ok("CURRENT_TIMESTAMP".to_string()),
        ElmExpression::Today {} => Ok("CURRENT_DATE".to_string()),
        ElmExpression::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } => {
            let y = compile_expression(year, ctx)?;
            let m = month
                .as_ref()
                .map(|e| compile_expression(e, ctx))
                .transpose()?
                .unwrap_or_else(|| "1".to_string());
            let d = day
                .as_ref()
                .map(|e| compile_expression(e, ctx))
                .transpose()?
                .unwrap_or_else(|| "1".to_string());
            let h = hour
                .as_ref()
                .map(|e| compile_expression(e, ctx))
                .transpose()?
                .unwrap_or_else(|| "0".to_string());
            let mi = minute
                .as_ref()
                .map(|e| compile_expression(e, ctx))
                .transpose()?
                .unwrap_or_else(|| "0".to_string());
            let s = second
                .as_ref()
                .map(|e| compile_expression(e, ctx))
                .transpose()?
                .unwrap_or_else(|| "0".to_string());
            Ok(format!(
                "MAKE_TIMESTAMP({}, {}, {}, {}, {}, {})",
                y, m, d, h, mi, s
            ))
        }

        ElmExpression::Unsupported => {
            Err("Unsupported ELM expression type".to_string())
        }
    }
}

fn binary_op(
    left: &ElmExpression,
    right: &ElmExpression,
    op: &str,
    ctx: &mut CompilationContext,
) -> Result<String, String> {
    let l = compile_expression(left, ctx)?;
    let r = compile_expression(right, ctx)?;
    Ok(format!("({} {} {})", l, op, r))
}

fn extract_resource_type(data_type: &str) -> &str {
    data_type
        .rsplit('}')
        .next()
        .unwrap_or(data_type)
}

/// Compile a measure population expression into a patient-count SQL query.
pub fn compile_measure_population(
    library: &ElmLibrary,
    schema_name: &str,
    expression_name: &str,
) -> Result<String, String> {
    let mut ctx = CompilationContext::new(schema_name);
    ctx.patient_context_alias = Some("p".to_string());

    if let Some(vs_defs) = &library.valueSets {
        for vs in &vs_defs.defs {
            ctx.valueset_urls.insert(vs.name.clone(), vs.id.clone());
        }
    }

    if let Some(code_defs) = &library.codes {
        for code in &code_defs.defs {
            let system_name = code
                .code_system
                .as_ref()
                .map(|cs| cs.name.clone())
                .unwrap_or_default();
            ctx.code_defs
                .insert(code.name.clone(), (system_name, code.id.clone()));
        }
    }

    let statements = library
        .statements
        .as_ref()
        .ok_or("Library has no statements")?;

    let mut target_sql = None;

    for def in &statements.defs {
        if def.name == "Patient" {
            continue;
        }

        let sql = compile_expression(&def.expression, &mut ctx)?;

        if def.name == expression_name {
            target_sql = Some(sql);
            break;
        }

        ctx.expression_ctes.insert(def.name.clone(), format!("({})", sql));
    }

    let boolean_expr = target_sql.ok_or_else(|| {
        format!("Expression '{}' not found in library", expression_name)
    })?;

    Ok(format!(
        "SELECT COUNT(*)::VARCHAR AS count FROM \"{}\".\"patient\" p WHERE NOT p._is_deleted AND ({})",
        schema_name, boolean_expr
    ))
}

fn elm_type_to_sql(elm_type: &str) -> &str {
    let type_name = extract_resource_type(elm_type);
    match type_name {
        "Boolean" => "BOOLEAN",
        "Integer" => "INTEGER",
        "Long" => "BIGINT",
        "Decimal" => "DOUBLE",
        "String" => "VARCHAR",
        "Date" => "DATE",
        "DateTime" => "TIMESTAMP",
        "Time" => "TIME",
        _ => "VARCHAR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_resource_type() {
        assert_eq!(
            extract_resource_type("{http://hl7.org/fhir}Condition"),
            "Condition"
        );
        assert_eq!(extract_resource_type("Patient"), "Patient");
    }

    #[test]
    fn test_compile_literal() {
        let mut ctx = CompilationContext::new("test_schema");
        let expr = ElmExpression::Literal {
            value_type: Some("{urn:hl7-org:elm-types:r1}String".to_string()),
            value: Some("hello".to_string()),
        };
        let result = compile_expression(&expr, &mut ctx).unwrap();
        assert_eq!(result, "'hello'");
    }

    #[test]
    fn test_compile_retrieve() {
        let mut ctx = CompilationContext::new("myds");
        let expr = ElmExpression::Retrieve {
            data_type: "{http://hl7.org/fhir}Patient".to_string(),
            template_id: None,
            code_property: None,
            codes: None,
            date_property: None,
            date_range: None,
        };
        let result = compile_expression(&expr, &mut ctx).unwrap();
        assert!(result.contains("\"myds\".\"patient\""));
        assert!(result.contains("NOT _is_deleted"));
    }

    #[test]
    fn test_compile_comparison() {
        let mut ctx = CompilationContext::new("test");
        let expr = ElmExpression::GreaterOrEqual {
            operand: [
                Box::new(ElmExpression::Property {
                    source: None,
                    path: "birthDate".to_string(),
                    scope: None,
                    result_type_name: None,
                }),
                Box::new(ElmExpression::Literal {
                    value_type: Some("{urn:hl7-org:elm-types:r1}Date".to_string()),
                    value: Some("2000-01-01".to_string()),
                }),
            ],
        };
        let result = compile_expression(&expr, &mut ctx).unwrap();
        assert!(result.contains(">="));
        assert!(result.contains("2000-01-01"));
    }
}
