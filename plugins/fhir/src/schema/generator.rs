use crate::fhir::structure_definition::{DefinitionRegistry, ElementInfo};
use crate::schema::type_mapping::{fhir_to_duckdb_type, is_primitive_type};

const MAX_RECURSION_DEPTH: usize = 4;

pub fn generate_ddl(
    registry: &DefinitionRegistry,
    resource_type: &str,
    schema_name: &str,
) -> Result<String, String> {
    let sd = registry
        .get_resource(resource_type)
        .ok_or_else(|| format!("Unknown resource type: {}", resource_type))?;

    let table_name = resource_type.to_lowercase();
    let mut columns = Vec::new();

    columns.push("    _id VARCHAR NOT NULL".to_string());
    columns.push("    _version_id INTEGER NOT NULL DEFAULT 1".to_string());
    columns.push("    _last_updated TIMESTAMP NOT NULL DEFAULT now()".to_string());
    columns.push("    _is_deleted BOOLEAN NOT NULL DEFAULT false".to_string());
    columns.push("    _raw JSON NOT NULL".to_string());

    for element in &sd.elements {
        if let Some(col_def) = element_to_column(registry, element, 0) {
            columns.push(format!("    {}", col_def));
        }
    }

    columns.push("    PRIMARY KEY (_id)".to_string());

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\"\n(\n{}\n)",
        schema_name,
        table_name,
        columns.join(",\n")
    ))
}

fn element_to_column(
    registry: &DefinitionRegistry,
    element: &ElementInfo,
    depth: usize,
) -> Option<String> {
    // Skip resourceType field (already known from table name)
    if element.name == "resourceType" {
        return None;
    }

    let col_name = quote_column_name(&element.name);
    let type_str = element_to_type(registry, element, depth)?;

    Some(format!("{} {}", col_name, type_str))
}

fn element_to_type(
    registry: &DefinitionRegistry,
    element: &ElementInfo,
    depth: usize,
) -> Option<String> {
    if element.content_reference.is_some() {
        if depth >= MAX_RECURSION_DEPTH {
            // Prevent infinite recursion in self-referential types.
            let base_type = "VARCHAR";
            return Some(if element.is_array {
                format!("{}[]", base_type)
            } else {
                base_type.to_string()
            });
        }
    }

    if element.is_choice && element.type_codes.len() > 1 {
        let variants: Vec<String> = element
            .type_codes
            .iter()
            .map(|tc| {
                let variant_name = format!(
                    "{}{}",
                    element.name.trim_end_matches("[x]"),
                    capitalize(tc)
                );
                let variant_type = resolve_type(registry, tc, depth);
                format!("{} {}", quote_column_name(&variant_name), variant_type)
            })
            .collect();

        let union_type = format!("UNION({})", variants.join(", "));
        return Some(if element.is_array {
            format!("{}[]", union_type)
        } else {
            union_type
        });
    }

    if element.type_codes.is_empty() {
        if !element.children.is_empty() {
            let struct_type = children_to_struct(registry, &element.children, depth);
            return Some(if element.is_array {
                format!("{}[]", struct_type)
            } else {
                struct_type
            });
        }
        return None;
    }

    let type_code = &element.type_codes[0];

    if !element.children.is_empty() {
        let struct_type = children_to_struct(registry, &element.children, depth);
        return Some(if element.is_array {
            format!("{}[]", struct_type)
        } else {
            struct_type
        });
    }

    let resolved = resolve_type(registry, type_code, depth);
    Some(if element.is_array {
        format!("{}[]", resolved)
    } else {
        resolved
    })
}

fn resolve_type(registry: &DefinitionRegistry, type_code: &str, depth: usize) -> String {
    if is_primitive_type(type_code) {
        return fhir_to_duckdb_type(type_code).to_string();
    }

    if let Some(type_def) = registry.get_type(type_code) {
        if type_def.elements.is_empty() {
            return "VARCHAR".to_string();
        }
        return children_to_struct(registry, &type_def.elements, depth + 1);
    }

    match type_code {
        "BackboneElement" | "Element" => "VARCHAR".to_string(),
        "Resource" => "JSON".to_string(),
        "Extension" => "VARCHAR".to_string(),
        "Narrative" => {
            "STRUCT(status VARCHAR, div VARCHAR)".to_string()
        }
        "Reference" => {
            "STRUCT(reference VARCHAR, type VARCHAR, display VARCHAR)".to_string()
        }
        "Meta" => {
            "STRUCT(versionId VARCHAR, lastUpdated VARCHAR, source VARCHAR, profile VARCHAR[], security STRUCT(system VARCHAR, code VARCHAR, display VARCHAR)[], tag STRUCT(system VARCHAR, code VARCHAR, display VARCHAR)[])".to_string()
        }
        _ => "VARCHAR".to_string(),
    }
}

fn children_to_struct(
    registry: &DefinitionRegistry,
    children: &[ElementInfo],
    depth: usize,
) -> String {
    if depth >= MAX_RECURSION_DEPTH {
        return "VARCHAR".to_string(); // JSON fallback
    }

    let fields: Vec<String> = children
        .iter()
        .filter_map(|child| {
            let type_str = element_to_type(registry, child, depth + 1)?;
            Some(format!("{} {}", quote_column_name(&child.name), type_str))
        })
        .collect();

    if fields.is_empty() {
        return "VARCHAR".to_string();
    }

    format!("STRUCT({})", fields.join(", "))
}

fn quote_column_name(name: &str) -> String {
    // Strip FHIR choice type suffix [x] — not valid in SQL identifiers
    let clean = name.trim_end_matches("[x]");
    // Always quote to avoid SQL reserved word conflicts (when, for, end, etc.)
    format!("\"{}\"", clean)
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub fn generate_all_ddl(
    registry: &DefinitionRegistry,
    schema_name: &str,
) -> Vec<(String, Result<String, String>)> {
    registry
        .resource_type_names()
        .into_iter()
        .map(|name| {
            let ddl = generate_ddl(registry, &name, schema_name);
            (name, ddl)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fhir_server::load_default_definitions;

    #[test]
    fn test_patient_ddl_no_bracket_x() {
        let defs = load_default_definitions().expect("load defs");
        let ddl = generate_ddl(&defs, "Patient", "test_schema").expect("generate ddl");
        // Check that [x] never appears in the DDL
        assert!(
            !ddl.contains("[x]"),
            "DDL contains [x]:\n{}",
            &ddl[..ddl.len().min(2000)]
        );
    }
}
