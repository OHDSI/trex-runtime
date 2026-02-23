use serde_json::Value;
use std::collections::HashMap;

use crate::fhir::resource_registry::ResourceRegistry;

#[derive(Debug, Clone)]
pub struct SearchParamDef {
    pub name: String,
    pub param_type: SearchParamType,
    pub expression: String,
    pub base: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SearchParamType {
    String,
    Token,
    Reference,
    Date,
    Quantity,
    Number,
    Uri,
    Composite,
    Special,
}

pub struct SearchParamRegistry {
    params: HashMap<(String, String), SearchParamDef>,
}

impl SearchParamRegistry {
    pub fn load_from_json(json_str: &str) -> Result<Self, String> {
        let bundle: Value =
            serde_json::from_str(json_str).map_err(|e| format!("Invalid JSON: {e}"))?;

        let entries = bundle
            .get("entry")
            .and_then(|v| v.as_array())
            .ok_or("Bundle missing 'entry' array")?;

        let mut params = HashMap::new();

        for entry in entries {
            let resource = match entry.get("resource") {
                Some(r) => r,
                None => continue,
            };

            let rt = resource
                .get("resourceType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if rt != "SearchParameter" {
                continue;
            }

            let name = match resource.get("code").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            if name.starts_with('_') {
                continue;
            }

            let param_type = match resource.get("type").and_then(|v| v.as_str()) {
                Some("string") => SearchParamType::String,
                Some("token") => SearchParamType::Token,
                Some("reference") => SearchParamType::Reference,
                Some("date") => SearchParamType::Date,
                Some("quantity") => SearchParamType::Quantity,
                Some("number") => SearchParamType::Number,
                Some("uri") => SearchParamType::Uri,
                Some("composite") => SearchParamType::Composite,
                Some("special") => SearchParamType::Special,
                _ => continue,
            };

            let expression = resource
                .get("expression")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if expression.is_empty() {
                continue;
            }

            let base: Vec<String> = resource
                .get("base")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let def = SearchParamDef {
                name: name.clone(),
                param_type,
                expression,
                base: base.clone(),
            };

            for resource_type in &base {
                params.insert((resource_type.clone(), name.clone()), def.clone());
            }
        }

        Ok(Self { params })
    }

    pub fn get(&self, resource_type: &str, param_name: &str) -> Option<&SearchParamDef> {
        self.params.get(&(resource_type.to_string(), param_name.to_string()))
    }

    pub fn params_for_type(&self, resource_type: &str) -> Vec<&SearchParamDef> {
        self.params
            .iter()
            .filter(|((rt, _), _)| rt == resource_type)
            .map(|(_, def)| def)
            .collect()
    }
}

pub fn generate_search_sql(
    registry: &SearchParamRegistry,
    resource_registry: &ResourceRegistry,
    resource_type: &str,
    params: &HashMap<String, String>,
) -> Result<String, String> {
    let mut conditions = Vec::new();

    for (param_name, param_value) in params {
        if param_name.starts_with('_') {
            continue;
        }

        let (base_name, modifier) = if let Some(pos) = param_name.find(':') {
            (&param_name[..pos], Some(&param_name[pos + 1..]))
        } else {
            (param_name.as_str(), None)
        };

        let param_def = registry
            .get(resource_type, base_name)
            .ok_or_else(|| format!("Unknown search parameter '{}' for {}", base_name, resource_type))?;

        let json_path = fhirpath_to_json_path(&param_def.expression, resource_type);

        let path_str = json_path.strip_prefix("$.").unwrap_or("");
        let segments: Vec<&str> = if path_str.is_empty() {
            Vec::new()
        } else {
            path_str.split('.').collect()
        };
        let array_indices = find_array_segments(resource_registry, resource_type, &segments);

        let condition = if !array_indices.is_empty() {
            build_array_condition(
                &segments,
                &array_indices,
                param_def.param_type,
                param_value,
                modifier,
            )?
        } else {
            match param_def.param_type {
                SearchParamType::String => {
                    generate_string_condition(&json_path, param_value, modifier)?
                }
                SearchParamType::Token => generate_token_condition(&json_path, param_value)?,
                SearchParamType::Reference => {
                    generate_reference_condition(&json_path, param_value)?
                }
                SearchParamType::Date => generate_date_condition(&json_path, param_value)?,
                SearchParamType::Number => generate_number_condition(&json_path, param_value)?,
                SearchParamType::Quantity => {
                    generate_quantity_condition(&json_path, param_value)?
                }
                SearchParamType::Uri => generate_uri_condition(&json_path, param_value)?,
                SearchParamType::Composite | SearchParamType::Special => {
                    continue;
                }
            }
        };

        conditions.push(condition);
    }

    if conditions.is_empty() {
        return Ok(String::new());
    }

    Ok(conditions.join(" AND "))
}

fn fhirpath_to_json_path(expression: &str, resource_type: &str) -> String {
    let expr = expression.split('|').next().unwrap_or(expression).trim();

    let path = if let Some(rest) = expr.strip_prefix(resource_type) {
        rest.trim_start_matches('.')
    } else if let Some(rest) = expr.strip_prefix("Resource.") {
        rest
    } else {
        expr
    };

    let clean_path = path
        .split('.')
        .filter(|segment| {
            !segment.starts_with("where(")
                && !segment.starts_with("as(")
                && !segment.starts_with("ofType(")
                && !segment.starts_with("resolve(")
                && !segment.is_empty()
        })
        .collect::<Vec<_>>()
        .join(".");

    if clean_path.is_empty() {
        return "$".to_string();
    }

    format!("$.{}", clean_path)
}

/// Returns indices of `segments` that correspond to array elements.
fn find_array_segments(
    resource_registry: &ResourceRegistry,
    resource_type: &str,
    segments: &[&str],
) -> Vec<usize> {
    let definitions = match resource_registry.definitions() {
        Some(d) => d,
        None => return Vec::new(),
    };

    let resource_def = match definitions.get_resource(resource_type) {
        Some(d) => d,
        None => return Vec::new(),
    };

    let mut result = Vec::new();
    let mut current_elements = &resource_def.elements;

    for (i, segment) in segments.iter().enumerate() {
        let elem = match current_elements.iter().find(|e| e.name == *segment) {
            Some(e) => e,
            None => break,
        };

        if elem.is_array {
            result.push(i);
        }

        if !elem.children.is_empty() {
            current_elements = &elem.children;
        } else if let Some(type_name) = elem.type_codes.first() {
            if let Some(type_def) = definitions.get_type(type_name) {
                current_elements = &type_def.elements;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    result
}

/// Build nested `EXISTS(... json_each(...))` for each array segment in the path.
fn build_array_condition(
    segments: &[&str],
    array_indices: &[usize],
    param_type: SearchParamType,
    value: &str,
    modifier: Option<&str>,
) -> Result<String, String> {
    let last_array_idx = *array_indices.last().unwrap();
    let inner_segments = &segments[last_array_idx + 1..];
    let inner_json_path = if inner_segments.is_empty() {
        "$".to_string()
    } else {
        format!("$.{}", inner_segments.join("."))
    };

    let inner_condition = match param_type {
        SearchParamType::String => generate_string_condition(&inner_json_path, value, modifier)?,
        SearchParamType::Token => generate_token_condition(&inner_json_path, value)?,
        SearchParamType::Reference => generate_reference_condition(&inner_json_path, value)?,
        SearchParamType::Date => generate_date_condition(&inner_json_path, value)?,
        SearchParamType::Number => generate_number_condition(&inner_json_path, value)?,
        SearchParamType::Quantity => generate_quantity_condition(&inner_json_path, value)?,
        SearchParamType::Uri => generate_uri_condition(&inner_json_path, value)?,
        _ => return Err("Unsupported search parameter type for array search".to_string()),
    };

    let innermost_depth = array_indices.len() - 1;
    let inner_condition =
        inner_condition.replace("_raw", &format!("_arr{}.value", innermost_depth));

    let mut result = inner_condition;
    for (depth, &arr_idx) in array_indices.iter().enumerate().rev() {
        let alias = format!("_arr{}", depth);

        let path_start = if depth == 0 {
            0
        } else {
            array_indices[depth - 1] + 1
        };
        let path_segments = &segments[path_start..=arr_idx];
        let json_path = format!("$.{}", path_segments.join("."));

        let base = if depth == 0 {
            "_raw".to_string()
        } else {
            format!("_arr{}.value", depth - 1)
        };

        result = format!(
            "EXISTS (SELECT 1 FROM json_each(json_extract({}, '{}')) AS {} WHERE {})",
            base, json_path, alias, result
        );
    }

    Ok(result)
}

fn generate_string_condition(
    json_path: &str,
    value: &str,
    modifier: Option<&str>,
) -> Result<String, String> {
    let escaped_value = value.replace('\'', "''");

    match modifier {
        Some("exact") => Ok(format!(
            "json_extract_string(_raw, '{}') = '{}'",
            json_path, escaped_value
        )),
        Some("contains") => Ok(format!(
            "LOWER(json_extract_string(_raw, '{}')) LIKE '%{}%'",
            json_path,
            escaped_value.to_lowercase()
        )),
        _ => {
            Ok(format!(
                "LOWER(json_extract_string(_raw, '{}')) LIKE '{}%'",
                json_path,
                escaped_value.to_lowercase()
            ))
        }
    }
}

fn generate_token_condition(json_path: &str, value: &str) -> Result<String, String> {
    let escaped_value = value.replace('\'', "''");

    if let Some(pos) = escaped_value.find('|') {
        let system = &escaped_value[..pos];
        let code = &escaped_value[pos + 1..];

        if system.is_empty() {
            Ok(format!(
                "json_extract_string(_raw, '{}.code') = '{}' OR \
                 EXISTS (SELECT 1 FROM json_each(json_extract(_raw, '{}.coding')) AS c WHERE json_extract_string(c.value, '$.code') = '{}')",
                json_path, code, json_path, code
            ))
        } else if code.is_empty() {
            Ok(format!(
                "json_extract_string(_raw, '{}.system') = '{}' OR \
                 EXISTS (SELECT 1 FROM json_each(json_extract(_raw, '{}.coding')) AS c WHERE json_extract_string(c.value, '$.system') = '{}')",
                json_path, system, json_path, system
            ))
        } else {
            Ok(format!(
                "(json_extract_string(_raw, '{path}.system') = '{sys}' AND json_extract_string(_raw, '{path}.code') = '{code}') OR \
                 EXISTS (SELECT 1 FROM json_each(json_extract(_raw, '{path}.coding')) AS c WHERE json_extract_string(c.value, '$.system') = '{sys}' AND json_extract_string(c.value, '$.code') = '{code}')",
                path = json_path, sys = system, code = code
            ))
        }
    } else {
        Ok(format!(
            "json_extract_string(_raw, '{}.code') = '{}' OR \
             json_extract_string(_raw, '{}') = '{}' OR \
             EXISTS (SELECT 1 FROM json_each(json_extract(_raw, '{}.coding')) AS c WHERE json_extract_string(c.value, '$.code') = '{}')",
            json_path, escaped_value, json_path, escaped_value, json_path, escaped_value
        ))
    }
}

fn generate_reference_condition(json_path: &str, value: &str) -> Result<String, String> {
    let escaped_value = value.replace('\'', "''");

    Ok(format!(
        "json_extract_string(_raw, '{}.reference') = '{}' OR \
         json_extract_string(_raw, '{}.reference') LIKE '%/{}'",
        json_path, escaped_value, json_path, escaped_value
    ))
}

fn generate_date_condition(json_path: &str, value: &str) -> Result<String, String> {
    let (prefix, date_value) = parse_prefix(value);
    let escaped_date = date_value.replace('\'', "''");

    let field = format!("json_extract_string(_raw, '{}')", json_path);

    match prefix {
        "eq" | "" => Ok(format!("{} = '{}'", field, escaped_date)),
        "ne" => Ok(format!("{} != '{}'", field, escaped_date)),
        "lt" | "eb" => Ok(format!("{} < '{}'", field, escaped_date)),
        "gt" | "sa" => Ok(format!("{} > '{}'", field, escaped_date)),
        "ge" => Ok(format!("{} >= '{}'", field, escaped_date)),
        "le" => Ok(format!("{} <= '{}'", field, escaped_date)),
        _ => Err(format!("Unknown date prefix: {}", prefix)),
    }
}

fn generate_number_condition(json_path: &str, value: &str) -> Result<String, String> {
    let (prefix, num_value) = parse_prefix(value);

    let parsed: f64 = num_value
        .parse()
        .map_err(|_| format!("Invalid numeric value: {}", num_value))?;

    let field = format!("CAST(json_extract_string(_raw, '{}') AS DOUBLE)", json_path);

    match prefix {
        "eq" | "" => Ok(format!("{} = {}", field, parsed)),
        "ne" => Ok(format!("{} != {}", field, parsed)),
        "lt" => Ok(format!("{} < {}", field, parsed)),
        "gt" => Ok(format!("{} > {}", field, parsed)),
        "ge" => Ok(format!("{} >= {}", field, parsed)),
        "le" => Ok(format!("{} <= {}", field, parsed)),
        _ => Err(format!("Unknown number prefix: {}", prefix)),
    }
}

fn generate_quantity_condition(json_path: &str, value: &str) -> Result<String, String> {
    let (prefix, rest) = parse_prefix(value);
    let parts: Vec<&str> = rest.splitn(3, '|').collect();

    let parsed: f64 = parts[0]
        .parse()
        .map_err(|_| format!("Invalid numeric quantity value: {}", parts[0]))?;
    let field = format!(
        "CAST(json_extract_string(_raw, '{}.value') AS DOUBLE)",
        json_path
    );

    let num_condition = match prefix {
        "eq" | "" => format!("{} = {}", field, parsed),
        "ne" => format!("{} != {}", field, parsed),
        "lt" => format!("{} < {}", field, parsed),
        "gt" => format!("{} > {}", field, parsed),
        "ge" => format!("{} >= {}", field, parsed),
        "le" => format!("{} <= {}", field, parsed),
        _ => return Err(format!("Unknown quantity prefix: {}", prefix)),
    };

    if parts.len() >= 3 {
        let system = parts[1].replace('\'', "''");
        let code = parts[2].replace('\'', "''");
        Ok(format!(
            "({} AND json_extract_string(_raw, '{}.system') = '{}' AND json_extract_string(_raw, '{}.code') = '{}')",
            num_condition, json_path, system, json_path, code
        ))
    } else if parts.len() == 2 {
        let code = parts[1].replace('\'', "''");
        Ok(format!(
            "({} AND (json_extract_string(_raw, '{}.code') = '{}' OR json_extract_string(_raw, '{}.unit') = '{}'))",
            num_condition, json_path, code, json_path, code
        ))
    } else {
        Ok(num_condition)
    }
}

fn generate_uri_condition(json_path: &str, value: &str) -> Result<String, String> {
    let escaped_value = value.replace('\'', "''");
    Ok(format!(
        "json_extract_string(_raw, '{}') = '{}'",
        json_path, escaped_value
    ))
}

fn parse_prefix(value: &str) -> (&str, &str) {
    let prefixes = ["eq", "ne", "lt", "gt", "ge", "le", "sa", "eb", "ap"];
    for prefix in &prefixes {
        if value.starts_with(prefix) {
            let rest = &value[prefix.len()..];
            if rest.starts_with(|c: char| c.is_ascii_digit() || c == '-' || c == '+') {
                return (prefix, rest);
            }
        }
    }
    ("", value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prefix() {
        assert_eq!(parse_prefix("2020-01-01"), ("", "2020-01-01"));
        assert_eq!(parse_prefix("ge2020-01-01"), ("ge", "2020-01-01"));
        assert_eq!(parse_prefix("lt100"), ("lt", "100"));
        assert_eq!(parse_prefix("exact"), ("", "exact")); // Not a prefix since 'a' follows
    }

    #[test]
    fn test_fhirpath_to_json_path() {
        assert_eq!(
            fhirpath_to_json_path("Patient.name.family", "Patient"),
            "$.name.family"
        );
        assert_eq!(
            fhirpath_to_json_path("Patient.birthDate", "Patient"),
            "$.birthDate"
        );
        assert_eq!(
            fhirpath_to_json_path("Observation.value.as(Quantity)", "Observation"),
            "$.value"
        );
        assert_eq!(
            fhirpath_to_json_path("Patient.name | Practitioner.name", "Patient"),
            "$.name"
        );
    }

    #[test]
    fn test_string_condition() {
        let cond = generate_string_condition("$.name.family", "Smith", None).unwrap();
        assert!(cond.contains("LIKE 'smith%'"));

        let cond = generate_string_condition("$.name.family", "Smith", Some("exact")).unwrap();
        assert!(cond.contains("= 'Smith'"));

        let cond = generate_string_condition("$.name.family", "mith", Some("contains")).unwrap();
        assert!(cond.contains("LIKE '%mith%'"));
    }

    #[test]
    fn test_token_condition_system_code() {
        let cond = generate_token_condition("$.identifier", "http://example.com|12345").unwrap();
        assert!(cond.contains("http://example.com"));
        assert!(cond.contains("12345"));
    }

    #[test]
    fn test_token_condition_code_only() {
        let cond = generate_token_condition("$.status", "active").unwrap();
        assert!(cond.contains("active"));
    }

    #[test]
    fn test_date_condition() {
        let cond = generate_date_condition("$.birthDate", "ge2000-01-01").unwrap();
        assert!(cond.contains(">= '2000-01-01'"));

        let cond = generate_date_condition("$.birthDate", "2020-06-15").unwrap();
        assert!(cond.contains("= '2020-06-15'"));
    }

    #[test]
    fn test_reference_condition() {
        let cond = generate_reference_condition("$.subject", "Patient/123").unwrap();
        assert!(cond.contains("Patient/123"));
    }

    fn test_resource_registry() -> ResourceRegistry {
        use crate::fhir::structure_definition::DefinitionRegistry;

        let resources_json = serde_json::json!({
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [{
                "resource": {
                    "resourceType": "StructureDefinition",
                    "name": "Patient",
                    "type": "Patient",
                    "kind": "resource",
                    "abstract": false,
                    "derivation": "specialization",
                    "snapshot": {
                        "element": [
                            {"path": "Patient", "min": 0, "max": "*"},
                            {"path": "Patient.name", "min": 0, "max": "*", "type": [{"code": "HumanName"}]},
                            {"path": "Patient.birthDate", "min": 0, "max": "1", "type": [{"code": "date"}]},
                            {"path": "Patient.gender", "min": 0, "max": "1", "type": [{"code": "code"}]},
                            {"path": "Patient.identifier", "min": 0, "max": "*", "type": [{"code": "Identifier"}]},
                            {"path": "Patient.contact", "min": 0, "max": "*", "type": [{"code": "BackboneElement"}]},
                            {"path": "Patient.contact.name", "min": 0, "max": "1", "type": [{"code": "HumanName"}]}
                        ]
                    }
                }
            }]
        });

        let types_json = serde_json::json!({
            "resourceType": "Bundle",
            "type": "collection",
            "entry": [{
                "resource": {
                    "resourceType": "StructureDefinition",
                    "name": "HumanName",
                    "type": "HumanName",
                    "kind": "complex-type",
                    "abstract": false,
                    "derivation": "specialization",
                    "snapshot": {
                        "element": [
                            {"path": "HumanName", "min": 0, "max": "*"},
                            {"path": "HumanName.family", "min": 0, "max": "1", "type": [{"code": "string"}]},
                            {"path": "HumanName.given", "min": 0, "max": "*", "type": [{"code": "string"}]}
                        ]
                    }
                }
            }]
        });

        let definitions = DefinitionRegistry::load_from_json(
            &resources_json.to_string(),
            &types_json.to_string(),
        )
        .unwrap();
        ResourceRegistry::with_definitions(definitions)
    }

    #[test]
    fn test_find_array_segments_name_family() {
        let registry = test_resource_registry();
        let segments = vec!["name", "family"];
        let result = find_array_segments(&registry, "Patient", &segments);
        assert_eq!(result, vec![0]); // "name" is array (0..*)
    }

    #[test]
    fn test_find_array_segments_no_arrays() {
        let registry = test_resource_registry();
        let segments = vec!["birthDate"];
        let result = find_array_segments(&registry, "Patient", &segments);
        assert_eq!(result, Vec::<usize>::new());
    }

    #[test]
    fn test_build_array_condition_string() {
        let segments = vec!["name", "family"];
        let array_indices = vec![0];
        let result = build_array_condition(
            &segments,
            &array_indices,
            SearchParamType::String,
            "smith",
            None,
        )
        .unwrap();
        assert!(result.contains("json_each"));
        assert!(result.contains("_arr0.value"));
        assert!(result.contains("$.family"));
        assert!(result.contains("smith%"));
        assert!(result.starts_with("EXISTS"));
    }

    #[test]
    fn test_array_condition_fallback_no_definitions() {
        let registry = ResourceRegistry::new();
        let segments = vec!["name", "family"];
        let result = find_array_segments(&registry, "Patient", &segments);
        assert_eq!(result, Vec::<usize>::new());
    }
}
