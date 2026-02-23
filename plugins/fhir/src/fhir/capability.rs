use serde_json::{json, Value};

use crate::fhir::resource_registry::ResourceRegistry;
use crate::fhir::search_parameter::{SearchParamRegistry, SearchParamType};

pub fn build_capability_statement(
    registry: &ResourceRegistry,
    search_params: &SearchParamRegistry,
    dataset_id: &str,
) -> Value {
    let resource_types = registry.resource_type_names();

    let resources: Vec<Value> = resource_types
        .iter()
        .map(|rt| {
            let params = search_params.params_for_type(rt);
            let search_params_json: Vec<Value> = params
                .iter()
                .map(|p| {
                    json!({
                        "name": p.name,
                        "type": search_param_type_str(p.param_type),
                        "documentation": p.expression
                    })
                })
                .collect();

            let mut resource = json!({
                "type": rt,
                "interaction": [
                    {"code": "read"},
                    {"code": "create"},
                    {"code": "update"},
                    {"code": "delete"},
                    {"code": "search-type"},
                    {"code": "history-instance"}
                ],
                "versioning": "versioned",
                "readHistory": true,
                "updateCreate": true,
                "conditionalCreate": false,
                "conditionalRead": "not-supported",
                "conditionalUpdate": false,
                "conditionalDelete": "not-supported"
            });

            if !search_params_json.is_empty() {
                resource["searchParam"] = Value::Array(search_params_json);
            }

            resource
        })
        .collect();

    json!({
        "resourceType": "CapabilityStatement",
        "id": format!("{}-capability", dataset_id),
        "status": "active",
        "date": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "kind": "instance",
        "software": {
            "name": "TrexSQL FHIR Server",
            "version": "0.1.0"
        },
        "implementation": {
            "description": format!("TrexSQL FHIR R4 Server - Dataset: {}", dataset_id)
        },
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "rest": [{
            "mode": "server",
            "resource": resources,
            "interaction": [
                {"code": "transaction"},
                {"code": "batch"}
            ],
            "operation": [
                {
                    "name": "export",
                    "definition": "http://hl7.org/fhir/uv/bulkdata/OperationDefinition/export"
                },
                {
                    "name": "cql",
                    "definition": "http://hl7.org/fhir/uv/cql/OperationDefinition/cql"
                }
            ]
        }]
    })
}

fn search_param_type_str(t: SearchParamType) -> &'static str {
    match t {
        SearchParamType::String => "string",
        SearchParamType::Token => "token",
        SearchParamType::Reference => "reference",
        SearchParamType::Date => "date",
        SearchParamType::Quantity => "quantity",
        SearchParamType::Number => "number",
        SearchParamType::Uri => "uri",
        SearchParamType::Composite => "composite",
        SearchParamType::Special => "special",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_capability_statement_structure() {
        let registry = ResourceRegistry::new();
        let search_params = SearchParamRegistry::load_from_json(r#"{
            "resourceType": "Bundle",
            "entry": []
        }"#).unwrap();

        let cs = build_capability_statement(&registry, &search_params, "test-ds");

        assert_eq!(cs["resourceType"], "CapabilityStatement");
        assert_eq!(cs["fhirVersion"], "4.0.1");
        assert_eq!(cs["kind"], "instance");
        assert_eq!(cs["status"], "active");
        assert!(cs["rest"].as_array().unwrap().len() == 1);
        assert_eq!(cs["rest"][0]["mode"], "server");
    }
}
