use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug)]
pub struct ProcessedEntry {
    pub resource: Value,
    pub resource_type: String,
    pub server_id: String,
    pub method: String,
}

pub fn process_bundle_entries(
    bundle: &Value,
    max_entries: usize,
) -> Result<Vec<ProcessedEntry>, String> {
    let entries = bundle
        .get("entry")
        .and_then(|v| v.as_array())
        .ok_or("Bundle missing 'entry' array")?;

    if entries.is_empty() {
        return Ok(Vec::new());
    }

    if entries.len() > max_entries {
        return Err(format!(
            "Bundle exceeds maximum entry count: {} > {}",
            entries.len(),
            max_entries
        ));
    }

    let mut processed = Vec::with_capacity(entries.len());
    let mut ref_map: HashMap<String, String> = HashMap::new();

    for entry in entries {
        let resource = entry
            .get("resource")
            .ok_or("Bundle entry missing 'resource'")?
            .clone();

        let resource_type = resource
            .get("resourceType")
            .and_then(|v| v.as_str())
            .ok_or("Bundle entry resource missing 'resourceType'")?
            .to_string();

        let method = entry
            .get("request")
            .and_then(|r| r.get("method"))
            .and_then(|v| v.as_str())
            .unwrap_or("POST")
            .to_uppercase();

        let server_id = uuid::Uuid::new_v4().to_string();

        if let Some(full_url) = entry.get("fullUrl").and_then(|v| v.as_str()) {
            if full_url.starts_with("urn:uuid:") {
                let server_ref = format!("{}/{}", resource_type, server_id);
                ref_map.insert(full_url.to_string(), server_ref);
            }
        }

        processed.push(ProcessedEntry {
            resource,
            resource_type,
            server_id,
            method,
        });
    }

    for entry in &mut processed {
        resolve_references(&mut entry.resource, &ref_map);
    }

    Ok(processed)
}

fn resolve_references(value: &mut Value, ref_map: &HashMap<String, String>) {
    match value {
        Value::Object(obj) => {
            if let Some(ref_val) = obj.get_mut("reference") {
                if let Some(ref_str) = ref_val.as_str() {
                    if let Some(resolved) = ref_map.get(ref_str) {
                        *ref_val = Value::String(resolved.clone());
                    }
                }
            }
            for (_, v) in obj.iter_mut() {
                resolve_references(v, ref_map);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                resolve_references(v, ref_map);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_process_bundle_empty() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": []
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_process_bundle_basic() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [{
                "fullUrl": "urn:uuid:abc-123",
                "resource": {
                    "resourceType": "Patient",
                    "name": [{"family": "Smith"}]
                },
                "request": {
                    "method": "POST",
                    "url": "Patient"
                }
            }]
        });
        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].resource_type, "Patient");
        assert_eq!(result[0].method, "POST");
        assert!(!result[0].server_id.is_empty());
    }

    #[test]
    fn test_temp_reference_resolution() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-1",
                    "resource": {
                        "resourceType": "Patient",
                        "name": [{"family": "Smith"}]
                    },
                    "request": {"method": "POST", "url": "Patient"}
                },
                {
                    "fullUrl": "urn:uuid:obs-1",
                    "resource": {
                        "resourceType": "Observation",
                        "subject": {"reference": "urn:uuid:patient-1"},
                        "status": "final"
                    },
                    "request": {"method": "POST", "url": "Observation"}
                }
            ]
        });

        let result = process_bundle_entries(&bundle, 1000).unwrap();
        assert_eq!(result.len(), 2);

        let obs = &result[1];
        let subject_ref = obs
            .resource
            .get("subject")
            .and_then(|s| s.get("reference"))
            .and_then(|r| r.as_str())
            .unwrap();
        assert!(subject_ref.starts_with("Patient/"));
        assert!(!subject_ref.contains("urn:uuid:"));
    }

    #[test]
    fn test_max_entries_exceeded() {
        let bundle = json!({
            "resourceType": "Bundle",
            "type": "transaction",
            "entry": [
                {"resource": {"resourceType": "Patient"}, "request": {"method": "POST", "url": "Patient"}},
                {"resource": {"resourceType": "Patient"}, "request": {"method": "POST", "url": "Patient"}},
                {"resource": {"resourceType": "Patient"}, "request": {"method": "POST", "url": "Patient"}}
            ]
        });
        let result = process_bundle_entries(&bundle, 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }
}
