use std::collections::HashMap;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ElementInfo {
    pub path: String,
    pub name: String,
    pub type_codes: Vec<String>,
    pub min: u32,
    pub max: String,
    pub is_array: bool,
    pub is_choice: bool,
    pub content_reference: Option<String>,
    pub children: Vec<ElementInfo>,
}

#[derive(Debug, Clone)]
pub struct ParsedStructureDefinition {
    pub resource_type: String,
    pub kind: String,
    pub is_abstract: bool,
    pub elements: Vec<ElementInfo>,
}

pub struct DefinitionRegistry {
    pub resources: HashMap<String, ParsedStructureDefinition>,
    pub types: HashMap<String, ParsedStructureDefinition>,
}

impl DefinitionRegistry {
    pub fn new() -> Self {
        Self {
            resources: HashMap::new(),
            types: HashMap::new(),
        }
    }

    pub fn load_from_json(resources_json: &str, types_json: &str) -> Result<Self, String> {
        let resources_bundle: Value =
            serde_json::from_str(resources_json).map_err(|e| format!("Invalid resources JSON: {e}"))?;
        let types_bundle: Value =
            serde_json::from_str(types_json).map_err(|e| format!("Invalid types JSON: {e}"))?;

        let mut registry = Self::new();

        // Parse types first so they are available as lookup targets.
        let type_defs = Self::load_bundle(&types_bundle)?;
        for sd in type_defs {
            match sd.kind.as_str() {
                "complex-type" | "primitive-type" => {
                    registry.types.insert(sd.resource_type.clone(), sd);
                }
                "resource" => {
                    if !sd.is_abstract {
                        registry.resources.insert(sd.resource_type.clone(), sd);
                    }
                }
                _ => {
                    registry.types.insert(sd.resource_type.clone(), sd);
                }
            }
        }

        let resource_defs = Self::load_bundle(&resources_bundle)?;
        for sd in resource_defs {
            match sd.kind.as_str() {
                "resource" => {
                    if !sd.is_abstract {
                        registry.resources.insert(sd.resource_type.clone(), sd);
                    }
                }
                "complex-type" | "primitive-type" => {
                    registry.types.insert(sd.resource_type.clone(), sd);
                }
                _ => {}
            }
        }

        Ok(registry)
    }

    fn load_bundle(bundle: &Value) -> Result<Vec<ParsedStructureDefinition>, String> {
        let entries = bundle
            .get("entry")
            .and_then(|v| v.as_array())
            .ok_or("Bundle missing 'entry' array")?;

        let mut definitions = Vec::new();

        for entry in entries {
            let resource = match entry.get("resource") {
                Some(r) => r,
                None => continue,
            };

            let rt = resource
                .get("resourceType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if rt != "StructureDefinition" {
                continue;
            }

            // We only want specialization definitions (not constraints/profiles).
            let derivation = resource
                .get("derivation")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // Base abstract types lack a derivation field — accept those too.
            if !derivation.is_empty() && derivation != "specialization" {
                continue;
            }

            match Self::parse_structure_definition(resource) {
                Ok(sd) => definitions.push(sd),
                Err(_) => continue,
            }
        }

        Ok(definitions)
    }

    fn parse_structure_definition(sd: &Value) -> Result<ParsedStructureDefinition, String> {
        let kind = sd
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let is_abstract = sd
            .get("abstract")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let resource_type = sd
            .get("type")
            .or_else(|| sd.get("name"))
            .and_then(|v| v.as_str())
            .ok_or("StructureDefinition missing 'type'/'name'")?
            .to_string();

        let snapshot_elements = sd
            .get("snapshot")
            .and_then(|s| s.get("element"))
            .and_then(|e| e.as_array());

        let elements = match snapshot_elements {
            Some(elems) => Self::build_element_tree(elems, &resource_type),
            None => Vec::new(),
        };

        Ok(ParsedStructureDefinition {
            resource_type,
            kind,
            is_abstract,
            elements,
        })
    }

    fn build_element_tree(elements: &[Value], root_path: &str) -> Vec<ElementInfo> {
        if elements.is_empty() {
            return Vec::new();
        }

        let mut flat: Vec<ElementInfo> = Vec::new();
        for elem_val in elements.iter().skip(1) {
            if let Some(info) = Self::parse_element(elem_val, root_path) {
                flat.push(info);
            }
        }

        Self::nest_elements(flat, root_path)
    }

    // Build tree bottom-up so children attach before their parent does.
    fn nest_elements(flat: Vec<ElementInfo>, root_path: &str) -> Vec<ElementInfo> {
        if flat.is_empty() {
            return Vec::new();
        }

        let mut items: Vec<ElementInfo> = flat;
        let mut path_to_index: HashMap<String, usize> = HashMap::new();
        for (i, item) in items.iter().enumerate() {
            path_to_index.insert(item.path.clone(), i);
        }

        let len = items.len();
        for i in (0..len).rev() {
            let parent_path = {
                let path = &items[i].path;
                match path.rfind('.') {
                    Some(pos) => path[..pos].to_string(),
                    None => continue, // top-level under root – keep as-is
                }
            };

            if parent_path == root_path {
                continue;
            }

            if let Some(&parent_idx) = path_to_index.get(&parent_path) {
                let child = items[i].clone();
                items[parent_idx].children.insert(0, child);
                items[i].path = String::new();
            }
        }

        items
            .into_iter()
            .filter(|e| {
                if e.path.is_empty() {
                    return false;
                }
                match e.path.rfind('.') {
                    Some(pos) => &e.path[..pos] == root_path,
                    None => false,
                }
            })
            .collect()
    }

    fn parse_element(elem: &Value, root_path: &str) -> Option<ElementInfo> {
        let path = elem.get("path")?.as_str()?.to_string();

        if path == root_path {
            return None;
        }

        let name = path
            .rsplit('.')
            .next()
            .unwrap_or(&path)
            .to_string();

        let type_codes: Vec<String> = elem
            .get("type")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let code = t.get("code")?.as_str()?;
                        // Normalise fhirpath System URIs (e.g. System.String → string).
                        if code.starts_with("http://hl7.org/fhirpath/") {
                            let simple = code.rsplit('.').next().unwrap_or("String");
                            Some(simple.to_lowercase())
                        } else {
                            Some(code.to_string())
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let min = elem
            .get("min")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let max = elem
            .get("max")
            .and_then(|v| v.as_str())
            .unwrap_or("1")
            .to_string();
        let is_array = max == "*";

        let is_choice = name.ends_with("[x]");

        let content_reference = elem
            .get("contentReference")
            .and_then(|v| v.as_str())
            .map(|cr| {
                if let Some(stripped) = cr.strip_prefix('#') {
                    stripped.to_string()
                } else {
                    cr.to_string()
                }
            });

        Some(ElementInfo {
            path,
            name,
            type_codes,
            min,
            max,
            is_array,
            is_choice,
            content_reference,
            children: Vec::new(),
        })
    }

    pub fn resource_type_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.resources.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn get_resource(&self, name: &str) -> Option<&ParsedStructureDefinition> {
        self.resources.get(name)
    }

    pub fn get_type(&self, name: &str) -> Option<&ParsedStructureDefinition> {
        self.types.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_bundle(entries: Vec<Value>) -> Value {
        json!({
            "resourceType": "Bundle",
            "type": "collection",
            "entry": entries
        })
    }

    fn bundle_entry(sd: Value) -> Value {
        json!({ "resource": sd })
    }

    fn make_sd(
        name: &str,
        kind: &str,
        is_abstract: bool,
        derivation: Option<&str>,
        elements: Vec<Value>,
    ) -> Value {
        let mut sd = json!({
            "resourceType": "StructureDefinition",
            "name": name,
            "type": name,
            "kind": kind,
            "abstract": is_abstract,
            "snapshot": {
                "element": elements
            }
        });
        if let Some(d) = derivation {
            sd.as_object_mut()
                .unwrap()
                .insert("derivation".to_string(), json!(d));
        }
        sd
    }

    fn make_element(path: &str, type_codes: &[&str], min: u32, max: &str) -> Value {
        let types: Vec<Value> = type_codes
            .iter()
            .map(|tc| json!({ "code": *tc }))
            .collect();
        json!({
            "path": path,
            "min": min,
            "max": max,
            "type": types
        })
    }

    fn make_element_with_content_ref(
        path: &str,
        min: u32,
        max: &str,
        content_ref: &str,
    ) -> Value {
        json!({
            "path": path,
            "min": min,
            "max": max,
            "contentReference": content_ref
        })
    }

    #[test]
    fn test_empty_bundles() {
        let empty = json!({"resourceType": "Bundle", "type": "collection", "entry": []});
        let registry =
            DefinitionRegistry::load_from_json(&empty.to_string(), &empty.to_string()).unwrap();
        assert!(registry.resources.is_empty());
        assert!(registry.types.is_empty());
    }

    #[test]
    fn test_parse_simple_resource() {
        let sd = make_sd(
            "TestResource",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("TestResource", &[], 0, "*"),       // root – skipped
                make_element("TestResource.id", &["id"], 0, "1"),
                make_element("TestResource.name", &["string"], 0, "1"),
                make_element("TestResource.tag", &["Coding"], 0, "*"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        assert_eq!(registry.resource_type_names(), vec!["TestResource"]);

        let parsed = registry.get_resource("TestResource").unwrap();
        assert_eq!(parsed.resource_type, "TestResource");
        assert_eq!(parsed.kind, "resource");
        assert!(!parsed.is_abstract);
        assert_eq!(parsed.elements.len(), 3);

        let id_elem = &parsed.elements[0];
        assert_eq!(id_elem.name, "id");
        assert_eq!(id_elem.type_codes, vec!["id"]);
        assert!(!id_elem.is_array);

        let tag_elem = &parsed.elements[2];
        assert_eq!(tag_elem.name, "tag");
        assert!(tag_elem.is_array);
    }

    #[test]
    fn test_parse_complex_type() {
        let sd = make_sd(
            "HumanName",
            "complex-type",
            false,
            Some("specialization"),
            vec![
                make_element("HumanName", &[], 0, "*"),
                make_element("HumanName.use", &["code"], 0, "1"),
                make_element("HumanName.text", &["string"], 0, "1"),
                make_element("HumanName.family", &["string"], 0, "1"),
                make_element("HumanName.given", &["string"], 0, "*"),
                make_element("HumanName.prefix", &["string"], 0, "*"),
                make_element("HumanName.suffix", &["string"], 0, "*"),
            ],
        );

        let empty = make_bundle(vec![]);
        let types_bundle = make_bundle(vec![bundle_entry(sd)]);
        let registry =
            DefinitionRegistry::load_from_json(&empty.to_string(), &types_bundle.to_string())
                .unwrap();

        assert!(registry.resources.is_empty());
        let hn = registry.get_type("HumanName").unwrap();
        assert_eq!(hn.kind, "complex-type");
        assert_eq!(hn.elements.len(), 6);

        let given = hn.elements.iter().find(|e| e.name == "given").unwrap();
        assert!(given.is_array);
        assert_eq!(given.type_codes, vec!["string"]);
    }

    #[test]
    fn test_choice_type() {
        let sd = make_sd(
            "Observation",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Observation", &[], 0, "*"),
                make_element(
                    "Observation.value[x]",
                    &["Quantity", "CodeableConcept", "string", "boolean", "integer", "dateTime"],
                    0,
                    "1",
                ),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let obs = registry.get_resource("Observation").unwrap();
        let value = &obs.elements[0];
        assert!(value.is_choice);
        assert_eq!(value.name, "value[x]");
        assert_eq!(
            value.type_codes,
            vec!["Quantity", "CodeableConcept", "string", "boolean", "integer", "dateTime"]
        );
    }

    #[test]
    fn test_content_reference() {
        let sd = make_sd(
            "Questionnaire",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Questionnaire", &[], 0, "*"),
                make_element("Questionnaire.status", &["code"], 1, "1"),
                make_element("Questionnaire.item", &["BackboneElement"], 0, "*"),
                make_element("Questionnaire.item.text", &["string"], 0, "1"),
                make_element_with_content_ref(
                    "Questionnaire.item.item",
                    0,
                    "*",
                    "#Questionnaire.item",
                ),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let q = registry.get_resource("Questionnaire").unwrap();
        assert_eq!(q.elements.len(), 2); // status, item (top-level)

        let item = &q.elements[1];
        assert_eq!(item.name, "item");
        assert!(item.is_array);
        assert_eq!(item.children.len(), 2); // text, item (nested)

        let nested_item = &item.children[1];
        assert_eq!(nested_item.name, "item");
        assert_eq!(
            nested_item.content_reference,
            Some("Questionnaire.item".to_string())
        );
    }

    #[test]
    fn test_nested_backbone_elements() {
        let sd = make_sd(
            "Account",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Account", &[], 0, "*"),
                make_element("Account.status", &["code"], 1, "1"),
                make_element("Account.coverage", &["BackboneElement"], 0, "*"),
                make_element("Account.coverage.coverage", &["Reference"], 1, "1"),
                make_element("Account.coverage.priority", &["positiveInt"], 0, "1"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let acct = registry.get_resource("Account").unwrap();
        assert_eq!(acct.elements.len(), 2); // status, coverage

        let coverage = &acct.elements[1];
        assert_eq!(coverage.name, "coverage");
        assert!(coverage.is_array);
        assert_eq!(coverage.children.len(), 2);
        assert_eq!(coverage.children[0].name, "coverage");
        assert_eq!(coverage.children[1].name, "priority");
    }

    #[test]
    fn test_abstract_types_excluded_from_resources() {
        let abstract_sd = make_sd(
            "DomainResource",
            "resource",
            true,
            Some("specialization"),
            vec![make_element("DomainResource", &[], 0, "*")],
        );
        let concrete_sd = make_sd(
            "Patient",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Patient", &[], 0, "*"),
                make_element("Patient.active", &["boolean"], 0, "1"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(abstract_sd), bundle_entry(concrete_sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        assert!(!registry.resources.contains_key("DomainResource"));
        assert!(registry.resources.contains_key("Patient"));
    }

    #[test]
    fn test_non_structure_definitions_skipped() {
        let cap_stmt = json!({
            "resource": {
                "resourceType": "CapabilityStatement",
                "id": "base"
            }
        });
        let sd = make_sd(
            "Account",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Account", &[], 0, "*"),
                make_element("Account.status", &["code"], 1, "1"),
            ],
        );

        let bundle = make_bundle(vec![cap_stmt, bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        assert_eq!(registry.resource_type_names(), vec!["Account"]);
    }

    #[test]
    fn test_constraint_profiles_skipped() {
        // Profiles have derivation == "constraint" and should be excluded.
        let profile_sd = make_sd(
            "USCorePatient",
            "resource",
            false,
            Some("constraint"),
            vec![
                make_element("Patient", &[], 0, "*"),
                make_element("Patient.active", &["boolean"], 0, "1"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(profile_sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        assert!(registry.resources.is_empty());
    }

    #[test]
    fn test_fhirpath_system_string_normalised() {
        let sd = make_sd(
            "TestRes",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("TestRes", &[], 0, "*"),
                {
                    // Element with the special fhirpath URI type code.
                    json!({
                        "path": "TestRes.id",
                        "min": 0,
                        "max": "1",
                        "type": [{
                            "extension": [{
                                "url": "http://hl7.org/fhir/StructureDefinition/structuredefinition-fhir-type",
                                "valueUrl": "string"
                            }],
                            "code": "http://hl7.org/fhirpath/System.String"
                        }]
                    })
                },
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let res = registry.get_resource("TestRes").unwrap();
        assert_eq!(res.elements[0].type_codes, vec!["string"]);
    }

    #[test]
    fn test_resource_type_names_sorted() {
        let sd_z = make_sd(
            "Zebra",
            "resource",
            false,
            Some("specialization"),
            vec![make_element("Zebra", &[], 0, "*")],
        );
        let sd_a = make_sd(
            "Account",
            "resource",
            false,
            Some("specialization"),
            vec![make_element("Account", &[], 0, "*")],
        );
        let sd_m = make_sd(
            "MedicationRequest",
            "resource",
            false,
            Some("specialization"),
            vec![make_element("MedicationRequest", &[], 0, "*")],
        );

        let bundle = make_bundle(vec![
            bundle_entry(sd_z),
            bundle_entry(sd_a),
            bundle_entry(sd_m),
        ]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        assert_eq!(
            registry.resource_type_names(),
            vec!["Account", "MedicationRequest", "Zebra"]
        );
    }

    #[test]
    fn test_deeply_nested_elements() {
        // Three-level nesting: Resource.a.b.c
        let sd = make_sd(
            "Deep",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Deep", &[], 0, "*"),
                make_element("Deep.level1", &["BackboneElement"], 0, "*"),
                make_element("Deep.level1.level2", &["BackboneElement"], 0, "1"),
                make_element("Deep.level1.level2.level3", &["string"], 0, "1"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let deep = registry.get_resource("Deep").unwrap();
        assert_eq!(deep.elements.len(), 1);
        assert_eq!(deep.elements[0].name, "level1");
        assert_eq!(deep.elements[0].children.len(), 1);
        assert_eq!(deep.elements[0].children[0].name, "level2");
        assert_eq!(deep.elements[0].children[0].children.len(), 1);
        assert_eq!(deep.elements[0].children[0].children[0].name, "level3");
        assert_eq!(
            deep.elements[0].children[0].children[0].type_codes,
            vec!["string"]
        );
    }

    #[test]
    fn test_element_without_type_or_children() {
        // An element with no type codes and no children (e.g. an extension
        // element) should still be included.
        let sd = make_sd(
            "Ext",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Ext", &[], 0, "*"),
                make_element("Ext.extension", &[], 0, "*"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let ext = registry.get_resource("Ext").unwrap();
        assert_eq!(ext.elements.len(), 1);
        assert_eq!(ext.elements[0].name, "extension");
        assert!(ext.elements[0].type_codes.is_empty());
    }

    #[test]
    fn test_min_cardinality() {
        let sd = make_sd(
            "Required",
            "resource",
            false,
            Some("specialization"),
            vec![
                make_element("Required", &[], 0, "*"),
                make_element("Required.status", &["code"], 1, "1"),
                make_element("Required.optional", &["string"], 0, "1"),
            ],
        );

        let bundle = make_bundle(vec![bundle_entry(sd)]);
        let empty = make_bundle(vec![]);
        let registry =
            DefinitionRegistry::load_from_json(&bundle.to_string(), &empty.to_string()).unwrap();

        let req = registry.get_resource("Required").unwrap();
        assert_eq!(req.elements[0].min, 1);
        assert_eq!(req.elements[1].min, 0);
    }
}
