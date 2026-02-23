use std::collections::HashMap;
use std::sync::RwLock;

use crate::fhir::structure_definition::DefinitionRegistry;
use crate::schema::generator;
use crate::schema::json_transform;

pub struct ResourceRegistry {
    definitions: Option<DefinitionRegistry>,
    ddl_cache: RwLock<HashMap<String, String>>,
    transform_cache: RwLock<HashMap<String, String>>,
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self {
            definitions: None,
            ddl_cache: RwLock::new(HashMap::new()),
            transform_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_definitions(definitions: DefinitionRegistry) -> Self {
        Self {
            definitions: Some(definitions),
            ddl_cache: RwLock::new(HashMap::new()),
            transform_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn definitions(&self) -> Option<&DefinitionRegistry> {
        self.definitions.as_ref()
    }

    pub fn resource_type_names(&self) -> Vec<String> {
        self.definitions
            .as_ref()
            .map(|d| d.resource_type_names())
            .unwrap_or_default()
    }

    pub fn is_known_type(&self, resource_type: &str) -> bool {
        self.definitions
            .as_ref()
            .map(|d| d.get_resource(resource_type).is_some())
            .unwrap_or(false)
    }

    pub fn get_ddl(&self, resource_type: &str, schema_name: &str) -> Result<String, String> {
        let cache_key = format!("{}.{}", schema_name, resource_type);
        {
            let cache = self.ddl_cache.read().unwrap();
            if let Some(ddl) = cache.get(&cache_key) {
                return Ok(ddl.clone());
            }
        }

        let definitions = self
            .definitions
            .as_ref()
            .ok_or("No definitions loaded")?;
        let ddl = generator::generate_ddl(definitions, resource_type, schema_name)?;

        {
            let mut cache = self.ddl_cache.write().unwrap();
            cache.insert(cache_key, ddl.clone());
        }

        Ok(ddl)
    }

    pub fn get_json_transform(&self, resource_type: &str) -> Result<String, String> {
        {
            let cache = self.transform_cache.read().unwrap();
            if let Some(transform) = cache.get(resource_type) {
                return Ok(transform.clone());
            }
        }

        let definitions = self
            .definitions
            .as_ref()
            .ok_or("No definitions loaded")?;
        let transform = json_transform::generate_json_transform(definitions, resource_type)?;

        {
            let mut cache = self.transform_cache.write().unwrap();
            cache.insert(resource_type.to_string(), transform.clone());
        }

        Ok(transform)
    }

    pub fn table_name(resource_type: &str) -> String {
        resource_type.to_lowercase()
    }

    pub fn generate_all_ddl(&self, schema_name: &str) -> Vec<(String, Result<String, String>)> {
        self.definitions
            .as_ref()
            .map(|d| generator::generate_all_ddl(d, schema_name))
            .unwrap_or_default()
    }
}
