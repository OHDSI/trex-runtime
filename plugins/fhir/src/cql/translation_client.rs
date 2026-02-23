use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct CqlTranslationClient {
    base_url: String,
    cache: RwLock<HashMap<String, Value>>,
}

impl CqlTranslationClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn translate(
        &self,
        _cql_text: &str,
        library_url: Option<&str>,
        library_version: Option<&str>,
    ) -> Result<Value, String> {
        let cache_key = format!(
            "{}|{}",
            library_url.unwrap_or(""),
            library_version.unwrap_or("")
        );
        {
            let cache = self.cache.read().unwrap();
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached.clone());
            }
        }

        // HTTP client not available; users must provide pre-compiled ELM.
        Err(format!(
            "CQL translation service at {} is not available. \
             Please provide pre-compiled ELM JSON directly in the request body \
             using the 'library' field instead of CQL source text.",
            self.base_url
        ))
    }

    pub fn cache_elm(&self, library_url: &str, library_version: &str, elm: Value) {
        let cache_key = format!("{}|{}", library_url, library_version);
        let mut cache = self.cache.write().unwrap();
        cache.insert(cache_key, elm);
    }
}
