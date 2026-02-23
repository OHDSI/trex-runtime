//! SchemaProvider wrapping sharded (DistributedTableProvider) tables so they
//! can be registered as a child of MultiSchemaProvider alongside federation schemas.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::common::Result as DFResult;
use datafusion::datasource::TableProvider;

/// A read-only SchemaProvider backed by a pre-built map of table providers.
#[derive(Debug)]
pub struct ShardedSchemaProvider {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl ShardedSchemaProvider {
    pub fn new(tables: HashMap<String, Arc<dyn TableProvider>>) -> Self {
        Self { tables }
    }
}

#[async_trait]
impl SchemaProvider for ShardedSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::empty::EmptyTable;

    fn dummy_provider() -> Arc<dyn TableProvider> {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        Arc::new(EmptyTable::new(schema))
    }

    #[test]
    fn table_names_returns_all() {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), dummy_provider());
        tables.insert("customers".to_string(), dummy_provider());
        let provider = ShardedSchemaProvider::new(tables);
        let mut names = provider.table_names();
        names.sort();
        assert_eq!(names, vec!["customers", "orders"]);
    }

    #[test]
    fn table_exist_true_for_registered() {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), dummy_provider());
        let provider = ShardedSchemaProvider::new(tables);
        assert!(provider.table_exist("orders"));
        assert!(!provider.table_exist("missing"));
    }

    #[tokio::test]
    async fn table_lookup_returns_provider() {
        let mut tables = HashMap::new();
        tables.insert("orders".to_string(), dummy_provider());
        let provider = ShardedSchemaProvider::new(tables);
        let result = provider.table("orders").await.unwrap();
        assert!(result.is_some());
        let result = provider.table("missing").await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn empty_provider_has_no_tables() {
        let provider = ShardedSchemaProvider::new(HashMap::new());
        assert!(provider.table_names().is_empty());
        assert!(!provider.table_exist("anything"));
    }
}
