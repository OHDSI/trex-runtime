use crate::fhir::resource_registry::ResourceRegistry;
use crate::fhir::search_parameter::SearchParamRegistry;
use crate::query_executor::QueryExecutor;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<QueryExecutor>,
    pub registry: Arc<ResourceRegistry>,
    pub search_params: Arc<SearchParamRegistry>,
}

impl AppState {
    pub fn new(
        executor: Arc<QueryExecutor>,
        registry: Arc<ResourceRegistry>,
        search_params: Arc<SearchParamRegistry>,
    ) -> Self {
        Self {
            executor,
            registry,
            search_params,
        }
    }
}
