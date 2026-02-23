use std::sync::Arc;
use std::thread;
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::fhir::resource_registry::ResourceRegistry;
use crate::fhir::search_parameter::SearchParamRegistry;
use crate::fhir::structure_definition::DefinitionRegistry;
use crate::query_executor::QueryExecutor;
use crate::router::build_router;
use crate::server_registry::{ServerHandle, ServerRegistry};
use crate::state::AppState;
use crate::{get_query_executor, init_fhir_meta};

const FHIR_PROFILES_RESOURCES: &str = include_str!("../data/profiles-resources.json");
const FHIR_PROFILES_TYPES: &str = include_str!("../data/profiles-types.json");
const FHIR_SEARCH_PARAMETERS: &str = include_str!("../data/search-parameters.json");

pub fn load_default_definitions() -> Result<DefinitionRegistry, String> {
    DefinitionRegistry::load_from_json(FHIR_PROFILES_RESOURCES, FHIR_PROFILES_TYPES)
}

pub fn load_search_parameters() -> Result<SearchParamRegistry, String> {
    SearchParamRegistry::load_from_json(FHIR_SEARCH_PARAMETERS)
}

pub fn start_fhir_server(host: String, port: u16) -> Result<String, String> {
    if ServerRegistry::instance().is_server_running(&host, port) {
        return Err(format!("FHIR server already running on {}:{}", host, port));
    }

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_host = host.clone();
    let server_port = port;
    let success_host = host.clone();

    let thread_handle = thread::Builder::new()
        .name(format!("fhir-server-{}:{}", host, port))
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            let result = rt.block_on(async move {
                let executor = get_query_executor().ok_or_else(|| {
                    let msg = "No query executor available";
                    eprintln!("[fhir] ERROR: {msg}");
                    std::io::Error::new(std::io::ErrorKind::Other, msg)
                })?;

                init_fhir_meta(&executor).await;

                eprintln!("[fhir] Loading FHIR R4 StructureDefinitions...");
                let definitions = load_default_definitions().map_err(|e| {
                    eprintln!("[fhir] ERROR: Failed to load FHIR definitions: {e}");
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to load FHIR definitions: {}", e),
                    )
                })?;
                let resource_count = definitions.resources.len();
                let type_count = definitions.types.len();
                eprintln!(
                    "[fhir] Loaded {} resource types and {} data types",
                    resource_count, type_count
                );

                let registry = Arc::new(ResourceRegistry::with_definitions(definitions));

                eprintln!("[fhir] Loading FHIR R4 SearchParameter definitions...");
                let search_params = load_search_parameters().map_err(|e| {
                    eprintln!("[fhir] ERROR: Failed to load search parameters: {e}");
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to load search parameters: {}", e),
                    )
                })?;
                let search_params = Arc::new(search_params);
                eprintln!("[fhir] Search parameters loaded");

                let state = Arc::new(AppState::new(executor, registry, search_params));
                let app = build_router(state);

                let addr = format!("{}:{}", server_host, server_port);
                let listener = TcpListener::bind(&addr).await.map_err(|e| {
                    eprintln!("[fhir] ERROR: Failed to bind {addr}: {e}");
                    e
                })?;

                eprintln!(
                    "[fhir] Server listening on {}:{}",
                    server_host, server_port
                );

                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                        eprintln!("[fhir] Received shutdown signal");
                    })
                    .await?;

                Ok(())
            });

            if let Err(ref e) = result {
                eprintln!("[fhir] ERROR: Server thread exiting with error: {e}");
            }
            result
        })
        .map_err(|e| format!("Failed to spawn FHIR server thread: {}", e))?;

    let server_handle = ServerHandle {
        thread_handle,
        shutdown_tx,
        start_time: SystemTime::now(),
    };

    ServerRegistry::instance().register_server(host.clone(), port, server_handle)?;

    Ok(format!(
        "Started FHIR R4 server on {}:{}",
        success_host, port
    ))
}

pub fn stop_fhir_server(host: &str, port: u16) -> Result<String, String> {
    ServerRegistry::instance().stop_server(host, port)
}
