use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use std::sync::Arc;

use crate::error::AppError;
use crate::fhir::capability;
use crate::sql_safety::validate_dataset_id;
use crate::state::AppState;

pub async fn get_metadata(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let check_sql = format!(
        "SELECT id FROM _fhir_meta._datasets WHERE id = '{}'",
        dataset_id.replace('\'', "''")
    );
    match state.executor.submit(check_sql).await {
        crate::query_executor::QueryResult::Select { rows, .. } if !rows.is_empty() => {}
        _ => {
            return Err(AppError::NotFound(format!(
                "Dataset '{}' not found",
                dataset_id
            )));
        }
    }

    let cs = capability::build_capability_statement(
        &state.registry,
        &state.search_params,
        &dataset_id,
    );

    Ok(Json(cs))
}
