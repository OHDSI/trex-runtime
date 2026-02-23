use duckdb::Connection;
use std::sync::{Arc, Mutex};

use crate::credential_mask;
use crate::pipeline_registry::PipelineInfo;

/// Publish pipeline state to swarm gossip. Fails silently if swarm unavailable.
pub fn publish_pipeline_state(conn: &Arc<Mutex<Connection>>, info: &PipelineInfo) {
    let host = credential_mask::extract_param(&info.connection_string, "host")
        .unwrap_or("unknown");

    let config = serde_json::json!({
        "pipeline_name": info.name,
        "mode": info.mode.as_str(),
        "publication": info.publication,
        "rows_replicated": info.rows_replicated,
        "error": info.error_message.as_deref().unwrap_or(""),
    });

    let service_json = serde_json::json!({
        "host": host,
        "port": 0,
        "status": info.state.as_str(),
        "uptime": 0,
        "config": config,
    });

    let key = format!("service:etl:{}", info.name);
    let value = service_json.to_string();
    let escaped_key = key.replace('\'', "''");
    let escaped_value = value.replace('\'', "''");

    let sql = format!("SELECT swarm_set_key('{}', '{}')", escaped_key, escaped_value);

    let _ = (|| -> Result<(), Box<dyn std::error::Error>> {
        let c = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
        c.execute_batch(&sql)?;
        Ok(())
    })();
}

/// Remove pipeline state from swarm gossip. Fails silently if swarm unavailable.
pub fn remove_pipeline_state(conn: &Arc<Mutex<Connection>>, pipeline_name: &str) {
    let key = format!("service:etl:{}", pipeline_name);
    let escaped_key = key.replace('\'', "''");

    let sql = format!("SELECT swarm_delete_key('{}')", escaped_key);

    let _ = (|| -> Result<(), Box<dyn std::error::Error>> {
        let c = conn.lock().map_err(|e| format!("Lock error: {e}"))?;
        c.execute_batch(&sql)?;
        Ok(())
    })();
}
