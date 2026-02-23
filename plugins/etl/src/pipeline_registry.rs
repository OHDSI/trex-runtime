use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::SystemTime;
use tokio::sync::oneshot;

/// Pipeline execution mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineMode {
    CopyAndCdc,
    CdcOnly,
    CopyOnly,
}

impl PipelineMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            PipelineMode::CopyAndCdc => "copy_and_cdc",
            PipelineMode::CdcOnly => "cdc_only",
            PipelineMode::CopyOnly => "copy_only",
        }
    }

    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "copy_and_cdc" => Ok(PipelineMode::CopyAndCdc),
            "cdc_only" => Ok(PipelineMode::CdcOnly),
            "copy_only" => Ok(PipelineMode::CopyOnly),
            _ => Err(format!(
                "Invalid mode '{}'. Must be one of: copy_and_cdc, cdc_only, copy_only",
                s
            )),
        }
    }
}

/// Pipeline lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineState {
    Starting,
    Snapshotting,
    Streaming,
    Stopping,
    Stopped,
    Error,
}

impl PipelineState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PipelineState::Starting => "starting",
            PipelineState::Snapshotting => "snapshotting",
            PipelineState::Streaming => "streaming",
            PipelineState::Stopping => "stopping",
            PipelineState::Stopped => "stopped",
            PipelineState::Error => "error",
        }
    }
}

/// Runtime information for an active pipeline.
pub struct PipelineHandle {
    pub thread_handle: Option<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
    pub shutdown_tx: Option<oneshot::Sender<()>>,
    pub start_time: SystemTime,
}

/// Snapshot of pipeline metadata exposed to etl_status().
#[derive(Debug, Clone)]
pub struct PipelineInfo {
    pub name: String,
    pub state: PipelineState,
    pub mode: PipelineMode,
    pub connection_string: String,
    pub publication: String,
    pub snapshot_enabled: bool,
    pub rows_replicated: u64,
    pub last_activity: Option<SystemTime>,
    pub error_message: Option<String>,
}

/// Global registry of active ETL pipelines.
pub struct PipelineRegistry {
    pipelines: Mutex<HashMap<String, (PipelineHandle, PipelineInfo)>>,
}

impl PipelineRegistry {
    fn new() -> Self {
        Self {
            pipelines: Mutex::new(HashMap::new()),
        }
    }

    pub fn instance() -> &'static PipelineRegistry {
        static INSTANCE: OnceLock<PipelineRegistry> = OnceLock::new();
        INSTANCE.get_or_init(PipelineRegistry::new)
    }

    /// Reserve a slot for a new pipeline. Returns error if name already taken.
    pub fn reserve(
        &self,
        name: &str,
        connection_string: &str,
        publication: &str,
        mode: PipelineMode,
        shutdown_tx: oneshot::Sender<()>,
    ) -> Result<(), String> {
        let snapshot_enabled = matches!(mode, PipelineMode::CopyAndCdc | PipelineMode::CopyOnly);

        let _info_snapshot = {
            let mut pipelines = self.pipelines.lock().unwrap();
            if pipelines.contains_key(name) {
                return Err(format!("Pipeline '{}' already exists", name));
            }

            let handle = PipelineHandle {
                thread_handle: None,
                shutdown_tx: Some(shutdown_tx),
                start_time: SystemTime::now(),
            };

            let info = PipelineInfo {
                name: name.to_string(),
                state: PipelineState::Starting,
                mode,
                connection_string: connection_string.to_string(),
                publication: publication.to_string(),
                snapshot_enabled,
                rows_replicated: 0,
                last_activity: None,
                error_message: None,
            };

            let snapshot = info.clone();
            pipelines.insert(name.to_string(), (handle, info));
            snapshot
        };

        #[cfg(feature = "loadable-extension")]
        if let Some(conn) = crate::get_shared_connection() {
            crate::gossip_bridge::publish_pipeline_state(&conn, &_info_snapshot);
        }

        Ok(())
    }

    pub fn set_thread_handle(
        &self,
        name: &str,
        handle: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    ) {
        let mut pipelines = self.pipelines.lock().unwrap();
        if let Some((ph, _)) = pipelines.get_mut(name) {
            ph.thread_handle = Some(handle);
        }
    }

    pub fn update_state(&self, name: &str, state: PipelineState) {
        let _info_snapshot = {
            let mut pipelines = self.pipelines.lock().unwrap();
            if let Some((_, info)) = pipelines.get_mut(name) {
                info.state = state;
                Some(info.clone())
            } else {
                None
            }
        };

        #[cfg(feature = "loadable-extension")]
        if let Some(snapshot) = _info_snapshot {
            if let Some(conn) = crate::get_shared_connection() {
                crate::gossip_bridge::publish_pipeline_state(&conn, &snapshot);
            }
        }
    }

    /// Transition pipeline from Snapshotting to Streaming (once).
    /// Used by the destination to mark when CDC streaming begins.
    pub fn transition_to_streaming_once(&self, name: &str) {
        let _info_snapshot = {
            let mut pipelines = self.pipelines.lock().unwrap();
            if let Some((_, info)) = pipelines.get_mut(name) {
                if info.state == PipelineState::Snapshotting {
                    info.state = PipelineState::Streaming;
                    Some(info.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        #[cfg(feature = "loadable-extension")]
        if let Some(snapshot) = _info_snapshot {
            if let Some(conn) = crate::get_shared_connection() {
                crate::gossip_bridge::publish_pipeline_state(&conn, &snapshot);
            }
        }
    }

    pub fn update_stats(&self, name: &str, rows_delta: u64) {
        let mut pipelines = self.pipelines.lock().unwrap();
        if let Some((_, info)) = pipelines.get_mut(name) {
            info.rows_replicated += rows_delta;
            info.last_activity = Some(SystemTime::now());
        }
    }

    pub fn set_error(&self, name: &str, error: &str) {
        let _info_snapshot = {
            let mut pipelines = self.pipelines.lock().unwrap();
            if let Some((_, info)) = pipelines.get_mut(name) {
                info.state = PipelineState::Error;
                info.error_message = Some(error.to_string());
                Some(info.clone())
            } else {
                None
            }
        };

        #[cfg(feature = "loadable-extension")]
        if let Some(snapshot) = _info_snapshot {
            if let Some(conn) = crate::get_shared_connection() {
                crate::gossip_bridge::publish_pipeline_state(&conn, &snapshot);
            }
        }
    }

    /// Stop a pipeline by sending the shutdown signal and joining the thread.
    pub fn stop(&self, name: &str) -> Result<String, String> {
        let entry = {
            let mut pipelines = self.pipelines.lock().unwrap();
            pipelines.remove(name)
        };

        match entry {
            Some((mut handle, _)) => {
                if let Some(tx) = handle.shutdown_tx.take() {
                    let _ = tx.send(());
                }
                if let Some(th) = handle.thread_handle.take() {
                    match th.join() {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => eprintln!("etl: pipeline '{}' ended with error: {}", name, e),
                        Err(_) => eprintln!("etl: pipeline '{}' thread panicked", name),
                    }
                }

                #[cfg(feature = "loadable-extension")]
                if let Some(conn) = crate::get_shared_connection() {
                    crate::gossip_bridge::remove_pipeline_state(&conn, name);
                }

                Ok(format!("Pipeline '{}' stopped", name))
            }
            None => Err(format!("Pipeline '{}' not found", name)),
        }
    }

    /// Remove a pipeline from the registry (e.g., after thread spawn failure).
    pub fn deregister(&self, name: &str) {
        {
            let mut pipelines = self.pipelines.lock().unwrap();
            pipelines.remove(name);
        }

        #[cfg(feature = "loadable-extension")]
        if let Some(conn) = crate::get_shared_connection() {
            crate::gossip_bridge::remove_pipeline_state(&conn, name);
        }
    }

    pub fn get_all_info(&self) -> Vec<PipelineInfo> {
        let pipelines = self.pipelines.lock().unwrap();
        pipelines.values().map(|(_, info)| info.clone()).collect()
    }
}

pub fn registry() -> &'static PipelineRegistry {
    PipelineRegistry::instance()
}
