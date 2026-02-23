use chitchat::transport::UdpTransport;
use chitchat::{spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use uuid::Uuid;

use crate::logging::SwarmLogger;

pub struct NodeInfo {
    pub node_id: String,
    pub node_name: String,
    pub gossip_addr: String,
    pub data_node: String,
    pub status: String,
}

/// All key-value pairs for a node. Used by the catalog module to resolve table locations.
pub struct NodeKeyValueInfo {
    pub node_id: String,
    pub node_name: String,
    pub gossip_addr: String,
    pub key_values: Vec<(String, String)>,
}

struct GossipHandle {
    chitchat_handle: ChitchatHandle,
    runtime: tokio::runtime::Runtime,
    node_id: String,
}

/// Execute `future` on the gossip runtime via `Handle::spawn()` + a blocking
/// `std::sync::mpsc` channel.  Unlike `runtime.block_on()`, this is safe to
/// call from within another tokio runtime because it never enters a nested
/// tokio context — it only blocks the current thread passively on a channel.
fn exec_on_runtime<F, T>(handle: &tokio::runtime::Handle, future: F) -> T
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    handle.spawn(async move {
        let _ = tx.send(future.await);
    });
    rx.recv().expect("gossip runtime is alive")
}

/// Process-wide singleton owning at most one active gossip instance.
///
/// # Tokio Runtime Safety
///
/// `GossipRegistry` owns its own `tokio::runtime::Runtime` for chitchat's
/// background tasks.  All public methods (except `start`/`stop`) use
/// `exec_on_runtime()` internally, which spawns work onto the gossip runtime
/// and waits via a plain `std::sync::mpsc` channel.  This means they are
/// **safe to call from any context**, including from within another tokio
/// runtime's `block_on()`.
///
/// Only `start()` and `stop()` use `runtime.block_on()` directly (for
/// lifecycle operations).  These are only called from SQL function handlers
/// which do not run inside a tokio context.
pub struct GossipRegistry {
    handle: Arc<Mutex<Option<GossipHandle>>>,
}

impl GossipRegistry {
    fn new() -> Self {
        Self {
            handle: Arc::new(Mutex::new(None)),
        }
    }

    pub fn instance() -> &'static GossipRegistry {
        static INSTANCE: OnceLock<GossipRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| GossipRegistry::new())
    }

    pub fn is_running(&self) -> bool {
        self.handle.lock().map(|g| g.is_some()).unwrap_or(false)
    }

    /// Start the gossip layer. Returns the generated UUID node-id on success.
    pub fn start(
        &self,
        host: &str,
        port: u16,
        cluster_id: &str,
        node_name: &str,
        data_node: &str,
        seeds: Vec<String>,
    ) -> Result<String, String> {
        let mut guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        if guard.is_some() {
            return Err("Gossip is already running".to_string());
        }

        // Multi-threaded so chitchat's background tasks run between SQL commands.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

        let node_id = Uuid::new_v4().to_string();

        let gossip_addr: SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| format!("Invalid gossip address {host}:{port}: {e}"))?;

        // Skip our own address -- chitchat does not need to seed itself.
        let seed_addrs: Vec<SocketAddr> = seeds
            .iter()
            .filter_map(|s| {
                let addr: SocketAddr = s.parse().ok()?;
                if addr == gossip_addr {
                    None
                } else {
                    Some(addr)
                }
            })
            .collect();

        let chitchat_id = ChitchatId::new(node_id.clone(), 0, gossip_addr);

        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: cluster_id.to_string(),
            gossip_interval: Duration::from_millis(500),
            listen_addr: gossip_addr,
            seed_nodes: seed_addrs.iter().map(|a| a.to_string()).collect(),
            failure_detector_config: FailureDetectorConfig::default(),
            marked_for_deletion_grace_period: Duration::from_secs(60),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let initial_kv: Vec<(String, String)> = vec![
            ("node_name".to_string(), node_name.to_string()),
            ("data_node".to_string(), data_node.to_string()),
            ("status".to_string(), "active".to_string()),
        ];

        let transport = UdpTransport;

        let chitchat_handle = runtime.block_on(async {
            spawn_chitchat(config, initial_kv, &transport)
                .await
                .map_err(|e| format!("Failed to spawn chitchat: {e}"))
        })?;

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Info,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "start"),
                ("name", node_name),
                ("addr", &gossip_addr.to_string()),
                ("cluster", cluster_id),
            ],
            "Gossip started",
        );

        *guard = Some(GossipHandle {
            chitchat_handle,
            runtime,
            node_id: node_id.clone(),
        });

        Ok(node_id)
    }

    /// Mark this node as draining, then tear down the gossip layer.
    pub fn stop(&self) -> Result<String, String> {
        let mut guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
        let gossip = guard
            .as_ref()
            .ok_or_else(|| "Gossip is not running".to_string())?;

        let chitchat = gossip.chitchat_handle.chitchat();
        let _ = gossip.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut cc = chitchat.lock().await;
                cc.self_node_state().set("status", "draining");
            })
            .await
        });

        let node_id = gossip.node_id.clone();

        if let Some(handle) = guard.take() {
            handle.runtime.shutdown_timeout(Duration::from_secs(5));
        }

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Info,
            "gossip",
            &[("node_id", &node_id), ("operation", "stop")],
            "Gossip stopped",
        );

        Ok(format!("Gossip stopped for node {node_id}"))
    }

    /// Set a key-value pair on this node's gossip state.
    pub fn set_key(&self, key: &str, value: &str) -> Result<(), String> {
        let (handle, chitchat, node_id) = {
            let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
            let gossip = guard
                .as_ref()
                .ok_or_else(|| "Gossip is not running".to_string())?;
            (
                gossip.runtime.handle().clone(),
                gossip.chitchat_handle.chitchat(),
                gossip.node_id.clone(),
            )
        };

        let key_owned = key.to_string();
        let value_owned = value.to_string();
        exec_on_runtime(&handle, async move {
            let mut cc = chitchat.lock().await;
            cc.self_node_state().set(&key_owned, &value_owned);
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "set_key"),
                ("key", key),
            ],
            &format!("Set key: {key}={value}"),
        );

        Ok(())
    }

    /// Return the state of every node known to the gossip layer.
    pub fn get_node_states(&self) -> Result<Vec<NodeInfo>, String> {
        let (handle, chitchat, node_id) = {
            let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
            let gossip = guard
                .as_ref()
                .ok_or_else(|| "Gossip is not running".to_string())?;
            (
                gossip.runtime.handle().clone(),
                gossip.chitchat_handle.chitchat(),
                gossip.node_id.clone(),
            )
        };

        let nodes = exec_on_runtime(&handle, async move {
            let cc = chitchat.lock().await;
            cc.node_states()
                .iter()
                .map(|(id, state)| {
                    let node_name = state
                        .get("node_name")
                        .unwrap_or("")
                        .to_string();
                    let data_node = state
                        .get("data_node")
                        .unwrap_or("false")
                        .to_string();
                    let status = state
                        .get("status")
                        .unwrap_or("unknown")
                        .to_string();

                    NodeInfo {
                        node_id: id.node_id.clone(),
                        node_name,
                        gossip_addr: id.gossip_advertise_addr.to_string(),
                        data_node,
                        status,
                    }
                })
                .collect::<Vec<_>>()
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "get_node_states"),
                ("count", &nodes.len().to_string()),
            ],
            "Retrieved node states",
        );

        Ok(nodes)
    }

    /// Return this node's current gossip configuration as key-value pairs.
    pub fn get_self_config(&self) -> Result<Vec<(String, String)>, String> {
        let (handle, chitchat) = {
            let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
            let gossip = guard
                .as_ref()
                .ok_or_else(|| "Gossip is not running".to_string())?;
            (
                gossip.runtime.handle().clone(),
                gossip.chitchat_handle.chitchat(),
            )
        };

        let config = exec_on_runtime(&handle, async move {
            let mut cc = chitchat.lock().await;
            let id = cc.self_chitchat_id();
            let mut pairs = vec![
                ("node_id".to_string(), id.node_id.clone()),
                ("generation_id".to_string(), id.generation_id.to_string()),
                (
                    "gossip_advertise_addr".to_string(),
                    id.gossip_advertise_addr.to_string(),
                ),
                ("cluster_id".to_string(), cc.cluster_id().to_string()),
            ];

            let self_state = cc.self_node_state();
            for (key, value) in self_state.key_values() {
                pairs.push((key.to_string(), value.to_string()));
            }

            pairs
        });

        Ok(config)
    }

    /// Delete a key from this node's gossip state (tombstoned until GC).
    pub fn delete_key(&self, key: &str) -> Result<(), String> {
        let (handle, chitchat, node_id) = {
            let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
            let gossip = guard
                .as_ref()
                .ok_or_else(|| "Gossip is not running".to_string())?;
            (
                gossip.runtime.handle().clone(),
                gossip.chitchat_handle.chitchat(),
                gossip.node_id.clone(),
            )
        };

        let key_owned = key.to_string();
        exec_on_runtime(&handle, async move {
            let mut cc = chitchat.lock().await;
            cc.self_node_state().delete(&key_owned);
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "delete_key"),
                ("key", key),
            ],
            &format!("Deleted key: {key}"),
        );

        Ok(())
    }

    pub fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>, String> {
        let (handle, chitchat, node_id) = {
            let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
            let gossip = guard
                .as_ref()
                .ok_or_else(|| "Gossip is not running".to_string())?;
            (
                gossip.runtime.handle().clone(),
                gossip.chitchat_handle.chitchat(),
                gossip.node_id.clone(),
            )
        };

        let prefix_owned = prefix.to_string();
        let keys = exec_on_runtime(&handle, async move {
            let mut cc = chitchat.lock().await;
            let state = cc.self_node_state();
            state
                .key_values()
                .filter(|(k, _)| k.starts_with(&prefix_owned))
                .map(|(k, _)| k.to_string())
                .collect::<Vec<_>>()
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "list_keys_with_prefix"),
                ("prefix", prefix),
                ("count", &keys.len().to_string()),
            ],
            "Listed keys with prefix",
        );

        Ok(keys)
    }

    /// Return all key-value pairs for every node (including `catalog:*` and `service:*`).
    pub fn get_node_key_values(&self) -> Result<Vec<NodeKeyValueInfo>, String> {
        let (handle, chitchat, node_id) = {
            let guard = self.handle.lock().map_err(|_| "Gossip lock poisoned".to_string())?;
            let gossip = guard
                .as_ref()
                .ok_or_else(|| "Gossip is not running".to_string())?;
            (
                gossip.runtime.handle().clone(),
                gossip.chitchat_handle.chitchat(),
                gossip.node_id.clone(),
            )
        };

        let nodes = exec_on_runtime(&handle, async move {
            let cc = chitchat.lock().await;
            cc.node_states()
                .iter()
                .map(|(id, state)| {
                    let node_name = state
                        .get("node_name")
                        .unwrap_or("")
                        .to_string();

                    let key_values: Vec<(String, String)> = state
                        .key_values()
                        .map(|(key, value)| {
                            (key.to_string(), value.to_string())
                        })
                        .collect();

                    NodeKeyValueInfo {
                        node_id: id.node_id.clone(),
                        node_name,
                        gossip_addr: id.gossip_advertise_addr.to_string(),
                        key_values,
                    }
                })
                .collect::<Vec<_>>()
        });

        SwarmLogger::log_with_context(
            crate::logging::LogLevel::Debug,
            "gossip",
            &[
                ("node_id", &node_id),
                ("operation", "get_node_key_values"),
                ("count", &nodes.len().to_string()),
            ],
            "Retrieved node key-values",
        );

        Ok(nodes)
    }
}

// TODO: implement gossip encryption (derive key from CA cert, encrypt/decrypt values)
