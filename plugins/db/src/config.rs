use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::net::SocketAddr;

/// Cluster configuration parsed from the `SWARM_CONFIG` env var.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: String,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    /// When true, `swarm_query()` uses DataFusion instead of the legacy coordinator.
    #[serde(default)]
    pub distributed_engine: bool,
    #[serde(default)]
    pub admission: Option<AdmissionConfig>,
    pub nodes: HashMap<String, NodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionConfig {
    #[serde(default = "default_max_concurrent")]
    pub default_max_concurrent: u32,
    /// Reject queries when cluster memory exceeds this percentage.
    #[serde(default = "default_memory_threshold")]
    pub memory_rejection_threshold_pct: f64,
    #[serde(default = "default_priority")]
    pub default_priority: String,
}

fn default_max_concurrent() -> u32 {
    10
}

fn default_memory_threshold() -> f64 {
    85.0
}

fn default_priority() -> String {
    "interactive".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub ca_cert: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTlsConfig {
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub gossip_addr: String,
    #[serde(default = "default_true")]
    pub data_node: bool,
    #[serde(default = "default_roles")]
    pub roles: Vec<String>,
    #[serde(default)]
    pub tls: Option<NodeTlsConfig>,
    #[serde(default)]
    pub extensions: Vec<ExtensionConfig>,
}

fn default_roles() -> Vec<String> {
    vec!["executor".to_string()]
}

fn default_true() -> bool {
    true
}

/// Extension hosted by a node. The opaque `config` JSON carries service-specific
/// settings; swarm extracts `host`/`port` for gossip, passes the rest through.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionConfig {
    pub name: String,
    #[serde(default)]
    pub config: Option<serde_json::Value>,
}

impl ClusterConfig {
    pub fn from_env() -> Result<Self, String> {
        let raw = env::var("SWARM_CONFIG").map_err(|_| {
            "SWARM_CONFIG environment variable is not set".to_string()
        })?;

        Self::from_json(&raw)
    }

    pub fn from_json(json: &str) -> Result<Self, String> {
        let config: ClusterConfig = serde_json::from_str(json).map_err(|e| {
            let msg = format!("Failed to parse SWARM_CONFIG JSON: {e}");
            eprintln!("{msg}");
            msg
        })?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_id.trim().is_empty() {
            return Err("cluster_id must be non-empty".to_string());
        }

        let mut seen_addrs: HashSet<SocketAddr> = HashSet::new();

        for (name, node) in &self.nodes {
            let addr: SocketAddr = node.gossip_addr.parse().map_err(|e| {
                format!(
                    "node '{name}': gossip_addr '{}' is not a valid SocketAddr: {e}",
                    node.gossip_addr
                )
            })?;

            if !seen_addrs.insert(addr) {
                return Err(format!(
                    "node '{name}': gossip_addr '{}' is a duplicate (already used by another node)",
                    node.gossip_addr
                ));
            }

            for ext in &node.extensions {
                if let Some(ref cfg) = ext.config {
                    let has_host = cfg.get("host").and_then(|v| v.as_str()).is_some();
                    let has_port = cfg.get("port").and_then(|v| v.as_u64()).is_some();
                    if has_host && !has_port {
                        return Err(format!(
                            "node '{name}', extension '{}': config.host is set but config.port is missing",
                            ext.name
                        ));
                    }
                }
            }
        }

        if self.distributed_engine {
            let has_scheduler = self.nodes.values().any(|n| n.roles.contains(&"scheduler".to_string()));
            if !has_scheduler {
                return Err(
                    "distributed_engine is enabled but no node has the 'scheduler' role".to_string(),
                );
            }

            for (name, node) in &self.nodes {
                if node.roles.contains(&"executor".to_string()) {
                    let has_flight = node.extensions.iter().any(|e| e.name == "flight");
                    if !has_flight {
                        eprintln!(
                            "Warning: node '{}' has executor role but no 'flight' extension configured \
                             (required for distributed query transport)",
                            name,
                        );
                    }
                }
            }
        }

        Ok(())
    }
}

pub fn get_node_name() -> Result<String, String> {
    env::var("SWARM_NODE")
        .map_err(|_| "SWARM_NODE environment variable is not set".to_string())
}

/// Look up this node (via `SWARM_NODE` env var) in the cluster config.
pub fn get_this_node_config(config: &ClusterConfig) -> Option<(&str, &NodeConfig)> {
    let name = env::var("SWARM_NODE").ok()?;
    get_node_config_by_name(config, &name)
}

/// Look up a node by name in the cluster config.
pub fn get_node_config_by_name<'a>(config: &'a ClusterConfig, name: &str) -> Option<(&'a str, &'a NodeConfig)> {
    config
        .nodes
        .get_key_value(name)
        .map(|(k, v)| (k.as_str(), v))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_json() -> &'static str {
        r#"{
            "cluster_id": "test-cluster",
            "nodes": {
                "node-a": {
                    "gossip_addr": "127.0.0.1:7100",
                    "extensions": [
                        { "name": "flight", "config": {"host": "0.0.0.0", "port": 8815} }
                    ]
                },
                "node-b": {
                    "gossip_addr": "127.0.0.1:7101",
                    "data_node": false
                }
            }
        }"#
    }

    #[test]
    fn parse_valid_config() {
        let cfg = ClusterConfig::from_json(sample_json()).unwrap();
        assert_eq!(cfg.cluster_id, "test-cluster");
        assert_eq!(cfg.nodes.len(), 2);

        let a = &cfg.nodes["node-a"];
        assert!(a.data_node); // default_true
        assert_eq!(a.extensions.len(), 1);
        assert_eq!(a.extensions[0].name, "flight");
        let ext_cfg = a.extensions[0].config.as_ref().unwrap();
        assert_eq!(ext_cfg["port"].as_u64(), Some(8815));

        let b = &cfg.nodes["node-b"];
        assert!(!b.data_node);
        assert!(b.extensions.is_empty());
    }

    #[test]
    fn parse_with_tls() {
        let json = r#"{
            "cluster_id": "secure",
            "tls": { "ca_cert": "/etc/ssl/ca.pem" },
            "nodes": {
                "n1": {
                    "gossip_addr": "10.0.0.1:7100",
                    "tls": { "cert": "/etc/ssl/n1.pem", "key": "/etc/ssl/n1.key" }
                }
            }
        }"#;
        let cfg = ClusterConfig::from_json(json).unwrap();
        assert_eq!(cfg.tls.as_ref().unwrap().ca_cert, "/etc/ssl/ca.pem");
        let n1 = &cfg.nodes["n1"];
        assert_eq!(n1.tls.as_ref().unwrap().cert, "/etc/ssl/n1.pem");
        assert_eq!(n1.tls.as_ref().unwrap().key, "/etc/ssl/n1.key");
    }

    #[test]
    fn empty_cluster_id_rejected() {
        let json = r#"{ "cluster_id": "  ", "nodes": {} }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("cluster_id"), "error was: {err}");
    }

    #[test]
    fn invalid_gossip_addr_rejected() {
        let json = r#"{
            "cluster_id": "c",
            "nodes": { "n": { "gossip_addr": "not-a-socket-addr" } }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("gossip_addr"), "error was: {err}");
        assert!(err.contains("not-a-socket-addr"), "error was: {err}");
    }

    #[test]
    fn duplicate_gossip_addr_rejected() {
        let json = r#"{
            "cluster_id": "c",
            "nodes": {
                "a": { "gossip_addr": "127.0.0.1:7100" },
                "b": { "gossip_addr": "127.0.0.1:7100" }
            }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("duplicate"), "error was: {err}");
    }

    #[test]
    fn host_without_port_rejected() {
        let json = r#"{
            "cluster_id": "c",
            "nodes": {
                "n": {
                    "gossip_addr": "127.0.0.1:7100",
                    "extensions": [{ "name": "x", "config": {"host": "0.0.0.0"} }]
                }
            }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("port"), "error was: {err}");
    }

    #[test]
    fn malformed_json_returns_err() {
        let result = ClusterConfig::from_json("{ not valid json }}}");
        assert!(result.is_err());
    }

    #[test]
    fn get_node_name_from_env() {
        env::set_var("SWARM_NODE", "node-a");
        assert_eq!(get_node_name().unwrap(), "node-a");
        env::remove_var("SWARM_NODE");
    }

    #[test]
    fn get_node_config_by_name_returns_matching_node() {
        let cfg = ClusterConfig::from_json(sample_json()).unwrap();

        let (name, node) = get_node_config_by_name(&cfg, "node-b").unwrap();
        assert_eq!(name, "node-b");
        assert!(!node.data_node);
    }

    #[test]
    fn get_node_config_by_name_returns_none_for_unknown() {
        let cfg = ClusterConfig::from_json(sample_json()).unwrap();

        assert!(get_node_config_by_name(&cfg, "no-such-node").is_none());
    }

    #[test]
    fn distributed_engine_requires_scheduler_role() {
        let json = r#"{
            "cluster_id": "c",
            "distributed_engine": true,
            "nodes": {
                "n": {
                    "gossip_addr": "127.0.0.1:7100",
                    "roles": ["executor"],
                    "extensions": [{ "name": "flight", "config": {"host": "0.0.0.0", "port": 8815} }]
                }
            }
        }"#;
        let err = ClusterConfig::from_json(json).unwrap_err();
        assert!(err.contains("scheduler"), "error was: {err}");
    }

    #[test]
    fn distributed_engine_with_scheduler_role_ok() {
        let json = r#"{
            "cluster_id": "c",
            "distributed_engine": true,
            "nodes": {
                "n": {
                    "gossip_addr": "127.0.0.1:7100",
                    "roles": ["scheduler"],
                    "extensions": [{ "name": "flight", "config": {"host": "0.0.0.0", "port": 8815} }]
                }
            }
        }"#;
        assert!(ClusterConfig::from_json(json).is_ok());
    }

    #[test]
    fn distributed_engine_false_no_scheduler_ok() {
        let json = r#"{
            "cluster_id": "c",
            "distributed_engine": false,
            "nodes": {
                "n": {
                    "gossip_addr": "127.0.0.1:7100",
                    "roles": ["executor"]
                }
            }
        }"#;
        assert!(ClusterConfig::from_json(json).is_ok());
    }
}
