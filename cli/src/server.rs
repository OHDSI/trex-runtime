use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use anyhow::Result;
use crate::ServerConfig;

/// Global server manager to track running servers
pub struct ServerManager {
    servers: Arc<Mutex<HashMap<String, ServerInfo>>>,
}

#[derive(Debug, Clone)]
struct ServerInfo {
    config: ServerConfig,
    // We can't store the actual handle here due to async nature,
    // but we can store metadata
    status: String,
    pid: Option<u32>,
    started_at: chrono::DateTime<chrono::Utc>,
}

impl ServerManager {
    pub fn new() -> Self {
        Self {
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register_server(&self, id: String, config: ServerConfig) -> Result<()> {
        let mut servers = self.servers.lock().unwrap();
        servers.insert(id, ServerInfo {
            config,
            status: "running".to_string(),
            pid: None,
            started_at: chrono::Utc::now(),
        });
        Ok(())
    }

    pub fn unregister_server(&self, id: &str) -> Result<()> {
        let mut servers = self.servers.lock().unwrap();
        servers.remove(id);
        Ok(())
    }

    pub fn list_servers(&self) -> Result<Vec<(String, ServerConfig, String)>> {
        let servers = self.servers.lock().unwrap();
        let result = servers
            .iter()
            .map(|(id, info)| (id.clone(), info.config.clone(), info.status.clone()))
            .collect();
        Ok(result)
    }

    pub fn get_server_info(&self, id: &str) -> Result<Option<(ServerConfig, String)>> {
        let servers = self.servers.lock().unwrap();
        if let Some(info) = servers.get(id) {
            Ok(Some((info.config.clone(), info.status.clone())))
        } else {
            Ok(None)
        }
    }

    pub fn stop_all_servers(&self) -> Result<usize> {
        let mut servers = self.servers.lock().unwrap();
        let count = servers.len();
        servers.clear();
        Ok(count)
    }
}

lazy_static::lazy_static! {
    static ref GLOBAL_SERVER_MANAGER: ServerManager = ServerManager::new();
}

pub fn get_global_server_manager() -> &'static ServerManager {
    &GLOBAL_SERVER_MANAGER
}
