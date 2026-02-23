use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::sync::oneshot;

pub struct ServerHandle {
    pub thread_handle: Option<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
    pub shutdown_tx: oneshot::Sender<()>,
    pub start_time: std::time::SystemTime,
    pub tls_enabled: bool,
}

pub struct ServerRegistry {
    servers: Arc<Mutex<HashMap<String, ServerHandle>>>,
}

impl ServerRegistry {
    pub fn new() -> Self {
        Self {
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn instance() -> &'static ServerRegistry {
        static INSTANCE: std::sync::OnceLock<ServerRegistry> = std::sync::OnceLock::new();
        INSTANCE.get_or_init(|| ServerRegistry::new())
    }

    fn server_key(host: &str, port: u16) -> String {
        format!("{}:{}", host, port)
    }

    /// Atomically check availability and reserve a slot before spawning.
    pub fn reserve(
        &self,
        host: &str,
        port: u16,
        shutdown_tx: oneshot::Sender<()>,
        tls_enabled: bool,
    ) -> Result<(), String> {
        let mut servers = self.servers.lock().unwrap();
        let key = Self::server_key(host, port);

        if servers.contains_key(&key) {
            return Err(format!("Server already running on {}:{}", host, port));
        }

        servers.insert(
            key,
            ServerHandle {
                thread_handle: None,
                shutdown_tx,
                start_time: std::time::SystemTime::now(),
                tls_enabled,
            },
        );
        Ok(())
    }

    /// Attach the spawned thread handle to a reserved slot.
    pub fn set_thread_handle(
        &self,
        host: &str,
        port: u16,
        handle: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    ) {
        let mut servers = self.servers.lock().unwrap();
        let key = Self::server_key(host, port);
        if let Some(entry) = servers.get_mut(&key) {
            entry.thread_handle = Some(handle);
        }
    }

    /// Remove a reserved slot (e.g. when thread spawn fails).
    pub fn deregister(&self, host: &str, port: u16) {
        let mut servers = self.servers.lock().unwrap();
        let key = Self::server_key(host, port);
        servers.remove(&key);
    }

    pub fn stop_server(&self, host: &str, port: u16) -> Result<String, String> {
        let handle = {
            let mut servers = self.servers.lock().unwrap();
            let key = Self::server_key(host, port);
            servers.remove(&key)
        };

        if let Some(handle) = handle {
            let _ = handle.shutdown_tx.send(());
            if let Some(th) = handle.thread_handle {
                let _ = th.join();
            }
            Ok(format!("Server {}:{} stopped", host, port))
        } else {
            Err(format!("No server running on {}:{}", host, port))
        }
    }

    pub fn get_servers_info(&self) -> Vec<(String, u16, u64, bool)> {
        let servers = self.servers.lock().unwrap();
        let mut server_info = Vec::new();

        for (key, handle) in servers.iter() {
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() == 2 {
                let host = parts[0].to_string();
                let port = parts[1].parse::<u16>().unwrap_or(0);
                let uptime_secs = handle
                    .start_time
                    .elapsed()
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let tls_enabled = handle.tls_enabled;

                server_info.push((host, port, uptime_secs, tls_enabled));
            }
        }

        server_info
    }
}
