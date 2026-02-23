use crate::config::ExtensionConfig;
use crate::gossip::GossipRegistry;
use crate::logging::SwarmLogger;
use crate::service_functions::get_start_service_sql;

/// Load extensions, start their services, and publish endpoints to gossip.
pub fn orchestrate_extensions(extensions: &[ExtensionConfig]) -> Vec<String> {
    let conn_arc = match crate::get_shared_connection() {
        Some(c) => c,
        None => {
            SwarmLogger::error("orchestrator", "Shared trexsql connection is not available");
            return extensions
                .iter()
                .map(|ext| format!("{}: error — no shared connection", ext.name))
                .collect();
        }
    };

    let conn = match conn_arc.lock() {
        Ok(c) => c,
        Err(_) => {
            SwarmLogger::error("orchestrator", "Shared connection lock poisoned");
            return extensions
                .iter()
                .map(|ext| format!("{}: error — connection lock poisoned", ext.name))
                .collect();
        }
    };
    let mut statuses: Vec<String> = Vec::with_capacity(extensions.len());

    for ext in extensions {
        if !crate::catalog::is_valid_extension_name(&ext.name) {
            let msg = format!("{}: invalid extension name", ext.name);
            SwarmLogger::error("orchestrator", &msg);
            statuses.push(msg);
            continue;
        }
        let load_sql = format!("LOAD '{}.trex'", ext.name);
        SwarmLogger::info("orchestrator", &format!("Loading extension: {}", ext.name));

        if let Err(e) = conn.execute_batch(&load_sql) {
            let msg = format!("{}: load failed — {}", ext.name, e);
            SwarmLogger::error("orchestrator", &msg);
            statuses.push(msg);
            continue;
        }

        let config_json = match &ext.config {
            Some(cfg) => serde_json::to_string(cfg).unwrap_or_else(|_| "{}".to_string()),
            None => {
                let msg = format!("{}: loaded", ext.name);
                SwarmLogger::info("orchestrator", &msg);
                statuses.push(msg);
                continue;
            }
        };

        let start_sql = match get_start_service_sql(&ext.name, &config_json) {
            Ok(Some(sql)) => sql,
            Ok(None) => {
                SwarmLogger::warn(
                    "orchestrator",
                    &format!(
                        "No start function mapping for extension '{}'; loaded only",
                        ext.name
                    ),
                );
                statuses.push(format!("{}: loaded (no start function)", ext.name));
                continue;
            }
            Err(e) => {
                let msg = format!("{}: config error — {}", ext.name, e);
                SwarmLogger::error("orchestrator", &msg);
                statuses.push(msg);
                continue;
            }
        };

        let cfg_val: serde_json::Value =
            serde_json::from_str(&config_json).unwrap_or_default();
        let host = cfg_val["host"].as_str().unwrap_or("");
        let port = cfg_val["port"].as_u64().unwrap_or(0);

        SwarmLogger::info(
            "orchestrator",
            &format!("Starting service: {} on {}:{}", ext.name, host, port),
        );

        if let Err(e) = conn.execute_batch(&start_sql) {
            let msg = format!("{}: start failed — {}", ext.name, e);
            SwarmLogger::error("orchestrator", &msg);
            statuses.push(msg);
            continue;
        }

        let registry = GossipRegistry::instance();
        if registry.is_running() {
            let gossip_key = format!("service:{}", ext.name);
            let gossip_value = serde_json::json!({
                "host": host,
                "port": port,
                "status": "running",
                "config": cfg_val
            })
            .to_string();

            if let Err(e) = registry.set_key(&gossip_key, &gossip_value) {
                SwarmLogger::warn(
                    "orchestrator",
                    &format!("Failed to publish service:{} to gossip: {}", ext.name, e),
                );
            }
        }

        let msg = format!("{}: started on {}:{}", ext.name, host, port);
        SwarmLogger::info("orchestrator", &msg);
        statuses.push(msg);
    }

    statuses
}

/// Start distributed scheduler/executor based on node roles.
pub fn start_distributed_for_roles(
    roles: &[String],
    gossip_addr: &str,
) -> Vec<String> {
    let mut statuses = Vec::new();

    for role in roles {
        match role.as_str() {
            "scheduler" => {
                let host = gossip_addr
                    .split(':')
                    .next()
                    .unwrap_or("0.0.0.0");

                let config = crate::distributed_scheduler::SchedulerConfig {
                    bind_addr: format!("{}:50050", host),
                };

                match crate::distributed_scheduler::start_scheduler(config) {
                    Ok(()) => {
                        let msg = format!("distributed-scheduler: started on {}:50050", host);
                        SwarmLogger::info("orchestrator", &msg);
                        statuses.push(msg);

                        let registry = GossipRegistry::instance();
                        if registry.is_running() {
                            let value = serde_json::json!({
                                "host": host,
                                "port": 50050,
                                "status": "running",
                            })
                            .to_string();
                            if let Err(e) = registry.set_key("service:distributed-scheduler", &value)
                            {
                                SwarmLogger::warn(
                                    "orchestrator",
                                    &format!(
                                        "Failed to publish service:distributed-scheduler to gossip: {}",
                                        e
                                    ),
                                );
                            }
                        }
                    }
                    Err(e) => {
                        let msg = format!("distributed-scheduler: failed — {}", e);
                        SwarmLogger::error("orchestrator", &msg);
                        statuses.push(msg);
                    }
                }
            }
            "executor" => {
                let has_flight = crate::config::ClusterConfig::from_env()
                    .ok()
                    .and_then(|cfg| {
                        crate::config::get_this_node_config(&cfg)
                            .map(|(_, node)| node.extensions.iter().any(|e| e.name == "flight"))
                    })
                    .unwrap_or(false);

                if has_flight {
                    let msg =
                        "distributed-executor: Flight extension configured (handles remote queries)"
                            .to_string();
                    SwarmLogger::info("orchestrator", &msg);
                    statuses.push(msg);
                } else {
                    let msg =
                        "distributed-executor: WARNING — no Flight extension configured; \
                         this executor node cannot serve remote queries"
                            .to_string();
                    SwarmLogger::warn("orchestrator", &msg);
                    statuses.push(msg);
                }
            }
            _ => {}
        }
    }

    statuses
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_sql_flight() {
        let sql = get_start_service_sql("flight", r#"{"host":"0.0.0.0","port":8815}"#)
            .unwrap()
            .unwrap();
        assert_eq!(sql, "SELECT start_flight_server('0.0.0.0', 8815)");
    }

    #[test]
    fn start_sql_flight_tls() {
        let sql = get_start_service_sql(
            "flight",
            r#"{"host":"0.0.0.0","port":8815,"cert_path":"/x/cert.pem","key_path":"/x/key.pem","ca_cert_path":"/x/ca.pem"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            sql,
            "SELECT start_flight_server_tls('0.0.0.0', 8815, '/x/cert.pem', '/x/key.pem', '/x/ca.pem')"
        );
    }

    #[test]
    fn start_sql_pgwire() {
        let sql = get_start_service_sql("pgwire", r#"{"host":"127.0.0.1","port":5432}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            sql,
            "SELECT start_pgwire_server('127.0.0.1', 5432, '', '')"
        );
    }

    #[test]
    fn start_sql_pgwire_with_password() {
        let sql = get_start_service_sql(
            "pgwire",
            r#"{"host":"127.0.0.1","port":5432,"password":"secret"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            sql,
            "SELECT start_pgwire_server('127.0.0.1', 5432, 'secret', '')"
        );
    }

    #[test]
    fn start_sql_trexas() {
        let json = r#"{"host":"10.0.0.1","port":9090}"#;
        let sql = get_start_service_sql("trexas", json).unwrap().unwrap();
        let escaped = json.replace('\'', "''");
        assert_eq!(
            sql,
            format!("SELECT trex_start_server_with_config('{escaped}')")
        );
    }

    #[test]
    fn start_sql_chdb_no_path() {
        let sql = get_start_service_sql("chdb", "{}").unwrap().unwrap();
        assert_eq!(sql, "SELECT chdb_start_database()");
    }

    #[test]
    fn start_sql_chdb_with_path() {
        let sql =
            get_start_service_sql("chdb", r#"{"data_path":"/tmp/chdb"}"#)
                .unwrap()
                .unwrap();
        assert_eq!(sql, "SELECT chdb_start_database('/tmp/chdb')");
    }

    #[test]
    fn start_sql_unknown_returns_none() {
        assert!(get_start_service_sql("hana", "{}").unwrap().is_none());
        assert!(get_start_service_sql("llama", "{}").unwrap().is_none());
        assert!(get_start_service_sql("nonexistent", "{}").unwrap().is_none());
    }

    #[test]
    fn start_sql_invalid_json_returns_err() {
        assert!(get_start_service_sql("flight", "not json").is_err());
    }

    #[test]
    fn orchestrate_without_connection_returns_error_per_extension() {
        let extensions = vec![
            ExtensionConfig {
                name: "hana".to_string(),
                config: None,
            },
            ExtensionConfig {
                name: "flight".to_string(),
                config: Some(serde_json::json!({"host": "0.0.0.0", "port": 8815})),
            },
        ];

        let statuses = orchestrate_extensions(&extensions);

        assert_eq!(statuses.len(), 2);
        for status in &statuses {
            assert!(
                status.contains("no shared connection"),
                "unexpected status: {status}"
            );
        }
    }
}
