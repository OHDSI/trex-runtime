use crossbeam_channel::{unbounded, Receiver, Sender};
use duckdb::Connection;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

pub struct QueryRequest {
    pub query: String,
    pub params: Vec<String>,
    pub response_tx: tokio::sync::oneshot::Sender<QueryResult>,
}

#[derive(Debug)]
pub enum QueryResult {
    Select {
        rows: Vec<Vec<serde_json::Value>>,
        columns: Vec<String>,
    },
    Execute {
        rows_affected: usize,
    },
    Json(String),
    Error(String),
}

struct Worker {
    handle: Option<JoinHandle<()>>,
}

pub struct QueryExecutor {
    senders: Vec<Sender<QueryRequest>>,
    workers: Vec<Worker>,
    next_worker: AtomicUsize,
}

impl Drop for QueryExecutor {
    fn drop(&mut self) {
        self.senders.clear();
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

impl QueryExecutor {
    pub fn new(connection: &Connection, pool_size: usize) -> Result<Self, String> {
        if pool_size == 0 {
            return Err("pool_size must be > 0".into());
        }
        let mut connections = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            connections.push(
                connection
                    .try_clone()
                    .map_err(|e| format!("connection clone {i}: {e}"))?,
            );
        }

        let mut senders = Vec::with_capacity(pool_size);
        let mut workers = Vec::with_capacity(pool_size);
        for (i, conn) in connections.into_iter().enumerate() {
            let (tx, rx): (Sender<QueryRequest>, Receiver<QueryRequest>) = unbounded();
            senders.push(tx);
            let handle = thread::Builder::new()
                .name(format!("fhir-executor-{i}"))
                .spawn(move || worker_loop(conn, rx))
                .map_err(|e| format!("spawn worker {i}: {e}"))?;
            workers.push(Worker {
                handle: Some(handle),
            });
        }

        Ok(Self {
            senders,
            workers,
            next_worker: AtomicUsize::new(0),
        })
    }

    pub fn next_worker_id(&self) -> usize {
        self.next_worker.fetch_add(1, Ordering::Relaxed) % self.senders.len()
    }

    pub fn submit_to(
        &self,
        worker_id: usize,
        query: String,
    ) -> tokio::sync::oneshot::Receiver<QueryResult> {
        let sender = &self.senders[worker_id % self.senders.len()];
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        if let Err(e) = sender.send(QueryRequest {
            query,
            params: vec![],
            response_tx,
        }) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QueryResult::Error(format!("executor closed: {e}")));
            return rx;
        }

        response_rx
    }

    pub async fn submit(&self, query: String) -> QueryResult {
        let worker_id = self.next_worker_id();
        self.submit_on(worker_id, query).await
    }

    pub async fn submit_on(&self, worker_id: usize, query: String) -> QueryResult {
        let rx = self.submit_to(worker_id, query);
        rx.await.unwrap_or(QueryResult::Error(
            "Query execution channel closed".to_string(),
        ))
    }

    pub async fn submit_params(&self, query: String, params: Vec<String>) -> QueryResult {
        let rx = self.submit_with_params(query, params);
        rx.await.unwrap_or(QueryResult::Error(
            "Query execution channel closed".to_string(),
        ))
    }

    pub fn submit_with_params(
        &self,
        query: String,
        params: Vec<String>,
    ) -> tokio::sync::oneshot::Receiver<QueryResult> {
        let worker_id = self.next_worker_id();
        let sender = &self.senders[worker_id % self.senders.len()];
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        if let Err(e) = sender.send(QueryRequest {
            query,
            params,
            response_tx,
        }) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QueryResult::Error(format!("executor closed: {e}")));
            return rx;
        }

        response_rx
    }

    pub fn pool_size(&self) -> usize {
        self.workers.len()
    }

    /// Run a trivial query on every worker to force catalog refresh after DDL.
    pub async fn sync_all(&self) {
        let mut rxs = Vec::new();
        for i in 0..self.senders.len() {
            rxs.push(self.submit_to(i, "SELECT 1".to_string()));
        }
        for rx in rxs {
            let _ = rx.await;
        }
    }
}

fn worker_loop(conn: Connection, receiver: Receiver<QueryRequest>) {
    while let Ok(req) = receiver.recv() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            execute_query(&conn, &req.query, &req.params)
        }));
        match result {
            Ok(r) => {
                let _ = req.response_tx.send(r);
            }
            Err(panic_err) => {
                let msg = extract_panic_message(panic_err);
                eprintln!("[fhir] Worker panicked, terminating: {msg}");
                let _ = req.response_tx.send(QueryResult::Error(format!("query panicked: {msg}")));
                break; // Connection unsafe after panic
            }
        };
    }
}

fn extract_panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn execute_query(conn: &Connection, query: &str, params: &[String]) -> QueryResult {
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("FROM")
        || upper.starts_with("VALUES")
    {
        execute_select(conn, trimmed, params)
    } else {
        execute_non_select(conn, trimmed, params)
    }
}

fn execute_select(conn: &Connection, query: &str, params: &[String]) -> QueryResult {
    let result = conn.prepare(query).and_then(|mut stmt| {
        // Execute first — column_count/column_name panic if not yet executed.
        if params.is_empty() {
            stmt.execute(duckdb::params![])?;
        } else {
            let param_refs: Vec<&dyn duckdb::types::ToSql> =
                params.iter().map(|s| s as &dyn duckdb::types::ToSql).collect();
            stmt.execute(param_refs.as_slice())?;
        }

        let col_count = stmt.column_count();
        let columns: Vec<String> = (0..col_count)
            .map(|i| {
                stmt.column_name(i)
                    .map(|n| n.clone())
                    .unwrap_or_else(|_| "?".to_string())
            })
            .collect();

        let mut rows = stmt.raw_query();
        let mut rows_data = Vec::new();
        while let Some(row) = rows.next()? {
            let mut row_values = Vec::new();
            for i in 0..col_count {
                let val: Result<String, _> = row.get(i);
                match val {
                    Ok(s) => row_values.push(serde_json::Value::String(s)),
                    Err(_) => row_values.push(serde_json::Value::Null),
                }
            }
            rows_data.push(row_values);
        }
        Ok((columns, rows_data))
    });

    match result {
        Ok((columns, rows)) => QueryResult::Select { rows, columns },
        Err(e) => QueryResult::Error(format!("query: {e}")),
    }
}

fn execute_non_select(conn: &Connection, query: &str, params: &[String]) -> QueryResult {
    let result = if params.is_empty() {
        conn.execute_batch(query).map(|_| 0)
    } else {
        let param_refs: Vec<&dyn duckdb::types::ToSql> =
            params.iter().map(|s| s as &dyn duckdb::types::ToSql).collect();
        conn.execute(query, param_refs.as_slice())
    };

    match result {
        Ok(rows) => QueryResult::Execute {
            rows_affected: rows,
        },
        Err(e) => QueryResult::Error(format!("exec: {e}")),
    }
}
