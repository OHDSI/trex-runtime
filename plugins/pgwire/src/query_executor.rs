//! Thread pool executor for parallel trexsql query execution.

use crossbeam_channel::{unbounded, Receiver, Sender};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, Connection};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

pub struct QueryRequest {
    pub query: String,
    pub response_tx: tokio::sync::oneshot::Sender<QueryResult>,
}

pub enum QueryResult {
    Select {
        schema: Arc<duckdb::arrow::datatypes::Schema>,
        batches: Vec<RecordBatch>,
    },
    Execute {
        rows_affected: usize,
    },
    Error(String),
}

struct Worker {
    handle: Option<JoinHandle<()>>,
}

/// Distributes queries across a pool of worker threads with pre-cloned connections.
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
    /// Creates executor pool. Must be called from the connection's origin thread.
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
                .name(format!("pgwire-executor-{i}"))
                .spawn(move || worker_loop(conn, rx))
                .map_err(|e| format!("spawn worker {i}: {e}"))?;
            workers.push(Worker { handle: Some(handle) });
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

    /// Sends request to a specific worker's channel (pinned connection).
    pub fn submit_to(
        &self,
        worker_id: usize,
        query: String,
    ) -> tokio::sync::oneshot::Receiver<QueryResult> {
        let sender = &self.senders[worker_id % self.senders.len()];
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        if let Err(e) = sender.send(QueryRequest { query, response_tx }) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tx.send(QueryResult::Error(format!("executor closed: {e}")));
            return rx;
        }

        response_rx
    }

    pub fn submit(&self, query: String) -> tokio::sync::oneshot::Receiver<QueryResult> {
        let worker_id = self.next_worker_id();
        self.submit_to(worker_id, query)
    }

    pub fn pool_size(&self) -> usize {
        self.workers.len()
    }
}

fn worker_loop(conn: Connection, receiver: Receiver<QueryRequest>) {
    while let Ok(req) = receiver.recv() {
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            execute_query(&conn, &req.query)
        }));
        let query_result = match result {
            Ok(r) => r,
            Err(panic_err) => {
                let msg = extract_panic_message(panic_err);
                eprintln!("pgwire query panicked: {msg}");
                QueryResult::Error(format!("query panicked: {msg}"))
            }
        };
        let _ = req.response_tx.send(query_result);
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

fn execute_query(conn: &Connection, query: &str) -> QueryResult {
    let trimmed = query.trim();
    let upper = trimmed.to_uppercase();

    if is_result_returning_query(&upper) {
        match execute_select(conn, trimmed) {
            Ok((schema, batches)) => QueryResult::Select { schema, batches },
            Err(e) => QueryResult::Error(e),
        }
    } else {
        match execute_non_select(conn, trimmed) {
            Ok(rows) => QueryResult::Execute { rows_affected: rows },
            Err(e) => QueryResult::Error(e),
        }
    }
}

fn is_result_returning_query(upper: &str) -> bool {
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
        || upper.starts_with("FROM")
        || (upper.starts_with("PRAGMA") && !is_action_pragma(upper))
}

/// Action PRAGMAs that internally expand to multiple statements and cannot be prepared.
fn is_action_pragma(upper: &str) -> bool {
    let after_pragma = upper["PRAGMA".len()..].trim_start();
    after_pragma.starts_with("CREATE_FTS_INDEX")
        || after_pragma.starts_with("DROP_FTS_INDEX")
        || after_pragma.starts_with("COPY_DATABASE")
        || after_pragma.starts_with("IMPORT_DATABASE")
}

fn execute_select(
    conn: &Connection,
    query: &str,
) -> Result<(Arc<duckdb::arrow::datatypes::Schema>, Vec<RecordBatch>), String> {
    let mut stmt = conn.prepare(query).map_err(|e| format!("prepare: {e}"))?;
    let arrow_result = stmt.query_arrow(params![]).map_err(|e| format!("query: {e}"))?;
    Ok((arrow_result.get_schema(), arrow_result.collect()))
}

fn execute_non_select(conn: &Connection, query: &str) -> Result<usize, String> {
    let upper = query.to_uppercase();
    if upper.starts_with("SET")
        && (upper.contains("EXTRA_FLOAT_DIGITS") || upper.contains("APPLICATION_NAME"))
    {
        return Ok(0);
    }

    conn.execute_batch(query).map_err(|e| format!("exec: {e}"))?;
    Ok(0)
}
