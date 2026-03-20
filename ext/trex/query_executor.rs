//! Thread pool executor for parallel DuckDB query execution.

use crossbeam_channel::{unbounded, Receiver, Sender};
use duckdb::Connection;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use tracing::warn;

pub struct QueryRequest {
  pub database: String,
  pub sql: String,
  pub params_json: String,
  pub response_tx: std::sync::mpsc::SyncSender<QueryResult>,
}

pub enum QueryResult {
  Success(String),
  Error(String),
}

struct Worker {
  _handle: JoinHandle<()>,
}

/// Distributes queries across a pool of worker threads with pre-cloned connections.
pub struct QueryExecutor {
  senders: Vec<Sender<QueryRequest>>,
  #[allow(dead_code)]
  workers: Vec<Worker>,
  next_worker: AtomicUsize,
}

impl QueryExecutor {
  /// Creates executor pool. Must be called from the connection's origin thread.
  pub fn new(
    connection: &Connection,
    pool_size: usize,
  ) -> Result<Self, String> {
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
      let (sender, receiver): (Sender<QueryRequest>, Receiver<QueryRequest>) =
        unbounded();
      senders.push(sender);
      let handle = thread::Builder::new()
        .name(format!("trex-executor-{i}"))
        .spawn(move || worker_loop(conn, receiver))
        .map_err(|e| format!("spawn worker {i}: {e}"))?;
      workers.push(Worker { _handle: handle });
    }

    Ok(Self {
      senders,
      workers,
      next_worker: AtomicUsize::new(0),
    })
  }

  /// Returns next worker index via round-robin.
  pub fn next_worker_id(&self) -> usize {
    self.next_worker.fetch_add(1, Ordering::Relaxed) % self.senders.len()
  }

  /// Sends request to a specific worker's channel (pinned connection).
  pub fn submit_to(
    &self,
    worker_id: usize,
    database: String,
    sql: String,
    params_json: String,
  ) -> std::sync::mpsc::Receiver<QueryResult> {
    let (response_tx, response_rx) = std::sync::mpsc::sync_channel(1);

    let sender = &self.senders[worker_id % self.senders.len()];
    if let Err(e) = sender.send(QueryRequest {
      database,
      sql,
      params_json,
      response_tx,
    }) {
      let (tx, rx) = std::sync::mpsc::sync_channel(1);
      let _ = tx.send(QueryResult::Error(format!("executor closed: {e}")));
      return rx;
    }

    response_rx
  }

  /// Sends request via round-robin (unpinned).
  pub fn submit(
    &self,
    database: String,
    sql: String,
    params_json: String,
  ) -> std::sync::mpsc::Receiver<QueryResult> {
    let worker_id = self.next_worker_id();
    self.submit_to(worker_id, database, sql, params_json)
  }

  pub fn pool_size(&self) -> usize {
    self.senders.len()
  }
}

fn worker_loop(conn: Connection, receiver: Receiver<QueryRequest>) {
  while let Ok(req) = receiver.recv() {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
      execute_query(&conn, &req.database, &req.sql, &req.params_json)
    }));
    let query_result = match result {
      Ok(r) => r,
      Err(panic_err) => {
        let msg = crate::extract_panic_message(panic_err);
        warn!(error = %msg, "query panicked");
        QueryResult::Error(format!("query panicked: {msg}"))
      }
    };
    let _ = req.response_tx.send(query_result);
  }
}

fn execute_query(
  conn: &Connection,
  database: &str,
  sql: &str,
  params_json: &str,
) -> QueryResult {
  use duckdb::arrow::record_batch::RecordBatch;
  use duckdb::params_from_iter;

  if let Err(e) = conn.execute(&format!("USE {database}"), []) {
    warn!(database, error = %e, "failed to switch database");
  }

  if sql.trim().is_empty() {
    return QueryResult::Success("[]".to_string());
  }

  let params: Vec<crate::TrexType> = match serde_json::from_str(params_json) {
    Ok(p) => p,
    Err(e) => return QueryResult::Error(format!("param parse: {e}")),
  };

  match conn.prepare(sql) {
    Ok(mut stmt) => match stmt.query_arrow(params_from_iter(params.iter())) {
      Ok(iter) => {
        let batches: Vec<RecordBatch> = iter.collect();
        QueryResult::Success(crate::record_batches_to_json(&batches))
      }
      Err(e) => QueryResult::Error(format!("query exec: {e}")),
    },
    Err(e) => {
      use std::error::Error as StdError;
      let mut msg = e.to_string();
      let mut source = (&e as &dyn StdError).source();
      while let Some(s) = source {
        msg = format!("{msg}: {s}");
        source = s.source();
      }
      QueryResult::Error(msg)
    }
  }
}
