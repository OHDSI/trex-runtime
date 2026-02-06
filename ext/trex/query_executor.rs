//! Thread pool executor for parallel DuckDB query execution.

use crossbeam_channel::{unbounded, Receiver, Sender};
use duckdb::Connection;
use std::thread::{self, JoinHandle};
use tracing::warn;

pub const EXECUTOR_POOL_SIZE: usize = 4;

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
  sender: Sender<QueryRequest>,
  #[allow(dead_code)]
  workers: Vec<Worker>,
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

    let (sender, receiver): (Sender<QueryRequest>, Receiver<QueryRequest>) =
      unbounded();

    let mut workers = Vec::with_capacity(pool_size);
    for (i, conn) in connections.into_iter().enumerate() {
      let rx = receiver.clone();
      let handle = thread::Builder::new()
        .name(format!("trex-executor-{i}"))
        .spawn(move || worker_loop(conn, rx))
        .map_err(|e| format!("spawn worker {i}: {e}"))?;
      workers.push(Worker { _handle: handle });
    }

    Ok(Self { sender, workers })
  }

  pub fn submit(
    &self,
    database: String,
    sql: String,
    params_json: String,
  ) -> std::sync::mpsc::Receiver<QueryResult> {
    let (response_tx, response_rx) = std::sync::mpsc::sync_channel(1);

    if let Err(e) = self.sender.send(QueryRequest {
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
}

fn worker_loop(conn: Connection, receiver: Receiver<QueryRequest>) {
  while let Ok(req) = receiver.recv() {
    let result =
      execute_query(&conn, &req.database, &req.sql, &req.params_json);
    let _ = req.response_tx.send(result);
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
