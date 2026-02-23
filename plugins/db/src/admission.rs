//! Query admission control with priority queuing, per-user concurrency limits,
//! and memory estimation.

use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering as CmpOrdering;
use std::sync::{Mutex, OnceLock};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use crate::catalog;
use crate::gossip::GossipRegistry;
use crate::logging::SwarmLogger;
use crate::metrics;

/// Query priority levels. Higher numeric value = higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Batch = 0,
    Interactive = 1,
    System = 2,
}

impl Priority {
    pub fn from_str(s: &str) -> Option<Priority> {
        match s.to_lowercase().as_str() {
            "batch" => Some(Priority::Batch),
            "interactive" => Some(Priority::Interactive),
            "system" => Some(Priority::System),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Priority::Batch => "batch",
            Priority::Interactive => "interactive",
            Priority::System => "system",
        }
    }

    pub fn from_u8(v: u8) -> Priority {
        match v {
            0 => Priority::Batch,
            2 => Priority::System,
            _ => Priority::Interactive,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Status of a submitted query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Queued { position: usize },
    Running,
    Completed,
    Failed(String),
    Rejected(String),
    Cancelled,
}

impl QueryStatus {
    pub fn as_str(&self) -> String {
        match self {
            QueryStatus::Queued { position } => format!("queued({})", position),
            QueryStatus::Running => "running".to_string(),
            QueryStatus::Completed => "completed".to_string(),
            QueryStatus::Failed(msg) => format!("failed: {}", msg),
            QueryStatus::Rejected(msg) => format!("rejected: {}", msg),
            QueryStatus::Cancelled => "cancelled".to_string(),
        }
    }
}

/// A query waiting in the admission queue.
pub struct QueuedQuery {
    pub query_id: String,
    pub sql: String,
    pub user_id: String,
    pub priority: Priority,
    pub submitted_at: Instant,
    pub estimated_memory_bytes: u64,
}

/// Ordering: highest priority first, FIFO within same priority (max-heap).
impl Eq for QueuedQuery {}

impl PartialEq for QueuedQuery {
    fn eq(&self, other: &Self) -> bool {
        self.query_id == other.query_id
    }
}

impl PartialOrd for QueuedQuery {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedQuery {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.priority.cmp(&other.priority) {
            CmpOrdering::Equal => {
                // Reverse: earlier submitted_at = Greater in max-heap (FIFO).
                other.submitted_at.cmp(&self.submitted_at)
            }
            ord => ord,
        }
    }
}

pub struct AdmissionConfig {
    pub default_max_concurrent: usize,
    pub max_memory_utilization_pct: f64,
    pub max_queue_size: usize,
    pub timeout_secs: u64,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        AdmissionConfig {
            default_max_concurrent: 10,
            max_memory_utilization_pct: 85.0,
            max_queue_size: 100,
            timeout_secs: 300, // 5 minutes
        }
    }
}

struct UserState {
    active_count: usize,
    max_concurrent: usize,
}

struct ActiveQuery {
    _query_id: String,
    user_id: String,
    pub started_at: Instant,
}

pub struct ClusterStatus {
    pub total_nodes: usize,
    pub active_queries: usize,
    pub queued_queries: usize,
    pub memory_utilization_pct: f64,
}

pub struct QueryInfo {
    pub query_id: String,
    pub user_id: String,
    pub status: String,
    pub queue_position: String,
    pub submitted_at: String,
}

/// Process-wide query admission controller.
pub struct AdmissionController {
    queue: BinaryHeap<QueuedQuery>,
    active_queries: HashMap<String, ActiveQuery>,
    user_state: HashMap<String, UserState>,
    config: AdmissionConfig,
    submitted_times: HashMap<String, Instant>,
}

impl AdmissionController {
    pub fn new(config: AdmissionConfig) -> Self {
        AdmissionController {
            queue: BinaryHeap::new(),
            active_queries: HashMap::new(),
            user_state: HashMap::new(),
            config,
            submitted_times: HashMap::new(),
        }
    }

    /// Submit a query for admission. Returns (status, query_id).
    /// The query_id is needed to call `complete_query` when done.
    pub fn submit_query(
        &mut self,
        sql: &str,
        user_id: &str,
        priority: Priority,
    ) -> Result<(QueryStatus, String), String> {
        let query_id = uuid::Uuid::new_v4().to_string();
        let now = Instant::now();

        metrics::instance().record_query_submitted();

        let estimated_memory = estimate_query_memory_from_sql(sql);
        let mem_pct = self.current_memory_utilization_pct();
        if mem_pct >= self.config.max_memory_utilization_pct {
            metrics::instance().record_query_rejected();
            SwarmLogger::info(
                "admission",
                &format!(
                    "Query {} rejected: memory utilization {:.1}% >= {:.1}% threshold",
                    query_id, mem_pct, self.config.max_memory_utilization_pct,
                ),
            );
            self.update_gauges();
            return Ok((QueryStatus::Rejected(format!(
                "Memory utilization {:.1}% exceeds threshold {:.1}%",
                mem_pct, self.config.max_memory_utilization_pct,
            )), query_id));
        }

        let user = self
            .user_state
            .entry(user_id.to_string())
            .or_insert_with(|| UserState {
                active_count: 0,
                max_concurrent: self.config.default_max_concurrent,
            });

        if user.active_count >= user.max_concurrent {
            if self.queue.len() >= self.config.max_queue_size {
                metrics::instance().record_query_rejected();
                SwarmLogger::info(
                    "admission",
                    &format!(
                        "Query {} rejected: queue full ({}/{})",
                        query_id,
                        self.queue.len(),
                        self.config.max_queue_size,
                    ),
                );
                self.update_gauges();
                return Ok((QueryStatus::Rejected(format!(
                    "Queue full ({}/{})",
                    self.queue.len(),
                    self.config.max_queue_size,
                )), query_id));
            }

            let position = self.queue.len() + 1;
            self.queue.push(QueuedQuery {
                query_id: query_id.clone(),
                sql: sql.to_string(),
                user_id: user_id.to_string(),
                priority,
                submitted_at: now,
                estimated_memory_bytes: estimated_memory,
            });
            self.submitted_times.insert(query_id.clone(), now);

            SwarmLogger::debug(
                "admission",
                &format!(
                    "Query {} queued at position {} (user {} at {}/{} concurrent)",
                    query_id,
                    position,
                    user_id,
                    user.active_count,
                    user.max_concurrent,
                ),
            );

            self.update_gauges();
            return Ok((QueryStatus::Queued { position }, query_id));
        }

        user.active_count += 1;
        self.active_queries.insert(
            query_id.clone(),
            ActiveQuery {
                _query_id: query_id.clone(),
                user_id: user_id.to_string(),
                started_at: now,
            },
        );
        self.submitted_times.insert(query_id.clone(), now);

        SwarmLogger::debug(
            "admission",
            &format!(
                "Query {} admitted (user {} now at {}/{} concurrent)",
                query_id,
                user_id,
                user.active_count,
                user.max_concurrent,
            ),
        );

        self.update_gauges();
        Ok((QueryStatus::Running, query_id))
    }

    pub fn complete_query(&mut self, query_id: &str) {
        if let Some(active) = self.active_queries.remove(query_id) {
            let duration_secs = active.started_at.elapsed().as_secs_f64();
            if let Some(user) = self.user_state.get_mut(&active.user_id) {
                user.active_count = user.active_count.saturating_sub(1);
            }
            metrics::instance().record_query_completed(duration_secs);
            SwarmLogger::debug(
                "admission",
                &format!("Query {} completed ({:.3}s)", query_id, duration_secs),
            );
        }
        self.submitted_times.remove(query_id);
        self.update_gauges();
    }

    pub fn cancel_query(&mut self, query_id: &str) -> Result<QueryStatus, String> {
        if let Some(active) = self.active_queries.remove(query_id) {
            if let Some(user) = self.user_state.get_mut(&active.user_id) {
                user.active_count = user.active_count.saturating_sub(1);
            }
            self.submitted_times.remove(query_id);
            self.update_gauges();
            return Ok(QueryStatus::Cancelled);
        }

        // Rebuild heap without the cancelled entry.
        let drained: Vec<QueuedQuery> = self.queue.drain().collect();
        let mut found = false;
        for item in drained {
            if item.query_id == query_id {
                found = true;
            } else {
                self.queue.push(item);
            }
        }

        if found {
            self.submitted_times.remove(query_id);
            self.update_gauges();
            Ok(QueryStatus::Cancelled)
        } else {
            Err(format!("Query {} not found", query_id))
        }
    }

    pub fn get_query_status(&self, query_id: &str) -> Option<QueryStatus> {
        if self.active_queries.contains_key(query_id) {
            return Some(QueryStatus::Running);
        }

        // BinaryHeap iteration is unordered; sort for position.
        let mut queued: Vec<&QueuedQuery> = self.queue.iter().collect();
        queued.sort_by(|a, b| b.cmp(a));
        for (idx, q) in queued.iter().enumerate() {
            if q.query_id == query_id {
                let position = idx + 1;
                return Some(QueryStatus::Queued { position });
            }
        }

        None
    }

    pub fn get_cluster_status(&self) -> ClusterStatus {
        let total_nodes = count_cluster_nodes();
        ClusterStatus {
            total_nodes,
            active_queries: self.active_queries.len(),
            queued_queries: self.queue.len(),
            memory_utilization_pct: self.current_memory_utilization_pct(),
        }
    }

    pub fn set_user_quota(&mut self, user_id: &str, max_concurrent: usize) {
        let user = self
            .user_state
            .entry(user_id.to_string())
            .or_insert_with(|| UserState {
                active_count: 0,
                max_concurrent: self.config.default_max_concurrent,
            });
        user.max_concurrent = max_concurrent;

        SwarmLogger::info(
            "admission",
            &format!(
                "User '{}' quota set to {} concurrent queries",
                user_id, max_concurrent,
            ),
        );
    }

    pub fn get_all_query_info(&self) -> Vec<QueryInfo> {
        let mut infos = Vec::new();

        for (qid, active) in &self.active_queries {
            let elapsed = active.started_at.elapsed().as_secs();
            infos.push(QueryInfo {
                query_id: qid.clone(),
                user_id: active.user_id.clone(),
                status: "running".to_string(),
                queue_position: "-".to_string(),
                submitted_at: format!("{}s ago", elapsed),
            });
        }

        let mut queued: Vec<&QueuedQuery> = self.queue.iter().collect();
        queued.sort_by(|a, b| b.cmp(a));
        for (idx, q) in queued.iter().enumerate() {
            let elapsed = q.submitted_at.elapsed().as_secs();
            infos.push(QueryInfo {
                query_id: q.query_id.clone(),
                user_id: q.user_id.clone(),
                status: "queued".to_string(),
                queue_position: format!("{}", idx + 1),
                submitted_at: format!("{}s ago", elapsed),
            });
        }

        infos
    }

    fn update_gauges(&self) {
        metrics::instance().set_active_queries(self.active_queries.len() as u64);
        metrics::instance().set_queued_queries(self.queue.len() as u64);
    }

    /// Returns query IDs that have exceeded the configured timeout.
    pub fn check_timeouts(&self) -> Vec<String> {
        let timeout = std::time::Duration::from_secs(self.config.timeout_secs);
        let mut timed_out = Vec::new();

        for (query_id, active) in &self.active_queries {
            if active.started_at.elapsed() > timeout {
                timed_out.push(query_id.clone());
            }
        }

        timed_out
    }

    /// Simple model: each query ~10% of one node's capacity, spread across N nodes.
    fn current_memory_utilization_pct(&self) -> f64 {
        let active = self.active_queries.len() as f64;
        let nodes = count_cluster_nodes().max(1) as f64;
        let utilization = (active * 10.0) / nodes;
        utilization.min(100.0)
    }
}

/// Estimate memory: approx_rows * avg_row_size + shuffle_buffer.
pub fn estimate_query_memory(table_names: &[String]) -> u64 {
    const AVG_ROW_SIZE: u64 = 256;
    const SHUFFLE_BUFFER: u64 = 10 * 1024 * 1024; // 10 MB

    if table_names.is_empty() {
        return SHUFFLE_BUFFER;
    }

    let entries = match catalog::get_all_tables() {
        Ok(entries) => entries,
        Err(_) => return SHUFFLE_BUFFER,
    };

    let mut total_bytes: u64 = 0;
    for name in table_names {
        let max_rows = entries
            .iter()
            .filter(|e| e.table_name == *name)
            .map(|e| e.approx_rows)
            .max()
            .unwrap_or(0);
        total_bytes += max_rows * AVG_ROW_SIZE;
    }

    total_bytes + SHUFFLE_BUFFER
}

fn estimate_query_memory_from_sql(sql: &str) -> u64 {
    let table_names = crate::distributed_scheduler::extract_table_names_from_sql(sql);
    estimate_query_memory(&table_names)
}

fn count_cluster_nodes() -> usize {
    match GossipRegistry::instance().get_node_states() {
        Ok(nodes) => nodes.len().max(1),
        Err(_) => 1,
    }
}

static ADMISSION_CONTROLLER: OnceLock<Mutex<AdmissionController>> = OnceLock::new();

fn admission_lock() -> &'static Mutex<AdmissionController> {
    ADMISSION_CONTROLLER.get_or_init(|| {
        Mutex::new(AdmissionController::new(AdmissionConfig::default()))
    })
}

pub fn instance() -> &'static Mutex<AdmissionController> {
    admission_lock()
}

static SESSION_PRIORITY: AtomicU8 = AtomicU8::new(1); // Default: Interactive

pub fn set_session_priority(priority: Priority) {
    SESSION_PRIORITY.store(priority.to_u8(), Ordering::Relaxed);
}

pub fn get_session_priority() -> Priority {
    Priority::from_u8(SESSION_PRIORITY.load(Ordering::Relaxed))
}

pub fn submit_or_check(
    sql: &str,
    user_id: &str,
    priority: Priority,
) -> Result<(QueryStatus, String), String> {
    let mut ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    ctrl.submit_query(sql, user_id, priority)
}

pub fn complete(query_id: &str) -> Result<(), String> {
    let mut ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    ctrl.complete_query(query_id);
    Ok(())
}

pub fn get_all_query_info() -> Result<Vec<QueryInfo>, String> {
    let ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    Ok(ctrl.get_all_query_info())
}

pub fn get_cluster_status() -> Result<ClusterStatus, String> {
    let ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    Ok(ctrl.get_cluster_status())
}

pub fn cancel_query(query_id: &str) -> Result<QueryStatus, String> {
    let mut ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    ctrl.cancel_query(query_id)
}

pub fn check_timeouts() -> Result<Vec<String>, String> {
    let ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    Ok(ctrl.check_timeouts())
}

pub fn set_user_quota(user_id: &str, max_concurrent: usize) -> Result<(), String> {
    let mut ctrl = admission_lock()
        .lock()
        .map_err(|_| "Admission controller lock poisoned".to_string())?;
    ctrl.set_user_quota(user_id, max_concurrent);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_controller(max_concurrent: usize, max_queue_size: usize) -> AdmissionController {
        AdmissionController::new(AdmissionConfig {
            default_max_concurrent: max_concurrent,
            max_memory_utilization_pct: 85.0,
            max_queue_size,
            timeout_secs: 300,
        })
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::System > Priority::Interactive);
        assert!(Priority::Interactive > Priority::Batch);
        assert!(Priority::System > Priority::Batch);
    }

    #[test]
    fn priority_from_str() {
        assert_eq!(Priority::from_str("batch"), Some(Priority::Batch));
        assert_eq!(Priority::from_str("INTERACTIVE"), Some(Priority::Interactive));
        assert_eq!(Priority::from_str("System"), Some(Priority::System));
        assert_eq!(Priority::from_str("unknown"), None);
    }

    #[test]
    fn priority_roundtrip_u8() {
        for p in &[Priority::Batch, Priority::Interactive, Priority::System] {
            assert_eq!(Priority::from_u8(p.to_u8()), *p);
        }
    }

    #[test]
    fn query_status_display() {
        assert_eq!(QueryStatus::Running.as_str(), "running");
        assert_eq!(
            QueryStatus::Queued { position: 3 }.as_str(),
            "queued(3)"
        );
        assert_eq!(QueryStatus::Completed.as_str(), "completed");
        assert_eq!(QueryStatus::Cancelled.as_str(), "cancelled");
        assert!(QueryStatus::Failed("oops".into()).as_str().contains("oops"));
        assert!(
            QueryStatus::Rejected("full".into())
                .as_str()
                .contains("full")
        );
    }

    #[test]
    fn submit_query_admits_when_under_limit() {
        let mut ctrl = make_controller(5, 100);
        let (status, _) = ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        assert_eq!(status, QueryStatus::Running);
        assert_eq!(ctrl.active_queries.len(), 1);
    }

    #[test]
    fn complete_query_decrements_counters() {
        let mut ctrl = make_controller(5, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let query_id: String = ctrl.active_queries.keys().next().unwrap().clone();
        ctrl.complete_query(&query_id);
        assert_eq!(ctrl.active_queries.len(), 0);
        assert_eq!(ctrl.user_state["user-a"].active_count, 0);
    }

    #[test]
    fn cancel_active_query() {
        let mut ctrl = make_controller(5, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let query_id: String = ctrl.active_queries.keys().next().unwrap().clone();
        let status = ctrl.cancel_query(&query_id).unwrap();
        assert_eq!(status, QueryStatus::Cancelled);
        assert_eq!(ctrl.active_queries.len(), 0);
    }

    #[test]
    fn cancel_nonexistent_query_returns_error() {
        let mut ctrl = make_controller(5, 100);
        let result = ctrl.cancel_query("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn get_query_status_active() {
        let mut ctrl = make_controller(5, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let query_id: String = ctrl.active_queries.keys().next().unwrap().clone();
        let status = ctrl.get_query_status(&query_id);
        assert_eq!(status, Some(QueryStatus::Running));
    }

    #[test]
    fn get_query_status_not_found() {
        let ctrl = make_controller(5, 100);
        assert_eq!(ctrl.get_query_status("nonexistent"), None);
    }

    #[test]
    fn submit_query_queues_when_at_limit() {
        let mut ctrl = make_controller(2, 100);

        // Fill to capacity.
        let (s1, _) = ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let (s2, _) = ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap();
        assert_eq!(s1, QueryStatus::Running);
        assert_eq!(s2, QueryStatus::Running);

        // Third query should be queued.
        let (s3, _) = ctrl.submit_query("SELECT 3", "user-a", Priority::Interactive).unwrap();
        assert!(matches!(s3, QueryStatus::Queued { position: 1 }));
        assert_eq!(ctrl.queue.len(), 1);
    }

    #[test]
    fn different_users_have_independent_limits() {
        let mut ctrl = make_controller(1, 100);

        let (s1, _) = ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let (s2, _) = ctrl.submit_query("SELECT 2", "user-b", Priority::Interactive).unwrap();
        assert_eq!(s1, QueryStatus::Running);
        assert_eq!(s2, QueryStatus::Running);

        let (s3, _) = ctrl.submit_query("SELECT 3", "user-a", Priority::Interactive).unwrap();
        assert!(matches!(s3, QueryStatus::Queued { .. }));

        let (s4, _) = ctrl.submit_query("SELECT 4", "user-b", Priority::Interactive).unwrap();
        assert!(matches!(s4, QueryStatus::Queued { .. }));
    }

    #[test]
    fn queue_full_rejects() {
        let mut ctrl = make_controller(1, 2);

        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap(); // admitted
        ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap(); // queued 1
        ctrl.submit_query("SELECT 3", "user-a", Priority::Interactive).unwrap(); // queued 2

        let (s4, _) = ctrl.submit_query("SELECT 4", "user-a", Priority::Interactive).unwrap();
        assert!(matches!(s4, QueryStatus::Rejected(_)));
    }

    #[test]
    fn estimate_query_memory_empty_tables() {
        let mem = estimate_query_memory(&[]);
        assert!(mem >= 10 * 1024 * 1024);
    }

    #[test]
    fn estimate_query_memory_no_catalog() {
        let mem = estimate_query_memory(&["orders".to_string()]);
        assert!(mem >= 10 * 1024 * 1024);
    }

    #[test]
    fn queued_query_ordering_priority() {
        let now = Instant::now();

        let batch = QueuedQuery {
            query_id: "q1".to_string(),
            sql: "SELECT 1".to_string(),
            user_id: "user-a".to_string(),
            priority: Priority::Batch,
            submitted_at: now,
            estimated_memory_bytes: 0,
        };

        let system = QueuedQuery {
            query_id: "q2".to_string(),
            sql: "SELECT 2".to_string(),
            user_id: "user-a".to_string(),
            priority: Priority::System,
            submitted_at: now,
            estimated_memory_bytes: 0,
        };

        let interactive = QueuedQuery {
            query_id: "q3".to_string(),
            sql: "SELECT 3".to_string(),
            user_id: "user-a".to_string(),
            priority: Priority::Interactive,
            submitted_at: now,
            estimated_memory_bytes: 0,
        };

        let mut heap = BinaryHeap::new();
        heap.push(batch);
        heap.push(interactive);
        heap.push(system);

        assert_eq!(heap.pop().unwrap().query_id, "q2"); // System
        assert_eq!(heap.pop().unwrap().query_id, "q3"); // Interactive
        assert_eq!(heap.pop().unwrap().query_id, "q1"); // Batch
    }

    #[test]
    fn queued_query_ordering_fifo_within_priority() {
        let now = Instant::now();
        let later = now + Duration::from_millis(100);

        let first = QueuedQuery {
            query_id: "q1".to_string(),
            sql: "SELECT 1".to_string(),
            user_id: "user-a".to_string(),
            priority: Priority::Interactive,
            submitted_at: now,
            estimated_memory_bytes: 0,
        };

        let second = QueuedQuery {
            query_id: "q2".to_string(),
            sql: "SELECT 2".to_string(),
            user_id: "user-b".to_string(),
            priority: Priority::Interactive,
            submitted_at: later,
            estimated_memory_bytes: 0,
        };

        let mut heap = BinaryHeap::new();
        heap.push(second);
        heap.push(first);

        assert_eq!(heap.pop().unwrap().query_id, "q1");
        assert_eq!(heap.pop().unwrap().query_id, "q2");
    }

    #[test]
    fn cluster_status_reflects_controller_state() {
        let mut ctrl = make_controller(10, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap();

        let status = ctrl.get_cluster_status();
        assert_eq!(status.active_queries, 2);
        assert_eq!(status.queued_queries, 0);
        assert!(status.total_nodes >= 1);
    }

    #[test]
    fn set_user_quota_overrides_default() {
        let mut ctrl = make_controller(10, 100);
        ctrl.set_user_quota("user-a", 1);

        let (s1, _) = ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        assert_eq!(s1, QueryStatus::Running);

        let (s2, _) = ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap();
        assert!(matches!(s2, QueryStatus::Queued { .. }));
    }

    #[test]
    fn get_all_query_info_returns_active_and_queued() {
        let mut ctrl = make_controller(1, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap(); // queued

        let infos = ctrl.get_all_query_info();
        assert_eq!(infos.len(), 2);

        let running: Vec<_> = infos.iter().filter(|i| i.status == "running").collect();
        let queued: Vec<_> = infos.iter().filter(|i| i.status == "queued").collect();
        assert_eq!(running.len(), 1);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].queue_position, "1");
    }

    #[test]
    fn cancel_queued_query() {
        let mut ctrl = make_controller(1, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap();

        let queued_infos = ctrl.get_all_query_info();
        let queued_id = queued_infos
            .iter()
            .find(|i| i.status == "queued")
            .unwrap()
            .query_id
            .clone();

        let status = ctrl.cancel_query(&queued_id).unwrap();
        assert_eq!(status, QueryStatus::Cancelled);
        assert_eq!(ctrl.queue.len(), 0);
    }

    #[test]
    fn session_priority_default_is_interactive() {
        // Reset to known state for test isolation.
        SESSION_PRIORITY.store(Priority::Interactive.to_u8(), Ordering::Relaxed);
        assert_eq!(get_session_priority(), Priority::Interactive);
    }

    #[test]
    fn set_and_get_session_priority() {
        // Save and restore to avoid polluting other tests.
        let original = SESSION_PRIORITY.load(Ordering::Relaxed);

        set_session_priority(Priority::System);
        assert_eq!(get_session_priority(), Priority::System);

        set_session_priority(Priority::Batch);
        assert_eq!(get_session_priority(), Priority::Batch);

        SESSION_PRIORITY.store(original, Ordering::Relaxed);
    }

    #[test]
    fn singleton_is_accessible() {
        // Only verify the singleton is reachable — use a read-only operation
        // to avoid mutating shared state.
        let lock = instance();
        let ctrl = lock.lock().unwrap();
        let _status = ctrl.get_cluster_status();
    }

    #[test]
    fn submit_or_check_works() {
        // Use the global singleton but clean up after ourselves.
        let result = submit_or_check("SELECT 1", "test-user", Priority::Interactive);
        assert!(result.is_ok());
        let (_, query_id) = result.unwrap();
        // Complete the query we just submitted to leave the singleton clean.
        let _ = complete(&query_id);
    }

    #[test]
    fn check_timeouts_empty_when_no_queries() {
        let ctrl = make_controller(10, 100);
        let timed_out = ctrl.check_timeouts();
        assert!(timed_out.is_empty());
    }

    #[test]
    fn check_timeouts_not_exceeded() {
        let mut ctrl = make_controller(10, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let timed_out = ctrl.check_timeouts();
        assert!(timed_out.is_empty());
    }

    #[test]
    fn check_timeouts_with_zero_timeout() {
        let mut ctrl = AdmissionController::new(AdmissionConfig {
            default_max_concurrent: 10,
            max_memory_utilization_pct: 85.0,
            max_queue_size: 100,
            timeout_secs: 0,
        });
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let timed_out = ctrl.check_timeouts();
        assert_eq!(timed_out.len(), 1);
    }

    #[test]
    fn submit_query_updates_metrics() {
        let before = metrics::instance().queries_submitted.load(Ordering::Relaxed);
        let mut ctrl = make_controller(10, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let after = metrics::instance().queries_submitted.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn complete_query_updates_metrics() {
        let before = metrics::instance().queries_completed.load(Ordering::Relaxed);
        let mut ctrl = make_controller(10, 100);
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap();
        let query_id: String = ctrl.active_queries.keys().next().unwrap().clone();
        ctrl.complete_query(&query_id);
        let after = metrics::instance().queries_completed.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn rejection_updates_metrics() {
        let before = metrics::instance().queries_rejected.load(Ordering::Relaxed);
        let mut ctrl = make_controller(1, 0); // max_queue_size=0 means queue full immediately
        ctrl.submit_query("SELECT 1", "user-a", Priority::Interactive).unwrap(); // admitted
        let (status, _) = ctrl.submit_query("SELECT 2", "user-a", Priority::Interactive).unwrap(); // rejected (queue full)
        assert!(matches!(status, QueryStatus::Rejected(_)));
        let after = metrics::instance().queries_rejected.load(Ordering::Relaxed);
        assert!(after > before);
    }

    #[test]
    fn cancel_query_convenience_not_found() {
        // Uses global singleton but is read-only (query doesn't exist).
        let result = cancel_query("nonexistent-id");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
