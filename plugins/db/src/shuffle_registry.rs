//! In-process registry for coordinating shuffle data between the Flight
//! DoExchange handler (writer) and ShuffleReaderExec (consumer).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use arrow::array::RecordBatch;
use tokio::sync::Notify;

use crate::logging::SwarmLogger;

/// Maximum time a shuffle entry may live before being considered stale.
const STALE_SHUFFLE_SECS: u64 = 300; // 5 minutes

/// Maximum time `wait_for_partition` will block before returning an error.
const WAIT_TIMEOUT_SECS: u64 = 120; // 2 minutes

/// State for a single shuffle operation.
pub struct ShuffleState {
    /// partition_id → accumulated batches from all source nodes.
    partitions: HashMap<usize, Vec<RecordBatch>>,
    /// How many source nodes are expected to send data.
    expected_sources: usize,
    /// How many source nodes have completed sending.
    received_sources: usize,
    /// Notifier for readers waiting on partition data.
    notify: Notify,
    /// When this shuffle was registered (for stale-entry cleanup).
    created_at: Instant,
}

static SHUFFLE_REGISTRY: OnceLock<Mutex<HashMap<String, ShuffleState>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, ShuffleState>> {
    SHUFFLE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register a new shuffle operation. Must be called before any data arrives.
///
/// Also opportunistically cleans up stale entries from previous failed queries
/// that were never explicitly cleaned up.
pub fn register_shuffle(shuffle_id: &str, expected_sources: usize) {
    let mut map = registry().lock().expect("shuffle registry lock poisoned");

    cleanup_stale_entries(&mut map);

    if map.contains_key(shuffle_id) {
        SwarmLogger::warn(
            "shuffle-registry",
            &format!("Shuffle '{}' already registered, skipping", shuffle_id),
        );
        return;
    }
    map.insert(
        shuffle_id.to_string(),
        ShuffleState {
            partitions: HashMap::new(),
            expected_sources,
            received_sources: 0,
            notify: Notify::new(),
            created_at: Instant::now(),
        },
    );
    SwarmLogger::debug(
        "shuffle-registry",
        &format!(
            "Registered shuffle '{}' expecting {} source(s)",
            shuffle_id, expected_sources,
        ),
    );
}

/// Remove entries older than `STALE_SHUFFLE_SECS`. Called under lock.
fn cleanup_stale_entries(map: &mut HashMap<String, ShuffleState>) {
    let cutoff = std::time::Duration::from_secs(STALE_SHUFFLE_SECS);
    let stale_ids: Vec<String> = map
        .iter()
        .filter(|(_, state)| state.created_at.elapsed() > cutoff)
        .map(|(id, _)| id.clone())
        .collect();
    for id in &stale_ids {
        map.remove(id);
    }
    if !stale_ids.is_empty() {
        SwarmLogger::info(
            "shuffle-registry",
            &format!(
                "Cleaned up {} stale shuffle entries (older than {}s)",
                stale_ids.len(),
                STALE_SHUFFLE_SECS,
            ),
        );
    }
}

/// Submit partition data from one source node. Called by the DoExchange handler.
pub fn submit_partition(shuffle_id: &str, partition_id: usize, batches: Vec<RecordBatch>) {
    let mut map = registry().lock().expect("shuffle registry lock poisoned");
    if let Some(state) = map.get_mut(shuffle_id) {
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        state
            .partitions
            .entry(partition_id)
            .or_default()
            .extend(batches);
        state.received_sources += 1;
        SwarmLogger::debug(
            "shuffle-registry",
            &format!(
                "Shuffle '{}' partition {}: received {} row(s) ({}/{} sources)",
                shuffle_id, partition_id, rows, state.received_sources, state.expected_sources,
            ),
        );
        state.notify.notify_waiters();
    } else {
        SwarmLogger::warn(
            "shuffle-registry",
            &format!(
                "Shuffle '{}' not registered, dropping partition {} data",
                shuffle_id, partition_id,
            ),
        );
    }
}

/// Wait until the specified partition has data from all expected sources,
/// then take and return all accumulated batches.
///
/// Returns an error if the wait exceeds `WAIT_TIMEOUT_SECS` (prevents
/// infinite hangs on failed queries where sources never send data).
pub async fn wait_for_partition(
    shuffle_id: &str,
    partition_id: usize,
    expected_sources: usize,
) -> Result<Vec<RecordBatch>, String> {
    let deadline = Instant::now() + std::time::Duration::from_secs(WAIT_TIMEOUT_SECS);

    loop {
        let ready: bool = {
            let map = registry().lock().expect("shuffle registry lock poisoned");
            if let Some(state) = map.get(shuffle_id) {
                let count = state
                    .partitions
                    .get(&partition_id)
                    .map(|v| v.len())
                    .unwrap_or(0);
                state.received_sources >= expected_sources || count >= expected_sources
            } else {
                false
            }
        };

        if ready {
            break;
        }

        if Instant::now() >= deadline {
            SwarmLogger::warn(
                "shuffle-registry",
                &format!(
                    "Shuffle '{}' partition {} timed out after {}s waiting for {}/{} sources",
                    shuffle_id, partition_id, WAIT_TIMEOUT_SECS,
                    {
                        let map = registry().lock().expect("lock");
                        map.get(shuffle_id)
                            .map(|s| s.received_sources)
                            .unwrap_or(0)
                    },
                    expected_sources,
                ),
            );
            return Err(format!(
                "Shuffle '{}' partition {} timed out after {}s",
                shuffle_id, partition_id, WAIT_TIMEOUT_SECS,
            ));
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let mut map = registry().lock().expect("shuffle registry lock poisoned");
    if let Some(state) = map.get_mut(shuffle_id) {
        Ok(state.partitions.remove(&partition_id).unwrap_or_default())
    } else {
        Ok(Vec::new())
    }
}

/// Remove a completed shuffle from the registry.
pub fn cleanup_shuffle(shuffle_id: &str) {
    let mut map = registry().lock().expect("shuffle registry lock poisoned");
    if map.remove(shuffle_id).is_some() {
        SwarmLogger::debug(
            "shuffle-registry",
            &format!("Cleaned up shuffle '{}'", shuffle_id),
        );
    }
}

/// Check if a shuffle is registered.
pub fn is_registered(shuffle_id: &str) -> bool {
    let map = registry().lock().expect("shuffle registry lock poisoned");
    map.contains_key(shuffle_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    #[test]
    fn register_and_submit() {
        let id = "test-register-submit";
        register_shuffle(id, 1);
        assert!(is_registered(id));

        submit_partition(id, 0, vec![make_batch(vec![1, 2, 3])]);
        cleanup_shuffle(id);
        assert!(!is_registered(id));
    }

    #[test]
    fn submit_to_unregistered_shuffle_does_not_panic() {
        submit_partition("nonexistent", 0, vec![make_batch(vec![1])]);
    }

    #[test]
    fn cleanup_nonexistent_does_not_panic() {
        cleanup_shuffle("nonexistent-cleanup");
    }

    #[test]
    fn double_register_is_noop() {
        let id = "test-double-register";
        register_shuffle(id, 1);
        register_shuffle(id, 2); // should warn but not panic
        cleanup_shuffle(id);
    }

    #[tokio::test]
    async fn wait_for_partition_returns_data() {
        let id = "test-wait-partition";
        register_shuffle(id, 1);
        submit_partition(id, 0, vec![make_batch(vec![10, 20])]);

        let batches = wait_for_partition(id, 0, 1).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        cleanup_shuffle(id);
    }

    #[tokio::test]
    async fn wait_for_partition_multiple_sources() {
        let id = "test-multi-source";
        register_shuffle(id, 2);
        submit_partition(id, 0, vec![make_batch(vec![1, 2])]);
        submit_partition(id, 0, vec![make_batch(vec![3, 4])]);

        let batches = wait_for_partition(id, 0, 2).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
        cleanup_shuffle(id);
    }
}
