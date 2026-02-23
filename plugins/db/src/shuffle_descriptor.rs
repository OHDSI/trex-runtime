//! Shared metadata for shuffle coordination between nodes.

use serde::{Deserialize, Serialize};

/// Describes a shuffle operation: which columns to hash, how many partitions,
/// and where each partition should be sent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleDescriptor {
    /// Unique identifier for this shuffle operation.
    pub shuffle_id: String,
    /// Column names used for hash partitioning.
    pub join_keys: Vec<String>,
    /// Number of partitions (typically = number of participating nodes).
    pub num_partitions: usize,
    /// Mapping from partition_id to the node that should receive it.
    pub partition_targets: Vec<ShuffleTarget>,
    /// If set, the receiver inserts batches into this local table instead of
    /// storing them in the shuffle registry.
    #[serde(default)]
    pub target_table: Option<String>,
}

/// Target endpoint for a single shuffle partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShuffleTarget {
    /// Which partition this target handles.
    pub partition_id: usize,
    /// Arrow Flight endpoint (e.g. "http://10.0.0.1:8815").
    pub flight_endpoint: String,
    /// Human-readable node name.
    pub node_name: String,
}

impl ShuffleDescriptor {
    /// Look up the target for a given partition_id.
    pub fn target_for_partition(&self, partition_id: usize) -> Option<&ShuffleTarget> {
        self.partition_targets
            .iter()
            .find(|t| t.partition_id == partition_id)
    }

    /// Serialize to JSON bytes for embedding in Flight descriptors.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|e| format!("Failed to serialize ShuffleDescriptor: {e}"))
    }

    /// Deserialize from JSON bytes received in a Flight descriptor.
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes)
            .map_err(|e| format!("Failed to deserialize ShuffleDescriptor: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_descriptor() -> ShuffleDescriptor {
        ShuffleDescriptor {
            shuffle_id: "test-shuffle-001".to_string(),
            join_keys: vec!["customer_id".to_string()],
            num_partitions: 2,
            partition_targets: vec![
                ShuffleTarget {
                    partition_id: 0,
                    flight_endpoint: "http://10.0.0.1:8815".to_string(),
                    node_name: "node-a".to_string(),
                },
                ShuffleTarget {
                    partition_id: 1,
                    flight_endpoint: "http://10.0.0.2:8815".to_string(),
                    node_name: "node-b".to_string(),
                },
            ],
            target_table: None,
        }
    }

    #[test]
    fn serialization_roundtrip() {
        let desc = sample_descriptor();
        let bytes = desc.to_json_bytes().unwrap();
        let restored = ShuffleDescriptor::from_json_bytes(&bytes).unwrap();
        assert_eq!(restored.shuffle_id, "test-shuffle-001");
        assert_eq!(restored.join_keys, vec!["customer_id"]);
        assert_eq!(restored.num_partitions, 2);
        assert_eq!(restored.partition_targets.len(), 2);
    }

    #[test]
    fn target_for_partition_found() {
        let desc = sample_descriptor();
        let target = desc.target_for_partition(1).unwrap();
        assert_eq!(target.node_name, "node-b");
        assert_eq!(target.flight_endpoint, "http://10.0.0.2:8815");
    }

    #[test]
    fn target_for_partition_not_found() {
        let desc = sample_descriptor();
        assert!(desc.target_for_partition(99).is_none());
    }

    #[test]
    fn deserialize_invalid_bytes() {
        let result = ShuffleDescriptor::from_json_bytes(b"not json");
        assert!(result.is_err());
    }

    #[test]
    fn multiple_join_keys() {
        let desc = ShuffleDescriptor {
            shuffle_id: "multi-key".to_string(),
            join_keys: vec!["col_a".to_string(), "col_b".to_string()],
            num_partitions: 3,
            partition_targets: vec![],
            target_table: None,
        };
        let bytes = desc.to_json_bytes().unwrap();
        let restored = ShuffleDescriptor::from_json_bytes(&bytes).unwrap();
        assert_eq!(restored.join_keys.len(), 2);
    }
}
