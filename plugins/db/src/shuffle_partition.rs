//! Hash-partitions Arrow RecordBatches by join key columns.

use arrow::array::RecordBatch;
use arrow::compute::take;
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::{DataFusionError, Result as DFResult};

/// Split a batch into `num_partitions` by `hash(join_key_columns) % num_partitions`.
///
/// Uses DataFusion's `create_hashes` for consistency with DataFusion's own hash
/// partitioning, and `arrow::compute::take` to split rows into partition-specific batches.
pub fn partition_batch(
    batch: &RecordBatch,
    join_key_indices: &[usize],
    num_partitions: usize,
) -> DFResult<Vec<RecordBatch>> {
    if num_partitions == 0 {
        return Err(DataFusionError::Internal(
            "num_partitions must be > 0".to_string(),
        ));
    }

    if batch.num_rows() == 0 {
        return Ok(vec![batch.clone(); num_partitions]);
    }

    let num_rows = batch.num_rows();

    let hash_columns: Vec<_> = join_key_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();

    let mut hashes = vec![0u64; num_rows];
    create_hashes(&hash_columns, &ahash::RandomState::with_seeds(0, 0, 0, 0), &mut hashes)?;

    let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for (row_idx, hash) in hashes.iter().enumerate() {
        let partition = (*hash as usize) % num_partitions;
        partition_indices[partition].push(row_idx as u32);
    }

    let schema = batch.schema();
    let mut result = Vec::with_capacity(num_partitions);

    for indices in &partition_indices {
        if indices.is_empty() {
            result.push(RecordBatch::new_empty(schema.clone()));
            continue;
        }

        let indices_array = arrow::array::UInt32Array::from(indices.clone());
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices_array, None).map_err(|e| {
                DataFusionError::ArrowError(Box::new(e), None)
            }))
            .collect::<DFResult<_>>()?;

        let partitioned_batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        result.push(partitioned_batch);
    }

    Ok(result)
}

/// Resolve column names to their indices in the schema.
pub fn resolve_key_indices(
    schema: &arrow::datatypes::SchemaRef,
    key_names: &[String],
) -> DFResult<Vec<usize>> {
    key_names
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "Shuffle join key '{}' not found in schema {:?}",
                    name,
                    schema.fields()
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
                Arc::new(StringArray::from(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h",
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn partition_preserves_total_rows() {
        let batch = test_batch();
        let partitions = partition_batch(&batch, &[0], 3).unwrap();
        assert_eq!(partitions.len(), 3);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 8, "Total rows across partitions must equal input");
    }

    #[test]
    fn partition_single_partition() {
        let batch = test_batch();
        let partitions = partition_batch(&batch, &[0], 1).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].num_rows(), 8);
    }

    #[test]
    fn partition_more_partitions_than_rows() {
        let batch = test_batch();
        let partitions = partition_batch(&batch, &[0], 100).unwrap();
        assert_eq!(partitions.len(), 100);
        let total: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn partition_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let partitions = partition_batch(&batch, &[0], 2).unwrap();
        assert_eq!(partitions.len(), 2);
        for p in &partitions {
            assert_eq!(p.num_rows(), 0);
        }
    }

    #[test]
    fn partition_zero_partitions_errors() {
        let batch = test_batch();
        assert!(partition_batch(&batch, &[0], 0).is_err());
    }

    #[test]
    fn partition_deterministic() {
        let batch = test_batch();
        let p1 = partition_batch(&batch, &[0], 3).unwrap();
        let p2 = partition_batch(&batch, &[0], 3).unwrap();
        for (a, b) in p1.iter().zip(p2.iter()) {
            assert_eq!(a.num_rows(), b.num_rows());
        }
    }

    #[test]
    fn resolve_key_indices_found() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let indices = resolve_key_indices(&schema, &["name".to_string(), "id".to_string()]).unwrap();
        assert_eq!(indices, vec![1, 0]);
    }

    #[test]
    fn resolve_key_indices_not_found() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        assert!(resolve_key_indices(&schema, &["missing".to_string()]).is_err());
    }
}
