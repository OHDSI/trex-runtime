//! DataFusion `ExecutionPlan` that reads from a child plan, hash-partitions
//! the output by join key columns, and routes each partition to the target node.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;

use crate::logging::SwarmLogger;
use crate::shuffle_descriptor::ShuffleDescriptor;
use crate::shuffle_partition;
use crate::shuffle_registry;
use crate::shuffle_transport;

/// Reads from a child plan, hash-partitions by join keys, stores local partition
/// in the shuffle registry, and sends remote partitions via Flight DoExchange.
#[derive(Debug)]
pub struct ShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    descriptor: ShuffleDescriptor,
    /// Indices of join key columns in the input schema.
    join_key_indices: Vec<usize>,
    /// Partition ID that is local to this node (stored in registry, not sent).
    local_partition_id: usize,
    runtime_handle: tokio::runtime::Handle,
    properties: PlanProperties,
}

impl ShuffleWriterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        descriptor: ShuffleDescriptor,
        join_key_indices: Vec<usize>,
        local_partition_id: usize,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let schema = input.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            // The writer produces one output partition (the local partition).
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            input,
            descriptor,
            join_key_indices,
            local_partition_id,
            runtime_handle,
            properties,
        }
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShuffleWriterExec: shuffle_id={}, keys={:?}, partitions={}, local_partition={}",
            self.descriptor.shuffle_id,
            self.descriptor.join_keys,
            self.descriptor.num_partitions,
            self.local_partition_id,
        )
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ShuffleWriterExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(ShuffleWriterExec::new(
            children[0].clone(),
            self.descriptor.clone(),
            self.join_key_indices.clone(),
            self.local_partition_id,
            self.runtime_handle.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.input.schema();
        let descriptor = self.descriptor.clone();
        let join_key_indices = self.join_key_indices.clone();
        let local_partition_id = self.local_partition_id;
        let num_partitions = descriptor.num_partitions;
        let runtime_handle = self.runtime_handle.clone();
        let out_schema = schema.clone();

        let join_handle = runtime_handle.spawn(async move {
            let mut input_stream = input_stream;
            let mut partition_buffers: Vec<Vec<arrow::array::RecordBatch>> =
                vec![Vec::new(); num_partitions];

            while let Some(result) = input_stream.next().await {
                let batch = result.map_err(|e| format!("Input stream error: {e}"))?;
                if batch.num_rows() == 0 {
                    continue;
                }

                let partitioned =
                    shuffle_partition::partition_batch(&batch, &join_key_indices, num_partitions)
                        .map_err(|e| format!("Partition error: {e}"))?;

                for (pid, pbatch) in partitioned.into_iter().enumerate() {
                    if pbatch.num_rows() > 0 {
                        partition_buffers[pid].push(pbatch);
                    }
                }
            }

            for (pid, batches) in partition_buffers.iter().enumerate() {
                if pid == local_partition_id {
                    continue; // Local partition handled below.
                }
                if batches.is_empty() {
                    continue;
                }
                if let Some(target) = descriptor.target_for_partition(pid) {
                    shuffle_transport::send_partition(
                        &target.flight_endpoint,
                        &descriptor,
                        pid,
                        schema.clone(),
                        batches.clone(),
                    )
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to send partition {} to {}: {e}",
                            pid, target.flight_endpoint,
                        )
                    })?;
                }
            }

            let local_batches = std::mem::take(&mut partition_buffers[local_partition_id]);
            let local_rows: usize = local_batches.iter().map(|b| b.num_rows()).sum();
            shuffle_registry::submit_partition(
                &descriptor.shuffle_id,
                local_partition_id,
                local_batches.clone(),
            );

            SwarmLogger::debug(
                "shuffle-writer",
                &format!(
                    "Shuffle '{}': wrote {} local row(s) to partition {}",
                    descriptor.shuffle_id, local_rows, local_partition_id,
                ),
            );

            Ok::<Vec<arrow::array::RecordBatch>, String>(local_batches)
        });

        let result_stream = futures::stream::once(async move {
            let batches = join_handle
                .await
                .map_err(|e| {
                    DataFusionError::Internal(format!("Shuffle writer task panicked: {e}"))
                })?
                .map_err(|e| DataFusionError::External(e.into()))?;
            Ok::<Vec<arrow::array::RecordBatch>, DataFusionError>(batches)
        })
        .flat_map(|result| match result {
            Ok(batches) => futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            out_schema,
            result_stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shuffle_descriptor::ShuffleTarget;

    fn sample_descriptor() -> ShuffleDescriptor {
        ShuffleDescriptor {
            shuffle_id: "test-writer".to_string(),
            join_keys: vec!["id".to_string()],
            num_partitions: 2,
            target_table: None,
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
        }
    }

    #[test]
    fn shuffle_writer_name() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));

        // Use an empty exec for testing metadata only.
        let empty = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema));
        let writer = ShuffleWriterExec::new(
            empty,
            sample_descriptor(),
            vec![0],
            0,
            rt.handle().clone(),
        );
        assert_eq!(writer.name(), "ShuffleWriterExec");
    }

    #[test]
    fn shuffle_writer_has_one_child() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let empty = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema));
        let writer = ShuffleWriterExec::new(
            empty,
            sample_descriptor(),
            vec![0],
            0,
            rt.handle().clone(),
        );
        assert_eq!(writer.children().len(), 1);
    }

    #[test]
    fn shuffle_writer_output_partitioning() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let empty = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema));
        let writer = ShuffleWriterExec::new(
            empty,
            sample_descriptor(),
            vec![0],
            0,
            rt.handle().clone(),
        );
        assert_eq!(
            writer.properties().output_partitioning().partition_count(),
            1,
        );
    }
}
