//! DataFusion `ExecutionPlan` that receives shuffled partitions from the
//! shuffle registry, waiting for all expected source nodes to deliver data.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
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
use crate::shuffle_registry;

/// Reads shuffle data from the in-process shuffle registry. Blocks (async)
/// until all expected source nodes have submitted their partition data.
#[derive(Debug)]
pub struct ShuffleReaderExec {
    descriptor: ShuffleDescriptor,
    /// Which partition this reader handles.
    partition_id: usize,
    /// How many source nodes will send data for this partition.
    expected_sources: usize,
    schema: SchemaRef,
    runtime_handle: tokio::runtime::Handle,
    properties: PlanProperties,
}

impl ShuffleReaderExec {
    pub fn new(
        descriptor: ShuffleDescriptor,
        partition_id: usize,
        expected_sources: usize,
        schema: SchemaRef,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            descriptor,
            partition_id,
            expected_sources,
            schema,
            runtime_handle,
            properties,
        }
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ShuffleReaderExec: shuffle_id={}, partition={}, expected_sources={}",
            self.descriptor.shuffle_id, self.partition_id, self.expected_sources,
        )
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn name(&self) -> &str {
        "ShuffleReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let shuffle_id = self.descriptor.shuffle_id.clone();
        let partition_id = self.partition_id;
        let expected_sources = self.expected_sources;
        let schema = self.schema.clone();

        SwarmLogger::debug(
            "shuffle-reader",
            &format!(
                "Waiting for shuffle '{}' partition {} ({} expected source(s))",
                shuffle_id, partition_id, expected_sources,
            ),
        );

        let join_handle = self.runtime_handle.spawn(async move {
            let batches =
                shuffle_registry::wait_for_partition(&shuffle_id, partition_id, expected_sources)
                    .await
                    .map_err(|e| DataFusionError::Internal(e))?;

            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            SwarmLogger::debug(
                "shuffle-reader",
                &format!(
                    "Shuffle '{}' partition {}: received {} batch(es), {} row(s)",
                    shuffle_id,
                    partition_id,
                    batches.len(),
                    rows,
                ),
            );

            Ok::<_, DataFusionError>(batches)
        });

        let result_stream = futures::stream::once(async move {
            let batches = join_handle
                .await
                .map_err(|e| {
                    DataFusionError::Internal(format!("Shuffle reader task panicked: {e}"))
                })??;
            Ok::<Vec<arrow::array::RecordBatch>, DataFusionError>(batches)
        })
        .flat_map(|result| match result {
            Ok(batches) => futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
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
            shuffle_id: "test-reader".to_string(),
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
    fn shuffle_reader_name() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let reader = ShuffleReaderExec::new(
            sample_descriptor(),
            0,
            1,
            schema,
            rt.handle().clone(),
        );
        assert_eq!(reader.name(), "ShuffleReaderExec");
    }

    #[test]
    fn shuffle_reader_no_children() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let reader = ShuffleReaderExec::new(
            sample_descriptor(),
            0,
            1,
            schema,
            rt.handle().clone(),
        );
        assert!(reader.children().is_empty());
    }

    #[test]
    fn shuffle_reader_output_partitioning() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let reader = ShuffleReaderExec::new(
            sample_descriptor(),
            0,
            2,
            schema,
            rt.handle().clone(),
        );
        assert_eq!(
            reader.properties().output_partitioning().partition_count(),
            1,
        );
    }

    #[tokio::test]
    async fn shuffle_reader_receives_pre_submitted_data() {
        let desc = sample_descriptor();
        shuffle_registry::register_shuffle(&desc.shuffle_id, 1);
        shuffle_registry::submit_partition(
            &desc.shuffle_id,
            0,
            vec![arrow::array::RecordBatch::try_new(
                Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                ])),
                vec![Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30]))],
            )
            .unwrap()],
        );

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));

        let reader = ShuffleReaderExec::new(
            desc.clone(),
            0,
            1,
            schema,
            tokio::runtime::Handle::current(),
        );

        let ctx = Arc::new(TaskContext::default());
        let stream = reader.execute(0, ctx).unwrap();

        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        shuffle_registry::cleanup_shuffle(&desc.shuffle_id);
    }
}
