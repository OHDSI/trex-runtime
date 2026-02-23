//! Flight DoExchange-based partition streaming between nodes.
//!
//! Contains the client for sending partitions to remote nodes via Flight
//! DoExchange. The receiving side is handled by DuckDBFlightService's
//! do_exchange() method in flight_server.rs — since flight and swarm are
//! now in the same cdylib, no separate shuffle service is needed.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use futures::TryStreamExt;
use tonic::transport::Endpoint;
use tonic::Request;

use crate::logging::SwarmLogger;
use crate::shuffle_descriptor::ShuffleDescriptor;

/// Send partitioned batches to a remote node via Flight DoExchange.
///
/// The `FlightDescriptor` carries the `ShuffleDescriptor` JSON in `cmd` and the
/// `partition_id` as the first path element. Connects directly to the flight
/// endpoint (DoExchange is handled by the merged flight server).
pub async fn send_partition(
    endpoint: &str,
    descriptor: &ShuffleDescriptor,
    partition_id: usize,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(), String> {
    if batches.is_empty() {
        SwarmLogger::debug(
            "shuffle-transport",
            &format!(
                "Skipping empty partition {} for shuffle '{}'",
                partition_id, descriptor.shuffle_id,
            ),
        );
        return Ok(());
    }

    SwarmLogger::debug(
        "shuffle-transport",
        &format!(
            "Sending partition {} ({} batch(es)) to {} for shuffle '{}'",
            partition_id,
            batches.len(),
            endpoint,
            descriptor.shuffle_id,
        ),
    );

    let channel = Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| format!("Invalid flight endpoint {endpoint}: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("Failed to connect to flight server {endpoint}: {e}"))?;

    let mut client = FlightServiceClient::new(channel);

    let desc_bytes = descriptor.to_json_bytes()?;
    let flight_descriptor = FlightDescriptor {
        r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
        cmd: desc_bytes.into(),
        path: vec![partition_id.to_string()],
    };

    let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_flight_descriptor(Some(flight_descriptor))
        .build(batch_stream)
        .map_err(|e| format!("Flight encoding error: {e}"));

    let flight_data: Vec<FlightData> = flight_data_stream
        .try_collect()
        .await
        .map_err(|e| format!("Failed to encode shuffle data: {e}"))?;

    let request = Request::new(futures::stream::iter(flight_data.into_iter()));

    let _response = client
        .do_exchange(request)
        .await
        .map_err(|e| format!("DoExchange failed for shuffle '{}' partition {}: {e}", descriptor.shuffle_id, partition_id))?;

    SwarmLogger::debug(
        "shuffle-transport",
        &format!(
            "Successfully sent partition {} for shuffle '{}'",
            partition_id, descriptor.shuffle_id,
        ),
    );

    Ok(())
}
