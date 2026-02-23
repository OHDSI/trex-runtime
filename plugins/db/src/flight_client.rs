use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use futures::TryStreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::logging::SwarmLogger;

/// Arrow Flight gRPC client for executing SQL queries via DoGet.
#[derive(Debug)]
pub struct FlightClient {
    endpoint: String,
    client: FlightServiceClient<Channel>,
}

impl FlightClient {
    pub async fn connect(endpoint: &str) -> Result<Self, String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("Connecting to Flight server at {endpoint}"),
        );

        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("Failed to connect to {endpoint}: invalid URI: {e}"))?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {endpoint}: {e}"))?;

        let client = FlightServiceClient::new(channel);

        SwarmLogger::debug(
            "flight-client",
            &format!("Connected to Flight server at {endpoint}"),
        );

        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    /// Connect with mutual TLS (mTLS). Endpoint should use `https://`.
    pub async fn connect_with_tls(
        endpoint: &str,
        cert_path: &str,
        key_path: &str,
        ca_cert_path: &str,
    ) -> Result<Self, String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("Connecting to Flight server at {endpoint} with mTLS"),
        );

        let cert = std::fs::read(cert_path)
            .map_err(|e| format!("Failed to read client certificate {cert_path}: {e}"))?;
        let key = std::fs::read(key_path)
            .map_err(|e| format!("Failed to read client key {key_path}: {e}"))?;
        let ca_cert = std::fs::read(ca_cert_path)
            .map_err(|e| format!("Failed to read CA certificate {ca_cert_path}: {e}"))?;

        let identity = Identity::from_pem(cert, key);
        let ca = Certificate::from_pem(ca_cert);

        let tls_config = ClientTlsConfig::new()
            .identity(identity)
            .ca_certificate(ca);

        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| format!("Failed to connect to {endpoint}: invalid URI: {e}"))?
            .tls_config(tls_config)
            .map_err(|e| format!("Failed to configure TLS for {endpoint}: {e}"))?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to {endpoint} with TLS: {e}"))?;

        let client = FlightServiceClient::new(channel);

        SwarmLogger::debug(
            "flight-client",
            &format!("Connected to Flight server at {endpoint} with mTLS"),
        );

        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    /// Execute SQL via DoGet with a JSON ticket `{"query": "<sql>"}`.
    pub async fn execute_query(
        &mut self,
        sql: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("Executing query on {}: {sql}", self.endpoint),
        );

        let ticket_payload = serde_json::json!({ "query": sql }).to_string();
        let ticket = Ticket::new(ticket_payload.into_bytes());

        let response = self
            .client
            .do_get(ticket)
            .await
            .map_err(|e| format!("Flight query failed on {}: {e}", self.endpoint))?;

        let flight_stream = FlightRecordBatchStream::new_from_flight_data(
            response
                .into_inner()
                .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let mut batches: Vec<RecordBatch> = Vec::new();

        futures::pin_mut!(flight_stream);

        while let Some(batch) = flight_stream.try_next().await.map_err(|e| {
            format!(
                "Failed to decode Flight response from {}: {e}",
                self.endpoint
            )
        })? {
            batches.push(batch);
        }

        // The stream parses the schema from the first Flight message
        // (even for 0-row results where no RecordBatch is yielded).
        let schema = flight_stream
            .schema()
            .cloned()
            .or_else(|| batches.first().map(|b| b.schema()))
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));

        SwarmLogger::debug(
            "flight-client",
            &format!(
                "Query on {} returned {} batch(es), {} total row(s)",
                self.endpoint,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            ),
        );

        Ok((schema, batches))
    }

    /// Execute a Flight action (e.g. DDL/DML via "query") and return the response body.
    pub async fn do_action(&mut self, action_type: &str, body: &str) -> Result<String, String> {
        SwarmLogger::debug(
            "flight-client",
            &format!("DoAction '{}' on {}: {body}", action_type, self.endpoint),
        );

        let action = Action {
            r#type: action_type.to_string(),
            body: body.as_bytes().to_vec().into(),
        };

        let mut response = self
            .client
            .do_action(action)
            .await
            .map_err(|e| format!("DoAction '{}' failed on {}: {e}", action_type, self.endpoint))?
            .into_inner();

        let result_body = if let Some(result) = response
            .message()
            .await
            .map_err(|e| format!("Failed to read DoAction response from {}: {e}", self.endpoint))?
        {
            String::from_utf8(result.body.to_vec())
                .unwrap_or_else(|_| "<non-utf8 response>".to_string())
        } else {
            String::new()
        };

        SwarmLogger::debug(
            "flight-client",
            &format!("DoAction '{}' on {} succeeded", action_type, self.endpoint),
        );

        Ok(result_body)
    }
}

/// One-shot: connect, execute SQL remotely via DoAction("query"). Must be called within tokio.
pub async fn execute_remote_sql(endpoint: &str, sql: &str) -> Result<(), String> {
    let mut client = FlightClient::connect(endpoint).await?;
    let body = serde_json::json!({ "query": sql }).to_string();
    client.do_action("query", &body).await?;
    Ok(())
}

/// One-shot: trigger catalog refresh on a remote node.
pub async fn refresh_remote_catalog(endpoint: &str) -> Result<(), String> {
    let mut client = FlightClient::connect(endpoint).await?;
    client.do_action("refresh_catalog", "{}").await?;
    Ok(())
}

/// One-shot: connect, execute, return batches. Must be called within tokio.
pub async fn query_node(endpoint: &str, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let mut client = FlightClient::connect(endpoint).await?;
    let (_schema, batches) = client.execute_query(sql).await?;
    Ok(batches)
}

/// One-shot: connect, execute, return schema and batches.
pub async fn query_node_with_schema(
    endpoint: &str,
    sql: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let mut client = FlightClient::connect(endpoint).await?;
    client.execute_query(sql).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_to_invalid_endpoint_fails() {
        let result = FlightClient::connect("http://127.0.0.1:1").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.starts_with("Failed to connect to http://127.0.0.1:1:"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn connect_to_malformed_uri_fails() {
        let result = FlightClient::connect("not a uri").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("Failed to connect to not a uri"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn query_node_connection_error() {
        let result = query_node("http://127.0.0.1:1", "SELECT 1").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("Failed to connect to http://127.0.0.1:1"),
            "unexpected error: {err}"
        );
    }
}
