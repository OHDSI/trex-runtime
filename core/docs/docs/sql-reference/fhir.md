---
sidebar_position: 7
---

# fhir — FHIR Server

The `fhir` extension starts a FHIR-compliant HTTP server (built on Axum) that serves healthcare data from trexsql in HL7 FHIR format.

## Functions

### `trex_fhir_start(host, port)`

Start the FHIR HTTP server.

| Parameter | Type | Description |
|-----------|------|-------------|
| host | VARCHAR | Bind address |
| port | INTEGER | Server port |

**Returns:** VARCHAR

```sql
SELECT trex_fhir_start('0.0.0.0', 8080);
```

### `trex_fhir_stop(host, port)`

Stop the FHIR HTTP server.

| Parameter | Type | Description |
|-----------|------|-------------|
| host | VARCHAR | Server host |
| port | INTEGER | Server port |

**Returns:** VARCHAR

```sql
SELECT trex_fhir_stop('0.0.0.0', 8080);
```

### `trex_fhir_version()`

Return the FHIR extension version.

**Returns:** VARCHAR

```sql
SELECT trex_fhir_version();
```

### `trex_fhir_status()`

Show status of all running FHIR servers.

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| hostname | VARCHAR | Server hostname |
| port | VARCHAR | Server port |
| uptime_seconds | VARCHAR | Server uptime |

```sql
SELECT * FROM trex_fhir_status();
```
