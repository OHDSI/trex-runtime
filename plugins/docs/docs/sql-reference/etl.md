---
sidebar_position: 6
---

# etl — PostgreSQL CDC Replication

The `etl` extension provides Change Data Capture (CDC) replication from PostgreSQL into trexsql using logical replication.

## Functions

### `trex_etl_start(name, connection_string, ...)`

Start a CDC replication pipeline. Multiple signatures are available:

**Minimal:**

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Pipeline name |
| connection_string | VARCHAR | PostgreSQL connection string |

```sql
SELECT trex_etl_start('my_pipeline', 'postgres://user:pass@host:5432/db');
```

**With mode:**

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Pipeline name |
| connection_string | VARCHAR | PostgreSQL connection string |
| mode | VARCHAR | Replication mode |

```sql
SELECT trex_etl_start('my_pipeline', 'postgres://user:pass@host:5432/db', 'snapshot');
```

**Full configuration:**

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Pipeline name |
| connection_string | VARCHAR | PostgreSQL connection string |
| mode | VARCHAR | Replication mode |
| batch_size | INTEGER | Rows per batch |
| batch_timeout | INTEGER | Batch timeout (ms) |
| retry_delay | INTEGER | Retry delay (ms) |
| retry_max | INTEGER | Max retry attempts |

**Returns:** VARCHAR

```sql
SELECT trex_etl_start('my_pipeline', 'postgres://user:pass@host:5432/db', 'cdc', 1000, 5000, 3000, 5);
```

### `trex_etl_stop(name)`

Stop a running replication pipeline.

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Pipeline name |

**Returns:** VARCHAR

```sql
SELECT trex_etl_stop('my_pipeline');
```

### `trex_etl_status()`

Show status of all replication pipelines.

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| name | VARCHAR | Pipeline name |
| state | VARCHAR | Pipeline state |
| mode | VARCHAR | Replication mode |
| connection | VARCHAR | Connection string |
| publication | VARCHAR | PostgreSQL publication |
| snapshot | VARCHAR | Snapshot info |
| rows_replicated | VARCHAR | Total rows replicated |
| last_activity | VARCHAR | Last activity timestamp |
| error | VARCHAR | Last error (if any) |

```sql
SELECT * FROM trex_etl_status();
```
