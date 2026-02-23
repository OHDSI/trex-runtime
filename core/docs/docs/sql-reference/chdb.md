---
sidebar_position: 5
---

# chdb — ClickHouse Integration

The `chdb` extension embeds ClickHouse (via libchdb) for executing ClickHouse SQL queries directly within trexsql.

## Functions

### `trex_chdb_start()`

Start the embedded ClickHouse engine with default settings.

**Returns:** VARCHAR

```sql
SELECT trex_chdb_start();
```

### `trex_chdb_start(path)`

Start the embedded ClickHouse engine with a persistent data path.

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Data directory path |

**Returns:** VARCHAR

```sql
SELECT trex_chdb_start('/data/chdb');
```

### `trex_chdb_stop()`

Stop the embedded ClickHouse engine.

**Returns:** VARCHAR

```sql
SELECT trex_chdb_stop();
```

### `trex_chdb_execute(query)`

Execute a ClickHouse SQL query.

| Parameter | Type | Description |
|-----------|------|-------------|
| query | VARCHAR | ClickHouse SQL query |

**Returns:** VARCHAR

```sql
SELECT trex_chdb_execute('CREATE TABLE test (id UInt32, name String) ENGINE = MergeTree ORDER BY id');
```

### `trex_chdb_scan()`

Scan results from the last ClickHouse query as a table.

**Returns:** TABLE (dynamic columns)

```sql
SELECT * FROM trex_chdb_scan();
```

:::note
`trex_chdb_query` is an alias for `trex_chdb_scan`.
:::
