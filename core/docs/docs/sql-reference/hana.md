---
sidebar_position: 3
---

# hana — SAP HANA Scanner

The `hana` extension provides a scanner for SAP HANA databases, allowing queries against HANA from within trexsql. Uses `hdbsqls://` connection URLs with TLS.

## Functions

### `trex_hana_scan(query, url)`

Execute a SQL query against a SAP HANA database and return results as a table.

| Parameter | Type | Description |
|-----------|------|-------------|
| query | VARCHAR | SQL query to execute on HANA |
| url | VARCHAR | HANA connection URL |

**Returns:** TABLE (dynamic columns from query schema)

```sql
SELECT * FROM trex_hana_scan(
  'SELECT TOP 10 * FROM PATIENTS',
  'hdbsqls://user:pass@hana-host:39015/HDB?insecure_omit_server_certificate_check'
);
```

:::note
`hana_scan` and `trex_hana_query` are aliases for this function.
:::

### `trex_hana_attach(url, dbname, schema)`

Attach a HANA schema, creating local virtual tables for all tables in the remote schema.

| Parameter | Type | Description |
|-----------|------|-------------|
| url | VARCHAR | HANA connection URL |
| dbname | VARCHAR | Local database name |
| schema | VARCHAR | HANA schema name |

**Returns:** TABLE(table_name VARCHAR, full_name VARCHAR)

```sql
SELECT * FROM trex_hana_attach(
  'hdbsqls://user:pass@hana-host:39015/HDB?insecure_omit_server_certificate_check',
  'hana_db',
  'CDM'
);
```

### `trex_hana_detach(dbname, schema)`

Detach a previously attached HANA schema and remove all associated virtual tables.

| Parameter | Type | Description |
|-----------|------|-------------|
| dbname | VARCHAR | Local database name |
| schema | VARCHAR | Schema name to detach |

**Returns:** VARCHAR

```sql
SELECT trex_hana_detach('hana_db', 'CDM');
```

### `trex_hana_tables()`

List all currently attached HANA tables.

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| table_name | VARCHAR | Table name |
| schema_name | VARCHAR | HANA schema |
| dbname | VARCHAR | Local database name |
| full_name | VARCHAR | Fully qualified name |

```sql
SELECT * FROM trex_hana_tables();
```

### `trex_hana_execute(connection_string, sql_statement)`

Execute a SQL statement (DDL/DML) on HANA without returning results.

| Parameter | Type | Description |
|-----------|------|-------------|
| connection_string | VARCHAR | HANA connection URL |
| sql_statement | VARCHAR | SQL to execute |

**Returns:** VARCHAR — execution status

```sql
SELECT trex_hana_execute(
  'hdbsqls://user:pass@hana-host:39015/HDB?insecure_omit_server_certificate_check',
  'CREATE TABLE TEST (id INTEGER, name NVARCHAR(100))'
);
```

:::tip
HANA Express Docker images require `vm.max_map_count >= 262144`. Set with `sysctl -w vm.max_map_count=262144`.
:::
