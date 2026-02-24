---
sidebar_position: 4
---

# pgwire — PostgreSQL Wire Protocol

The `pgwire` extension starts a PostgreSQL-compatible wire protocol server, allowing any PostgreSQL client (psql, JDBC, etc.) to connect and query trexsql.

## Functions

### `trex_pgwire_start(host, port, password, db_credentials)`

Start the PostgreSQL wire protocol server.

| Parameter | Type | Description |
|-----------|------|-------------|
| host | VARCHAR | Bind address |
| port | INTEGER | Server port |
| password | VARCHAR | Default connection password |
| db_credentials | VARCHAR | JSON credentials for external databases |

**Returns:** VARCHAR

```sql
SELECT trex_pgwire_start('0.0.0.0', 5432, 'mypassword', '{}');
```

:::note
`start_pgwire_server` is a deprecated alias for this function.
:::

### `trex_pgwire_stop(host, port)`

Stop the PostgreSQL wire protocol server.

| Parameter | Type | Description |
|-----------|------|-------------|
| host | VARCHAR | Server host |
| port | INTEGER | Server port |

**Returns:** VARCHAR

```sql
SELECT trex_pgwire_stop('0.0.0.0', 5432);
```

### `trex_pgwire_set_credentials(credentials)`

Update database credentials for the pgwire server at runtime.

| Parameter | Type | Description |
|-----------|------|-------------|
| credentials | VARCHAR | JSON credentials object |

**Returns:** VARCHAR

```sql
SELECT trex_pgwire_set_credentials('{"default": {"password": "newpass"}}');
```

:::note
`update_db_credentials` is a deprecated alias for this function.
:::

### `trex_pgwire_version()`

Return the pgwire extension version.

**Returns:** VARCHAR

```sql
SELECT trex_pgwire_version();
```

### `trex_pgwire_status()`

Show status of all running pgwire servers.

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| hostname | VARCHAR | Server hostname |
| port | VARCHAR | Server port |
| uptime_seconds | VARCHAR | Server uptime |
| has_credentials | VARCHAR | Whether credentials are configured |

```sql
SELECT * FROM trex_pgwire_status();
```

## Connecting with psql

Once the pgwire server is running, connect with any PostgreSQL client:

```bash
psql -h localhost -p 5432 -U trex -d main
```
