---
sidebar_position: 8
---

# migration — Schema Migrations

The `migration` extension provides SQL schema migration management with version tracking, checksum verification, and multi-database support (trexsql and PostgreSQL).

## Concepts

### File Naming Convention

Migration files must follow the pattern `V<version>__<name>.sql`:

- **V prefix** — uppercase `V` is required
- **Version** — positive integer (e.g., `1`, `42`, `100`)
- **Separator** — double underscore `__`
- **Name** — alphanumeric characters and underscores only
- **Extension** — `.sql`

```
V1__create_tables.sql
V2__add_indexes.sql
V10__backfill_data.sql
```

Files that don't match this pattern are silently skipped. Duplicate version numbers cause an error.

### History Table

Each target schema gets a `refinery_schema_history` table:

| Column | Type | Description |
|--------|------|-------------|
| `version` | INT4 (PK) | Migration version number |
| `name` | VARCHAR | Migration name from filename |
| `applied_on` | VARCHAR | Application timestamp |
| `checksum` | VARCHAR | SipHash-1-3 integrity checksum |

### Checksum Integrity

Checksums are computed from the migration name, version number, and SQL content using SipHash-1-3. On each run, stored checksums are compared against current file checksums. A mismatch aborts execution to prevent applying migrations against a modified history.

### Multi-Database Support

The `_schema` variants support both trexsql and PostgreSQL databases:

| Database value | Target |
|----------------|--------|
| `_config` | PostgreSQL metadata database |
| `memory` | trexsql in-memory database |
| *other* | A named trexsql attached database |

### Transaction Safety

Each migration runs in a transaction where supported. trexsql supports transactional DDL, so failed migrations are rolled back. PostgreSQL migrations also run transactionally.

## Functions

### `trex_migration_run(path)`

Discover and execute pending migrations from a directory. Migrations are SQL files named with a numeric version prefix (e.g., `001_create_tables.sql`).

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | Execution status |

```sql
SELECT * FROM trex_migration_run('./migrations');
```

### `trex_migration_status(path)`

Show status of all discovered migrations (applied, pending, or checksum mismatch).

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | applied, pending, or checksum_mismatch |
| applied_on | VARCHAR | Application timestamp |
| checksum | VARCHAR | File checksum |

```sql
SELECT * FROM trex_migration_status('./migrations');
```

### `trex_migration_run_schema(path, schema, database)`

Run migrations in a specific schema and database. Supports both trexsql and PostgreSQL databases.

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |
| schema | VARCHAR | Target schema name |
| database | VARCHAR | Target database name |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | Execution status |

```sql
SELECT * FROM trex_migration_run_schema('./migrations', 'my_schema', 'my_database');
```

### `trex_migration_status_schema(path, schema, database)`

Show migration status in a specific schema and database.

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |
| schema | VARCHAR | Target schema name |
| database | VARCHAR | Target database name |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | applied, pending, or checksum_mismatch |
| applied_on | VARCHAR | Application timestamp |
| checksum | VARCHAR | File checksum |

```sql
SELECT * FROM trex_migration_status_schema('./migrations', 'my_schema', 'my_database');
```

## Migration Lifecycle

```mermaid
flowchart LR
    Discover["Discover files"] --> Verify["Verify checksums"]
    Verify --> Execute["Execute pending"]
    Execute --> Record["Record in history"]
```

## Error Scenarios

| Error | Cause | Resolution |
|-------|-------|------------|
| No migration files found | Directory is empty or files don't match naming pattern | Add files matching `V<n>__<name>.sql` |
| Duplicate version | Two files share the same version number | Renumber one of the conflicting files |
| Checksum mismatch | A previously applied migration file was modified | Restore the original file or reset the schema history |
| SQL failure | A migration statement failed to execute | Fix the SQL error and re-run |
| Directory not found | The specified path does not exist | Verify the path passed to the function |
