---
sidebar_position: 10
---

# atlas — OHDSI Cohort SQL

The `atlas` extension provides OHDSI Atlas/Circe cohort definition rendering and SQL translation. It converts cohort JSON definitions into executable SQL for various database dialects.

## Functions

### `trex_atlas_json_to_sql(definition, options)`

Convert an OHDSI cohort JSON definition to SQL.

| Parameter | Type | Description |
|-----------|------|-------------|
| definition | VARCHAR | Cohort JSON definition |
| options | VARCHAR | Rendering options |

**Returns:** VARCHAR — Generated SQL

```sql
SELECT trex_atlas_json_to_sql('{"cohort": ...}', '{}');
```

:::note
`circe_json_to_sql` is a deprecated alias for this function.
:::

### `trex_atlas_sql_render(cohort_definition, render_options)`

Render a cohort definition to SQL with specified rendering options.

| Parameter | Type | Description |
|-----------|------|-------------|
| cohort_definition | VARCHAR | Cohort JSON definition |
| render_options | VARCHAR | Rendering options JSON |

**Returns:** VARCHAR — Rendered SQL

```sql
SELECT trex_atlas_sql_render('{"cohort": ...}', '{"cdmSchema": "cdm", "targetSchema": "results"}');
```

### `trex_atlas_sql_translate(sql, dialect)`

Translate generated SQL to a target database dialect.

| Parameter | Type | Description |
|-----------|------|-------------|
| sql | VARCHAR | Source SQL |
| dialect | VARCHAR | Target dialect (e.g., `postgresql`, `spark`, `bigquery`) |

**Returns:** VARCHAR — Translated SQL

```sql
SELECT trex_atlas_sql_translate('SELECT DATEADD(day, 1, start_date) ...', 'postgresql');
```

### `trex_atlas_sql_render_translate(cohort_definition, render_options, target_dialect)`

Render a cohort definition and translate to a target dialect in one step.

| Parameter | Type | Description |
|-----------|------|-------------|
| cohort_definition | VARCHAR | Cohort JSON definition |
| render_options | VARCHAR | Rendering options JSON |
| target_dialect | VARCHAR | Target SQL dialect |

**Returns:** VARCHAR — Rendered and translated SQL

```sql
SELECT trex_atlas_sql_render_translate('{"cohort": ...}', '{"cdmSchema": "cdm"}', 'postgresql');
```

:::note
`circe_sql_render_translate` is a deprecated alias for this function.
:::

### `trex_atlas_generate_and_translate(specification, target_dialect)`

Generate SQL from a specification and translate to a target dialect.

| Parameter | Type | Description |
|-----------|------|-------------|
| specification | VARCHAR | Cohort specification |
| target_dialect | VARCHAR | Target SQL dialect |

**Returns:** VARCHAR

```sql
SELECT trex_atlas_generate_and_translate('{"specification": ...}', 'spark');
```

### `trex_atlas_check_cohort(cohort_json)`

Validate a cohort JSON definition.

| Parameter | Type | Description |
|-----------|------|-------------|
| cohort_json | VARCHAR | Cohort JSON to validate |

**Returns:** VARCHAR — Validation result

```sql
SELECT trex_atlas_check_cohort('{"cohort": ...}');
```

:::note
`circe_check_cohort` is a deprecated alias for this function.
:::

### `trex_atlas_hello(name)`

Test function to verify the extension is loaded.

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Input string |

**Returns:** VARCHAR

```sql
SELECT trex_atlas_hello('world');
```

### `trex_atlas_openssl_version(version_type)`

Return the OpenSSL version linked by the extension.

| Parameter | Type | Description |
|-----------|------|-------------|
| version_type | VARCHAR | Version string type |

**Returns:** VARCHAR

```sql
SELECT trex_atlas_openssl_version('full');
```
