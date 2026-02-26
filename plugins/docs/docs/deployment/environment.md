---
sidebar_position: 2
---

# Environment Variables

## trex Binary

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_PATH` | `:memory:` | trexsql database file path |
| `EXTENSION_DIR` | `/usr/lib/trexsql/extensions` | Directory to scan for `.trex` and `.duckdb_extension` files |
| `STARTUP_SQL` | — | SQL to execute after loading extensions |

## Core Server

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string |
| `BASE_PATH` | `/trex` | URL base path for all routes |
| `SCHEMA_DIR` | — | Path to PostgreSQL schema migration files |
| `PORT` | `8000` | Server listen port |

## Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `BETTER_AUTH_SECRET` | — | Session signing secret (min 32 characters) |
| `BETTER_AUTH_URL` | `http://localhost:8000` | Public URL for auth callbacks |
| `BETTER_AUTH_TRUSTED_ORIGINS` | `http://localhost:5173` | Comma-separated trusted origins for CORS |
| `ADMIN_EMAIL` | — | Email address auto-promoted to admin |
| `TREX_INITIAL_API_KEY_NAME` | — | Bootstrap API key name for initial setup |

## Social Providers

| Variable | Description |
|----------|-------------|
| `GOOGLE_CLIENT_ID` | Google OAuth client ID |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret |
| `GITHUB_CLIENT_ID` | GitHub OAuth client ID |
| `GITHUB_CLIENT_SECRET` | GitHub OAuth client secret |
| `MICROSOFT_CLIENT_ID` | Microsoft OAuth client ID |
| `MICROSOFT_CLIENT_SECRET` | Microsoft OAuth client secret |

## Plugins

| Variable | Default | Description |
|----------|---------|-------------|
| `PLUGINS_PATH` | `./plugins` | Plugin installation directory |
| `PLUGINS_DEV_PATH` | `./plugins-dev` | Development plugins (NODE_ENV=development only) |
| `PLUGINS_BASE_PATH` | `/plugins` | URL path prefix for plugin routes |
| `PLUGINS_INFORMATION_URL` | — | Package feed URL for plugin discovery |
| `TPM_REGISTRY_URL` | — | NPM registry URL for plugin installation |
| `PLUGINS_IMAGE_TAG` | — | Docker image tag appended to flow plugins |
| `PUBLIC_FQDN` | — | Public FQDN for `$$FQDN$$` substitution in UI plugins |

## Cluster (SWARM_CONFIG)

The `SWARM_CONFIG` environment variable is a JSON object controlling cluster initialization:

```json
{
  "cluster_id": "local",
  "nodes": {
    "local": {
      "gossip_addr": "0.0.0.0:4200",
      "extensions": [
        { "name": "trexas", "config": { "host": "0.0.0.0", "port": 8001 } },
        { "name": "flight", "config": { "host": "0.0.0.0", "port": 8815 } },
        { "name": "pgwire", "config": { "host": "0.0.0.0", "port": 5432 } }
      ]
    }
  }
}
```

| Variable | Description |
|----------|-------------|
| `SWARM_CONFIG` | JSON cluster configuration |
| `SWARM_NODE` | Node name within the cluster config |

## Flows (Prefect)

| Variable | Description |
|----------|-------------|
| `PREFECT_API_URL` | Prefect API URL for flow deployments |
