---
sidebar_position: 1
---

# Docker Deployment

trexsql is distributed as a Docker image that bundles the trex binary, all extensions, and the core management application.

## Quick Start

```bash
docker compose up
```

This starts:

- **PostgreSQL 16** on port `65433` — metadata store with auto-schema migration
- **trex** on port `8001` — trexsql engine with core web UI

## Docker Compose

```yaml
services:
  postgres:
    image: postgres:16
    ports:
      - 65433:5432
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: mypass
      POSTGRES_DB: testdb
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./core/seed.sql:/docker-entrypoint-initdb.d/seed.sql
      - ./core/schema:/docker-entrypoint-initdb.d/schema

  trex:
    build: .
    ports:
      - 8001:8001
    volumes:
      - ./core:/usr/src/core
      - ./functions:/usr/src/functions
      - ./docs:/usr/src/docs
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://postgres:mypass@postgres:5432/testdb
      BETTER_AUTH_SECRET: <32+ character secret>
      BASE_PATH: /trex
      BETTER_AUTH_URL: http://localhost:8001/trex
```

The volume mounts enable hot-reload during development — changes to `core/`, `functions/`, and `docs/` are immediately reflected.

## Dockerfile

The Dockerfile uses a two-stage build:

1. **Builder** (Debian trixie-slim) — Compiles the Rust `trex` binary and `libtrexsql_engine`
2. **Runtime** (node:20-trixie-slim) — Installs extensions via npm, downloads official trexsql extensions, and copies the core application

Extensions are installed from the configured npm registry and collected into `/usr/lib/trexsql/extensions/`.

## Services Started by Default

The `STARTUP_SQL` environment variable bootstraps services on container start. The default configuration starts:

- **trexas** — Core web server (port 8001)
- **flight** — Arrow Flight SQL server (port 8815)
- **pgwire** — PostgreSQL wire protocol server (port 5432)

## Accessing Services

| Service | URL |
|---------|-----|
| Web UI | http://localhost:8001/trex/ |
| GraphiQL | http://localhost:8001/trex/graphiql |
| Documentation | http://localhost:8001/trex/docs/ |
| PostgreSQL (metadata) | `postgresql://postgres:mypass@localhost:65433/testdb` |
| Flight SQL | `grpc://localhost:8815` |
| pgwire | `postgresql://localhost:5432` |
