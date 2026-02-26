---
slug: /
sidebar_position: 1
---

# Getting Started

trexsql is a distributed SQL engine built on top of a columnar database core. It provides clustering, federated queries, Arrow Flight SQL transport, and a full-stack management application with a plugin system.

## Architecture

trexsql extends the core database engine with loadable extensions (`.trex` files) that add distributed query capabilities, protocol servers, ETL pipelines, and more. The management application (`core/`) provides a web UI, GraphQL API, authentication, and a plugin system for extending functionality.

```mermaid
flowchart TD
    subgraph Frontend
        WebUI["Web UI · /trex/"]
        GraphQL["GraphQL API · /trex/graphql"]
        Docs["Docs · /trex/docs/"]
    end

    subgraph Server
        Express["Express + PostGraphile"]
        Auth["Better Auth"]
        MCP["MCP Server"]
        PluginSys["Plugin System"]
    end

    subgraph Plugins["Plugin Types"]
        Functions
        UI
        Flows
        Transforms
        Migrations
    end

    subgraph Data["Data Layer"]
        PostgreSQL["PostgreSQL (metadata)"]
        Engine["trexsql Engine"]
    end

    subgraph Extensions
        db & tpm & hana & pgwire & chdb
        etl & fhir & migration & ai & atlas
    end

    Frontend --> Server
    Server --> Plugins
    Server --> Data
    Engine --> Extensions
```

## Cluster Architecture

In a distributed deployment, multiple trexsql nodes form a cluster using a gossip protocol for membership and Arrow Flight SQL for data transport.

```mermaid
flowchart LR
    Client["Client"] --> Coord

    subgraph Node1["Node 1 (Coordinator)"]
        Coord["Coordinator"]
        G1["Gossip"]
        F1["Flight SQL"]
        E1["Engine"]
        Coord --> E1
        Coord --> F1
    end

    subgraph Node2["Node 2"]
        G2["Gossip"]
        F2["Flight SQL"]
        E2["Engine"]
    end

    subgraph Node3["Node 3"]
        G3["Gossip"]
        F3["Flight SQL"]
        E3["Engine"]
    end

    G1 <-.->|gossip| G2
    G2 <-.->|gossip| G3
    G1 <-.->|gossip| G3

    F1 <-->|Arrow Flight| F2
    F1 <-->|Arrow Flight| F3
```

## Quick Start

```bash
docker compose up
```

This starts PostgreSQL and the trex service. The application is available at:

- **Web UI**: [http://localhost:8001/trex/](http://localhost:8001/trex/)
- **GraphiQL**: [http://localhost:8001/trex/graphiql](http://localhost:8001/trex/graphiql)
- **Docs**: [http://localhost:8001/trex/docs/](http://localhost:8001/trex/docs/)

## Extensions

Each extension adds SQL functions that can be called directly:

| Extension | Description |
|-----------|-------------|
| [db](sql-reference/db) | Distributed clustering, Arrow Flight, federated queries |
| [tpm](sql-reference/tpm) | Plugin package manager |
| [hana](sql-reference/hana) | SAP HANA database scanner |
| [pgwire](sql-reference/pgwire) | PostgreSQL wire protocol server |
| [chdb](sql-reference/chdb) | ClickHouse integration |
| [etl](sql-reference/etl) | PostgreSQL CDC replication |
| [fhir](sql-reference/fhir) | FHIR server |
| [migration](sql-reference/migration) | Database schema migrations |
| [ai](sql-reference/ai) | LLM inference via llama.cpp |
| [atlas](sql-reference/atlas) | OHDSI Atlas cohort SQL rendering |

## What's in This Documentation

- **[SQL Reference](sql-reference/db)** — Complete function reference for every extension
- **[Plugins](plugins/overview)** — How the plugin system works and how to build plugins
- **[JS / API Interface](js-interface/graphql)** — GraphQL, Auth, MCP, and Function Worker APIs
- **[Deployment](deployment/docker)** — Docker configuration and environment variables
