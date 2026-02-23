---
sidebar_position: 1
---

# GraphQL API

trexsql exposes a GraphQL API via [PostGraphile 5.0](https://postgraphile.org/), which automatically generates a GraphQL schema from the PostgreSQL database.

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /trex/graphql` | GraphQL query/mutation endpoint |
| `GET /trex/graphiql` | Interactive GraphiQL IDE |

## Authentication

GraphQL requests use the same session-based authentication as the web UI. The auth middleware extracts the session and sets PostgreSQL settings:

- `app.user_id` — current user ID
- `app.user_role` — current user role

These settings enable PostgreSQL Row-Level Security (RLS) policies.

## Schema

The GraphQL schema is derived from the PostgreSQL `trex` schema tables:

- **user** — User accounts
- **session** — Active sessions
- **account** — OAuth provider accounts
- **verification** — Email verification tokens
- **role** — Application roles
- **api_key** — MCP API keys
- **sso_provider** — SSO provider configuration

PostGraphile's connection filter plugin enables advanced filtering on all queries.

## Example Query

```graphql
query {
  allUsers(first: 10) {
    nodes {
      id
      name
      email
      role
      createdAt
    }
  }
}
```

## Subscriptions

Real-time subscriptions are supported via WebSocket at the `/trex/graphql` endpoint using the `graphql-ws` protocol.
