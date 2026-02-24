---
sidebar_position: 3
---

# MCP Server

trexsql provides a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server for AI assistant integration. The MCP server exposes trexsql management operations as structured tools.

## Endpoint

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/trex/mcp` | Create session or send request |
| GET | `/trex/mcp` | Retrieve response for existing session |
| DELETE | `/trex/mcp` | Close session |

## Authentication

All requests require a Bearer token API key:

```
Authorization: Bearer trex_<48-hex-characters>
```

API keys are created via the admin UI or MCP tools. Only admin users can have API keys.

### Creating API Keys

Via SQL:

```sql
-- Keys are generated through the server API, not directly via SQL
```

Via MCP tool (if you already have a key):

```json
{
  "tool": "api-key-create",
  "arguments": {
    "userId": "admin-user-id",
    "name": "my-client"
  }
}
```

## Session Management

- Sessions are tracked via the `mcp-session-id` header
- Maximum 100 concurrent sessions
- Sessions expire after 30 minutes of inactivity

## Available Tools

### Cluster Management

| Tool | Description |
|------|-------------|
| `cluster-list-nodes` | List distributed cluster nodes |
| `cluster-list-services` | List running services |
| `cluster-get-status` | Cluster health summary |
| `cluster-start-service` | Start an extension service |
| `cluster-stop-service` | Stop an extension service |

### Database

| Tool | Description |
|------|-------------|
| `trexdb-*` | Database query and management |
| `database-*` | External database connections |
| `migration-*` | Schema migration management |
| `etl-*` | CDC replication operations |

### Administration

| Tool | Description |
|------|-------------|
| `user-*` | User account management |
| `session-*` | Session management |
| `role-*` | Role and permission management |
| `sso-*` | SSO provider configuration |
| `app-*` | Application configuration |
| `api-key-*` | API key management |
| `plugin-*` | Plugin management |

## Resources

The MCP server exposes the following resources:

| URI | Description |
|-----|-------------|
| `trex://plugin-development-guide` | Plugin development guide (Markdown) |

## Client Configuration

### Claude Desktop

```json
{
  "mcpServers": {
    "trexsql": {
      "url": "http://localhost:8001/trex/mcp",
      "headers": {
        "Authorization": "Bearer trex_your_api_key_here"
      }
    }
  }
}
```
