---
sidebar_position: 4
---

# Function Plugins

Function plugins register HTTP API endpoints powered by Deno EdgeRuntime workers. Each function runs in an isolated Deno environment with configurable permissions.

## Configuration

```json
{
  "trex": {
    "functions": {
      "env": {
        "_shared": {
          "DATABASE_URL": "${DATABASE_URL}",
          "API_KEY": "${MY_API_KEY:-}"
        }
      },
      "roles": {
        "my-plugin-admin": ["my-plugin:read", "my-plugin:write"],
        "my-plugin-viewer": ["my-plugin:read"]
      },
      "scopes": [
        { "path": "/plugins/my-plugin/admin/*", "scopes": ["my-plugin:write"] },
        { "path": "/plugins/my-plugin/*", "scopes": ["my-plugin:read"] }
      ],
      "api": [
        {
          "source": "/my-plugin",
          "function": "/functions/index.ts",
          "env": "production",
          "imports": "/functions/import_map.json"
        }
      ]
    }
  }
}
```

## API Route Configuration

Each entry in `api` registers an Express route:

| Field | Description |
|-------|-------------|
| `source` | URL path (mounted under `/trex/plugins/`) |
| `function` | Path to worker script (relative to plugin dir) |
| `env` | Required `NODE_ENV` (omit to always load) |
| `imports` | Path to Deno import map |

Routes handle all HTTP methods (GET, POST, PUT, DELETE, PATCH) at the mounted path.

## Roles & Scopes

Plugins define their own authorization model:

- **Roles** map a role name to a set of scopes
- **Scopes** protect URL patterns, requiring the caller to have specific scopes

Roles are automatically created in the `trex.role` database table. Admin users bypass all scope checks. Regular users must have the appropriate roles assigned to access protected endpoints.

## Environment Variables

The `_shared` env block provides variables to all functions in the plugin. Variables support [substitution syntax](developing#environment-variable-substitution) for referencing server environment variables.

The `TREX_FUNCTION_PATH` variable is automatically set to the plugin's directory path.

## Worker Isolation

Each function runs in an isolated Deno worker with:

- Memory and CPU limits
- Configurable permissions
- Import map support for module resolution
- Access to the `Deno` global and standard library
