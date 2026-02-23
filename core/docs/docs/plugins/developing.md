---
sidebar_position: 2
---

# Developing Plugins

This guide covers creating a trexsql plugin from scratch.

## Package Structure

A plugin is an NPM package with a `trex` configuration in `package.json`:

```
my-plugin/
  package.json
  functions/           # API endpoint source files
    index.ts
    import_map.json
  dist/                # Built frontend assets
  migrations/          # SQL migration files
    001_init.sql
```

## package.json

The `trex` key defines what the plugin provides:

```json
{
  "name": "@trex/my-plugin",
  "version": "1.0.0",
  "trex": {
    "functions": {
      "env": {
        "_shared": {
          "DATABASE_URL": "${DATABASE_URL}"
        }
      },
      "api": [
        {
          "source": "/my-plugin",
          "function": "/functions/index.ts",
          "imports": "/functions/import_map.json"
        }
      ]
    },
    "ui": {
      "routes": [
        { "path": "/my-plugin", "dir": "dist" }
      ],
      "uiplugins": {
        "sidebar": [
          { "route": "/my-plugin", "label": "My Plugin", "icon": "LayoutDashboard" }
        ]
      }
    },
    "migrations": {
      "schema": "my_plugin",
      "database": "_config"
    }
  }
}
```

## Environment Variable Substitution

Function plugins support environment variable expansion in the `env` block:

| Pattern | Behavior |
|---------|----------|
| `${VAR}` | Value of env var (empty string if unset) |
| `${VAR:-default}` | Value or default if unset/empty |
| `${VAR-default}` | Value or default if unset |
| `${VAR:?error}` | Throw error if unset/empty |
| `${VAR:+alternate}` | Alternate value if set and non-empty |

## Init Functions

Run one-time setup tasks at plugin startup:

```json
{
  "trex": {
    "functions": {
      "init": [
        {
          "function": "/functions/setup.ts",
          "env": "production",
          "waitfor": "http://localhost:5432",
          "delay": 1000
        }
      ]
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `function` | Path to init script |
| `env` | Required `NODE_ENV` to run |
| `waitfor` | URL to poll before running |
| `waitforEnvVar` | Env var containing URL to poll |
| `delay` | Milliseconds to wait after init |

## Development Workflow

1. Create your plugin directory in `plugins-dev/` (auto-discovered when `NODE_ENV=development`)
2. Add your `package.json` with the `trex` configuration
3. Start the server with `docker compose up` — volume mounts enable hot reload
4. Access function endpoints at `/trex/plugins/<source>/*`
5. Access UI routes at the configured paths

## Publishing

Plugins are published as NPM packages to the configured registry and can be installed via:

```sql
SELECT * FROM trex_plugin_install_with_deps('@trex/my-plugin@1.0.0', './plugins');
```
