import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

const PLUGIN_DEVELOPMENT_GUIDE = `# Trex Plugin Development Guide

## Overview

Trex plugins extend the core application with custom functions (API endpoints), UI components,
database migrations, and workflow definitions. Plugins are NPM packages with a \`trex\` config
object in their \`package.json\`.

## Plugin Discovery

Plugins are loaded from two directories at server startup:

- **\`PLUGINS_PATH\`** (default: \`./plugins\`) — production plugins (installed via NPM)
- **\`PLUGINS_DEV_PATH\`** (default: \`./plugins-dev\`) — development plugins (only loaded when \`NODE_ENV=development\`)

Dev plugins get a \`-dev\` suffix appended to their version string automatically.

The scanner walks each directory, entering scoped directories (starting with \`@\`), and reads
each subdirectory's \`package.json\`. The short name is derived from the package name — if
scoped (e.g. \`@trex/my-plugin\`), only the part after \`/\` is used (\`my-plugin\`).

## Plugin Directory Structure

\`\`\`
my-plugin/
├── package.json          # Required — must contain "trex" config object
├── functions/            # Deno function workers (for "functions" type)
│   └── index.ts          # Handler using Deno.serve()
├── migrations/           # SQL migration files (for "migrations" type)
│   ├── 001_create_tables.sql
│   └── 002_add_indexes.sql
├── dist/                 # Built frontend assets (for "ui" type)
│   └── index.html
└── flows/                # Prefect workflow definitions (for "flow" type)
\`\`\`

## package.json Format

\`\`\`json
{
  "name": "@trex/my-plugin",
  "version": "1.0.0",
  "trex": {
    "functions": { ... },
    "ui": { ... },
    "migrations": { ... },
    "flow": { ... }
  }
}
\`\`\`

The \`trex\` object can contain any combination of the four plugin types. Each type is processed
by a dedicated handler. If \`trex\` is missing, the plugin is skipped with a log message.

---

## Plugin Type: Functions

Function plugins register Deno worker-based API endpoints. The full config structure:

\`\`\`json
{
  "trex": {
    "functions": {
      "env": {
        "_shared": {
          "DATABASE_URL": "\${DATABASE_URL}",
          "SECRET": "\${MY_SECRET:-default_value}"
        },
        "production": {
          "LOG_LEVEL": "warn"
        }
      },
      "roles": {
        "my-plugin-admin": ["my-plugin:read", "my-plugin:write"],
        "my-plugin-viewer": ["my-plugin:read"]
      },
      "scopes": [
        { "path": "/plugins/my-plugin/admin/*", "scopes": ["my-plugin:write"] }
      ],
      "init": [
        {
          "function": "/functions/setup.ts",
          "env": "production",
          "waitfor": "http://localhost:5432",
          "delay": 1000
        }
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
\`\`\`

### Config Fields

- **\`env\`** — Environment variable groups. \`_shared\` is always included. The group named by
  each function's \`env\` field is merged on top. Supports bash-like substitution:
  - \`\${VAR}\` — value of env var (empty string if unset)
  - \`\${VAR:-default}\` — value or default if unset/empty
  - \`\${VAR-default}\` — value or default if unset
  - \`\${VAR:?error}\` — value or throw if unset/empty
  - \`\${VAR:+alternate}\` — alternate if set and non-empty, else empty
- **\`roles\`** — Maps role names to arrays of scope strings. Accumulated across all plugins.
- **\`scopes\`** — Required URL scopes. Array of \`{ path, scopes }\` objects. Enforced by
  the \`pluginAuthz\` middleware.
- **\`init\`** — Functions executed once at startup (before API routes). Supports \`waitfor\`
  (URL to poll before running), \`waitforEnvVar\` (env var containing URL), and \`delay\`
  (ms to wait after init completes).
- **\`api\`** — Functions registered as Express routes. Each entry:
  - \`source\` — URL path segment (route is mounted at \`PLUGINS_BASE_PATH + source + "/*"\`)
  - \`function\` — path to the Deno worker script (relative to plugin dir)
  - \`env\` — which env group to use
  - \`imports\` — optional import map path (relative to plugin dir, or absolute URL if contains \`:\`)
  - \`eszip\` — optional pre-bundled eszip path

### Function Handler Pattern

Function handlers use the standard \`Deno.serve()\` pattern:

\`\`\`typescript
Deno.serve(async (req: Request) => {
  const url = new URL(req.url);

  if (req.method === "GET" && url.pathname.endsWith("/health")) {
    return new Response(JSON.stringify({ status: "ok" }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  if (req.method === "POST" && url.pathname.endsWith("/data")) {
    const body = await req.json();
    // Process request...
    return new Response(JSON.stringify({ result: "success" }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  return new Response("Not Found", { status: 404 });
});
\`\`\`

### Route Mounting

API routes are mounted at: \`PLUGINS_BASE_PATH + source + "/*"\`

With default config (\`PLUGINS_BASE_PATH=/plugins\`), a function with \`"source": "/my-plugin"\`
is accessible at \`/plugins/my-plugin/*\`.

All plugin routes pass through \`authContext\` and \`pluginAuthz\` middleware, so unauthenticated
requests will receive 401/403 responses.

The \`TREX_FUNCTION_PATH\` env var is automatically set to the plugin directory path in every worker.

---

## Plugin Type: UI

UI plugins serve static files and register menu items in the web UI.

\`\`\`json
{
  "trex": {
    "ui": {
      "routes": [
        {
          "path": "/my-plugin",
          "dir": "dist"
        }
      ],
      "uiplugins": {
        "sidebar": [
          {
            "route": "/my-plugin",
            "label": "My Plugin",
            "icon": "LayoutDashboard",
            "children": [
              {
                "route": "/my-plugin/settings",
                "label": "Settings"
              }
            ]
          }
        ]
      }
    }
  }
}
\`\`\`

### Config Fields

- **\`routes\`** — Static file serving. Each entry maps a URL path prefix to a directory:
  - \`path\` (or \`source\`) — URL prefix (mounted at \`PLUGINS_BASE_PATH + path\`)
  - \`dir\` (or \`target\`) — directory in the plugin containing static files
- **\`uiplugins\`** — Menu item definitions. Keys are menu categories (e.g. \`"sidebar"\`).
  Values are arrays of menu item objects with \`route\`, \`label\`, \`icon\`, and optional
  \`children\`. Items are merged with existing entries by route — matching routes update
  in place, new routes are appended.

### FQDN Substitution

In UI plugin JSON, the string \`$$FQDN$$\` is replaced with the value of the \`PUBLIC_FQDN\`
environment variable. Use this for absolute URLs in plugin configurations.

---

## Plugin Type: Migrations

Migration plugins run SQL schema migrations against trexsql databases.

\`\`\`json
{
  "trex": {
    "migrations": {
      "schema": "my_plugin",
      "database": "_config"
    }
  }
}
\`\`\`

### Config Fields

- **\`schema\`** (required) — The database schema name for this plugin's tables
- **\`database\`** (optional, default: \`"_config"\`) — The trexsql database connection name

### Migration Files

SQL migration files must be placed in a \`migrations/\` subdirectory of the plugin:

\`\`\`
my-plugin/
├── package.json
└── migrations/
    ├── 001_create_tables.sql
    ├── 002_add_indexes.sql
    └── 003_add_columns.sql
\`\`\`

Files are named with a numeric prefix for ordering: \`NNN_description.sql\`. They are executed
via \`trex_migration_run_schema(migrations_path, schema, database)\`.

Migration status can be checked with \`trex_migration_status_schema(migrations_path, schema, database)\`.

---

## Plugin Type: Flow

Flow plugins define Prefect workflow deployments with Docker-based execution.

\`\`\`json
{
  "trex": {
    "flow": {
      "image": "my-org/my-plugin-flows",
      "flows": [
        {
          "name": "my-etl-flow",
          "entrypoint": "flows/etl.py:main",
          "image": "my-org/my-etl:latest",
          "tags": ["etl", "production"],
          "concurrencyLimitName": "my-etl-limit",
          "concurrencyLimit": 3,
          "concurrencyLimitOptions": [
            { "tag": "my-etl-limit", "limit": "3" }
          ],
          "parameter_openapi_schema": {
            "type": "object",
            "properties": {
              "source": { "type": "string" }
            }
          }
        }
      ]
    }
  }
}
\`\`\`

### Config Fields

- **\`image\`** — Default Docker image for all flows in this plugin. The \`PLUGINS_IMAGE_TAG\`
  env var is appended as the tag.
- **\`flows\`** — Array of flow definitions:
  - \`name\` — Flow/deployment name in Prefect
  - \`entrypoint\` — Python entrypoint (\`file:function\`)
  - \`image\` — Override Docker image for this specific flow
  - \`tags\` — Prefect tags array
  - \`concurrencyLimitName\` / \`concurrencyLimit\` — Deployment-level concurrency
  - \`concurrencyLimitOptions\` — Global concurrency limits to ensure exist
  - \`parameter_openapi_schema\` — OpenAPI schema for flow parameters

Requires \`PREFECT_API_URL\` environment variable to be set. Flow plugins are skipped if Prefect is not configured.

---

## Development Workflow

### 1. Create Plugin

Create your plugin in the dev directory:

\`\`\`bash
mkdir -p plugins-dev/my-plugin/functions
\`\`\`

Create \`plugins-dev/my-plugin/package.json\`:

\`\`\`json
{
  "name": "@trex/my-plugin",
  "version": "0.1.0",
  "trex": {
    "functions": {
      "api": [
        {
          "source": "/my-plugin",
          "function": "/functions/index.ts"
        }
      ]
    }
  }
}
\`\`\`

Create \`plugins-dev/my-plugin/functions/index.ts\`:

\`\`\`typescript
Deno.serve(async (req: Request) => {
  return new Response(JSON.stringify({ message: "Hello from my-plugin!" }), {
    headers: { "Content-Type": "application/json" },
  });
});
\`\`\`

### 2. Start Server

Ensure \`NODE_ENV=development\` is set so dev plugins are loaded:

\`\`\`bash
docker compose up
\`\`\`

### 3. Test

Use the \`plugin-function-invoke\` MCP tool to test your endpoint:

- path: \`/plugins/my-plugin/health\`
- method: \`GET\`

Or use the \`plugin-get-info\` MCP tool to inspect your plugin's configuration and status.

### 4. Iterate

Restart the server to pick up changes to plugin configuration. Function worker code
changes may be picked up automatically depending on caching settings.

## Deployment

1. Publish your plugin to NPM: \`npm publish\` (as \`@trex/my-plugin\`)
2. Install in production: use the \`plugin-install\` MCP tool with \`@trex/my-plugin\`
3. Restart the server to load the new plugin
`;

export function registerPluginResources(server: McpServer) {
  server.resource(
    "plugin-development-guide",
    "trex://plugin-development-guide",
    {
      description:
        "Comprehensive guide for developing trex plugins — covers plugin types (functions, UI, migrations, flows), package.json format, handler patterns, and development workflow",
      mimeType: "text/markdown",
    },
    async () => ({
      contents: [
        {
          uri: "trex://plugin-development-guide",
          mimeType: "text/markdown",
          text: PLUGIN_DEVELOPMENT_GUIDE,
        },
      ],
    })
  );
}
