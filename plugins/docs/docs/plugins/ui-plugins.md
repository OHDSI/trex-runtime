---
sidebar_position: 3
---

# UI Plugins

UI plugins serve static frontend assets and register navigation items in the web application sidebar.

## Configuration

```json
{
  "trex": {
    "ui": {
      "routes": [
        { "path": "/my-plugin", "dir": "dist" }
      ],
      "uiplugins": {
        "sidebar": [
          {
            "route": "/my-plugin",
            "label": "My Plugin",
            "icon": "LayoutDashboard",
            "children": [
              { "route": "/my-plugin/settings", "label": "Settings" }
            ]
          }
        ]
      }
    }
  }
}
```

## Static Routes

Each entry in `routes` maps a URL path to a directory of static files:

| Field | Description |
|-------|-------------|
| `path` | URL path prefix (relative to base) |
| `dir` | Directory containing built assets |

The static files are served via Deno's `op_register_static_route` operation.

## Sidebar Navigation

The `uiplugins.sidebar` array registers menu items in the web application:

| Field | Description |
|-------|-------------|
| `route` | Navigation route |
| `label` | Display label |
| `icon` | Lucide icon name |
| `children` | Nested navigation items |

Menu items are merged by route — if a route already exists, the existing entry is updated; otherwise a new entry is appended.

## FQDN Substitution

Use `$$FQDN$$` in configuration values to have it replaced with the `PUBLIC_FQDN` environment variable at runtime. This is useful for plugins that need to reference the server's public URL.

## Building UI Plugins

UI plugins typically use a frontend framework (React, Vue, etc.) that builds to static HTML/JS/CSS:

```bash
cd my-plugin
npm run build   # Outputs to dist/
```

The built `dist/` directory is what gets referenced in the `routes` configuration.
