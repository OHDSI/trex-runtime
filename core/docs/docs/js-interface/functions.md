---
sidebar_position: 4
---

# Function Workers

Deno function workers provide serverless HTTP endpoints that run within the trexsql process. Both built-in functions (from `functions/`) and plugin functions share the same runtime.

## Built-in Functions

The `functions/` directory contains function workers loaded at startup. Each subdirectory with an `index.ts` becomes a route:

```
functions/
  hello-world/
    index.ts    → /trex/fn/hello-world
```

## Endpoint Pattern

| Pattern | Description |
|---------|-------------|
| `/trex/fn/:service_name` | Built-in function workers |
| `/trex/plugins/:source/*` | Plugin function workers |

All HTTP methods (GET, POST, PUT, DELETE, PATCH) are routed to the worker.

## Writing a Function

Functions export a default handler compatible with the Fetch API:

```typescript
export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET") {
      return new Response(JSON.stringify({ status: "ok" }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response("Method not allowed", { status: 405 });
  },
};
```

## Environment Variables

Workers receive environment variables configured in the plugin's `trex.functions.env._shared` block, plus the automatically-set `TREX_FUNCTION_PATH` variable pointing to the plugin directory.

## Import Maps

Workers can use Deno import maps for module resolution:

```json
{
  "imports": {
    "postgres": "https://deno.land/x/postgres/mod.ts"
  }
}
```

Reference the import map in the plugin configuration:

```json
{
  "api": [{
    "source": "/my-plugin",
    "function": "/functions/index.ts",
    "imports": "/functions/import_map.json"
  }]
}
```
