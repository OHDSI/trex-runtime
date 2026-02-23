// @ts-ignore
import { STATUS_CODE } from "https://deno.land/std/http/status.ts";
import { join } from "jsr:@std/path@^1.0";
import express from "express";
import { createServer } from "node:http";
import { grafserv } from "postgraphile/grafserv/express/v4";
import cors from "cors";
import { BASE_PATH, FUNCTIONS_BASE_PATH } from "./config.ts";
import { auth, initAuthFromDB } from "./auth.ts";
import { createPostGraphile } from "./postgraphile.ts";
import { authContext } from "./middleware/auth-context.ts";
import { Plugins } from "./plugin/plugin.ts";
import { addPluginRoutes } from "./routes/plugin.ts";

console.log("main function started");
console.log(Deno.version);

addEventListener("beforeunload", () => {
  console.log("main worker exiting");
});

addEventListener("unhandledrejection", (ev) => {
  console.log(ev);
  ev.preventDefault();
});

const app = express();
const server = createServer(app);

const trustedOrigins = (Deno.env.get("BETTER_AUTH_TRUSTED_ORIGINS") || "").split(",").filter(Boolean);
app.use(cors({
  origin: trustedOrigins.length > 0 ? trustedOrigins : false,
  credentials: true,
}));

// Public settings endpoint — no auth required, only whitelisted keys
const PUBLIC_SETTING_KEYS = ["auth.selfRegistration"];

app.get(`${BASE_PATH}/api/settings/public`, async (_req, res) => {
  try {
    const { pool } = await import("./auth.ts");
    const result = await pool.query(
      `SELECT key, value FROM trex.setting WHERE key = ANY($1)`,
      [PUBLIC_SETTING_KEYS]
    );
    const settings: Record<string, any> = {};
    for (const row of result.rows) {
      settings[row.key] = row.value;
    }
    res.json(settings);
  } catch {
    // Table may not exist yet — return safe defaults
    res.json({ "auth.selfRegistration": false });
  }
});

// Block self-registration when disabled
app.post(`${BASE_PATH}/api/auth/sign-up/email`, async (req, res, next) => {
  try {
    const { pool } = await import("./auth.ts");
    const result = await pool.query(
      `SELECT value FROM trex.setting WHERE key = 'auth.selfRegistration'`
    );
    const enabled = result.rows.length > 0 && result.rows[0].value === true;
    if (!enabled) {
      res.status(403).json({ code: "REGISTRATION_DISABLED" });
      return;
    }
    next();
  } catch {
    // If setting table doesn't exist, block registration by default
    res.status(403).json({ code: "REGISTRATION_DISABLED" });
  }
});

// Clear mustChangePassword flag (called after successful password change)
// Must be registered before the Better Auth catch-all handler
app.post(`${BASE_PATH}/api/auth/password-changed`, async (req, res) => {
  try {
    const session = await auth.api.getSession({ headers: req.headers as any });
    if (!session?.user) {
      res.status(401).json({ error: "Unauthorized" });
      return;
    }
    const { pool } = await import("./auth.ts");
    await pool.query(
      'UPDATE trex."user" SET "mustChangePassword" = false, "updatedAt" = NOW() WHERE id = $1',
      [session.user.id]
    );
    res.json({ success: true });
  } catch (err) {
    console.error("password-changed error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Better Auth handler — construct a web Request from Express req
// since toNodeHandler has body-parsing issues in the Deno runtime
app.use(`${BASE_PATH}/api/auth`, async (req, res) => {
  console.log(`[auth] ${req.method} ${req.originalUrl}`);
  try {
    const host = req.get("host") || "localhost";
    const protocol = req.protocol || "http";
    const url = `${protocol}://${host}${req.originalUrl}`;
    console.log(`[auth] Constructed URL: ${url}`);

    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val) headers.set(key, Array.isArray(val) ? val.join(", ") : val);
    }

    let body: Blob | undefined;
    if (req.method !== "GET" && req.method !== "HEAD") {
      const chunks: Uint8Array[] = [];
      for await (const chunk of req) {
        chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
      }
      if (chunks.length > 0) body = new Blob(chunks);
    }

    const webReq = new Request(url, {
      method: req.method,
      headers,
      body,
    });

    const webRes = await auth.handler(webReq);
    console.log(`[auth] Response status: ${webRes.status}`);

    res.status(webRes.status);
    webRes.headers.forEach((value, key) => {
      res.append(key, value);
    });
    const text = await webRes.text();
    res.send(text);
  } catch (err) {
    console.error("Auth handler error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Deno doesn't have `global` — polyfill for npm packages that expect Node.js
if (typeof (globalThis as any).global === "undefined") {
  (globalThis as any).global = globalThis;
}

// MCP server (before authContext — uses its own API key auth)
try {
  const { mountMcpServer } = await import("./mcp/index.ts");
  mountMcpServer(app);
  console.log(`MCP server mounted on ${BASE_PATH}/mcp`);
} catch (err) {
  console.error("MCP server failed to initialize:", err);
}

// API key management endpoint (session-authenticated for web UI bootstrap)
app.post(`${BASE_PATH}/api/api-keys`, express.json(), async (req, res) => {
  try {
    const session = await auth.api.getSession({ headers: req.headers as any });
    if (!session?.user || (session.user as any).role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const { generateApiKey } = await import("./mcp/auth.ts");
    const { name, expiresAt } = req.body || {};
    if (!name) {
      res.status(400).json({ error: "name is required" });
      return;
    }
    let expiresDate: Date | undefined;
    if (expiresAt) {
      expiresDate = new Date(expiresAt);
      if (isNaN(expiresDate.getTime())) {
        res.status(400).json({ error: "Invalid expiresAt date" });
        return;
      }
    }
    const result = await generateApiKey(
      session.user.id,
      name,
      expiresDate,
    );
    res.json(result);
  } catch (err) {
    console.error("API key creation error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get(`${BASE_PATH}/api/api-keys`, async (req, res) => {
  try {
    const session = await auth.api.getSession({ headers: req.headers as any });
    if (!session?.user || (session.user as any).role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const { pool } = await import("./auth.ts");
    const result = await pool.query(
      `SELECT id, name, key_prefix, "lastUsedAt", "expiresAt", "revokedAt", "createdAt" FROM trex.api_key WHERE "userId" = $1 ORDER BY "createdAt" DESC`,
      [session.user.id]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("API key list error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete(`${BASE_PATH}/api/api-keys/:id`, async (req, res) => {
  try {
    const session = await auth.api.getSession({ headers: req.headers as any });
    if (!session?.user || (session.user as any).role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const { pool } = await import("./auth.ts");
    const result = await pool.query(
      `UPDATE trex.api_key SET "revokedAt" = NOW() WHERE id = $1 AND "userId" = $2 AND "revokedAt" IS NULL RETURNING id`,
      [req.params.id, session.user.id]
    );
    if (result.rows.length === 0) {
      res.status(404).json({ error: "Key not found or already revoked" });
      return;
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("API key revoke error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.use(authContext);

try {
  await Plugins.initPlugins(app);
  addPluginRoutes(app);
  console.log("Plugin system initialized");
} catch (err) {
  console.error("Plugin system failed to initialize:", err);
}

// Run core schema migrations (SCHEMA_DIR) before plugin migrations
try {
  const schemaDir = Deno.env.get("SCHEMA_DIR");
  if (schemaDir) {
    // deno-lint-ignore no-explicit-any
    const conn = new (globalThis as any).Trex.TrexDB("memory");
    const { escapeSql: esc } = await import("./lib/sql.ts");
    const safeDir = esc(schemaDir);
    await conn.execute(`SELECT * FROM trex_migration_run_schema('${safeDir}', 'trex', '_config')`, []);
    console.log("Core schema migrations applied");
  }
} catch (err) {
  console.error("Core schema migration failed:", err);
}


// Run plugin migrations after plugin discovery
try {
  const { runAllPluginMigrations } = await import("./plugin/migration.ts");
  await runAllPluginMigrations();
} catch (err) {
  console.error("Plugin migration execution failed:", err);
}

// Auto-create roles declared by plugins in the PostgreSQL role table
try {
  const { ensureRolesExist } = await import("./plugin/function.ts");
  await ensureRolesExist();
} catch (err) {
  console.error("Role auto-creation failed:", err);
}

const databaseUrl = Deno.env.get("DATABASE_URL");
if (databaseUrl) {
  try {
    const schemas = (Deno.env.get("PG_SCHEMA") || "trex").split(",");
    const pgl = createPostGraphile(databaseUrl, schemas);
    const serv = pgl.createServ(grafserv);
    await serv.addTo(app, server);
    console.log(`PostGraphile mounted on ${BASE_PATH}/graphql and ${BASE_PATH}/graphiql`);
  } catch (err) {
    console.error("PostGraphile failed to initialize:", err);
  }
} else {
  console.warn("DATABASE_URL not set — PostGraphile disabled");
}

app.get(`${BASE_PATH}/_internal/health`, (_req, res) => {
  res.status(STATUS_CODE.OK).json({ message: "ok" });
});

app.get(`${BASE_PATH}/_internal/metric`, async (req, res) => {
  const session = await auth.api.getSession({ headers: req.headers as any });
  if (!session?.user || (session.user as any).role !== "admin") {
    res.status(401).json({ error: "Admin authentication required" });
    return;
  }
  const metric = await EdgeRuntime.getRuntimeMetrics();
  res.json(metric);
});

app.put(`${BASE_PATH}/_internal/upload`, async (req, res) => {
  try {
    const session = await auth.api.getSession({ headers: req.headers as any });
    if (!session?.user || (session.user as any).role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    let body = "";
    for await (const chunk of req) {
      body += chunk;
    }
    const dir = await Deno.makeTempDir();
    const path = join(dir, "index.ts");
    await Deno.writeTextFile(path, body);
    res.json({ path: dir });
  } catch (err) {
    res.status(STATUS_CODE.BadRequest).json(err);
  }
});

// Worker routing (must be before SPA catch-all)
// Scoped function route: /fn/:scope/:service_name -> ./functions/@:scope/:service_name
app.use(`${FUNCTIONS_BASE_PATH}/:scope/:service_name`, async (req, res, next) => {
  const { scope, service_name: serviceName } = req.params;
  const scopeDir = `@${scope}`;
  const servicePath = `./functions/${scopeDir}/${serviceName}`;

  try {
    await Deno.stat(servicePath);
  } catch {
    return next();
  }

  const createWorker = async () => {
    const memoryLimitMb = 150;
    const workerTimeoutMs = 5 * 60 * 1000;
    const noModuleCache = false;
    const envVarsObj = Deno.env.toObject();
    const envVars = Object.keys(envVarsObj).map((k) => [k, envVarsObj[k]]);
    const forceCreate = false;
    const cpuTimeSoftLimitMs = 10000;
    const cpuTimeHardLimitMs = 20000;

    return await EdgeRuntime.userWorkers.create({
      servicePath,
      memoryLimitMb,
      workerTimeoutMs,
      noModuleCache,
      envVars,
      forceCreate,
      cpuTimeSoftLimitMs,
      cpuTimeHardLimitMs,
      context: {
        useReadSyncFileAPI: true,
        unstableSloppyImports: true,
      },
    });
  };

  const host = req.get("host") || "localhost";
  const protocol = req.protocol || "http";
  const webUrl = `${protocol}://${host}${req.originalUrl}`;
  const webHeaders = new Headers();
  for (const [key, val] of Object.entries(req.headers)) {
    if (val) webHeaders.set(key, Array.isArray(val) ? val.join(", ") : val);
  }
  let reqBody: Blob | undefined;
  if (req.method !== "GET" && req.method !== "HEAD") {
    const chunks: Uint8Array[] = [];
    for await (const chunk of req) {
      chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
    }
    if (chunks.length > 0) reqBody = new Blob(chunks);
  }
  const webReq = new Request(webUrl, {
    method: req.method,
    headers: webHeaders,
    body: reqBody,
  });

  const callWorker = async (): Promise<Response> => {
    const MAX_RETRIES = 3;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const worker = await createWorker();
        const controller = new AbortController();
        return await worker.fetch(webReq, { signal: controller.signal });
      } catch (e) {
        if (e instanceof Deno.errors.WorkerAlreadyRetired && attempt < MAX_RETRIES - 1) {
          continue;
        }
        const error = { msg: e.toString() };
        return new Response(JSON.stringify(error), {
          status: STATUS_CODE.InternalServerError,
          headers: { "Content-Type": "application/json" },
        });
      }
    }
    return new Response(JSON.stringify({ msg: "Worker unavailable after retries" }), {
      status: STATUS_CODE.InternalServerError,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const workerResponse = await callWorker();
    res.status(workerResponse.status);
    workerResponse.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });
    const body = await workerResponse.text();
    res.send(body);
  } catch (err) {
    res.status(STATUS_CODE.InternalServerError).json({ msg: String(err) });
  }
});

// Unscoped function route: /fn/:service_name -> ./functions/:service_name
app.use(`${FUNCTIONS_BASE_PATH}/:service_name`, async (req, res, next) => {
  const serviceName = req.params.service_name;

  if (
    serviceName === "_internal" ||
    serviceName === "graphql" ||
    serviceName === "graphiql" ||
    serviceName === "api"
  ) {
    return next();
  }

  let servicePath: string;
  if (serviceName.startsWith("tmp")) {
    try {
      servicePath = await Deno.realPath(`/tmp/${serviceName}`);
      if (!servicePath.startsWith("/tmp/")) {
        res.status(400).json({ error: "Invalid service path" });
        return;
      }
    } catch (err) {
      res.status(STATUS_CODE.BadRequest).json(err);
      return;
    }
  } else {
    servicePath = `./functions/${serviceName}`;
  }

  try {
    await Deno.stat(servicePath);
  } catch {
    return next();
  }

  const createWorker = async () => {
    const memoryLimitMb = 150;
    const workerTimeoutMs = 5 * 60 * 1000;
    const noModuleCache = false;
    const envVarsObj = Deno.env.toObject();
    const envVars = Object.keys(envVarsObj).map((k) => [k, envVarsObj[k]]);
    const forceCreate = false;
    const cpuTimeSoftLimitMs = 10000;
    const cpuTimeHardLimitMs = 20000;

    return await EdgeRuntime.userWorkers.create({
      servicePath,
      memoryLimitMb,
      workerTimeoutMs,
      noModuleCache,
      envVars,
      forceCreate,
      cpuTimeSoftLimitMs,
      cpuTimeHardLimitMs,
      context: {
        useReadSyncFileAPI: true,
        unstableSloppyImports: true,
      },
    });
  };

  const host = req.get("host") || "localhost";
  const protocol = req.protocol || "http";
  const webUrl = `${protocol}://${host}${req.originalUrl}`;
  const webHeaders = new Headers();
  for (const [key, val] of Object.entries(req.headers)) {
    if (val) webHeaders.set(key, Array.isArray(val) ? val.join(", ") : val);
  }
  let reqBody: Blob | undefined;
  if (req.method !== "GET" && req.method !== "HEAD") {
    const chunks: Uint8Array[] = [];
    for await (const chunk of req) {
      chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
    }
    if (chunks.length > 0) reqBody = new Blob(chunks);
  }
  const webReq = new Request(webUrl, {
    method: req.method,
    headers: webHeaders,
    body: reqBody,
  });

  const callWorker = async (): Promise<Response> => {
    const MAX_RETRIES = 3;
    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const worker = await createWorker();
        const controller = new AbortController();
        return await worker.fetch(webReq, { signal: controller.signal });
      } catch (e) {
        if (e instanceof Deno.errors.WorkerAlreadyRetired && attempt < MAX_RETRIES - 1) {
          continue;
        }
        const error = { msg: e.toString() };
        return new Response(JSON.stringify(error), {
          status: STATUS_CODE.InternalServerError,
          headers: { "Content-Type": "application/json" },
        });
      }
    }
    return new Response(JSON.stringify({ msg: "Worker unavailable after retries" }), {
      status: STATUS_CODE.InternalServerError,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const workerResponse = await callWorker();
    res.status(workerResponse.status);
    workerResponse.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });
    const body = await workerResponse.text();
    res.send(body);
  } catch (err) {
    res.status(STATUS_CODE.InternalServerError).json({ msg: String(err) });
  }
});

try {
  const docsDistPath = join(Deno.cwd(), "docs", "build");
  await Deno.stat(docsDistPath);
  const serveDocsStatic = (await import("express")).default.static;
  app.use(`${BASE_PATH}/docs`, serveDocsStatic(docsDistPath));
  app.get(`${BASE_PATH}/docs/*`, (_req, res) => {
    res.sendFile(join(docsDistPath, "index.html"));
  });
  console.log("Serving docs from docs/build/");
} catch (_e) {
  console.warn("Docs not found — /trex/docs disabled");
}

// Serve self-hosted Shinylive assets (must be before SPA catch-all)
try {
  const shinyliveDistPath = join(Deno.cwd(), "shinylive");
  await Deno.stat(shinyliveDistPath);
  const serveShiny = (await import("express")).default.static;
  app.use(`${BASE_PATH}/shinylive`, serveShiny(shinyliveDistPath));
  console.log("Serving Shinylive assets from shinylive/");
} catch { /* shinylive assets not present — skip */ }

try {
  const schemaDir = Deno.env.get("SCHEMA_DIR");
  const coreRoot = schemaDir ? join(schemaDir, "..") : join(Deno.cwd(), "core");
  const webDistPath = join(coreRoot, "web", "dist");
  await Deno.stat(webDistPath);
  const serveStatic = (await import("express")).default.static;
  app.use(BASE_PATH, serveStatic(webDistPath));
  app.get(`${BASE_PATH}/*`, (_req, res, next) => {
    if (_req.path.startsWith(`${BASE_PATH}/api/`) || _req.path.startsWith(`${BASE_PATH}/graphql`) || _req.path.startsWith(`${BASE_PATH}/graphiql`) || _req.path.startsWith(`${BASE_PATH}/_internal`) || _req.path.startsWith(`${BASE_PATH}/docs`)) {
      return next();
    }
    res.sendFile(join(webDistPath, "index.html"));
  });
  console.log("Serving static files from core/web/dist/");
} catch (e) {
  console.warn("Static file serving disabled:", e);
}

app.get("/", (_req, res) => {
  res.redirect(`${BASE_PATH}/`);
});

await initAuthFromDB();

// Bootstrap initial API key from env var (for Docker/CI)
const initialKeyName = Deno.env.get("TREX_INITIAL_API_KEY_NAME");
if (initialKeyName) {
  try {
    const { pool } = await import("./auth.ts");
    const existing = await pool.query("SELECT 1 FROM trex.api_key LIMIT 1");
    if (existing.rows.length === 0) {
      const { generateApiKey } = await import("./mcp/auth.ts");
      const adminResult = await pool.query(
        `SELECT id FROM trex."user" WHERE role = 'admin' ORDER BY "createdAt" ASC LIMIT 1`
      );
      if (adminResult.rows.length > 0) {
        const result = await generateApiKey(adminResult.rows[0].id, initialKeyName);
        console.log(`[mcp] Initial API key created: ${result.key.slice(0, 13)}...(redacted)`);
      }
    }
  } catch (err) {
    console.error("[mcp] Failed to bootstrap initial API key:", err);
  }
}

server.listen(8000, () => {
  console.log("server listening on port 8000");
});
