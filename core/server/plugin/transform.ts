import type { Express, Request, Response } from "express";
import { Pool } from "pg";
import { authContext } from "../middleware/auth-context.ts";
import { pluginAuthz } from "../middleware/plugin-authz.ts";
import { ROLE_SCOPES, REQUIRED_URL_SCOPES } from "./function.ts";
import { PLUGINS_BASE_PATH } from "../config.ts";

declare const Trex: any;
declare const Deno: any;

export interface TransformPluginInfo {
  pluginName: string;
  projectPath: string;
}

interface RegisteredEndpoint {
  modelName: string;
  path: string;
  roles: string[];
  formats: string[];
  scope: string;
  destDb: string;
  destSchema: string;
}

interface TransformPluginEntry extends TransformPluginInfo {
  endpoints: Map<string, RegisteredEndpoint>;
}

const transformRegistry: Map<string, TransformPluginEntry> = new Map();

let pgPool: InstanceType<typeof Pool> | null = null;

function getPgPool(): InstanceType<typeof Pool> | null {
  if (!pgPool) {
    const databaseUrl = Deno.env.get("DATABASE_URL");
    if (!databaseUrl) return null;
    pgPool = new Pool({ connectionString: databaseUrl });
  }
  return pgPool;
}

import { escapeSql, escapeSqlIdentifier } from "../lib/sql.ts";

function escapeCsvField(val: unknown): string {
  if (val === null || val === undefined) return "";
  const str = String(val);
  return str.includes(",") || str.includes('"') || str.includes("\n")
    ? `"${str.replace(/"/g, '""')}"`
    : str;
}

export function addTransformPlugin(
  app: Express,
  _value: any,
  dir: string,
  shortName: string
) {
  const projectPath = `${dir}/project`;
  const entry: TransformPluginEntry = {
    pluginName: shortName,
    projectPath,
    endpoints: new Map(),
  };
  transformRegistry.set(shortName, entry);

  const basePath = `${PLUGINS_BASE_PATH}/transform/${shortName}`;
  app.get(`${basePath}/*`, authContext, pluginAuthz, async (req: Request, res: Response) => {
    const plugin = transformRegistry.get(shortName);
    if (!plugin) {
      res.status(404).json({ error: "Plugin not found" });
      return;
    }

    const subPath = req.path.slice(basePath.length);
    const endpoint = plugin.endpoints.get(subPath);
    if (!endpoint) {
      res.status(404).json({ error: "Endpoint not found" });
      return;
    }

    const format = (req.query.format as string || "json").toLowerCase();
    if (!endpoint.formats.includes(format)) {
      res.status(400).json({ error: `Unsupported format '${format}'. Allowed: ${endpoint.formats.join(", ")}` });
      return;
    }

    try {
      const conn = new Trex.TrexDB("memory");
      const tableFqn = `"${escapeSqlIdentifier(endpoint.destDb)}"."${escapeSqlIdentifier(endpoint.destSchema)}"."${escapeSqlIdentifier(endpoint.modelName)}"`;

      if (format === "csv") {
        const result = await conn.execute(`SELECT * FROM ${tableFqn}`, []);
        const rows: any[] = result?.rows || result || [];
        if (rows.length === 0) {
          res.setHeader("Content-Type", "text/csv");
          res.setHeader("Content-Disposition", `attachment; filename="${endpoint.modelName}.csv"`);
          res.send("");
          return;
        }
        const keys = Object.keys(rows[0]);
        const header = keys.map(escapeCsvField).join(",");
        const lines = rows.map((row: any) =>
          keys.map((k) => escapeCsvField(row[k])).join(",")
        );
        res.setHeader("Content-Type", "text/csv");
        res.setHeader("Content-Disposition", `attachment; filename="${endpoint.modelName}.csv"`);
        res.send(header + "\n" + lines.join("\n"));
      } else if (format === "arrow") {
        const tmpPath = await Deno.makeTempFile({ prefix: "trex_arrow_", suffix: ".arrow" });
        await conn.execute(
          `COPY (SELECT * FROM ${tableFqn}) TO '${escapeSql(tmpPath)}' WITH (FORMAT 'arrow')`,
          []
        );
        const data = await Deno.readFile(tmpPath);
        res.setHeader("Content-Type", "application/vnd.apache.arrow.stream");
        res.setHeader("Content-Disposition", `attachment; filename="${endpoint.modelName}.arrow"`);
        res.send(Buffer.from(data));
        try { await Deno.remove(tmpPath); } catch {}
      } else {
        const result = await conn.execute(`SELECT * FROM ${tableFqn}`, []);
        const rows = result?.rows || result || [];
        res.setHeader("Content-Type", "application/json");
        res.json(rows);
      }
    } catch (err: any) {
      console.error(`Transform endpoint error (${shortName}${subPath}):`, err);
      res.status(500).json({ error: err.message || "Query failed" });
    }
  });

  console.log(`Registered transform plugin ${shortName} (project: ${projectPath})`);

  recoverEndpoints(shortName).catch((err: any) => {
    // Expected before V2 migration is applied
    if (String(err).includes("does not exist")) return;
    console.error(`Failed to recover endpoints for ${shortName}:`, err);
  });
}

export function getTransformPlugins(): TransformPluginInfo[] {
  return Array.from(transformRegistry.values()).map((e) => ({
    pluginName: e.pluginName,
    projectPath: e.projectPath,
  }));
}

async function recoverEndpoints(pluginName: string) {
  const pool = getPgPool();
  if (!pool) return;

  const result = await pool.query(
    "SELECT dest_db, dest_schema FROM trex.transform_deployment WHERE plugin_name = $1",
    [pluginName]
  );
  if (result.rows.length > 0) {
    const { dest_db, dest_schema } = result.rows[0];
    console.log(`Recovering transform endpoints for ${pluginName} (${dest_db}.${dest_schema})`);
    await registerTransformEndpoints(pluginName, dest_db, dest_schema);
  }
}

export async function registerTransformEndpoints(
  pluginName: string,
  destDb: string,
  destSchema: string
) {
  const plugin = transformRegistry.get(pluginName);
  if (!plugin) throw new Error(`Transform plugin '${pluginName}' not found`);

  clearEndpointScopes(plugin);

  const conn = new Trex.TrexDB("memory");
  const sql = `SELECT * FROM trex_transform_compile('${escapeSql(plugin.projectPath)}')`;
  const result = await conn.execute(sql, []);
  const rows: any[] = result?.rows || result || [];

  const newEndpoints = new Map<string, RegisteredEndpoint>();

  for (const r of rows) {
    const endpointPath = r.endpoint_path || r[6] || "";
    if (!endpointPath) continue;

    const modelName = r.name || r[0] || "";
    const rolesStr = r.endpoint_roles || r[7] || "";
    const formatsStr = r.endpoint_formats || r[8] || "";

    const roles = rolesStr ? rolesStr.split(",").map((s: string) => s.trim()) : [];
    const formats = formatsStr ? formatsStr.split(",").map((s: string) => s.trim()) : ["json", "csv", "arrow"];

    const scope = `transform:${pluginName}:${modelName}`;

    for (const role of roles) {
      if (!ROLE_SCOPES[role]) ROLE_SCOPES[role] = [];
      if (!ROLE_SCOPES[role].includes(scope)) ROLE_SCOPES[role].push(scope);
    }

    REQUIRED_URL_SCOPES.push({
      path: `^${PLUGINS_BASE_PATH}/transform/${pluginName}${endpointPath.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`,
      scopes: [scope],
    });

    newEndpoints.set(endpointPath, {
      modelName,
      path: endpointPath,
      roles,
      formats,
      scope,
      destDb,
      destSchema,
    });

    console.log(`  Endpoint: ${PLUGINS_BASE_PATH}/transform/${pluginName}${endpointPath} -> ${destDb}.${destSchema}.${modelName}`);
  }

  plugin.endpoints = newEndpoints;
}

export async function upsertTransformDeployment(
  pluginName: string,
  destDb: string,
  destSchema: string
) {
  const pool = getPgPool();
  if (!pool) return;

  await pool.query(
    `INSERT INTO trex.transform_deployment (plugin_name, dest_db, dest_schema)
     VALUES ($1, $2, $3)
     ON CONFLICT (plugin_name) DO UPDATE SET
       dest_db = EXCLUDED.dest_db,
       dest_schema = EXCLUDED.dest_schema,
       deployed_at = NOW()`,
    [pluginName, destDb, destSchema]
  );
}

function clearEndpointScopes(plugin: TransformPluginEntry) {
  for (const ep of plugin.endpoints.values()) {
    for (const role of ep.roles) {
      if (ROLE_SCOPES[role]) {
        ROLE_SCOPES[role] = ROLE_SCOPES[role].filter((s) => s !== ep.scope);
      }
    }

    const pathPattern = `^${PLUGINS_BASE_PATH}/transform/${plugin.pluginName}${ep.path.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`;
    const idx = REQUIRED_URL_SCOPES.findIndex((e) => e.path === pathPattern);
    if (idx !== -1) REQUIRED_URL_SCOPES.splice(idx, 1);
  }
  plugin.endpoints.clear();
}
