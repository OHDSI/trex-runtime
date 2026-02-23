import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { getMigrationPlugins } from "../../plugin/migration.ts";

declare const Trex: any;
declare const Deno: any;

import { escapeSql } from "../../lib/sql.ts";

export function registerMigrationTools(server: McpServer) {
  server.tool(
    "migration-list",
    "List migration status for the core trex schema and all plugins. Shows each migration's version number, name, status (applied/pending), applied timestamp, and checksum. Use this to check if there are pending migrations that need to be run.",
    {},
    async () => {
      try {
        const conn = new Trex.TrexDB("memory");
        const summaries: any[] = [];

        const schemaDir = Deno.env.get("SCHEMA_DIR");
        if (schemaDir) {
          try {
            const sql = `SELECT version, name, status, applied_on, checksum FROM trex_migration_status_schema('${escapeSql(schemaDir)}', 'trex', '_config')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const migrations = rows.map((r: any) => ({
              version: parseInt(r.version ?? r[0] ?? "0", 10),
              name: r.name || r[1] || "",
              status: r.status || r[2] || "",
              appliedOn: r.applied_on || r[3] || null,
              checksum: r.checksum || r[4] || null,
            }));
            const appliedCount = migrations.filter((m: any) => m.status === "applied").length;
            const pendingCount = migrations.filter((m: any) => m.status === "pending").length;
            summaries.push({
              pluginName: "core",
              schema: "trex",
              database: "_config",
              appliedCount,
              pendingCount,
              migrations,
            });
          } catch (err: any) {
            summaries.push({ pluginName: "core", error: err.message });
          }
        }

        const plugins = getMigrationPlugins();
        for (const plugin of plugins) {
          try {
            const sql = `SELECT version, name, status, applied_on, checksum FROM trex_migration_status_schema('${escapeSql(plugin.migrationsPath)}', '${escapeSql(plugin.schema)}', '${escapeSql(plugin.database)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const migrations = rows.map((r: any) => ({
              version: parseInt(r.version ?? r[0] ?? "0", 10),
              name: r.name || r[1] || "",
              status: r.status || r[2] || "",
              appliedOn: r.applied_on || r[3] || null,
              checksum: r.checksum || r[4] || null,
            }));
            const appliedCount = migrations.filter((m: any) => m.status === "applied").length;
            const pendingCount = migrations.filter((m: any) => m.status === "pending").length;
            summaries.push({
              pluginName: plugin.pluginName,
              schema: plugin.schema,
              database: plugin.database,
              appliedCount,
              pendingCount,
              migrations,
            });
          } catch (err: any) {
            summaries.push({ pluginName: plugin.pluginName, error: err.message });
          }
        }

        return { content: [{ type: "text", text: JSON.stringify(summaries, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "migration-run",
    "Run pending database migrations. If pluginName is omitted, runs all pending migrations (core + plugins). If pluginName is 'core', runs only core schema migrations. Otherwise runs migrations for the specified plugin only.",
    {
      pluginName: z.string().optional().describe("Plugin name to run migrations for, or 'core' for core schema. Omit to run all."),
    },
    async ({ pluginName }) => {
      try {
        const conn = new Trex.TrexDB("memory");
        const allResults: any[] = [];

        type MigrationTarget = { name: string; path: string; schema: string; database: string };
        const targets: MigrationTarget[] = [];

        if (!pluginName || pluginName === "core") {
          const schemaDir = Deno.env.get("SCHEMA_DIR");
          if (schemaDir) {
            targets.push({ name: "core", path: schemaDir, schema: "trex", database: "_config" });
          }
        }

        if (!pluginName || pluginName !== "core") {
          const plugins = getMigrationPlugins();
          for (const plugin of plugins) {
            if (!pluginName || pluginName === plugin.pluginName) {
              targets.push({
                name: plugin.pluginName,
                path: plugin.migrationsPath,
                schema: plugin.schema,
                database: plugin.database,
              });
            }
          }
        }

        for (const target of targets) {
          const sql = `SELECT version, name, status FROM trex_migration_run_schema('${escapeSql(target.path)}', '${escapeSql(target.schema)}', '${escapeSql(target.database)}')`;
          const result = await conn.execute(sql, []);
          const rows = result?.rows || result || [];
          for (const r of rows) {
            allResults.push({
              plugin: target.name,
              version: parseInt(r.version ?? r[0] ?? "0", 10),
              name: r.name || r[1] || "",
              status: r.status || r[2] || "",
            });
          }
        }

        if (allResults.length === 0) {
          return { content: [{ type: "text", text: "No pending migrations to run." }] };
        }
        return { content: [{ type: "text", text: JSON.stringify(allResults, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
