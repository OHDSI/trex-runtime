import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { Plugins } from "../../plugin/plugin.ts";
import { getMigrationPlugins } from "../../plugin/migration.ts";
import { scanPluginDirectory } from "../../plugin/utils.ts";
import { scanDiskPlugins } from "../../routes/plugin.ts";

declare const Trex: any;
declare const Deno: any;

import { escapeSql } from "../../lib/sql.ts";

export function registerPluginTools(server: McpServer) {
  server.tool(
    "plugin-list",
    "List all trex plugins: installed on disk, actively loaded, and available from the plugin registry. Shows version info, active status, and whether a restart is needed to pick up changes. Plugins extend trex with custom functions, UI components, workflows, and database migrations.",
    {},
    async () => {
      try {
        const diskPlugins = await scanDiskPlugins();
        const activePlugins = Plugins.getActivePlugins();
        const pluginList: any[] = [];
        const seen = new Set<string>();

        for (const [name, diskInfo] of diskPlugins) {
          seen.add(name);
          const activeEntry = activePlugins.get(name);
          const active = !!activeEntry;
          const activeVersion = activeEntry?.version || null;
          const pendingRestart = !active || activeVersion !== diskInfo.version;
          pluginList.push({
            name,
            version: diskInfo.version,
            activeVersion,
            active,
            installed: true,
            pendingRestart,
          });
        }

        for (const [name, activeEntry] of activePlugins) {
          if (seen.has(name)) continue;
          pluginList.push({
            name,
            version: null,
            activeVersion: activeEntry.version,
            active: true,
            installed: false,
            pendingRestart: true,
          });
        }

        return { content: [{ type: "text", text: JSON.stringify(pluginList, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "plugin-install",
    "Install a trex plugin from the NPM registry. The packageSpec can be a package name (e.g. '@trex/my-plugin') or name@version. The plugin is downloaded to the plugins directory and will be loaded on next restart.",
    {
      packageSpec: z.string().describe("NPM package specifier, e.g. '@trex/my-plugin' or '@trex/my-plugin@1.0.0'"),
    },
    async ({ packageSpec }) => {
      try {
        const dir = Deno.env.get("PLUGINS_PATH") || "./plugins";
        const sql = `SELECT install_results FROM trex_plugin_install('${escapeSql(packageSpec)}', '${escapeSql(dir)}')`;
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute(sql, []);
        const rows = result?.rows || result || [];
        const parsed = rows.map((r: any) => {
          try {
            return JSON.parse(r.install_results || r[0]);
          } catch {
            return r.install_results || r[0];
          }
        });
        const installResult = parsed.length === 1 ? parsed[0] : parsed;
        if (installResult?.success === false || installResult?.error) {
          return {
            content: [{ type: "text", text: `Install failed: ${installResult.error || "Unknown error"}\n${JSON.stringify(installResult, null, 2)}` }],
            isError: true,
          };
        }
        return { content: [{ type: "text", text: JSON.stringify(installResult, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "plugin-uninstall",
    "Uninstall a trex plugin by its package name. Removes the plugin from the plugins directory. The plugin remains active until the next restart.",
    {
      packageName: z.string().describe("NPM package name to uninstall, e.g. '@trex/my-plugin'"),
    },
    async ({ packageName }) => {
      try {
        const dir = Deno.env.get("PLUGINS_PATH") || "./plugins";
        const sql = `SELECT delete_results FROM trex_plugin_delete('${escapeSql(packageName)}', '${escapeSql(dir)}')`;
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute(sql, []);
        const rows = result?.rows || result || [];
        const parsed = rows.map((r: any) => {
          try {
            return JSON.parse(r.delete_results || r[0]);
          } catch {
            return r.delete_results || r[0];
          }
        });
        return { content: [{ type: "text", text: JSON.stringify(parsed[0] || { success: true }, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "plugin-get-info",
    "Get detailed information about a specific trex plugin including its full trex config from package.json, active/installed status, registered types, and migration status (applied/pending counts). Use this to inspect a plugin's configuration and verify it is correctly set up.",
    {
      pluginName: z.string().describe("Short name of the plugin to inspect, e.g. 'my-plugin'"),
    },
    async ({ pluginName }) => {
      try {
        const pluginsPath = Deno.env.get("PLUGINS_PATH") || "./plugins";
        const devPath = Deno.env.get("PLUGINS_DEV_PATH") || "./plugins-dev";
        const dirs = [pluginsPath, devPath];

        let foundPkg: any = null;
        let foundDir: string | null = null;

        for (const baseDir of dirs) {
          const scanned = await scanPluginDirectory(baseDir);
          const match = scanned.find((s) => s.shortName === pluginName);
          if (match) {
            foundPkg = match.pkg;
            foundDir = match.dir;
            break;
          }
        }

        if (!foundPkg) {
          return {
            content: [{ type: "text", text: `Plugin "${pluginName}" not found on disk in ${dirs.join(" or ")}` }],
            isError: true,
          };
        }

        const activePlugins = Plugins.getActivePlugins();
        const activeEntry = activePlugins.get(pluginName);
        const active = !!activeEntry;
        const activeVersion = activeEntry?.version || null;
        const pendingRestart = !active || activeVersion !== foundPkg.version;

        const registeredTypes = Object.keys(foundPkg.trex || {});

        let migrationStatus: any = null;
        const migrationPlugins = getMigrationPlugins();
        const migInfo = migrationPlugins.find((m) => m.pluginName === pluginName);
        if (migInfo) {
          try {
            const sql = `SELECT * FROM trex_migration_status_schema('${escapeSql(migInfo.migrationsPath)}', '${escapeSql(migInfo.schema)}', '${escapeSql(migInfo.database)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            migrationStatus = {
              schema: migInfo.schema,
              database: migInfo.database,
              migrationsPath: migInfo.migrationsPath,
              details: rows,
            };
          } catch (err: any) {
            migrationStatus = {
              schema: migInfo.schema,
              database: migInfo.database,
              migrationsPath: migInfo.migrationsPath,
              error: err.message,
            };
          }
        }

        const info = {
          name: pluginName,
          version: foundPkg.version,
          directory: foundDir,
          active,
          activeVersion,
          pendingRestart,
          trexConfig: foundPkg.trex || null,
          registeredTypes,
          migrationStatus,
        };

        return { content: [{ type: "text", text: JSON.stringify(info, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "plugin-function-invoke",
    "Invoke a plugin function endpoint via HTTP for testing during development. Makes a fetch request to the local server and returns the full response including status, headers, and body. Note: plugin routes go through auth middleware — unauthenticated requests will get 401/403. Pass a session cookie or JWT Bearer token in the headers parameter for authenticated testing.",
    {
      path: z.string().describe("Request path, e.g. '/plugins/my-plugin/health' or '/functions/hello-world'"),
      method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]).default("GET").describe("HTTP method"),
      headers: z.string().optional().describe("JSON string of request headers, e.g. '{\"Authorization\": \"Bearer ...\"}'. Optional."),
      body: z.string().optional().describe("Request body string (for POST/PUT/PATCH). Optional."),
    },
    async ({ path, method, headers, body }) => {
      try {
        const allowedPrefixes = ["/plugins/", "/functions/", "/trex/fn/"];
        if (!allowedPrefixes.some((p) => path.startsWith(p))) {
          return {
            content: [{ type: "text", text: `Error: path must start with one of: ${allowedPrefixes.join(", ")}` }],
            isError: true,
          };
        }

        const port = Deno.env.get("PORT") || "8000";
        const url = `http://localhost:${port}${path}`;

        const fetchHeaders: Record<string, string> = {};
        if (headers) {
          try {
            Object.assign(fetchHeaders, JSON.parse(headers));
          } catch {
            return {
              content: [{ type: "text", text: "Error: headers parameter must be valid JSON" }],
              isError: true,
            };
          }
        }

        const fetchOptions: any = {
          method,
          headers: fetchHeaders,
        };
        if (body && method !== "GET") {
          fetchOptions.body = body;
          if (!fetchHeaders["Content-Type"] && !fetchHeaders["content-type"]) {
            fetchHeaders["Content-Type"] = "application/json";
          }
        }

        const response = await fetch(url, fetchOptions);
        const responseBody = await response.text();
        const responseHeaders: Record<string, string> = {};
        response.headers.forEach((value: string, key: string) => {
          responseHeaders[key] = value;
        });

        const result = {
          status: response.status,
          statusText: response.statusText,
          headers: responseHeaders,
          body: responseBody,
        };

        return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
