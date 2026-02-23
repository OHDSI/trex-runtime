import { makeExtendSchemaPlugin, gql } from "graphile-utils";
import { Plugins } from "../plugin/plugin.ts";
import { getMigrationPlugins } from "../plugin/migration.ts";
import { getTransformPlugins, registerTransformEndpoints, upsertTransformDeployment } from "../plugin/transform.ts";
import { scanDiskPlugins } from "../routes/plugin.ts";
import { reloadAuthProviders, pool as authPool } from "../auth.ts";
import { REGISTERED_FUNCTIONS, ROLE_SCOPES, REQUIRED_URL_SCOPES } from "../plugin/function.ts";
import { REGISTERED_UI_ROUTES, getPluginsJson } from "../plugin/ui.ts";
import { REGISTERED_FLOWS } from "../plugin/flow.ts";

declare const Trex: any;
declare const Deno: any;

import { escapeSql, validateInt } from "../lib/sql.ts";

function assertAdmin(context: any) {
  const role = context.pgSettings?.["app.user_role"];
  if (role !== "admin") {
    throw new Error("Forbidden");
  }
}

export const pluginOperationsPlugin = makeExtendSchemaPlugin(() => ({
    typeDefs: gql`
      type PluginInfo {
        name: String!
        packageName: String
        version: String
        activeVersion: String
        active: Boolean!
        installed: Boolean!
        pendingRestart: Boolean!
        description: String
        registryVersion: String
      }

      type PluginResult {
        success: Boolean!
        error: String
        results: JSON
      }

      type TestConnectionResult {
        success: Boolean!
        message: String!
      }

      type TrexNode {
        nodeId: String!
        nodeName: String!
        gossipAddr: String!
        dataNode: String!
        status: String!
      }

      type TrexService {
        nodeName: String!
        serviceName: String!
        host: String!
        port: String!
        status: String!
        uptimeSeconds: String!
        config: String
      }

      type TrexClusterStatus {
        totalNodes: String!
        activeQueries: String!
        queuedQueries: String!
        memoryUtilizationPct: String!
      }

      type ServiceActionResult {
        success: Boolean!
        message: String
        error: String
      }

      type TrexDatabase {
        databaseName: String!
        databaseOid: String!
        path: String
        internal: Boolean!
        type: String!
      }

      type TrexSchema {
        databaseName: String!
        schemaName: String!
        schemaOid: String!
        internal: Boolean!
      }

      type TrexTable {
        databaseName: String!
        schemaName: String!
        tableName: String!
        tableOid: String!
        internal: Boolean!
        hasPrimaryKey: Boolean!
        estimatedSize: String!
        columnCount: String!
        indexCount: String!
        temporary: Boolean!
      }

      type TrexExtension {
        extensionName: String!
        loaded: Boolean!
        installed: Boolean!
        extensionVersion: String
        description: String
        installPath: String
      }

      type PluginMigration {
        version: Int!
        name: String!
        status: String!
        appliedOn: String
        checksum: String
      }

      type PluginMigrationSummary {
        pluginName: String!
        schema: String!
        database: String!
        currentVersion: Int
        totalMigrations: Int!
        appliedCount: Int!
        pendingCount: Int!
        migrations: [PluginMigration!]!
      }

      type RunMigrationResult {
        success: Boolean!
        error: String
        results: [PluginMigration!]
      }

      type EtlPipeline {
        name: String!
        state: String!
        mode: String!
        connection: String!
        publication: String!
        snapshot: String!
        rowsReplicated: String!
        lastActivity: String!
        error: String
      }

      type EtlActionResult {
        success: Boolean!
        message: String
        error: String
      }

      type TransformProject {
        pluginName: String!
        projectPath: String!
      }

      type TransformCompileResult {
        name: String!
        materialized: String!
        order: Int!
        status: String!
      }

      type TransformPlanResult {
        name: String!
        action: String!
        materialized: String!
        reason: String!
      }

      type TransformRunResult {
        name: String!
        action: String!
        materialized: String!
        durationMs: String!
        message: String!
      }

      type TransformSeedResult {
        name: String!
        action: String!
        rows: String!
        message: String!
      }

      type TransformTestResult {
        name: String!
        status: String!
        rowsReturned: String!
      }

      type TransformFreshnessResult {
        name: String!
        status: String!
        maxLoadedAt: String!
        ageHours: Float!
        warnAfter: String!
        errorAfter: String!
      }

      type RegisteredFunction {
        pluginName: String!
        source: String!
        entryPoint: String!
      }

      type RoleScopeMapping {
        role: String!
        scopes: [String!]!
      }

      type UrlScopeRequirement {
        path: String!
        scopes: [String!]!
      }

      type UiPluginRoute {
        pluginName: String!
        urlPrefix: String!
        fsPath: String!
      }

      type RegisteredFlow {
        name: String!
        entrypoint: String!
        image: String!
        tags: [String!]!
      }

      type EventLogEntry {
        id: String!
        eventType: String!
        level: String!
        message: String!
        createdAt: String!
      }

      extend type Query {
        availablePlugins: [PluginInfo!]!
        trexNodes: [TrexNode!]!
        trexServices: [TrexService!]!
        trexClusterStatus: TrexClusterStatus
        trexDatabases: [TrexDatabase!]!
        trexSchemas: [TrexSchema!]!
        trexTables(database: String, schema: String): [TrexTable!]!
        trexExtensions: [TrexExtension!]!
        trexMigrations: [PluginMigrationSummary!]!
        etlPipelines: [EtlPipeline!]!
        transformProjects: [TransformProject!]!
        transformCompile(pluginName: String!): [TransformCompileResult!]!
        transformPlan(pluginName: String!, destDb: String!, destSchema: String!, sourceDb: String!, sourceSchema: String!): [TransformPlanResult!]!
        transformFreshness(pluginName: String!, destDb: String!, destSchema: String!): [TransformFreshnessResult!]!
        registeredFunctions: [RegisteredFunction!]!
        roleScopeMappings: [RoleScopeMapping!]!
        urlScopeRequirements: [UrlScopeRequirement!]!
        uiPluginRoutes: [UiPluginRoute!]!
        uiPluginsJson: JSON
        registeredFlows: [RegisteredFlow!]!
        eventLogs(level: String, limit: Int, before: String): [EventLogEntry!]!
      }

      extend type Mutation {
        installPlugin(packageSpec: String!): PluginResult!
        uninstallPlugin(packageName: String!): PluginResult!
        updatePlugin(packageName: String!, version: String): PluginResult!
        testDatabaseConnection(databaseId: String!): TestConnectionResult!
        reloadSsoProviders: Boolean!
        startService(extension: String!, config: String!): ServiceActionResult!
        stopService(extension: String!): ServiceActionResult!
        restartService(extension: String!, config: String!): ServiceActionResult!
        runPluginMigrations(pluginName: String): RunMigrationResult!
        startEtlPipeline(
          name: String!
          databaseId: String!
          mode: String!
          publication: String
          schema: String
          batchSize: Int
          batchTimeoutMs: Int
          retryDelayMs: Int
          retryMaxAttempts: Int
        ): EtlActionResult!
        stopEtlPipeline(name: String!): EtlActionResult!
        transformRun(pluginName: String!, destDb: String!, destSchema: String!, sourceDb: String!, sourceSchema: String!): [TransformRunResult!]!
        transformSeed(pluginName: String!, destDb: String!, destSchema: String!): [TransformSeedResult!]!
        transformTest(pluginName: String!, destDb: String!, destSchema: String!, sourceDb: String!, sourceSchema: String!): [TransformTestResult!]!
        loadExtension(extensionName: String!): ServiceActionResult!
      }
    `,
    resolvers: {
      Query: {
        async availablePlugins(_parent: any, _args: any, context: any) {
          assertAdmin(context);

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

          const registryUrl = Deno.env.get("PLUGINS_INFORMATION_URL");
          if (registryUrl) {
            try {
              const controller = new AbortController();
              const timeout = setTimeout(() => controller.abort(), 10000);
              const pkgsRes = await fetch(registryUrl, { signal: controller.signal });
              clearTimeout(timeout);
              const pkgsJson = await pkgsRes.json();
              const packages = pkgsJson.value || pkgsJson;

              const registryMap = new Map<string, { fullName: string; description: string; registryVersion: string }>();

              for (const pkg of packages) {
                const fullName = pkg.name || "";
                let bestVersion = { version: "", packageDescription: "" };
                if (pkg.versions && Array.isArray(pkg.versions)) {
                  bestVersion = pkg.versions.reduce((m: any, c: any) => {
                    return c.version > m.version ? c : m;
                  }, bestVersion);
                }
                registryMap.set(fullName, {
                  fullName,
                  description: bestVersion.packageDescription || pkg.description || "",
                  registryVersion: bestVersion.version || pkg.version || "",
                });
              }

              for (const plugin of pluginList) {
                const regInfo = registryMap.get(plugin.name);
                if (regInfo) {
                  plugin.packageName = regInfo.fullName;
                  plugin.description = regInfo.description;
                  plugin.registryVersion = regInfo.registryVersion;
                }
              }

              for (const [pkgname, regInfo] of registryMap) {
                if (!seen.has(pkgname) && !activePlugins.has(pkgname)) {
                  pluginList.push({
                    name: pkgname,
                    packageName: regInfo.fullName,
                    version: null,
                    activeVersion: null,
                    active: false,
                    installed: false,
                    pendingRestart: false,
                    description: regInfo.description,
                    registryVersion: regInfo.registryVersion,
                  });
                }
              }
            } catch (e) {
              console.error(`Failed to fetch registry info: ${e}`);
            }
          }

          return pluginList;
        },

        async trexNodes(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_db_nodes()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              nodeId: r.node_id || r[0] || "",
              nodeName: r.node_name || r[1] || "",
              gossipAddr: r.gossip_addr || r[2] || "",
              dataNode: r.data_node || r[3] || "",
              status: r.status || r[4] || "",
            }));
          } catch (err: any) {
            console.error("trexNodes error:", err);
            return [];
          }
        },

        async trexServices(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_db_services()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              nodeName: r.node_name || r[0] || "",
              serviceName: r.service_name || r[1] || "",
              host: r.host || r[2] || "",
              port: r.port || r[3] || "",
              status: r.status || r[4] || "",
              uptimeSeconds: r.uptime_seconds || r[5] || "",
              config: r.config || r[6] || null,
            }));
          } catch (err: any) {
            console.error("trexServices error:", err);
            return [];
          }
        },

        async trexClusterStatus(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_db_cluster_status()", []);
            const rows = result?.rows || result || [];
            if (rows.length === 0) return null;
            const r = rows[0];
            return {
              totalNodes: r.total_nodes || r[0] || "",
              activeQueries: r.active_queries || r[1] || "",
              queuedQueries: r.queued_queries || r[2] || "",
              memoryUtilizationPct: r.memory_utilization_pct || r[3] || "",
            };
          } catch (err: any) {
            console.error("trexClusterStatus error:", err);
            return null;
          }
        },

        async trexDatabases(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(
              "SELECT database_name, database_oid, path, internal, type FROM duckdb_databases()", []
            );
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              databaseName: r.database_name || r[0] || "",
              databaseOid: String(r.database_oid ?? r[1] ?? ""),
              path: r.path || r[2] || null,
              internal: r.internal ?? r[3] ?? false,
              type: r.type || r[4] || "",
            }));
          } catch (err: any) {
            console.error("trexDatabases error:", err);
            return [];
          }
        },

        async trexSchemas(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(
              "SELECT * FROM duckdb_schemas()", []
            );
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              databaseName: r.database_name || r[0] || "",
              schemaName: r.schema_name || r[1] || "",
              schemaOid: String(r.database_oid ?? r.schema_oid ?? r[2] ?? ""),
              internal: r.internal ?? r[3] ?? false,
            }));
          } catch (err: any) {
            console.error("trexSchemas error:", err);
            return [];
          }
        },

        async trexTables(_parent: any, args: { database?: string; schema?: string }, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            let sql = "SELECT * FROM duckdb_tables()";
            const conditions: string[] = [];
            if (args.database) {
              conditions.push(`database_name = '${escapeSql(args.database)}'`);
            }
            if (args.schema) {
              conditions.push(`schema_name = '${escapeSql(args.schema)}'`);
            }
            if (conditions.length > 0) {
              sql += " WHERE " + conditions.join(" AND ");
            }
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              databaseName: r.database_name || r[0] || "",
              schemaName: r.schema_name || r[1] || "",
              tableName: r.table_name || r[2] || "",
              tableOid: String(r.table_oid ?? r[3] ?? ""),
              internal: r.internal ?? r[4] ?? false,
              hasPrimaryKey: r.has_primary_key ?? r[5] ?? false,
              estimatedSize: String(r.estimated_size ?? r[6] ?? ""),
              columnCount: String(r.column_count ?? r[7] ?? ""),
              indexCount: String(r.index_count ?? r[8] ?? ""),
              temporary: r.temporary ?? r[9] ?? false,
            }));
          } catch (err: any) {
            console.error("trexTables error:", err);
            return [];
          }
        },

        async trexExtensions(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM duckdb_extensions()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              extensionName: r.extension_name || r[0] || "",
              loaded: r.loaded ?? r[1] ?? false,
              installed: r.installed ?? r[2] ?? false,
              extensionVersion: r.extension_version || r[3] || null,
              description: r.description || r[4] || null,
              installPath: r.install_path || r[5] || null,
            }));
          } catch (err: any) {
            console.error("trexExtensions error:", err);
            return [];
          }
        },

        async trexMigrations(_parent: any, _args: any, context: any) {
          assertAdmin(context);
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
                const maxVersion = migrations
                  .filter((m: any) => m.status === "applied")
                  .reduce((max: number, m: any) => Math.max(max, m.version), 0);
                summaries.push({
                  pluginName: "core",
                  schema: "trex",
                  database: "_config",
                  currentVersion: maxVersion || null,
                  totalMigrations: migrations.length,
                  appliedCount,
                  pendingCount,
                  migrations,
                });
              } catch (err: any) {
                console.error("Core migration status error:", err);
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
                const maxVersion = migrations
                  .filter((m: any) => m.status === "applied")
                  .reduce((max: number, m: any) => Math.max(max, m.version), 0);
                summaries.push({
                  pluginName: plugin.pluginName,
                  schema: plugin.schema,
                  database: plugin.database,
                  currentVersion: maxVersion || null,
                  totalMigrations: migrations.length,
                  appliedCount,
                  pendingCount,
                  migrations,
                });
              } catch (err: any) {
                console.error(`Plugin ${plugin.pluginName} migration status error:`, err);
              }
            }

            return summaries;
          } catch (err: any) {
            console.error("trexMigrations error:", err);
            return [];
          }
        },

        async etlPipelines(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute("SELECT * FROM trex_etl_status()", []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              state: r.state || r[1] || "",
              mode: r.mode || r[2] || "",
              connection: r.connection || r[3] || "",
              publication: r.publication || r[4] || "",
              snapshot: r.snapshot || r[5] || "",
              rowsReplicated: r.rows_replicated || r[6] || "0",
              lastActivity: r.last_activity || r[7] || "",
              error: r.error || r[8] || null,
            }));
          } catch (err: any) {
            console.error("etlPipelines error:", err);
            return [];
          }
        },

        async transformProjects(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          return getTransformPlugins();
        },

        async transformCompile(_parent: any, args: { pluginName: string }, context: any) {
          assertAdmin(context);
          const plugin = getTransformPlugins().find(p => p.pluginName === args.pluginName);
          if (!plugin) throw new Error(`Transform plugin '${args.pluginName}' not found`);
          try {
            const conn = new Trex.TrexDB("memory");
            const sql = `SELECT * FROM trex_transform_compile('${escapeSql(plugin.projectPath)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              materialized: r.materialized || r[1] || "",
              order: parseInt(r.order ?? r[3] ?? "0", 10),
              status: r.status || r[4] || "",
            }));
          } catch (err: any) {
            console.error("transformCompile error:", err);
            throw new Error(err.message || "Compile failed");
          }
        },

        async transformPlan(
          _parent: any,
          args: { pluginName: string; destDb: string; destSchema: string; sourceDb: string; sourceSchema: string },
          context: any,
        ) {
          assertAdmin(context);
          const plugin = getTransformPlugins().find(p => p.pluginName === args.pluginName);
          if (!plugin) throw new Error(`Transform plugin '${args.pluginName}' not found`);
          try {
            const destSchema = `${args.destDb}.${args.destSchema}`;
            const sourceSchema = `${args.sourceDb}.${args.sourceSchema}`;
            const conn = new Trex.TrexDB("memory");
            const sql = `SELECT * FROM trex_transform_plan('${escapeSql(plugin.projectPath)}', '${escapeSql(destSchema)}', source_schema := '${escapeSql(sourceSchema)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              action: r.action || r[1] || "",
              materialized: r.materialized || r[2] || "",
              reason: r.reason || r[3] || "",
            }));
          } catch (err: any) {
            console.error("transformPlan error:", err);
            throw new Error(err.message || "Plan failed");
          }
        },

        async transformFreshness(
          _parent: any,
          args: { pluginName: string; destDb: string; destSchema: string },
          context: any,
        ) {
          assertAdmin(context);
          const plugin = getTransformPlugins().find(p => p.pluginName === args.pluginName);
          if (!plugin) throw new Error(`Transform plugin '${args.pluginName}' not found`);
          try {
            const destSchema = `${args.destDb}.${args.destSchema}`;
            const conn = new Trex.TrexDB("memory");
            const sql = `SELECT * FROM trex_transform_freshness('${escapeSql(plugin.projectPath)}', '${escapeSql(destSchema)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              status: r.status || r[1] || "",
              maxLoadedAt: r.max_loaded_at || r[2] || "",
              ageHours: parseFloat(r.age_hours ?? r[3] ?? "0"),
              warnAfter: r.warn_after || r[4] || "",
              errorAfter: r.error_after || r[5] || "",
            }));
          } catch (err: any) {
            console.error("transformFreshness error:", err);
            throw new Error(err.message || "Freshness check failed");
          }
        },

        registeredFunctions(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          return REGISTERED_FUNCTIONS.map((f) => ({
            pluginName: f.name,
            source: f.source,
            entryPoint: f.function,
          }));
        },

        roleScopeMappings(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          return Object.entries(ROLE_SCOPES).map(([role, scopes]) => ({
            role,
            scopes,
          }));
        },

        urlScopeRequirements(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          return REQUIRED_URL_SCOPES.map((r) => ({
            path: r.path,
            scopes: r.scopes,
          }));
        },

        uiPluginRoutes(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          return REGISTERED_UI_ROUTES.map((r) => ({
            pluginName: r.pluginName,
            urlPrefix: r.urlPrefix,
            fsPath: r.fsPath,
          }));
        },

        uiPluginsJson(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          try {
            return JSON.parse(getPluginsJson());
          } catch {
            return null;
          }
        },

        registeredFlows(_parent: any, _args: any, context: any) {
          assertAdmin(context);
          return REGISTERED_FLOWS.map((f) => ({
            name: f.name,
            entrypoint: f.entrypoint,
            image: f.image,
            tags: f.tags,
          }));
        },

        async eventLogs(_parent: any, args: { level?: string; limit?: number; before?: string }, context: any) {
          assertAdmin(context);

          try {
            // Periodic retention: delete logs older than 30 days (fire-and-forget)
            authPool.query(
              `DELETE FROM trex.event_log WHERE created_at < NOW() - INTERVAL '30 days'`,
            ).catch(() => {});

            const limit = Math.min(Math.max(args.limit || 100, 1), 500);
            const conditions: string[] = [];
            const params: any[] = [];
            let paramIdx = 1;

            if (args.level) {
              conditions.push(`level = $${paramIdx++}`);
              params.push(args.level);
            }
            if (args.before) {
              conditions.push(`id < $${paramIdx++}`);
              params.push(args.before);
            }

            const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
            params.push(limit);

            const result = await authPool.query(
              `SELECT id, event_type, level, message, created_at FROM trex.event_log ${where} ORDER BY id DESC LIMIT $${paramIdx}`,
              params,
            );

            return result.rows.map((r: any) => ({
              id: String(r.id),
              eventType: r.event_type,
              level: r.level,
              message: r.message,
              createdAt: r.created_at instanceof Date ? r.created_at.toISOString() : String(r.created_at),
            }));
          } catch (err: any) {
            console.error("eventLogs error:", err);
            return [];
          }
        },
      },
      Mutation: {
        async installPlugin(_parent: any, args: { packageSpec: string }, context: any) {
          assertAdmin(context);

          const { packageSpec } = args;
          if (!packageSpec) {
            return { success: false, error: "packageSpec is required", results: null };
          }

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
            // Check if tpm returned an error in the result JSON
            if (installResult?.success === false || installResult?.error) {
              return { success: false, error: installResult.error || "Install failed", results: installResult };
            }
            return { success: true, error: null, results: installResult };
          } catch (err: any) {
            console.error("Plugin install error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async uninstallPlugin(_parent: any, args: { packageName: string }, context: any) {
          assertAdmin(context);

          const { packageName } = args;
          if (!packageName) {
            return { success: false, error: "packageName is required", results: null };
          }

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
            return { success: true, error: null, results: parsed[0] || null };
          } catch (err: any) {
            console.error("Plugin uninstall error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async updatePlugin(_parent: any, args: { packageName: string; version?: string }, context: any) {
          assertAdmin(context);

          const { packageName, version } = args;
          if (!packageName) {
            return { success: false, error: "packageName is required", results: null };
          }

          try {
            const dir = Deno.env.get("PLUGINS_PATH") || "./plugins";
            const conn = new Trex.TrexDB("memory");

            const delSql = `SELECT delete_results FROM trex_plugin_delete('${escapeSql(packageName)}', '${escapeSql(dir)}')`;
            await conn.execute(delSql, []);

            const spec = version ? `${packageName}@${escapeSql(version)}` : packageName;
            const installSql = `SELECT install_results FROM trex_plugin_install('${escapeSql(spec)}', '${escapeSql(dir)}')`;
            const result = await conn.execute(installSql, []);
            const rows = result?.rows || result || [];
            const parsed = rows.map((r: any) => {
              try {
                return JSON.parse(r.install_results || r[0]);
              } catch {
                return r.install_results || r[0];
              }
            });
            return { success: true, error: null, results: parsed.length === 1 ? parsed[0] : parsed };
          } catch (err: any) {
            console.error("Plugin update error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async testDatabaseConnection(_parent: any, args: { databaseId: string }, context: any) {
          assertAdmin(context);

          const { databaseId } = args;
          if (!databaseId) {
            return { success: false, message: "databaseId is required" };
          }

          const pg = (await import("pg")).default;
          const mainPool = new pg.Pool({
            connectionString: Deno.env.get("DATABASE_URL"),
          });

          try {
            const dbResult = await mainPool.query(
              `SELECT host, port, "databaseName", dialect FROM trex.database WHERE id = $1`,
              [databaseId]
            );
            if (dbResult.rows.length === 0) {
              return { success: false, message: "Database not found" };
            }

            const db = dbResult.rows[0];

            const credResult = await mainPool.query(
              `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
              [databaseId]
            );
            if (credResult.rows.length === 0) {
              return { success: false, message: "No credentials configured" };
            }

            const cred = credResult.rows[0];

            const testPool = new pg.Pool({
              host: db.host,
              port: db.port,
              database: db.databaseName,
              user: cred.username,
              password: cred.password,
              connectionTimeoutMillis: 5000,
              max: 1,
            });

            try {
              const client = await testPool.connect();
              await client.query("SELECT 1");
              client.release();
              return { success: true, message: "Connection successful" };
            } catch (connErr: any) {
              return { success: false, message: connErr.message || "Connection failed" };
            } finally {
              await testPool.end();
            }
          } catch (err: any) {
            console.error("Test connection error:", err);
            return { success: false, message: "Internal server error" };
          } finally {
            await mainPool.end();
          }
        },

        async reloadSsoProviders(_parent: any, _args: any, context: any) {
          assertAdmin(context);

          try {
            await reloadAuthProviders();
            return true;
          } catch (err: any) {
            console.error("SSO reload error:", err);
            throw new Error(err.message || "Failed to reload SSO providers");
          }
        },

        async startService(_parent: any, args: { extension: string; config: string }, context: any) {
          assertAdmin(context);
          const { extension, config } = args;
          try {
            const sql = `SELECT trex_db_start_service('${escapeSql(extension)}', '${escapeSql(config)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_db_start_service || "Service started";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("startService error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },

        async stopService(_parent: any, args: { extension: string }, context: any) {
          assertAdmin(context);
          const { extension } = args;
          try {
            const sql = `SELECT trex_db_stop_service('${escapeSql(extension)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_db_stop_service || "Service stopped";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("stopService error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },

        async restartService(_parent: any, args: { extension: string; config: string }, context: any) {
          assertAdmin(context);
          const { extension, config } = args;
          try {
            const conn = new Trex.TrexDB("memory");
            // Stop the service first (ignore errors if not running)
            try {
              await conn.execute(`SELECT trex_db_stop_service('${escapeSql(extension)}')`, []);
            } catch {}
            const sql = `SELECT trex_db_start_service('${escapeSql(extension)}', '${escapeSql(config)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_db_start_service || "Service restarted";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("restartService error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },

        async runPluginMigrations(_parent: any, args: { pluginName?: string }, context: any) {
          assertAdmin(context);
          try {
            const conn = new Trex.TrexDB("memory");
            const allResults: any[] = [];

            type MigrationTarget = { name: string; path: string; schema: string; database: string };
            const targets: MigrationTarget[] = [];

            if (!args.pluginName || args.pluginName === "core") {
              const schemaDir = Deno.env.get("SCHEMA_DIR");
              if (schemaDir) {
                targets.push({ name: "core", path: schemaDir, schema: "trex", database: "_config" });
              }
            }

            if (!args.pluginName || args.pluginName !== "core") {
              const plugins = getMigrationPlugins();
              for (const plugin of plugins) {
                if (!args.pluginName || args.pluginName === plugin.pluginName) {
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
                  version: parseInt(r.version ?? r[0] ?? "0", 10),
                  name: r.name || r[1] || "",
                  status: r.status || r[2] || "",
                  appliedOn: null,
                  checksum: null,
                });
              }
            }

            return { success: true, error: null, results: allResults };
          } catch (err: any) {
            console.error("runPluginMigrations error:", err);
            return { success: false, error: err.message || String(err), results: null };
          }
        },

        async startEtlPipeline(
          _parent: any,
          args: {
            name: string;
            databaseId: string;
            mode: string;
            publication?: string;
            schema?: string;
            batchSize?: number;
            batchTimeoutMs?: number;
            retryDelayMs?: number;
            retryMaxAttempts?: number;
          },
          context: any,
        ) {
          assertAdmin(context);

          const { name, databaseId, mode } = args;
          const validModes = ["copy_and_cdc", "cdc_only", "copy_only"];
          if (!validModes.includes(mode)) {
            return { success: false, message: null, error: `Invalid mode '${mode}'. Must be one of: ${validModes.join(", ")}` };
          }
          if ((mode === "copy_and_cdc" || mode === "cdc_only") && !args.publication) {
            return { success: false, message: null, error: "Publication name is required for CDC modes" };
          }

          const pg = (await import("pg")).default;
          const mainPool = new pg.Pool({
            connectionString: Deno.env.get("DATABASE_URL"),
          });

          try {
            const dbResult = await mainPool.query(
              `SELECT host, port, "databaseName" FROM trex.database WHERE id = $1`,
              [databaseId]
            );
            if (dbResult.rows.length === 0) {
              return { success: false, message: null, error: "Database not found" };
            }
            const db = dbResult.rows[0];

            const credResult = await mainPool.query(
              `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
              [databaseId]
            );
            if (credResult.rows.length === 0) {
              return { success: false, message: null, error: "No credentials configured for this database" };
            }
            const cred = credResult.rows[0];

            const parts: string[] = [
              `host='${escapeSql(db.host)}'`,
              `port='${escapeSql(String(db.port))}'`,
              `dbname='${escapeSql(db.databaseName)}'`,
              `user='${escapeSql(cred.username)}'`,
              `password='${escapeSql(cred.password)}'`,
            ];

            if (mode === "copy_and_cdc" || mode === "cdc_only") {
              parts.push(`publication='${escapeSql(args.publication!)}'`);
            }
            if (mode === "copy_only") {
              const schemaName = args.schema || "public";
              parts.push(`schema='${escapeSql(schemaName)}'`);
            }

            const connStr = parts.join(" ");

            let sql: string;
            if (args.batchSize != null || args.batchTimeoutMs != null || args.retryDelayMs != null || args.retryMaxAttempts != null) {
              const batchSize = validateInt(args.batchSize ?? 1000, "batchSize");
              const batchTimeout = validateInt(args.batchTimeoutMs ?? 5000, "batchTimeoutMs");
              const retryDelay = validateInt(args.retryDelayMs ?? 10000, "retryDelayMs");
              const retryMax = validateInt(args.retryMaxAttempts ?? 5, "retryMaxAttempts");
              sql = `SELECT trex_etl_start('${escapeSql(name)}', '${escapeSql(connStr)}', '${escapeSql(mode)}', ${batchSize}, ${batchTimeout}, ${retryDelay}, ${retryMax})`;
            } else {
              sql = `SELECT trex_etl_start('${escapeSql(name)}', '${escapeSql(connStr)}', '${escapeSql(mode)}')`;
            }

            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_etl_start || "Pipeline started";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("startEtlPipeline error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          } finally {
            await mainPool.end();
          }
        },

        async stopEtlPipeline(_parent: any, args: { name: string }, context: any) {
          assertAdmin(context);
          const { name } = args;
          try {
            const sql = `SELECT trex_etl_stop('${escapeSql(name)}')`;
            const conn = new Trex.TrexDB("memory");
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const message = rows[0]?.[0] || rows[0]?.trex_etl_stop || "Pipeline stopped";
            return { success: true, message, error: null };
          } catch (err: any) {
            console.error("stopEtlPipeline error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },

        async transformRun(
          _parent: any,
          args: { pluginName: string; destDb: string; destSchema: string; sourceDb: string; sourceSchema: string },
          context: any,
        ) {
          assertAdmin(context);
          const plugin = getTransformPlugins().find(p => p.pluginName === args.pluginName);
          if (!plugin) throw new Error(`Transform plugin '${args.pluginName}' not found`);
          try {
            const destSchema = `${args.destDb}.${args.destSchema}`;
            const sourceSchema = `${args.sourceDb}.${args.sourceSchema}`;
            const conn = new Trex.TrexDB("memory");
            const sql = `SELECT * FROM trex_transform_run('${escapeSql(plugin.projectPath)}', '${escapeSql(destSchema)}', source_schema := '${escapeSql(sourceSchema)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            const runResults = rows.map((r: any) => ({
              name: r.name || r[0] || "",
              action: r.action || r[1] || "",
              materialized: r.materialized || r[2] || "",
              durationMs: String(r.duration_ms ?? r[3] ?? "0"),
              message: r.message || r[4] || "",
            }));

            try {
              await registerTransformEndpoints(args.pluginName, args.destDb, args.destSchema);
              await upsertTransformDeployment(args.pluginName, args.destDb, args.destSchema);
            } catch (endpointErr: any) {
              console.error("Endpoint registration error:", endpointErr);
            }

            return runResults;
          } catch (err: any) {
            console.error("transformRun error:", err);
            throw new Error(err.message || "Run failed");
          }
        },

        async transformSeed(
          _parent: any,
          args: { pluginName: string; destDb: string; destSchema: string },
          context: any,
        ) {
          assertAdmin(context);
          const plugin = getTransformPlugins().find(p => p.pluginName === args.pluginName);
          if (!plugin) throw new Error(`Transform plugin '${args.pluginName}' not found`);
          try {
            const destSchema = `${args.destDb}.${args.destSchema}`;
            const conn = new Trex.TrexDB("memory");
            const sql = `SELECT * FROM trex_transform_seed('${escapeSql(plugin.projectPath)}', '${escapeSql(destSchema)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              action: r.action || r[1] || "",
              rows: r.rows || r[2] || "",
              message: r.message || r[3] || "",
            }));
          } catch (err: any) {
            console.error("transformSeed error:", err);
            throw new Error(err.message || "Seed failed");
          }
        },

        async transformTest(
          _parent: any,
          args: { pluginName: string; destDb: string; destSchema: string; sourceDb: string; sourceSchema: string },
          context: any,
        ) {
          assertAdmin(context);
          const plugin = getTransformPlugins().find(p => p.pluginName === args.pluginName);
          if (!plugin) throw new Error(`Transform plugin '${args.pluginName}' not found`);
          try {
            const destSchema = `${args.destDb}.${args.destSchema}`;
            const sourceSchema = `${args.sourceDb}.${args.sourceSchema}`;
            const conn = new Trex.TrexDB("memory");
            const sql = `SELECT * FROM trex_transform_test('${escapeSql(plugin.projectPath)}', '${escapeSql(destSchema)}', source_schema := '${escapeSql(sourceSchema)}')`;
            const result = await conn.execute(sql, []);
            const rows = result?.rows || result || [];
            return rows.map((r: any) => ({
              name: r.name || r[0] || "",
              status: r.status || r[1] || "",
              rowsReturned: r.rows_returned || r[2] || "",
            }));
          } catch (err: any) {
            console.error("transformTest error:", err);
            throw new Error(err.message || "Test failed");
          }
        },

        async loadExtension(_parent: any, args: { extensionName: string }, context: any) {
          assertAdmin(context);
          const { extensionName } = args;
          try {
            const conn = new Trex.TrexDB("memory");
            await conn.execute(`LOAD '${escapeSql(extensionName)}'`, []);
            return { success: true, message: `Extension '${extensionName}' loaded`, error: null };
          } catch (err: any) {
            console.error("loadExtension error:", err);
            return { success: false, message: null, error: err.message || String(err) };
          }
        },
      },
    },
}));
