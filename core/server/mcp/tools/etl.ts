import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";

declare const Trex: any;
declare const Deno: any;

import { escapeSql, validateInt } from "../../lib/sql.ts";

export function registerEtlTools(server: McpServer) {
  server.tool(
    "etl-list-pipelines",
    "List all ETL/CDC pipelines and their current state. Pipelines replicate data from external PostgreSQL databases into trexsql using logical replication (CDC) or bulk copy. Shows name, state (running/stopped/error), mode (copy_and_cdc/cdc_only/copy_only), connection info, rows replicated, and any errors.",
    {},
    async () => {
      try {
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute("SELECT * FROM trex_etl_status()", []);
        const rows = result?.rows || result || [];
        const pipelines = rows.map((r: any) => ({
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
        return { content: [{ type: "text", text: JSON.stringify(pipelines, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "etl-start-pipeline",
    "Start an ETL/CDC pipeline to replicate data from a federated PostgreSQL database into trexsql. The databaseId must reference a database configured in trex database management (with credentials). Modes: 'copy_and_cdc' (initial copy then streaming CDC), 'cdc_only' (CDC streaming only, requires publication), 'copy_only' (one-time bulk copy). For CDC modes, a PostgreSQL publication name is required.",
    {
      name: z.string().describe("Unique pipeline name"),
      databaseId: z.string().describe("ID of the source database (from database management)"),
      mode: z.enum(["copy_and_cdc", "cdc_only", "copy_only"]).describe("Replication mode"),
      publication: z.string().optional().describe("PostgreSQL publication name (required for CDC modes)"),
      schema: z.string().optional().describe("Schema to copy (for copy_only mode, defaults to 'public')"),
      batchSize: z.number().optional().describe("Batch size for CDC (default 1000)"),
      batchTimeoutMs: z.number().optional().describe("Batch timeout in ms (default 5000)"),
      retryDelayMs: z.number().optional().describe("Retry delay in ms (default 10000)"),
      retryMaxAttempts: z.number().optional().describe("Max retry attempts (default 5)"),
    },
    async ({ name, databaseId, mode, publication, schema, batchSize, batchTimeoutMs, retryDelayMs, retryMaxAttempts }) => {
      if ((mode === "copy_and_cdc" || mode === "cdc_only") && !publication) {
        return { content: [{ type: "text", text: "Error: Publication name is required for CDC modes" }], isError: true };
      }

      try {
        const dbResult = await pool.query(
          `SELECT host, port, "databaseName" FROM trex.database WHERE id = $1`,
          [databaseId],
        );
        if (dbResult.rows.length === 0) {
          return { content: [{ type: "text", text: "Error: Database not found" }], isError: true };
        }
        const db = dbResult.rows[0];

        const credResult = await pool.query(
          `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
          [databaseId],
        );
        if (credResult.rows.length === 0) {
          return { content: [{ type: "text", text: "Error: No credentials configured for this database" }], isError: true };
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
          parts.push(`publication='${escapeSql(publication!)}'`);
        }
        if (mode === "copy_only") {
          parts.push(`schema='${escapeSql(schema || "public")}'`);
        }
        const connStr = parts.join(" ");

        let sql: string;
        if (batchSize != null || batchTimeoutMs != null || retryDelayMs != null || retryMaxAttempts != null) {
          const bs = validateInt(batchSize ?? 1000, "batchSize");
          const bt = validateInt(batchTimeoutMs ?? 5000, "batchTimeoutMs");
          const rd = validateInt(retryDelayMs ?? 10000, "retryDelayMs");
          const rm = validateInt(retryMaxAttempts ?? 5, "retryMaxAttempts");
          sql = `SELECT trex_etl_start('${escapeSql(name)}', '${escapeSql(connStr)}', '${escapeSql(mode)}', ${bs}, ${bt}, ${rd}, ${rm})`;
        } else {
          sql = `SELECT trex_etl_start('${escapeSql(name)}', '${escapeSql(connStr)}', '${escapeSql(mode)}')`;
        }

        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute(sql, []);
        const rows = result?.rows || result || [];
        const message = rows[0]?.[0] || rows[0]?.trex_etl_start || "Pipeline started";
        return { content: [{ type: "text", text: message }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "etl-stop-pipeline",
    "Stop a running ETL/CDC pipeline by name. The pipeline will finish its current batch before stopping gracefully.",
    {
      name: z.string().describe("Name of the pipeline to stop"),
    },
    async ({ name }) => {
      try {
        const conn = new Trex.TrexDB("memory");
        const sql = `SELECT trex_etl_stop('${escapeSql(name)}')`;
        const result = await conn.execute(sql, []);
        const rows = result?.rows || result || [];
        const message = rows[0]?.[0] || rows[0]?.trex_etl_stop || "Pipeline stopped";
        return { content: [{ type: "text", text: message }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
