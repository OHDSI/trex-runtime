import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

declare const Trex: any;

import { escapeSql } from "../../lib/sql.ts";

export function registerTrexdbTools(server: McpServer) {
  server.tool(
    "trexdb-list-databases",
    "List all databases in the trexsql engine. Returns database name, OID, file path, whether it's internal, and type. The in-memory default database is always present; attached databases (Parquet, CSV, PostgreSQL via federation) also appear here.",
    {},
    async () => {
      try {
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute(
          "SELECT database_name, database_oid, path, internal, type FROM duckdb_databases()",
          [],
        );
        const rows = result?.rows || result || [];
        const dbs = rows.map((r: any) => ({
          databaseName: r.database_name || r[0] || "",
          databaseOid: String(r.database_oid ?? r[1] ?? ""),
          path: r.path || r[2] || null,
          internal: r.internal ?? r[3] ?? false,
          type: r.type || r[4] || "",
        }));
        return { content: [{ type: "text", text: JSON.stringify(dbs, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "trexdb-list-schemas",
    "List all schemas across all trexsql databases. Returns database name, schema name, schema OID, and whether the schema is internal (system schema).",
    {},
    async () => {
      try {
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute("SELECT * FROM duckdb_schemas()", []);
        const rows = result?.rows || result || [];
        const schemas = rows.map((r: any) => ({
          databaseName: r.database_name || r[0] || "",
          schemaName: r.schema_name || r[1] || "",
          schemaOid: String(r.database_oid ?? r.schema_oid ?? r[2] ?? ""),
          internal: r.internal ?? r[3] ?? false,
        }));
        return { content: [{ type: "text", text: JSON.stringify(schemas, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "trexdb-list-tables",
    "List tables in the trexsql engine, optionally filtered by database and/or schema. Returns table name, OID, primary key status, estimated size, column count, index count, and whether the table is temporary.",
    {
      database: z.string().optional().describe("Filter by database name"),
      schema: z.string().optional().describe("Filter by schema name"),
    },
    async ({ database, schema }) => {
      try {
        const conn = new Trex.TrexDB("memory");
        let sql = "SELECT * FROM duckdb_tables()";
        const conditions: string[] = [];
        if (database) conditions.push(`database_name = '${escapeSql(database)}'`);
        if (schema) conditions.push(`schema_name = '${escapeSql(schema)}'`);
        if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");

        const result = await conn.execute(sql, []);
        const rows = result?.rows || result || [];
        const tables = rows.map((r: any) => ({
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
        return { content: [{ type: "text", text: JSON.stringify(tables, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "trexdb-list-extensions",
    "List all trexsql extensions — both loaded and available. Shows extension name, loaded/installed status, version, description, and install path. Use this to see which extensions (db, pgwire, etl, hana, etc.) are active.",
    {},
    async () => {
      try {
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute("SELECT * FROM duckdb_extensions()", []);
        const rows = result?.rows || result || [];
        const extensions = rows.map((r: any) => ({
          extensionName: r.extension_name || r[0] || "",
          loaded: r.loaded ?? r[1] ?? false,
          installed: r.installed ?? r[2] ?? false,
          extensionVersion: r.extension_version || r[3] || null,
          description: r.description || r[4] || null,
          installPath: r.install_path || r[5] || null,
        }));
        return { content: [{ type: "text", text: JSON.stringify(extensions, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "trexdb-execute-sql",
    "Execute a SQL query against the trexsql engine. This runs directly on the in-memory trexsql instance, not on the PostgreSQL metadata store. Use for analytics queries, extension function calls, or any trexsql-native SQL. Returns results as JSON rows. Be careful with DDL/DML — changes persist in the trexsql database.",
    {
      sql: z.string().describe("SQL query to execute against the trexsql engine"),
    },
    async ({ sql }) => {
      try {
        const conn = new Trex.TrexDB("memory");
        const result = await conn.execute(sql, []);
        const rows = result?.rows || result || [];
        return { content: [{ type: "text", text: JSON.stringify(rows, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
