import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";

declare const Deno: any;

export function registerDatabaseTools(server: McpServer) {
  server.tool(
    "database-list",
    "List all federated database connections configured in trex. These are external databases (PostgreSQL, etc.) that trex can connect to for federation, ETL, or credential storage. Shows connection details, dialect, enabled status, and timestamps.",
    {
      search: z.string().optional().describe("Search by ID, host, database name, or description"),
    },
    async ({ search }) => {
      try {
        let sql = `SELECT id, host, port, "databaseName", dialect, description, enabled, "vocabSchemas", extra, "createdAt", "updatedAt" FROM trex.database`;
        const params: any[] = [];
        if (search) {
          params.push(`%${search}%`);
          sql += ` WHERE id ILIKE $1 OR host ILIKE $1 OR "databaseName" ILIKE $1 OR description ILIKE $1`;
        }
        sql += ` ORDER BY "createdAt" DESC`;

        const result = await pool.query(sql, params);
        return { content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "database-create",
    "Create a new federated database connection. The id must be alphanumeric/underscores only and serves as the primary identifier. After creating the database entry, use database-credential-save to add connection credentials.",
    {
      id: z.string().describe("Database identifier (alphanumeric and underscores only)"),
      host: z.string().describe("Database host"),
      port: z.number().optional().describe("Database port (default 5432)"),
      databaseName: z.string().describe("Database name on the remote server"),
      dialect: z.string().optional().describe("Database dialect (default 'postgresql')"),
      description: z.string().optional().describe("Human-readable description"),
      enabled: z.boolean().optional().describe("Whether the connection is enabled (default true)"),
    },
    async ({ id, host, port, databaseName, dialect, description, enabled }) => {
      try {
        const result = await pool.query(
          `INSERT INTO trex.database (id, host, port, "databaseName", dialect, description, enabled)
           VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
          [id, host, port || 5432, databaseName, dialect || "postgresql", description || null, enabled !== false],
        );
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "database-update",
    "Update an existing federated database connection. Only the fields you provide will be changed.",
    {
      id: z.string().describe("Database ID to update"),
      host: z.string().optional().describe("New host"),
      port: z.number().optional().describe("New port"),
      databaseName: z.string().optional().describe("New database name"),
      dialect: z.string().optional().describe("New dialect"),
      description: z.string().optional().describe("New description"),
      enabled: z.boolean().optional().describe("Enable or disable"),
    },
    async ({ id, host, port, databaseName, dialect, description, enabled }) => {
      try {
        const sets: string[] = [];
        const params: any[] = [];
        let idx = 1;

        if (host !== undefined) { sets.push(`host = $${idx}`); params.push(host); idx++; }
        if (port !== undefined) { sets.push(`port = $${idx}`); params.push(port); idx++; }
        if (databaseName !== undefined) { sets.push(`"databaseName" = $${idx}`); params.push(databaseName); idx++; }
        if (dialect !== undefined) { sets.push(`dialect = $${idx}`); params.push(dialect); idx++; }
        if (description !== undefined) { sets.push(`description = $${idx}`); params.push(description); idx++; }
        if (enabled !== undefined) { sets.push(`enabled = $${idx}`); params.push(enabled); idx++; }

        if (sets.length === 0) {
          return { content: [{ type: "text", text: "No fields to update" }], isError: true };
        }

        params.push(id);
        const sql = `UPDATE trex.database SET ${sets.join(", ")} WHERE id = $${idx} RETURNING *`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Database not found" }], isError: true };
        }
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "database-delete",
    "Delete a federated database connection and all its credentials. This cannot be undone.",
    {
      id: z.string().describe("Database ID to delete"),
    },
    async ({ id }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.database WHERE id = $1 RETURNING id`,
          [id],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Database not found" }], isError: true };
        }
        return { content: [{ type: "text", text: `Database '${id}' deleted` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "database-test-connection",
    "Test connectivity to a federated database using its stored credentials. Returns success/failure with a message. Only works for PostgreSQL databases currently.",
    {
      databaseId: z.string().describe("Database ID to test"),
    },
    async ({ databaseId }) => {
      const pg = (await import("pg")).default;
      try {
        const dbResult = await pool.query(
          `SELECT host, port, "databaseName", dialect FROM trex.database WHERE id = $1`,
          [databaseId],
        );
        if (dbResult.rows.length === 0) {
          return { content: [{ type: "text", text: "Database not found" }], isError: true };
        }
        const db = dbResult.rows[0];

        const credResult = await pool.query(
          `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
          [databaseId],
        );
        if (credResult.rows.length === 0) {
          return { content: [{ type: "text", text: "No credentials configured" }], isError: true };
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
          return { content: [{ type: "text", text: "Connection successful" }] };
        } catch (connErr: any) {
          return { content: [{ type: "text", text: `Connection failed: ${connErr.message}` }], isError: true };
        } finally {
          await testPool.end();
        }
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "database-credential-save",
    "Save or update credentials for a federated database connection. If credentials already exist for this database, they will be updated.",
    {
      databaseId: z.string().describe("Database ID to attach credentials to"),
      username: z.string().describe("Database username"),
      password: z.string().describe("Database password"),
      userScope: z.string().optional().describe("User scope for credential filtering"),
      serviceScope: z.string().optional().describe("Service scope for credential filtering"),
    },
    async ({ databaseId, username, password, userScope, serviceScope }) => {
      try {
        const result = await pool.query(
          `SELECT trex.save_database_credential($1, $2, $3, $4, $5)`,
          [databaseId, username, password, userScope || null, serviceScope || null],
        );
        return { content: [{ type: "text", text: `Credentials saved for database '${databaseId}'` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "database-credential-delete",
    "Delete credentials for a federated database connection.",
    {
      credentialId: z.string().describe("Credential ID to delete"),
    },
    async ({ credentialId }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.database_credential WHERE id = $1 RETURNING id`,
          [credentialId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Credential not found" }], isError: true };
        }
        return { content: [{ type: "text", text: `Credential deleted` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
