import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";

export function registerAppTools(server: McpServer) {
  server.tool(
    "app-list",
    "List all OAuth applications (OIDC clients) registered with trex. These are external applications that can authenticate users via trex's OIDC provider. Shows app name, client ID, redirect URLs, type, and disabled status. Client secrets are omitted.",
    {},
    async () => {
      try {
        const result = await pool.query(
          `SELECT id, name, icon, metadata, "clientId", "redirectURLs", type, disabled, "userId", "createdAt", "updatedAt" FROM trex.oauth_application ORDER BY "createdAt" DESC`,
        );
        return { content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "app-create",
    "Register a new OAuth application (OIDC client). A client ID and secret will be generated automatically. The redirect URLs should be comma-separated. The returned clientSecret is shown only once — store it securely.",
    {
      name: z.string().describe("Application name"),
      redirectURLs: z.string().describe("Comma-separated redirect URLs"),
      type: z.enum(["web", "native"]).optional().describe("Application type (default 'web')"),
      icon: z.string().optional().describe("Application icon URL"),
      metadata: z.string().optional().describe("Additional metadata (JSON string)"),
    },
    async ({ name, redirectURLs, type, icon, metadata }) => {
      try {
        const id = crypto.randomUUID();
        const clientId = crypto.randomUUID();
        const clientSecret = crypto.randomUUID();

        const result = await pool.query(
          `INSERT INTO trex.oauth_application (id, name, "clientId", "clientSecret", "redirectURLs", type, icon, metadata)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, name, "clientId", "clientSecret", "redirectURLs", type`,
          [id, name, clientId, clientSecret, redirectURLs, type || "web", icon || null, metadata || null],
        );
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "app-update",
    "Update an OAuth application's configuration. Only the fields you provide will be changed. Client secret cannot be changed via this tool.",
    {
      appId: z.string().describe("Application ID"),
      name: z.string().optional().describe("New name"),
      redirectURLs: z.string().optional().describe("New comma-separated redirect URLs"),
      type: z.enum(["web", "native"]).optional().describe("New type"),
      disabled: z.boolean().optional().describe("Disable or enable the app"),
      icon: z.string().optional().describe("New icon URL"),
      metadata: z.string().optional().describe("New metadata (JSON string)"),
    },
    async ({ appId, name, redirectURLs, type, disabled, icon, metadata }) => {
      try {
        const sets: string[] = [];
        const params: any[] = [];
        let idx = 1;

        if (name !== undefined) { sets.push(`name = $${idx}`); params.push(name); idx++; }
        if (redirectURLs !== undefined) { sets.push(`"redirectURLs" = $${idx}`); params.push(redirectURLs); idx++; }
        if (type !== undefined) { sets.push(`type = $${idx}`); params.push(type); idx++; }
        if (disabled !== undefined) { sets.push(`disabled = $${idx}`); params.push(disabled); idx++; }
        if (icon !== undefined) { sets.push(`icon = $${idx}`); params.push(icon); idx++; }
        if (metadata !== undefined) { sets.push(`metadata = $${idx}`); params.push(metadata); idx++; }

        if (sets.length === 0) {
          return { content: [{ type: "text", text: "No fields to update" }], isError: true };
        }

        params.push(appId);
        const sql = `UPDATE trex.oauth_application SET ${sets.join(", ")}, "updatedAt" = NOW() WHERE id = $${idx} RETURNING id, name, "clientId", "redirectURLs", type, disabled`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Application not found" }], isError: true };
        }
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "app-delete",
    "Delete an OAuth application. All associated access tokens, authorization codes, and user consents will be cascade-deleted.",
    {
      appId: z.string().describe("Application ID to delete"),
    },
    async ({ appId }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.oauth_application WHERE id = $1 RETURNING id, name`,
          [appId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Application not found" }], isError: true };
        }
        return { content: [{ type: "text", text: `Application '${result.rows[0].name}' deleted` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
