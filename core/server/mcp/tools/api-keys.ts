import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";
import { generateApiKey } from "../auth.ts";

export function registerApiKeyTools(server: McpServer) {
  server.tool(
    "api-key-list",
    "List all API keys. Shows key ID, name, prefix (first 13 chars for identification), associated user, last used time, expiration, and revocation status. The full key is never shown — it was only available at creation time.",
    {},
    async () => {
      try {
        const result = await pool.query(
          `SELECT ak.id, ak.name, ak.key_prefix, ak."userId", u.name AS "userName", u.email AS "userEmail",
                  ak."lastUsedAt", ak."expiresAt", ak."revokedAt", ak."createdAt"
           FROM trex.api_key ak
           JOIN trex."user" u ON ak."userId" = u.id
           ORDER BY ak."createdAt" DESC`,
        );
        return { content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "api-key-create",
    "Create a new API key for MCP server authentication. The key is shown only once in the response — store it securely. Keys are prefixed with 'trex_' for easy identification. The userId must be an admin user.",
    {
      userId: z.string().describe("User ID to associate the key with (must be admin)"),
      name: z.string().describe("Descriptive name for the key (e.g. 'claude-code-dev')"),
      expiresAt: z.string().optional().describe("Expiration date (ISO 8601). Omit for non-expiring key."),
    },
    async ({ userId, name, expiresAt }) => {
      try {
        const userResult = await pool.query(
          `SELECT role FROM trex."user" WHERE id = $1`,
          [userId],
        );
        if (userResult.rows.length === 0) {
          return { content: [{ type: "text", text: "User not found" }], isError: true };
        }
        if (userResult.rows[0].role !== "admin") {
          return { content: [{ type: "text", text: "API keys can only be created for admin users" }], isError: true };
        }

        let expiresDate: Date | undefined;
        if (expiresAt) {
          expiresDate = new Date(expiresAt);
          if (isNaN(expiresDate.getTime())) {
            return { content: [{ type: "text", text: "Invalid expiresAt date" }], isError: true };
          }
        }

        const result = await generateApiKey(
          userId,
          name,
          expiresDate,
        );

        return {
          content: [{
            type: "text",
            text: JSON.stringify({
              id: result.id,
              key: result.key,
              name,
              userId,
              note: "Store this key securely — it will not be shown again.",
            }, null, 2),
          }],
        };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "api-key-revoke",
    "Revoke an API key. The key will immediately stop working. This cannot be undone — create a new key if needed.",
    {
      keyId: z.string().describe("API key ID to revoke"),
    },
    async ({ keyId }) => {
      try {
        const result = await pool.query(
          `UPDATE trex.api_key SET "revokedAt" = NOW() WHERE id = $1 AND "revokedAt" IS NULL RETURNING id, name, key_prefix`,
          [keyId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "API key not found or already revoked" }], isError: true };
        }
        return { content: [{ type: "text", text: `API key '${result.rows[0].name}' (${result.rows[0].key_prefix}...) revoked` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
