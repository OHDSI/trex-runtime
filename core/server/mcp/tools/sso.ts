import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";
import { reloadAuthProviders } from "../../auth.ts";

export function registerSsoTools(server: McpServer) {
  server.tool(
    "sso-list",
    "List all configured SSO (Single Sign-On) providers. Shows provider ID, display name, client ID, enabled status, and timestamps. Client secrets are omitted for security. Supported providers include Google, GitHub, Microsoft, and any custom OIDC provider.",
    {},
    async () => {
      try {
        const result = await pool.query(
          `SELECT id, "displayName", "clientId", enabled, "createdAt", "updatedAt" FROM trex.sso_provider ORDER BY "createdAt" DESC`,
        );
        return { content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "sso-save",
    "Create or update an SSO provider. The id must be a lowercase identifier (e.g. 'google', 'github', 'microsoft', 'okta'). If a provider with this id exists, it will be updated. Pass an empty string for clientSecret to keep the existing secret unchanged. After saving, SSO providers are automatically reloaded.",
    {
      id: z.string().describe("Provider identifier (lowercase, e.g. 'google', 'github')"),
      displayName: z.string().describe("Display name shown on login page"),
      clientId: z.string().describe("OAuth client ID"),
      clientSecret: z.string().describe("OAuth client secret (empty string to keep existing)"),
      enabled: z.boolean().optional().describe("Whether the provider is enabled (default false)"),
    },
    async ({ id, displayName, clientId, clientSecret, enabled }) => {
      try {
        await pool.query(
          `SELECT trex.save_sso_provider($1, $2, $3, $4, $5)`,
          [id, displayName, clientId, clientSecret, enabled ?? false],
        );
        try {
          await reloadAuthProviders();
        } catch (e) {
          // Non-fatal
        }
        return { content: [{ type: "text", text: `SSO provider '${id}' saved` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "sso-delete",
    "Delete an SSO provider by ID. Users who authenticated via this provider will keep their accounts but won't be able to log in via SSO until the provider is re-added. After deletion, SSO providers are automatically reloaded.",
    {
      id: z.string().describe("Provider ID to delete"),
    },
    async ({ id }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.sso_provider WHERE id = $1 RETURNING id`,
          [id],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "SSO provider not found" }], isError: true };
        }
        try {
          await reloadAuthProviders();
        } catch (e) {
          // Non-fatal
        }
        return { content: [{ type: "text", text: `SSO provider '${id}' deleted` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
