import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";

export function registerSessionTools(server: McpServer) {
  server.tool(
    "session-list",
    "List active user sessions. Shows session ID, user info, IP address, user agent, expiration, and creation time. Expired sessions are excluded by default.",
    {
      userId: z.string().optional().describe("Filter sessions by user ID"),
      includeExpired: z.boolean().optional().describe("Include expired sessions (default false)"),
    },
    async ({ userId, includeExpired }) => {
      try {
        let sql = `SELECT s.id, s."userId", u.name AS "userName", u.email AS "userEmail", s."ipAddress", s."userAgent", s."expiresAt", s."createdAt"
                    FROM trex.session s JOIN trex."user" u ON s."userId" = u.id`;
        const conditions: string[] = [];
        const params: any[] = [];

        if (!includeExpired) {
          conditions.push(`s."expiresAt" > NOW()`);
        }
        if (userId) {
          params.push(userId);
          conditions.push(`s."userId" = $${params.length}`);
        }
        if (conditions.length > 0) {
          sql += " WHERE " + conditions.join(" AND ");
        }
        sql += ` ORDER BY s."createdAt" DESC`;

        const result = await pool.query(sql, params);
        return { content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "session-revoke",
    "Revoke (delete) a user session by ID, forcing the user to log in again. Use session-list to find session IDs.",
    {
      sessionId: z.string().describe("Session ID to revoke"),
    },
    async ({ sessionId }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.session WHERE id = $1 RETURNING id, "userId"`,
          [sessionId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Session not found" }], isError: true };
        }
        return { content: [{ type: "text", text: `Session ${sessionId} revoked` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
