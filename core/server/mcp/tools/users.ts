import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool, auth } from "../../auth.ts";

export function registerUserTools(server: McpServer) {
  server.tool(
    "user-list",
    "List all users in the trex system. Returns id, name, email, role (admin/user), banned status, email verification status, and timestamps. Does not include soft-deleted users by default.",
    {
      includeDeleted: z.boolean().optional().describe("Include soft-deleted users (default false)"),
      search: z.string().optional().describe("Search by name or email (case-insensitive)"),
    },
    async ({ includeDeleted, search }) => {
      try {
        let sql = `SELECT id, name, email, role, banned, "banReason", "emailVerified", "mustChangePassword", "deletedAt", "createdAt", "updatedAt" FROM trex."user"`;
        const conditions: string[] = [];
        const params: any[] = [];

        if (!includeDeleted) {
          conditions.push(`"deletedAt" IS NULL`);
        }
        if (search) {
          params.push(`%${search}%`);
          conditions.push(`(name ILIKE $${params.length} OR email ILIKE $${params.length})`);
        }
        if (conditions.length > 0) {
          sql += " WHERE " + conditions.join(" AND ");
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
    "user-get",
    "Get detailed information about a specific user by ID. Includes all user fields plus their assigned application roles.",
    {
      userId: z.string().describe("User ID"),
    },
    async ({ userId }) => {
      try {
        const userResult = await pool.query(
          `SELECT id, name, email, role, banned, "banReason", "banExpires", "emailVerified", "mustChangePassword", "deletedAt", "createdAt", "updatedAt" FROM trex."user" WHERE id = $1`,
          [userId],
        );
        if (userResult.rows.length === 0) {
          return { content: [{ type: "text", text: "User not found" }], isError: true };
        }

        const rolesResult = await pool.query(
          `SELECT r.id, r.name, r.description FROM trex.user_role ur JOIN trex.role r ON ur."roleId" = r.id WHERE ur."userId" = $1`,
          [userId],
        );

        const user = { ...userResult.rows[0], roles: rolesResult.rows };
        return { content: [{ type: "text", text: JSON.stringify(user, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "user-create",
    "Create a new user account. The user will have role 'user' by default. If no password is provided, the account will be created without credentials (SSO-only). The password should be a plaintext string — it will be hashed by Better Auth.",
    {
      name: z.string().describe("User's display name"),
      email: z.string().describe("User's email address"),
      password: z.string().optional().describe("Initial password (will be hashed)"),
      role: z.enum(["admin", "user"]).optional().describe("User role (default 'user')"),
    },
    async ({ name, email, password, role }) => {
      try {
        if (password) {
          const result = await auth.api.signUpEmail({
            body: { name, email, password },
          });
          if (!result?.user?.id) {
            return { content: [{ type: "text", text: "Error: Signup failed — no user returned" }], isError: true };
          }
          const id = result.user.id;
          await pool.query(
            `UPDATE trex."user" SET role = $1, "emailVerified" = true, "updatedAt" = NOW() WHERE id = $2`,
            [role || "user", id],
          );
          return { content: [{ type: "text", text: JSON.stringify({ id, name, email, role: role || "user" }, null, 2) }] };
        }

        const id = crypto.randomUUID();
        await pool.query(
          `INSERT INTO trex."user" (id, name, email, role, "emailVerified") VALUES ($1, $2, $3, $4, true)`,
          [id, name, email, role || "user"],
        );
        return { content: [{ type: "text", text: JSON.stringify({ id, name, email, role: role || "user" }, null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "user-update-role",
    "Change a user's system role to 'admin' or 'user'. This is the top-level role that controls access to admin features, separate from application roles.",
    {
      userId: z.string().describe("User ID"),
      role: z.enum(["admin", "user"]).describe("New role"),
    },
    async ({ userId, role }) => {
      try {
        const result = await pool.query(
          `UPDATE trex."user" SET role = $1, "updatedAt" = NOW() WHERE id = $2 RETURNING id, name, email, role`,
          [role, userId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "User not found" }], isError: true };
        }
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "user-ban",
    "Ban or unban a user. Banned users cannot log in or use API keys. Optionally specify a reason and expiration date for the ban.",
    {
      userId: z.string().describe("User ID"),
      banned: z.boolean().describe("true to ban, false to unban"),
      reason: z.string().optional().describe("Reason for the ban"),
      expiresAt: z.string().optional().describe("Ban expiration date (ISO 8601). Omit for permanent ban."),
    },
    async ({ userId, banned, reason, expiresAt }) => {
      try {
        const result = await pool.query(
          `UPDATE trex."user" SET banned = $1, "banReason" = $2, "banExpires" = $3, "updatedAt" = NOW() WHERE id = $4 RETURNING id, name, email, banned, "banReason"`,
          [banned, banned ? (reason || null) : null, banned ? (expiresAt || null) : null, userId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "User not found" }], isError: true };
        }
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
