import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { pool } from "../../auth.ts";

export function registerRoleTools(server: McpServer) {
  server.tool(
    "role-list",
    "List all application roles. These are custom roles (separate from the system admin/user role) that can be assigned to users for fine-grained access control. Plugins and RLS policies can check these roles.",
    {
      search: z.string().optional().describe("Search by role name or description"),
    },
    async ({ search }) => {
      try {
        let sql = `SELECT id, name, description, "createdAt", "updatedAt" FROM trex.role`;
        const params: any[] = [];
        if (search) {
          params.push(`%${search}%`);
          sql += ` WHERE name ILIKE $1 OR description ILIKE $1`;
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
    "role-create",
    "Create a new application role. Roles can then be assigned to users via user-role-assign.",
    {
      name: z.string().describe("Unique role name"),
      description: z.string().optional().describe("Role description"),
    },
    async ({ name, description }) => {
      try {
        const result = await pool.query(
          `INSERT INTO trex.role (name, description) VALUES ($1, $2) RETURNING *`,
          [name, description || null],
        );
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "role-update",
    "Update an application role's name or description.",
    {
      roleId: z.string().describe("Role ID to update"),
      name: z.string().optional().describe("New role name"),
      description: z.string().optional().describe("New description"),
    },
    async ({ roleId, name, description }) => {
      try {
        const sets: string[] = [];
        const params: any[] = [];
        let idx = 1;

        if (name !== undefined) { sets.push(`name = $${idx}`); params.push(name); idx++; }
        if (description !== undefined) { sets.push(`description = $${idx}`); params.push(description); idx++; }

        if (sets.length === 0) {
          return { content: [{ type: "text", text: "No fields to update" }], isError: true };
        }

        params.push(roleId);
        const sql = `UPDATE trex.role SET ${sets.join(", ")} WHERE id = $${idx} RETURNING *`;
        const result = await pool.query(sql, params);
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Role not found" }], isError: true };
        }
        return { content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "role-delete",
    "Delete an application role. All user-role assignments for this role will be cascade-deleted.",
    {
      roleId: z.string().describe("Role ID to delete"),
    },
    async ({ roleId }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.role WHERE id = $1 RETURNING id, name`,
          [roleId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Role not found" }], isError: true };
        }
        return { content: [{ type: "text", text: `Role '${result.rows[0].name}' deleted` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "user-role-assign",
    "Assign an application role to a user. The user will gain the permissions associated with this role in addition to their system role (admin/user).",
    {
      userId: z.string().describe("User ID"),
      roleId: z.string().describe("Role ID to assign"),
    },
    async ({ userId, roleId }) => {
      try {
        await pool.query(
          `INSERT INTO trex.user_role ("userId", "roleId") VALUES ($1, $2) ON CONFLICT ("userId", "roleId") DO NOTHING`,
          [userId, roleId],
        );
        return { content: [{ type: "text", text: `Role assigned to user` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );

  server.tool(
    "user-role-remove",
    "Remove an application role from a user.",
    {
      userId: z.string().describe("User ID"),
      roleId: z.string().describe("Role ID to remove"),
    },
    async ({ userId, roleId }) => {
      try {
        const result = await pool.query(
          `DELETE FROM trex.user_role WHERE "userId" = $1 AND "roleId" = $2 RETURNING id`,
          [userId, roleId],
        );
        if (result.rows.length === 0) {
          return { content: [{ type: "text", text: "Role assignment not found" }], isError: true };
        }
        return { content: [{ type: "text", text: `Role removed from user` }] };
      } catch (err: any) {
        return { content: [{ type: "text", text: `Error: ${err.message}` }], isError: true };
      }
    },
  );
}
