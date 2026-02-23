import type { Request, Response, NextFunction } from "express";
import { Pool } from "pg";
import { auth } from "../auth.ts";

let pool: InstanceType<typeof Pool> | null = null;

function getPool(): InstanceType<typeof Pool> {
  if (!pool) {
    const databaseUrl = Deno.env.get("DATABASE_URL");
    if (databaseUrl) {
      pool = new Pool({
        connectionString: databaseUrl,
        options: "-c search_path=trex,public",
      });
    }
  }
  return pool!;
}

export async function authContext(
  req: Request,
  _res: Response,
  next: NextFunction
) {
  try {
    const session = await auth.api.getSession({
      headers: req.headers as any,
    });

    if (session?.user) {
      (req as any).pgSettings = {
        "app.user_id": session.user.id,
        "app.user_role": (session.user as any).role || "user",
      };

      // Fetch application roles (DB owner bypasses RLS)
      try {
        const p = getPool();
        if (p) {
          const result = await p.query(
            `SELECT r.name FROM trex.user_role ur
             JOIN trex.role r ON ur."roleId" = r.id
             WHERE ur."userId" = $1`,
            [session.user.id]
          );
          (req as any).applicationRoles = result.rows.map(
            (row: any) => row.name
          );
        } else {
          (req as any).applicationRoles = [];
        }
      } catch {
        (req as any).applicationRoles = [];
      }
    } else {
      (req as any).pgSettings = {};
      (req as any).applicationRoles = [];
    }
  } catch {
    (req as any).pgSettings = {};
    (req as any).applicationRoles = [];
  }

  next();
}
