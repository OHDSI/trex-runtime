import type { Request, Response, NextFunction } from "express";
import { ROLE_SCOPES, REQUIRED_URL_SCOPES } from "../plugin/function.ts";

export function pluginAuthz(
  req: Request,
  res: Response,
  next: NextFunction
) {
  const pgSettings = (req as any).pgSettings || {};
  const platformRole = pgSettings["app.user_role"];

  // Admin platform role bypasses scope checks
  if (platformRole === "admin") {
    return next();
  }

  if (!pgSettings["app.user_id"]) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  const requestPath = req.originalUrl || req.path;
  const requiredScopes: string[] = [];
  for (const entry of REQUIRED_URL_SCOPES) {
    if (new RegExp(entry.path).test(requestPath)) {
      requiredScopes.push(...entry.scopes);
    }
  }

  if (requiredScopes.length === 0) {
    return next();
  }

  const applicationRoles: string[] = (req as any).applicationRoles || [];
  const userScopes = new Set<string>();
  for (const roleName of applicationRoles) {
    const scopes = ROLE_SCOPES[roleName];
    if (scopes) {
      for (const scope of scopes) {
        userScopes.add(scope);
      }
    }
  }

  const hasScope = requiredScopes.some((scope) => userScopes.has(scope));
  if (!hasScope) {
    res.status(403).json({ error: "Forbidden: insufficient scopes" });
    return;
  }

  next();
}
