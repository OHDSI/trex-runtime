import type { Express, Request, Response } from "express";
import express from "express";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { BASE_PATH } from "../config.ts";
import { validateApiKey, type ValidatedUser } from "./auth.ts";
import { registerClusterTools } from "./tools/cluster.ts";
import { registerTrexdbTools } from "./tools/trexdb.ts";
import { registerPluginTools } from "./tools/plugins.ts";
import { registerEtlTools } from "./tools/etl.ts";
import { registerMigrationTools } from "./tools/migrations.ts";
import { registerUserTools } from "./tools/users.ts";
import { registerSessionTools } from "./tools/sessions.ts";
import { registerDatabaseTools } from "./tools/databases.ts";
import { registerRoleTools } from "./tools/roles.ts";
import { registerSsoTools } from "./tools/sso.ts";
import { registerAppTools } from "./tools/apps.ts";
import { registerApiKeyTools } from "./tools/api-keys.ts";
import { registerPluginResources } from "./resources/plugins.ts";

interface SessionEntry {
  transport: StreamableHTTPServerTransport;
  lastActivity: number;
  userId: string;
}

const sessions = new Map<string, SessionEntry>();
const IDLE_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes
const MAX_SESSIONS = 100;

setInterval(() => {
  const now = Date.now();
  for (const [id, entry] of sessions) {
    if (now - entry.lastActivity > IDLE_TIMEOUT_MS) {
      entry.transport.close();
      sessions.delete(id);
    }
  }
}, 5 * 60 * 1000);

function createMcpServer(): McpServer {
  const server = new McpServer({
    name: "trex",
    version: "1.0.0",
  });

  registerClusterTools(server);
  registerTrexdbTools(server);
  registerPluginTools(server);
  registerEtlTools(server);
  registerMigrationTools(server);
  registerUserTools(server);
  registerSessionTools(server);
  registerDatabaseTools(server);
  registerRoleTools(server);
  registerSsoTools(server);
  registerAppTools(server);
  registerApiKeyTools(server);

  registerPluginResources(server);

  return server;
}

async function authenticateRequest(req: Request): Promise<ValidatedUser | null> {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) return null;
  const key = authHeader.slice(7);
  return validateApiKey(key);
}

export function mountMcpServer(app: Express) {
  const mcpPath = `${BASE_PATH}/mcp`;

  app.post(mcpPath, express.json(), async (req: Request, res: Response) => {
    const user = await authenticateRequest(req);
    if (!user) {
      res.status(401).json({ error: "Invalid or missing API key" });
      return;
    }

    const sessionId = req.headers["mcp-session-id"] as string | undefined;

    if (sessionId && sessions.has(sessionId)) {
      const entry = sessions.get(sessionId)!;
      if (entry.userId !== user.id) {
        res.status(403).json({ error: "Session belongs to a different user" });
        return;
      }
      entry.lastActivity = Date.now();
      await entry.transport.handleRequest(req, res, req.body);
      return;
    }

    if (sessions.size >= MAX_SESSIONS) {
      res.status(503).json({ error: "Too many active MCP sessions" });
      return;
    }

    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => crypto.randomUUID(),
      onsessioninitialized: (id) => {
        sessions.set(id, { transport, lastActivity: Date.now(), userId: user.id });
      },
    });

    const server = createMcpServer();
    (transport as any)._mcpUser = user;
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  });

  app.get(mcpPath, async (req: Request, res: Response) => {
    const user = await authenticateRequest(req);
    if (!user) {
      res.status(401).json({ error: "Invalid or missing API key" });
      return;
    }

    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    if (!sessionId || !sessions.has(sessionId)) {
      res.status(400).json({ error: "Invalid or missing session" });
      return;
    }

    const entry = sessions.get(sessionId)!;
    if (entry.userId !== user.id) {
      res.status(403).json({ error: "Session belongs to a different user" });
      return;
    }
    entry.lastActivity = Date.now();
    await entry.transport.handleRequest(req, res);
  });

  app.delete(mcpPath, async (req: Request, res: Response) => {
    const user = await authenticateRequest(req);
    if (!user) {
      res.status(401).json({ error: "Invalid or missing API key" });
      return;
    }

    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    if (sessionId && sessions.has(sessionId)) {
      const entry = sessions.get(sessionId)!;
      if (entry.userId !== user.id) {
        res.status(403).json({ error: "Session belongs to a different user" });
        return;
      }
      entry.transport.close();
      sessions.delete(sessionId);
    }
    res.status(200).json({ ok: true });
  });
}
