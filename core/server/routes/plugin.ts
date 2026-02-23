import type { Express, Request, Response } from "express";
import { Plugins } from "../plugin/plugin.ts";
import { getPluginsJson } from "../plugin/ui.ts";
import { scanPluginDirectory } from "../plugin/utils.ts";
import { authContext } from "../middleware/auth-context.ts";
import { PLUGINS_BASE_PATH } from "../config.ts";

function checkSemver(version: string, sver: string): boolean {
  if (!sver || sver === "latest" || sver === "all") return true;
  try {
    const vParts = version.split(".");
    const sParts = sver.replace(/[^\d.]/g, "").split(".");
    if (sParts[0] && vParts[0] !== sParts[0]) return false;
    return true;
  } catch (_) {
    return true;
  }
}

export async function scanDiskPlugins(): Promise<
  Map<string, { name: string; version: string; source: "dev" | "npm" }>
> {
  const devPath = Deno.env.get("PLUGINS_DEV_PATH") || "./plugins-dev";
  const pluginsPath = Deno.env.get("PLUGINS_PATH") || "./plugins";
  const diskPlugins = new Map<
    string,
    { name: string; version: string; source: "dev" | "npm" }
  >();

  // Dev plugins first — first entry wins
  for (const { shortName, pkg } of await scanPluginDirectory(devPath)) {
    diskPlugins.set(shortName, {
      name: shortName,
      version: pkg.version,
      source: "dev",
    });
  }
  for (const { shortName, pkg } of await scanPluginDirectory(pluginsPath)) {
    if (!diskPlugins.has(shortName)) {
      diskPlugins.set(shortName, {
        name: shortName,
        version: pkg.version,
        source: "npm",
      });
    }
  }

  return diskPlugins;
}

export function addPluginRoutes(app: Express) {
  app.get(`${PLUGINS_BASE_PATH}`, authContext, async (req: Request, res: Response) => {
    const pgSettings = (req as any).pgSettings || {};
    if (pgSettings["app.user_role"] !== "admin") {
      res.status(403).json({ error: "Forbidden" });
      return;
    }

    const q = (req.query.version as string) || "compatible";

    try {
      const diskPlugins = await scanDiskPlugins();
      const activePlugins = Plugins.getActivePlugins();

      const pluginList: any[] = [];
      const seen = new Set<string>();

      for (const [name, diskInfo] of diskPlugins) {
        seen.add(name);
        const activeEntry = activePlugins.get(name);
        const active = !!activeEntry;
        const activeVersion = activeEntry?.version || null;
        const pendingRestart =
          !active || activeVersion !== diskInfo.version;
        pluginList.push({
          name,
          version: diskInfo.version,
          activeVersion,
          active,
          installed: true,
          pendingRestart,
          source: activeEntry?.source || diskInfo.source,
        });
      }

      // Add active plugins not on disk (deleted but still serving)
      for (const [name, activeEntry] of activePlugins) {
        if (seen.has(name)) continue;
        pluginList.push({
          name,
          version: null,
          activeVersion: activeEntry.version,
          active: true,
          installed: false,
          pendingRestart: true,
          source: activeEntry.source,
        });
      }

      if (q === "none") {
        res.json(pluginList);
        return;
      }

      const registryUrl = Deno.env.get("PLUGINS_INFORMATION_URL");
      if (registryUrl) {
        try {
          const apiVersion =
            Deno.env.get("PLUGINS_API_VERSION") || "latest";
          const pkgsRes = await fetch(registryUrl);
          const pkgsJson = await pkgsRes.json();

          const registryMap = new Map<
            string,
            { description: string; registryVersion: string }
          >();
          const packages = pkgsJson.value || pkgsJson;

          for (const pkg of packages) {
            const pkgname = pkg.name || "";
            let bestVersion = { version: "", packageDescription: "" };
            if (pkg.versions && Array.isArray(pkg.versions)) {
              bestVersion = pkg.versions.reduce((m: any, c: any) => {
                return c.version > m.version &&
                  checkSemver(c.version, q === "compatible" ? apiVersion : q)
                  ? c
                  : m;
              }, bestVersion);
            }
            registryMap.set(pkgname, {
              description: bestVersion.packageDescription || pkg.description || "",
              registryVersion: bestVersion.version || pkg.version || "",
            });
          }

          for (const plugin of pluginList) {
            const regInfo = registryMap.get(plugin.name);
            if (regInfo) {
              plugin.description = regInfo.description;
              plugin.registryVersion = regInfo.registryVersion;
            }
          }

          for (const [pkgname, regInfo] of registryMap) {
            if (!seen.has(pkgname) && !activePlugins.has(pkgname)) {
              pluginList.push({
                name: pkgname,
                version: null,
                activeVersion: null,
                active: false,
                installed: false,
                pendingRestart: false,
                description: regInfo.description,
                registryVersion: regInfo.registryVersion,
              });
            }
          }
        } catch (e) {
          console.error(`Failed to fetch registry info: ${e}`);
        }
      }

      res.json(pluginList);
    } catch (err) {
      console.error("Plugin list error:", err);
      res.status(500).json({ error: "Internal server error" });
    }
  });

  app.get(`${PLUGINS_BASE_PATH}/ui`, (_req: Request, res: Response) => {
    res.setHeader("Content-Type", "application/json");
    res.send(getPluginsJson());
  });
}
