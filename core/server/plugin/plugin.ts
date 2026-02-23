import type { Express } from "express";
import { addPlugin as addFlowPlugin } from "./flow.ts";
import { addPlugin as addFunctionPlugin } from "./function.ts";
import { addMigrationPlugin } from "./migration.ts";
import { addTransformPlugin } from "./transform.ts";
import { addPlugin as addUIPlugin } from "./ui.ts";
import { scanPluginDirectory } from "./utils.ts";

interface ActivePluginEntry {
  name: string;
  version: string;
  source: "dev" | "npm";
  registeredAt: Date;
}

export class Plugins {
  static activeRegistry: Map<string, ActivePluginEntry> = new Map();

  private static addPlugin(
    app: Express,
    dir: string,
    pkg: any,
    shortName: string,
    fullName: string,
    source: "dev" | "npm"
  ) {
    try {
      if (!pkg.trex) {
        console.log(
          `Plugin ${fullName} has no trex config — skipping registration`
        );
        return;
      }
      for (const [key, value] of Object.entries(pkg.trex)) {
        switch (key) {
          case "functions":
            addFunctionPlugin(app, value, dir, fullName);
            break;
          case "ui":
            addUIPlugin(app, value, dir, fullName);
            break;
          case "flow":
            addFlowPlugin(value);
            break;
          case "migrations":
            addMigrationPlugin(value, dir, shortName);
            break;
          case "transform":
            addTransformPlugin(app, value, dir, shortName);
            break;
          default:
            console.log(`Unknown plugin type: ${key}`);
        }
      }
      Plugins.activeRegistry.set(shortName, {
        name: fullName,
        version: pkg.version,
        source,
        registeredAt: new Date(),
      });
    } catch (e) {
      console.error(`Failed to register plugin ${fullName}:`, e);
    }
  }

  private static async scanAndRegister(
    app: Express,
    dir: string,
    source: "dev" | "npm"
  ) {
    const scanned = await scanPluginDirectory(dir);
    for (const { shortName, dir: pluginDir, pkg } of scanned) {
      const existing = Plugins.activeRegistry.get(shortName);
      if (existing) {
        console.log(
          `Skipping duplicate plugin ${shortName} from ${source} — already registered from ${existing.source}`
        );
        continue;
      }
      console.log(
        `Found plugin ${shortName} (v${pkg.version}) [${source}] in ${pluginDir}`
      );
      const fullName = pkg.name || shortName;
      Plugins.addPlugin(app, pluginDir, pkg, shortName, fullName, source);
      console.log(`Registered plugin ${shortName} [${source}]`);
    }
  }

  static async initPlugins(app: Express) {
    const devPath = Deno.env.get("PLUGINS_DEV_PATH") || "./plugins-dev";
    const pluginsPath = Deno.env.get("PLUGINS_PATH") || "./plugins";
    console.log("Scanning and registering plugins");

    // Dev plugins have highest priority — scanned first
    await Plugins.scanAndRegister(app, devPath, "dev");
    await Plugins.scanAndRegister(app, pluginsPath, "npm");

    console.log(
      `Plugin registration complete: ${Plugins.activeRegistry.size} plugins active`
    );
  }

  static getActivePlugins(): Map<string, ActivePluginEntry> {
    return Plugins.activeRegistry;
  }
}
