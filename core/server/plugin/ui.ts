import type { Express } from "express";
import { PLUGINS_BASE_PATH } from "../config.ts";
import { scopeUrlPrefix } from "./utils.ts";

let pluginsJson: string = "{}";

export const REGISTERED_UI_ROUTES: Array<{
  pluginName: string;
  urlPrefix: string;
  fsPath: string;
}> = [];

export function getPluginsJson(): string {
  return pluginsJson;
}

export function addPlugin(_app: Express, value: any, dir: string, fullName: string = "") {
  const scopePrefix = scopeUrlPrefix(fullName);
  if (value.routes) {
    for (const r of value.routes) {
      const urlPrefix = r.path || r.source;
      const fsPath = `${dir}/${r.dir || r.target}`;
      const fullPrefix = `${PLUGINS_BASE_PATH}${scopePrefix}${urlPrefix}`;
      console.log(`Registering static route: ${fullPrefix} -> ${fsPath}`);
      REGISTERED_UI_ROUTES.push({ pluginName: name, urlPrefix: fullPrefix, fsPath });
      try {
        // deno-lint-ignore no-explicit-any
        (Deno as any).core.ops.op_register_static_route(fullPrefix, fsPath);
      } catch (e) {
        console.error(`Failed to register static route ${fullPrefix}: ${e}`);
      }
    }
  }

  if (value.uiplugins) {
    pluginsJson = updatePluginJson(JSON.parse(pluginsJson), value.uiplugins);
    console.log("Updated UI plugins JSON");
  }
}

export function mergeChildren(existingItem: any, incomingItem: any): void {
  if (!incomingItem.children || !Array.isArray(incomingItem.children)) {
    return;
  }

  if (!existingItem.children) {
    existingItem.children = [];
  }

  const existingChildrenByRoute = new Map<string, any>();
  existingItem.children.forEach((child: any) => {
    if (child.route) {
      existingChildrenByRoute.set(child.route, child);
    }
  });

  incomingItem.children.forEach((incomingChild: any) => {
    if (!incomingChild.route) return;

    if (existingChildrenByRoute.has(incomingChild.route)) {
      Object.assign(
        existingChildrenByRoute.get(incomingChild.route),
        incomingChild
      );
    } else {
      existingItem.children.push(incomingChild);
    }
  });
}

export function mergePluginItem(existingItem: any, incomingItem: any): void {
  if (incomingItem.children && Array.isArray(incomingItem.children)) {
    mergeChildren(existingItem, incomingItem);
  }

  const existingChildren = existingItem.children;
  Object.assign(existingItem, incomingItem);
  if (existingChildren) {
    existingItem.children = existingChildren;
  }
}

export function updatePluginJson(plugins: any, uiPlugins: any): string {
  for (const [key, value] of Object.entries(uiPlugins)) {
    const pluginArray = value as any[];

    if (plugins[key]) {
      const existingItemsByRoute = new Map<string, any>();
      plugins[key].forEach((item: any) => {
        if (item.route) {
          existingItemsByRoute.set(item.route, item);
        }
      });

      pluginArray.forEach((incomingItem: any) => {
        const route = incomingItem.route;
        if (!route) return;

        if (existingItemsByRoute.has(route)) {
          mergePluginItem(existingItemsByRoute.get(route), incomingItem);
        } else {
          plugins[key].push(incomingItem);
        }
      });
    } else {
      plugins[key] = pluginArray;
    }
  }

  const fqdn = Deno.env.get("PUBLIC_FQDN") || "";
  return JSON.stringify(plugins).replace(/\$\$FQDN\$\$/g, fqdn);
}
