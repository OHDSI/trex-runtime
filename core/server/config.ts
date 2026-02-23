const raw = Deno.env.get("BASE_PATH") || "/trex";
export const BASE_PATH = raw.endsWith("/") ? raw.slice(0, -1) : raw;

const rawPlugins = Deno.env.get("PLUGINS_BASE_PATH") || "/plugins";
export const PLUGINS_BASE_PATH = rawPlugins.endsWith("/") ? rawPlugins.slice(0, -1) : rawPlugins;

const rawFunctions = Deno.env.get("FUNCTIONS_BASE_PATH") || "/functions";
export const FUNCTIONS_BASE_PATH = rawFunctions.endsWith("/") ? rawFunctions.slice(0, -1) : rawFunctions;
