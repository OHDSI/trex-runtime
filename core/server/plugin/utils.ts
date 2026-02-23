export interface ScannedPlugin {
  shortName: string;
  dir: string;
  pkg: any;
}

export async function scanPluginDirectory(
  baseDir: string,
): Promise<ScannedPlugin[]> {
  const results: ScannedPlugin[] = [];

  async function scanLevel(scanDir: string) {
    try {
      for await (const entry of Deno.readDir(scanDir)) {
        if (!entry.isDirectory) continue;
        if (entry.name.startsWith("@")) {
          await scanLevel(`${scanDir}/${entry.name}`);
          continue;
        }
        try {
          const pkgJsonPath = `${scanDir}/${entry.name}/package.json`;
          const pkg = JSON.parse(await Deno.readTextFile(pkgJsonPath));
          const shortName = pkg.name?.includes("/")
            ? pkg.name.split("/").pop()
            : pkg.name || entry.name;
          results.push({
            shortName,
            dir: `${scanDir}/${entry.name}`,
            pkg,
          });
        } catch {
          // no valid package.json
        }
      }
    } catch {
      // not readable
    }
  }

  await scanLevel(baseDir);
  return results;
}

export function scopeUrlPrefix(fullName: string): string {
  if (!fullName.startsWith("@") || !fullName.includes("/")) return "";
  return "/" + fullName.slice(1, fullName.indexOf("/"));
}

export async function waitfor(url: string): Promise<string> {
  let reachable = false;
  while (!reachable) {
    try {
      await fetch(url);
      reachable = true;
    } catch (_e) {
      console.log(`${url} not reachable. waiting ...`);
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }
  return "OK";
}
