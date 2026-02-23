// @ts-ignore
import { STATUS_CODE } from "https://deno.land/std/http/status.ts";
import type { Express, Request, Response } from "express";
import { authContext } from "../middleware/auth-context.ts";
import { pluginAuthz } from "../middleware/plugin-authz.ts";
import { scopeUrlPrefix, waitfor } from "./utils.ts";
import { PLUGINS_BASE_PATH } from "../config.ts";

export const ROLE_SCOPES: Record<string, string[]> = {};
export const REQUIRED_URL_SCOPES: Array<{ path: string; scopes: string[] }> = [];

export const REGISTERED_FUNCTIONS: Array<{
  name: string;
  source: string;
  function: string;
}> = [];

// Inter-service request map (function name -> handler)
const fnmap: Record<string, (req: globalThis.Request) => Promise<globalThis.Response>> = {};

// deno-lint-ignore no-explicit-any
const Trex = (globalThis as any).Trex;
if (Trex?.createRequestListener) {
  Trex.createRequestListener(
    async (
      message: any,
      respond: (data: any) => void
    ) => {
      try {
        if (!message?.request) {
          respond({
            ok: false,
            status: 400,
            statusText: "Bad Request",
            headers: { "Content-Type": "application/json" },
            body: { error: "Invalid message structure" },
          });
          return;
        }

        const handler = fnmap[message.service];
        if (!handler) {
          respond({
            ok: false,
            status: 404,
            statusText: "Not Found",
            headers: { "Content-Type": "application/json" },
            body: { error: `Unknown service: ${message.service}` },
          });
          return;
        }

        const httpResponse = await handler(message.request);
        if (httpResponse instanceof Response) {
          const responseBody = await httpResponse.text();
          respond({
            body: responseBody,
            status: httpResponse.status,
            statusText: httpResponse.statusText,
            ok: httpResponse.ok,
            headers: Object.fromEntries(httpResponse.headers.entries()),
            url: httpResponse.url,
          });
        } else {
          respond(httpResponse);
        }
      } catch (error: any) {
        respond({
          ok: false,
          status: 500,
          statusText: "Internal Server Error",
          headers: { "Content-Type": "application/json" },
          body: { error: error.message },
        });
      }
    }
  );
}

function substituteEnvVars(input: string): string {
  let result = input;
  const maxIterations = 10;
  let iteration = 0;
  while (iteration < maxIterations) {
    const before = result;
    result = processVariables(result);
    if (result === before) break;
    iteration++;
  }
  return result;
}

function processVariables(input: string): string {
  let result = "";
  let i = 0;
  while (i < input.length) {
    if (input[i] === "$" && input[i + 1] === "{") {
      const varContentStart = i + 2;
      let braceCount = 1;
      let j = varContentStart;
      while (j < input.length && braceCount > 0) {
        if (input[j] === "{") braceCount++;
        else if (input[j] === "}") braceCount--;
        j++;
      }
      if (braceCount === 0) {
        const varExpression = input.substring(varContentStart, j - 1);
        result += substituteVariable(varExpression);
        i = j;
      } else {
        result += input[i];
        i++;
      }
    } else {
      result += input[i];
      i++;
    }
  }
  return result;
}

function substituteVariable(varExpression: string): string {
  const operatorMatch = varExpression.match(/^([^:?+-]+)([:+-]?[?+-])(.*)$/);
  if (operatorMatch) {
    const [, varName, operator, operand] = operatorMatch;
    const envValue = Deno.env.get(varName);
    const isSet = envValue !== undefined;
    const isNonEmpty = isSet && envValue !== "";
    switch (operator) {
      case ":-":
        return isNonEmpty ? envValue! : operand;
      case "-":
        return isSet ? envValue! : operand;
      case ":?":
        if (!isNonEmpty)
          throw new Error(operand || `${varName}: parameter null or not set`);
        return envValue!;
      case "?":
        if (!isSet)
          throw new Error(operand || `${varName}: parameter not set`);
        return envValue!;
      case ":+":
        return isNonEmpty ? operand : "";
      case "+":
        return isSet ? operand : "";
      default:
        return "${" + varExpression + "}";
    }
  }
  const envValue = Deno.env.get(varExpression);
  return envValue !== undefined ? envValue : "";
}

function substituteEnvVarsInObject(obj: any): any {
  if (typeof obj === "string") return substituteEnvVars(obj);
  if (Array.isArray(obj)) return obj.map(substituteEnvVarsInObject);
  if (obj !== null && typeof obj === "object") {
    const result: any = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = substituteEnvVarsInObject(value);
    }
    return result;
  }
  return obj;
}

async function _callWorker(
  req: globalThis.Request,
  servicePath: string,
  imports: string | null,
  fncfg: any,
  dir: string,
  xenv: any
): Promise<globalThis.Response> {
  const myenv = Object.assign(
    {},
    xenv["_shared"],
    fncfg.env in xenv ? xenv[fncfg.env] : {},
    { TREX_FUNCTION_PATH: dir }
  );
  const _myenv = Object.keys(myenv).map((k) => [
    k,
    typeof myenv[k] === "string" ? myenv[k] : JSON.stringify(myenv[k]),
  ]);

  const options: any = {
    servicePath,
    memoryLimitMb: 1000,
    workerTimeoutMs: 30 * 60 * 1000,
    noModuleCache: false,
    importMapPath: imports,
    envVars: _myenv,
    forceCreate: false,
    netAccessDisabled: false,
    cpuTimeSoftLimitMs: 1000000,
    cpuTimeHardLimitMs: 2000000,
    context: {
      useReadSyncFileAPI: true,
      unstableSloppyImports: true,
    },
  };

  if (fncfg.eszip) {
    options.maybeEszip = await Deno.readFile(`${dir}${fncfg.eszip}`);
  }

  try {
    // deno-lint-ignore no-explicit-any
    const worker = await (globalThis as any).EdgeRuntime.userWorkers.create(options);
    const controller = new AbortController();
    return await worker.fetch(req, { signal: controller.signal });
  } catch (e: any) {
    if (e instanceof Deno.errors.WorkerRequestCancelled) {
      // deno-lint-ignore no-explicit-any
      const worker = await (globalThis as any).EdgeRuntime.userWorkers.create(options);
      const controller = new AbortController();
      return await worker.fetch(req, { signal: controller.signal });
    }
    console.error("Worker call error:", e);
    return new globalThis.Response(JSON.stringify({ msg: e.toString() }), {
      status: STATUS_CODE.InternalServerError,
      headers: { "Content-Type": "application/json" },
    });
  }
}

async function _callInit(
  servicePath: string,
  imports: string | null,
  fnEnv: string,
  xenv: any,
  eszip: string | null,
  dir: string
) {
  const myenv = Object.assign(
    {},
    xenv["_shared"],
    fnEnv in xenv ? xenv[fnEnv] : {},
    { TREX_FUNCTION_PATH: dir }
  );
  const _myenv = Object.keys(myenv).map((k) => [
    k,
    typeof myenv[k] === "string" ? myenv[k] : JSON.stringify(myenv[k]),
  ]);

  const options: any = {
    servicePath,
    memoryLimitMb: 1000,
    workerTimeoutMs: 3 * 60 * 1000,
    noModuleCache: false,
    importMapPath: imports,
    envVars: _myenv,
    forceCreate: false,
    netAccessDisabled: false,
    cpuTimeSoftLimitMs: 100000,
    cpuTimeHardLimitMs: 200000,
    context: {
      useReadSyncFileAPI: true,
      unstableSloppyImports: true,
    },
  };

  if (eszip) {
    options.maybeEszip = await Deno.readFile(`${dir}${eszip}`);
  }

  try {
    // deno-lint-ignore no-explicit-any
    await (globalThis as any).EdgeRuntime.userWorkers.create(options);
  } catch (e) {
    console.error("Init worker error:", e);
  }
}

function _addFunction(
  app: Express,
  url: string,
  path: string,
  imports: string | null,
  fncfg: any,
  dir: string,
  name: string,
  xenv: any
) {
  REGISTERED_FUNCTIONS.push({ name, source: url, function: fncfg.function });

  fnmap[`${name}${fncfg.function}`] = (req: globalThis.Request) =>
    _callWorker(req, path, imports, fncfg, dir, xenv);

  const scopePrefix = scopeUrlPrefix(name);
  app.all(PLUGINS_BASE_PATH + scopePrefix + url + "/*", authContext, pluginAuthz, async (req: Request, res: Response) => {
    try {
      const host = req.get("host") || "localhost";
      const protocol = req.protocol || "http";
      const requestUrl = `${protocol}://${host}${req.originalUrl}`;

      const headers = new Headers();
      for (const [key, val] of Object.entries(req.headers)) {
        if (val) headers.set(key, Array.isArray(val) ? val.join(", ") : String(val));
      }

      let body: Blob | undefined;
      if (req.method !== "GET" && req.method !== "HEAD") {
        const chunks: Uint8Array[] = [];
        for await (const chunk of req as any) {
          chunks.push(
            typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk
          );
        }
        if (chunks.length > 0) body = new Blob(chunks);
      }

      const webReq = new globalThis.Request(requestUrl, {
        method: req.method,
        headers,
        body,
      });

      const workerResponse = await _callWorker(webReq, path, imports, fncfg, dir, xenv);

      res.status(workerResponse.status);
      workerResponse.headers.forEach((value: string, key: string) => {
        res.setHeader(key, value);
      });
      const responseBody = await workerResponse.text();
      res.send(responseBody);
    } catch (err) {
      console.error("Plugin route error:", err);
      res.status(500).json({ msg: String(err) });
    }
  });
}

export async function addPlugin(
  app: Express,
  value: any,
  dir: string,
  name: string
) {
  const xenv = substituteEnvVarsInObject(value.env || {});

  if (value.init) {
    for (const r of value.init) {
      if (r.function) {
        console.log(`add init fn @ ${dir}${r.function}`);
        const waitforUrl = r.waitfor ??
          (r.waitforEnvVar ? Deno.env.get(r.waitforEnvVar) ?? "" : "");
        if (waitforUrl) await waitfor(waitforUrl);

        await _callInit(
          `${dir}${r.function}`,
          r.imports
            ? r.imports.indexOf(":") < 0
              ? `${dir}${r.imports}`
              : r.imports
            : null,
          r.env,
          xenv,
          r.eszip || null,
          dir
        );

        if (r.delay) {
          await new Promise((resolve) => setTimeout(resolve, r.delay));
        }
        console.log(`add init fn done @ ${dir}${r.function}`);
      }
    }
  }

  if (value.roles) {
    for (const [roleName, scopes] of Object.entries(value.roles)) {
      if (ROLE_SCOPES[roleName]) {
        ROLE_SCOPES[roleName] = ROLE_SCOPES[roleName]
          .concat(scopes as string[])
          .filter((v: string, i: number, self: string[]) => self.indexOf(v) === i);
      } else {
        ROLE_SCOPES[roleName] = scopes as string[];
      }
    }
  }

  if (value.scopes) {
    REQUIRED_URL_SCOPES.push(...value.scopes);
  }

  if (value.api) {
    for (const r of value.api) {
      if (r.function) {
        console.log(`add fn ${r.source} @ ${dir}${r.function}`);
        _addFunction(
          app,
          r.source,
          `${dir}${r.function}`,
          r.imports
            ? r.imports.indexOf(":") < 0
              ? `${dir}${r.imports}`
              : r.imports
            : null,
          r,
          dir,
          name,
          xenv
        );
      }
    }
  }
}

export async function ensureRolesExist() {
  const roleNames = Object.keys(ROLE_SCOPES);
  if (roleNames.length === 0) return;

  try {
    const pg = (await import("pg")).default;
    const pool = new pg.Pool({ connectionString: Deno.env.get("DATABASE_URL") });
    try {
      const existing = await pool.query("SELECT name FROM trex.role");
      const existingNames = new Set(existing.rows.map((r: any) => r.name));
      for (const name of roleNames) {
        if (existingNames.has(name)) continue;
        const id = crypto.randomUUID();
        await pool.query(
          'INSERT INTO trex.role (id, name, description) VALUES ($1, $2, $3) ON CONFLICT (name) DO NOTHING',
          [id, name, "Auto-created from plugin registration"]
        );
        console.log(`Auto-created role: ${name}`);
      }
    } finally {
      await pool.end();
    }
  } catch (err) {
    console.error("ensureRolesExist error:", err);
  }
}
