import { STATUS_CODE } from "https://deno.land/std/http/status.ts";
import { handleRegistryRequest } from "./registry/mod.ts";
import { join } from "jsr:@std/path@^1.0";

addEventListener("unhandledrejection", (ev) => {
  console.error("Unhandled rejection:", ev.reason);
  ev.preventDefault();
});

Deno.serve(async (req: Request) => {
  const headers = new Headers({
    "Content-Type": "application/json",
  });

  const url = new URL(req.url);
  const { pathname } = url;

  if (pathname === "/_internal/health") {
    return new Response(
      JSON.stringify({ "message": "ok" }),
      {
        status: STATUS_CODE.OK,
        headers,
      },
    );
  }

  if (pathname === "/_internal/metric") {
    const metric = await EdgeRuntime.getRuntimeMetrics();
    return Response.json(metric);
  }

  const REGISTRY_PREFIX = "/_internal/registry";
  if (pathname.startsWith(REGISTRY_PREFIX)) {
    return await handleRegistryRequest(REGISTRY_PREFIX, req);
  }

  if (req.method === "PUT" && pathname === "/_internal/upload") {
    try {
      const content = await req.text();
      const dir = await Deno.makeTempDir();
      const path = join(dir, "index.ts");

      await Deno.writeTextFile(path, content);
      return Response.json({
        path: dir,
      });
    } catch (err) {
      return Response.json(err, {
        status: STATUS_CODE.BadRequest,
      });
    }
  }

  let servicePath = pathname;
  if (!pathname.startsWith("/tmp/")) {
    const path_parts = pathname.split("/");
    const service_name = path_parts[1];

    if (!service_name || service_name === "") {
      const error = { msg: "missing function name in request" };
      return new Response(
        JSON.stringify(error),
        {
          status: STATUS_CODE.BadRequest,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    servicePath = `./examples/${service_name}`;
  } else {
    try {
      servicePath = await Deno.realPath(servicePath);
    } catch (err) {
      return Response.json(err, {
        status: STATUS_CODE.BadRequest,
      });
    }
  }

  const createWorker = async () => {
    const memoryLimitMb = 150;
    const workerTimeoutMs = 5 * 60 * 1000;
    const noModuleCache = false;

    const envVarsObj = Deno.env.toObject();
    const envVars = Object.keys(envVarsObj).map((k) => [k, envVarsObj[k]]);
    const forceCreate = false;
    const cpuTimeSoftLimitMs = 10000;
    const cpuTimeHardLimitMs = 20000;
    const staticPatterns = [
      "./examples/**/*.html",
    ];

    return await EdgeRuntime.userWorkers.create({
      servicePath,
      memoryLimitMb,
      workerTimeoutMs,
      noModuleCache,
      envVars,
      forceCreate,
      cpuTimeSoftLimitMs,
      cpuTimeHardLimitMs,
      staticPatterns,
      context: {
        useReadSyncFileAPI: true,
      },
    });
  };

  const callWorker = async () => {
    try {
      const worker = await createWorker();
      const controller = new AbortController();
      const signal = controller.signal;

      return await worker.fetch(req, { signal });
    } catch (e) {
      if (e instanceof Deno.errors.WorkerAlreadyRetired) {
        return await callWorker();
      }
      if (e instanceof Deno.errors.WorkerRequestCancelled) {
        headers.append("Connection", "close");
      }

      const error = { msg: e.toString() };
      return new Response(
        JSON.stringify(error),
        {
          status: STATUS_CODE.InternalServerError,
          headers,
        },
      );
    }
  };

  return callWorker();
});
