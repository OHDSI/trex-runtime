import { core, primordials } from "ext:core/mod.js";

import { MAIN_WORKER_API, USER_WORKER_API } from "ext:ai/ai.js";
import { SUPABASE_USER_WORKERS } from "ext:user_workers/user_workers.js";
import { applySupabaseTag } from "ext:runtime/http.js";
import { waitUntil } from "ext:runtime/async_hook.js";
import {
  builtinTracer,
  enterSpan,
  METRICS_ENABLED,
  TRACING_ENABLED,
} from "ext:deno_telemetry/telemetry.ts";
import { TrexHttpClient, req, createRequestListener, prompt, op_add_replication, PluginManager, TrexDB, DatabaseManager, UserDatabaseManager } from "ext:trex/trex_lib.js";

const ops = core.ops;
const { ObjectDefineProperty } = primordials;

/**
 * @param {"user" | "main" | "event"} kind 
 * @param {number} terminationRequestTokenRid 
 */
function installTrexNamespace(kind, terminationRequestTokenRid) {

	/** TREX */

	let propsTrex = {
		scheduleTermination: () => ops.op_cancel_drop_token(terminationRequestTokenRid)
	};

	switch (kind) {
		case "main":
			propsTrex = {
				ai: MAIN_WORKER_API,
				userWorkers: SUPABASE_USER_WORKERS,
				getRuntimeMetrics: () => /* async */ ops.op_runtime_metrics(),
				applySupabaseTag: (src, dest) => applySupabaseTag(src, dest),
				systemMemoryInfo: () => ops.op_system_memory_info(),
				raiseSegfault: () => ops.op_raise_segfault(),
				PluginManager: PluginManager,
				DatabaseManager: DatabaseManager,
				userDatabaseManager: () => { return new UserDatabaseManager(SUPABASE_USER_WORKERS)},
				TrexDB: TrexDB,
				addReplication: op_add_replication,
				addDB: op_add_replication,
				ask: prompt,
				req: req,
				createRequestListener: createRequestListener,
        httpClient: (service) => { return new TrexHttpClient(service) },
				exit: (c) => ops.op_exit(c),
				...propsTrex,
			};
			break;

		case "event":
			propsTrex = {
				...propsTrex
			};
			break;

		case "user":
			propsTrex = {
				waitUntil,
				ask: prompt,
				req: req,
        httpClient: (service) => { return new TrexHttpClient(service) },
				databaseManager: () => { return new UserDatabaseManager(SUPABASE_USER_WORKERS)},
			};
			break;
	}

	if (propsTrex === void 0) {
		return;
	}

	ObjectDefineProperty(globalThis, "Trex", {
		get() {
			return propsTrex;
		},
		configurable: true,
	});
}

/*
 * @param {"user" | "main" | "event"} kind
 * @param {number} terminationRequestTokenRid
 */
function installEdgeRuntimeNamespace(kind, terminationRequestTokenRid) {
  let props = {
    scheduleTermination: () =>
      ops.op_cancel_drop_token(terminationRequestTokenRid),
  };

  switch (kind) {
    case "main":
      props = {
        ai: MAIN_WORKER_API,
        userWorkers: SUPABASE_USER_WORKERS,
        getRuntimeMetrics: () => /* async */ ops.op_runtime_metrics(),
        applySupabaseTag: (src, dest) => applySupabaseTag(src, dest),
        systemMemoryInfo: () => ops.op_system_memory_info(),
        raiseSegfault: () => ops.op_raise_segfault(),
        ...props,
      };
      break;

    case "event":
      props = {
        builtinTracer,
        enterSpan,
        METRICS_ENABLED,
        TRACING_ENABLED,
        ...props,
      };
      break;

    case "user":
      props = {
        waitUntil,
      };
      break;
  }

  if (props === void 0) {
    return;
  }

  ObjectDefineProperty(globalThis, "EdgeRuntime", {
    get() {
      return props;
    },
    configurable: true,
  });
}

/**
 * @param {"user" | "main" | "event"} _kind
 */
function installSupabaseNamespace(_kind) {
  const props = {
    ai: USER_WORKER_API,
  };

  ObjectDefineProperty(globalThis, "Supabase", {
    get() {
      return props;
    },
    configurable: true,
  });
}

export { installEdgeRuntimeNamespace, installSupabaseNamespace, installTrexNamespace };
