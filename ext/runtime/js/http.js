core.loadExtScript("ext:deno_http/01_http.js");

import { core, internals, primordials } from "ext:core/mod.js";
const { RequestPrototype } = core.loadExtScript("ext:deno_fetch/23_request.js");
const {
  fromInnerResponse,
  newInnerResponse,
} = core.loadExtScript("ext:deno_fetch/23_response.js");
const { upgradeWebSocket } = core.loadExtScript("ext:deno_http/02_websocket.ts");
import { HttpConn } from "ext:runtime/01_http.js";
const { UpgradedConn } = core.loadExtScript("ext:deno_net/01_net.js");
const {
  builtinTracer,
  ContextManager,
  currentSnapshot,
  enterSpan,
  PROPAGATORS,
  restoreSnapshot,
  TRACING_ENABLED,
} = core.loadExtScript("ext:deno_telemetry/telemetry.ts");
const {
  updateSpanFromRequest,
  updateSpanFromServerResponse,
} = core.loadExtScript("ext:deno_telemetry/util.ts");

const ops = core.ops;

const {
  BadResourcePrototype,
  internalRidSymbol,
} = core;
const {
  ArrayPrototypeFind,
  ArrayPrototypeMap,
  ArrayPrototypePush,
  SafeArrayIterator,
  ObjectPrototypeIsPrototypeOf,
  SafePromisePrototypeFinally,
} = primordials;

const HttpConnPrototypeNextRequest = HttpConn.prototype.nextRequest;
const HttpConnPrototypeClose = HttpConn.prototype.close;

const kSupabaseTag = Symbol("kSupabaseTag");

let ACTIVE_REQUESTS = 0;

const HTTP_CONNS = new Set();
const RAW_UPGRADE_RESPONSE_SENTINEL = fromInnerResponse(
  newInnerResponse(101),
  "immutable",
);

function internalServerError() {
  // "Internal Server Error"
  return new Response(
    new Uint8Array([
      73,
      110,
      116,
      101,
      114,
      110,
      97,
      108,
      32,
      83,
      101,
      114,
      118,
      101,
      114,
      32,
      69,
      114,
      114,
      111,
      114,
    ]),
    { status: 500 },
  );
}

function serveHttp(conn) {
  let closed = false;

  const [connRid, watcherRid] = ops.op_http_start(conn[internalRidSymbol]);
  const httpConn = new HttpConn(connRid, conn.remoteAddr, conn.localAddr);

  httpConn.nextRequest = async () => {
    const nextRequest = await HttpConnPrototypeNextRequest.call(httpConn);

    if (nextRequest === null) {
      return null;
    }

    nextRequest.request[kSupabaseTag] = {
      watcherRid,
      streamRid: nextRequest.streamRid,
    };

    return nextRequest;
  };

  httpConn.close = () => {
    if (!closed) {
      closed = true;
      HTTP_CONNS.delete(httpConn);
      core.tryClose(watcherRid);
      HttpConnPrototypeClose.call(httpConn);
    }
  };

  HTTP_CONNS.add(httpConn);

  return httpConn;
}

function serve(args1, args2) {
  const options = {
    port: 9999,
    hostname: "0.0.0.0",
    transport: "tcp",
  };

  if (typeof args1 === "function") {
    options["handler"] = args1;
  } else if (typeof args2 === "function") {
    options["handler"] = args2;
  } else if (
    typeof args1 === "object" && typeof args1["handler"] === "function"
  ) {
    options["handler"] = args1["handler"];
  } else {
    throw new TypeError("A handler function must be provided.");
  }

  if (typeof args1 === "object") {
    if (args1["port"] !== undefined) {
      options["port"] = args1["port"];
    }
    if (args1["hostname"] !== undefined) {
      options["hostname"] = args1["hostname"];
    }
    if (typeof args1["onListen"] === "function") {
      options["onListen"] = args1["onListen"];
    }
    if (typeof args1["onError"] === "function") {
      options["onError"] = args1["onError"];
    }
  }

  const listener = Deno.listen(options);
  const snapshot = currentSnapshot();

  const handleHttp = async (conn) => {
    const currentHttpConn = serveHttp(conn);

    try {
      for await (const requestEvent of currentHttpConn) {
        ACTIVE_REQUESTS++;
        // NOTE: Respond to the request. Note we do not await this async
        // method to allow the connection to handle multiple requests in
        // the case of h2.
        //
        // [1]: https://deno.land/std@0.131.0/http/server.ts?source=#L338
        respond(requestEvent, currentHttpConn, options, snapshot).then(() => {
          ACTIVE_REQUESTS--;
        });
      }
    } catch {
      // connection has been closed
    } finally {
      closeHttpConn(currentHttpConn);
    }
  };

  const finished = (async () => {
    options["onListen"]?.({
      hostname: options.hostname,
      port: options.port,
    });

    try {
      for await (const conn of listener) {
        handleHttp(conn);
      }
    } catch (error) {
      // Listener closed during shutdown - this is expected
      if (error?.message?.includes("listener closed") ||
          error?.message?.includes("invalid_argument")) {
        return;
      }
      throw error;
    }
  })();

  const kind = internals.worker.kind;
  const shutdownEventName = kind === "user" ? "drain" : "beforeunload";
  const handleShutdownEvent = () => {
    shutdown();
  };

  const shutdown = () => {
    removeEventListener(shutdownEventName, handleShutdownEvent);

    try {
      listener.close();
    } catch (error) {
      if (
        ObjectPrototypeIsPrototypeOf(BadResourcePrototype, error)
      ) {
        return;
      }

      throw error;
    }

    for (const httpConn of HTTP_CONNS) {
      closeHttpConn(httpConn);
    }
  };

  addEventListener(shutdownEventName, handleShutdownEvent, { once: true });

  return {
    finished,
    shutdown,
    ref() {
      // TODO: Not currently supported
    },
    unref() {
      // TODO: Not currently supported
    },
  };
}

async function respond(requestEvent, httpConn, options, snapshot) {
  const mapped = async function (requestEvent, httpConn, options, span) {
    /** @type {Response} */
    let response;
    try {
      if (span) {
        updateSpanFromRequest(span, requestEvent.request);
      }

      response = await options["handler"](requestEvent.request, {
        remoteAddr: {
          port: options.port,
          hostname: options.hostname,
          transport: options.transport,
        },
      });
    } catch (error) {
      if (options["onError"] !== void 0) {
        /** @throwable */
        response = await options["onError"](error);
      } else {
        console.error(error);
        response = internalServerError();
      }
    }

    if (span) {
      updateSpanFromServerResponse(span, response);
    }

    if (response === internals.RAW_UPGRADE_RESPONSE_SENTINEL) {
      const { fenceRid } = getSupabaseTag(requestEvent.request);

      if (fenceRid === void 0) {
        throw TypeError("Cannot find a fence for upgrading response");
      }

      setTimeout(async () => {
        const {
          status,
          headers,
        } = await ops.op_http_upgrade_raw2_fence(fenceRid);

        try {
          await requestEvent.respondWith(
            new Response(null, {
              headers,
              status,
            }),
          );
        } catch (error) {
          console.error(error);
          closeHttpConn(httpConn);
        }
      });
    } else {
      try {
        // send the response
        await requestEvent.respondWith(response);
      } catch {
        // respondWith() fails when the connection has already been closed,
        // or there is some other error with responding on this connection
        // that prompts us to close it and open a new connection.
        return closeHttpConn(httpConn);
      }
    }
  };

  if (TRACING_ENABLED) {
    const oldSnapshot = currentSnapshot();
    restoreSnapshot(snapshot);

    const reqHeaders = requestEvent.request.headers;
    const headers = [];
    for (const key of reqHeaders.keys()) {
      ArrayPrototypePush(headers, [key, reqHeaders.get(key)]);
    }
    let activeContext = ContextManager.active();
    for (const propagator of new SafeArrayIterator(PROPAGATORS)) {
      activeContext = propagator.extract(activeContext, headers, {
        get(carrier, key) {
          return ArrayPrototypeFind(
            carrier,
            (carrierEntry) => carrierEntry[0] === key,
          )?.[1];
        },
        keys(carrier) {
          return ArrayPrototypeMap(
            carrier,
            (carrierEntry) => carrierEntry[0],
          );
        },
      });
    }

    const span = builtinTracer().startSpan(
      "deno.serve",
      { kind: 1 },
      activeContext,
    );
    enterSpan(span);
    try {
      return SafePromisePrototypeFinally(
        mapped(
          requestEvent,
          httpConn,
          options,
          span,
        ),
        () => span.end(),
      );
    } finally {
      restoreSnapshot(oldSnapshot);
    }
  } else {
    const oldSnapshot = currentSnapshot();
    restoreSnapshot(snapshot);
    try {
      return mapped(
        requestEvent,
        httpConn,
        options,
        undefined,
      );
    } finally {
      restoreSnapshot(oldSnapshot);
    }
  }
}

function closeHttpConn(httpConn) {
  try {
    httpConn.close();
  } catch {
    // connection has already been closed
  }
}

function getSupabaseTag(request) {
  return request[kSupabaseTag];
}

function applySupabaseTag(src, dest) {
  if (
    !ObjectPrototypeIsPrototypeOf(RequestPrototype, src) ||
    !ObjectPrototypeIsPrototypeOf(RequestPrototype, dest)
  ) {
    throw new TypeError("Only Request instance can apply the supabase tag");
  }

  dest[kSupabaseTag] = src[kSupabaseTag];
}

internals.getSupabaseTag = getSupabaseTag;
internals.RAW_UPGRADE_RESPONSE_SENTINEL = RAW_UPGRADE_RESPONSE_SENTINEL;

// Override upstream Deno's upgradeHttpRaw (from ext:deno_http/00_serve.ts)
// to use trexas's own raw upgrade machinery. The upstream version requires
// the request to come from Deno.serve (it checks InnerRequest._wantsUpgrade),
// but trexas serves requests via op_http_start over a duplex stream; those
// are NOT InnerRequest instances, so upstream upgradeHttpRaw throws
// 'may only be used with Deno.serve'. Without this override, node:http's
// 'upgrade' event is unreachable: ext/node/polyfills/http.ts calls
// upgradeHttpRaw before emitting the event.
//
// IMPORTANT: this override must be installed AFTER 00_serve.ts has loaded
// (00_serve.ts reassigns internals.upgradeHttpRaw to the upstream default
// at module-load time). Module load order between this file and 00_serve.ts
// is non-deterministic across deno_core extensions, so we install the
// override from bootstrap.js; see installTrexasUpgradeHttpRaw().
function trexasUpgradeHttpRaw(request) {
  const tag = getSupabaseTag(request);
  if (tag === undefined || tag.streamRid === undefined) {
    throw new TypeError(
      "upgradeHttpRaw: request was not produced by trexas serve",
    );
  }
  // op_http_upgrade_raw2 returns [upgradeRid, fenceRid].
  //   upgradeRid: duplex stream the user writes the 101 response to and
  //               then exchanges WS frames over.
  //   fenceRid:   oneshot the responder awaits to know when the 101 has
  //               been parsed; once it resolves, the upstream hyper
  //               connection is actually upgraded.
  const [upgradeRid, fenceRid] = ops.op_http_upgrade_raw2(tag.streamRid);
  // Stash fenceRid on the tag so the responder picks it up when it sees
  // RAW_UPGRADE_RESPONSE_SENTINEL.
  tag.fenceRid = fenceRid;
  // Build a Conn-shaped object whose internalRidSymbol points at the
  // post-upgrade duplex. addr fields are dummies; node:http's TCP wrapper
  // does not consult them for SERVER-type upgrades.
  const dummyAddr = { transport: "tcp", hostname: "0.0.0.0", port: 9999 };
  const conn = new UpgradedConn(upgradeRid, dummyAddr, dummyAddr);
  return { conn, response: RAW_UPGRADE_RESPONSE_SENTINEL };
}

function installTrexasUpgradeHttpRaw() {
  internals.upgradeHttpRaw = trexasUpgradeHttpRaw;
}

export { applySupabaseTag, getSupabaseTag, installTrexasUpgradeHttpRaw, serve, serveHttp, upgradeWebSocket };
