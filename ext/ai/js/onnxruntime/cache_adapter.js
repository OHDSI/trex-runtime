import { primordials } from "ext:core/mod.js";
import * as webidl from "ext:deno_webidl/00_webidl.js";
import * as DenoCaches from "ext:deno_cache/01_cache.js";

const ALLOWED_CACHE_NAMES = ["transformers-cache"];
const {
  ObjectPrototypeIsPrototypeOf,
} = primordials;

async function open(cacheName, next) {
  if (!ALLOWED_CACHE_NAMES.includes(cacheName)) {
    return await next(cacheName);
  }

  const cache = webidl.createBranded(DenoCaches.Cache);
  cache[Symbol("id")] = "ai_cache";

  const _cacheMatch = CachePrototype.match;

  cache.match = async function (args) {
    return await match(
      args,
      (interceptedArgs) => _cacheMatch.call(cache, interceptedArgs),
    );
  };

  return cache;
}

async function match(req, _next) {
  const requestUrl = ObjectPrototypeIsPrototypeOf(Request.prototype, req)
    ? req.url()
    : req;
  if (!URL.canParse(requestUrl)) {
    return undefined;
  }

  if (!requestUrl.includes("onnx")) {
    // NOTE(kallebysantos): Same ... from previous method, it should call `next()` in order to
    // continue the middleware flow.
    return undefined;
  }

  return new Response(requestUrl, { status: 200 });
}

const CacheStoragePrototype = DenoCaches.CacheStorage.prototype;
const CachePrototype = DenoCaches.Cache.prototype;

// TODO(kallebysantos): Refactor to an `applyInterceptor` function.
const _cacheStoragOpen = CacheStoragePrototype.open;
CacheStoragePrototype.open = async function (args) {
  return await open(
    args,
    (interceptedArgs) => _cacheStoragOpen.call(this, interceptedArgs),
  );
};

