// https://github.com/seanmonstar/deno/blob/main/tests/unit_node/http2_test.ts#L115
// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

import * as http2 from "node:http2";
import { assert, assertEquals } from "jsr:@std/assert";

const sites = [
  "https://www.example.com",
  "https://www.google.com",
  "https://httpbin.org",
];

function tryHttp2(url: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const clientSession = http2.connect(url, {
      rejectUnauthorized: false,
    });
    clientSession.on("error", (err: Error) => {
      clientSession.close();
      reject(err);
    });
    const req = clientSession.request({
      ":method": "GET",
      ":path": "/",
    });
    let headers = {};
    let status: number | undefined = 0;
    let chunk = new Uint8Array();
    req.on("response", (h) => {
      status = h[":status"];
      headers = h;
    });
    req.on("data", (c) => {
      chunk = c;
    });
    req.on("error", (err: Error) => {
      clientSession.close();
      req.close();
      reject(err);
    });
    req.on("end", () => {
      clientSession.close();
      req.close();
      try {
        assert(Object.keys(headers).length > 0);
        assertEquals(status, 200);
        assert(chunk.length > 0);
        resolve();
      } catch (e) {
        reject(e);
      }
    });
    req.end();
  });
}

let lastErr: Error | undefined;
for (const site of sites) {
  try {
    await tryHttp2(site);
    lastErr = undefined;
    break;
  } catch (e) {
    lastErr = e as Error;
  }
}
if (lastErr) {
  throw lastErr;
}

export default {
  fetch() {
    return new Response(null);
  },
};
