import { assertEquals } from "jsr:@std/assert";

export default {
  async fetch(req: Request) {
    const port = parseInt(req.headers.get("x-port") ?? "");
    if (isNaN(port)) {
      return new Response(null, {
        status: 500,
      });
    }

    const caCerts = [];

    if (req.method === "POST") {
      const arr = await req.arrayBuffer();
      const dec = new TextDecoder();
      const ca = dec.decode(arr);
      caCerts.push(ca);
    }

    const client = Deno.createHttpClient({
      caCerts,
    });

    try {
      const resp = await fetch(`https://localhost:${port}`, { client });
      assertEquals(resp.status, 200);
      return new Response(await resp.text());
    } catch (ex) {
      if (ex instanceof TypeError) {
        // deno 2.9.5 no longer folds the transport/TLS detail into the
        // TypeError's own message; fetch now throws
        // `new TypeError("fetch failed", { cause: new Error(detail) })`
        // (deno_fetch 26_fetch.js). Serialise the cause alongside the error so
        // the integration test can still assert on the TLS-level reason.
        const cause = ex.cause instanceof Error
          ? ex.cause.message
          : String(ex.cause ?? "");

        return new Response(`${ex.toString()}\ncause: ${cause}`, {
          status: 500,
        });
      }

      return new Response(null, {
        status: 500,
      });
    }
  },
};
