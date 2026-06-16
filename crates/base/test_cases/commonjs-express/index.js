const express = require("express");
const app = express();
const port = 8080;

console.log(require);

app.get("/commonjs-express", (_, res) => {
  res.send("meow");
});

// Regression guard: an embedder server like express re-parents the request via
// `Object.setPrototypeOf(req, app.request)`. After the Deno 2.7.14 node:http
// split that dropped the server IncomingMessage's header getters, leaving
// `req.headers` empty for the trex edge-runtime worker-dispatch path (all
// request headers, incl. authorization, were lost). Echo the header back so the
// integration test can assert it survives.
app.get("/commonjs-express/echo-auth", (req, res) => {
  res.send(req.headers.authorization ?? "MISSING");
});

app.listen(port, () => {
  console.log(`app listening on port ${port}`);
});
