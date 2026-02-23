Deno.serve(() => new Response(JSON.stringify({ message: "Hello, World!" }), {
  headers: { "Content-Type": "application/json" },
}));
