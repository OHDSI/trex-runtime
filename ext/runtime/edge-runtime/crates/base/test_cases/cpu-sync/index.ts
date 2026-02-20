// For CPU time regulation testing only (top-level synchronous mode)

function mySlowFunction(baseNumber) {
  const iterations = Math.pow(baseNumber, 7);
  let result = 0;
  for (var i = iterations; i >= 0; i--) {
    result += Math.atan(i) * Math.tan(i);
  }
  return result;
}

mySlowFunction(19);

Deno.serve((_req) => new Response("meow"));
