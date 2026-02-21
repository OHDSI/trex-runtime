// Example demonstrating unstable_sloppy_imports usage
// This worker imports a module without the .ts extension

import { helper } from './worker/utils';  // No .ts extension needed with sloppy imports!

console.log('Sloppy imports worker started');

Deno.serve((_req) => {
  return new Response(`Hello! Helper says: ${helper()}`);
});
