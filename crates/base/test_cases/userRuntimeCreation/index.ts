// Verifies EdgeRuntime with limited APIs (only waitUntil) is available

if (typeof EdgeRuntime === 'undefined') {
  throw new Error('EdgeRuntime is not defined in user runtime');
}

if (typeof EdgeRuntime !== 'object' || EdgeRuntime === null) {
  throw new Error(`Expected EdgeRuntime to be an object, got: ${typeof EdgeRuntime}`);
}

if (typeof EdgeRuntime.waitUntil !== 'function') {
  throw new Error('EdgeRuntime.waitUntil is not a function in user runtime');
}

if (EdgeRuntime.userWorkers !== undefined) {
  throw new Error('EdgeRuntime.userWorkers should not be defined in user runtime');
}

console.log('userRuntimeCreation test passed');
console.log('EdgeRuntime keys:', Object.keys(EdgeRuntime));
