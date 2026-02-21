// Verifies EdgeRuntime global with userWorkers API is available

if (typeof EdgeRuntime === 'undefined') {
  throw new Error('EdgeRuntime is not defined in main runtime');
}

if (typeof EdgeRuntime !== 'object' || EdgeRuntime === null) {
  throw new Error(`Expected EdgeRuntime to be an object, got: ${typeof EdgeRuntime}`);
}

if (!EdgeRuntime.userWorkers) {
  throw new Error('EdgeRuntime.userWorkers is not defined in main runtime');
}

console.log('mainRuntimeCreation test passed');
console.log('EdgeRuntime keys:', Object.keys(EdgeRuntime));
