// Test that EdgeRuntime is available in main runtime global scope

if (typeof EdgeRuntime === 'undefined') {
  throw new Error('EdgeRuntime is not defined in main runtime');
}

// Verify EdgeRuntime is an object
if (typeof EdgeRuntime !== 'object' || EdgeRuntime === null) {
  throw new Error(`Expected EdgeRuntime to be an object, got: ${typeof EdgeRuntime}`);
}

// Main runtime should have userWorkers API
if (!EdgeRuntime.userWorkers) {
  throw new Error('EdgeRuntime.userWorkers is not defined in main runtime');
}

console.log('mainRuntimeCreation test passed');
console.log('EdgeRuntime keys:', Object.keys(EdgeRuntime));
