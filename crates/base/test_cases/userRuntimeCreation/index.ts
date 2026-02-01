// Test that EdgeRuntime is available in user runtime global scope
// User runtime should have limited EdgeRuntime APIs (only waitUntil)

if (typeof EdgeRuntime === 'undefined') {
  throw new Error('EdgeRuntime is not defined in user runtime');
}

// Verify EdgeRuntime is an object
if (typeof EdgeRuntime !== 'object' || EdgeRuntime === null) {
  throw new Error(`Expected EdgeRuntime to be an object, got: ${typeof EdgeRuntime}`);
}

// User runtime should have waitUntil
if (typeof EdgeRuntime.waitUntil !== 'function') {
  throw new Error('EdgeRuntime.waitUntil is not a function in user runtime');
}

// User runtime should NOT have userWorkers (that's main-only)
if (EdgeRuntime.userWorkers !== undefined) {
  throw new Error('EdgeRuntime.userWorkers should not be defined in user runtime');
}

console.log('userRuntimeCreation test passed');
console.log('EdgeRuntime keys:', Object.keys(EdgeRuntime));
