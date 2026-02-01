// Test that environment variables work correctly in user worker runtime
// User worker without env vars passed should return null for env.get

// Test that env.get returns null for undefined env vars
const testValue = Deno.env.get('TREX_TEST_ENV_VAR');
if (testValue !== undefined) {
  throw new Error(
    `Expected TREX_TEST_ENV_VAR to be undefined (not passed to user worker), got: ${JSON.stringify(testValue)}`
  );
}

console.log('envVarsUser test passed');
