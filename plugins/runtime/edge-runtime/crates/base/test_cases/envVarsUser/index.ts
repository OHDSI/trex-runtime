// User worker: env vars not passed should return undefined

const testValue = Deno.env.get('TREX_TEST_ENV_VAR');
if (testValue !== undefined) {
  throw new Error(
    `Expected TREX_TEST_ENV_VAR to be undefined (not passed to user worker), got: ${JSON.stringify(testValue)}`
  );
}

console.log('envVarsUser test passed');
