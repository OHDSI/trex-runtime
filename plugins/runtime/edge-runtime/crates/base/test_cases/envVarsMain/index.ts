// Main runtime: GET env vars allowed, SET blocked

let setBlocked = false;
try {
  Deno.env.set('Test_Env_Set', 'test_value');
} catch (e) {
  if (e.message.includes('NotSupported') || e.message.includes('not supported')) {
    setBlocked = true;
  } else {
    throw new Error(`Unexpected error when setting env: ${e.message}`);
  }
}

if (!setBlocked) {
  throw new Error('Expected Deno.env.set to be blocked');
}

const testValue = Deno.env.get('TREX_TEST_ENV_VAR');
if (testValue !== 'test_value_123') {
  throw new Error(
    `Expected TREX_TEST_ENV_VAR to be "test_value_123", got: ${JSON.stringify(testValue)}`
  );
}

console.log('envVarsMain test passed');
