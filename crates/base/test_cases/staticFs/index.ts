// Test that static file patterns work correctly with Deno.readTextFileSync
// This test requires the static pattern "./test_cases/**/*.md" to be set

// Read the content.md file from the main test_cases directory
const content = Deno.readTextFileSync('./test_cases/main/content.md');
const expected = 'Some test file\n';

if (content !== expected) {
  throw new Error(
    `Static file content mismatch. Expected: ${JSON.stringify(expected)}, Got: ${JSON.stringify(content)}`
  );
}

console.log('staticFs test passed');
