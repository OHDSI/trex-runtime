// Requires static pattern "./test_cases/**/*.md"

const content = Deno.readTextFileSync('./test_cases/main/content.md');
const expected = 'Some test file\n';

if (content !== expected) {
  throw new Error(
    `Static file content mismatch. Expected: ${JSON.stringify(expected)}, Got: ${JSON.stringify(content)}`
  );
}

console.log('staticFs test passed');
