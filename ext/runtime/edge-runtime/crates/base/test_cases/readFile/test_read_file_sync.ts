const content = Deno.readTextFileSync("./test_cases/readFile/hello_world.json");
const expected = '{\n  "hello": "world"\n}\n';

if (content !== expected) {
  throw new Error(`File content mismatch. Expected: ${JSON.stringify(expected)}, Got: ${JSON.stringify(content)}`);
}

console.log("readTextFileSync test passed");
