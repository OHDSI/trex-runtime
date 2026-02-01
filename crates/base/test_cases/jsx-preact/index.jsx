// Test that JSX import source works correctly with Preact
// This verifies the deno.jsonc "jsxImportSource": "https://esm.sh/preact" configuration

const hello = <div>Hello</div>;

// Verify the JSX was transformed into a Preact VNode
if (typeof hello !== 'object' || hello === null) {
  throw new Error(`Expected JSX to produce an object, got: ${typeof hello}`);
}

if (hello.type !== 'div') {
  throw new Error(`Expected type "div", got: ${JSON.stringify(hello.type)}`);
}

if (!hello.props || hello.props.children !== 'Hello') {
  throw new Error(`Expected props.children "Hello", got: ${JSON.stringify(hello.props)}`);
}

console.log('jsx-preact test passed');
console.log('VNode:', JSON.stringify(hello));
