// Verifies jsxImportSource config transforms JSX to Preact VNodes

const hello = <div>Hello</div>;

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
