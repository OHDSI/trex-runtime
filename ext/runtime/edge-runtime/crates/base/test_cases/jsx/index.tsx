// Simple JSX factory that creates Preact-like VNodes
// React.createElement signature: (type, props, ...children)
function createElement(type: string, props: Record<string, unknown> | null, ...children: unknown[]) {
  const finalProps: Record<string, unknown> = props ? { ...props } : {};
  if (children.length === 1) {
    finalProps.children = children[0];
  } else if (children.length > 1) {
    finalProps.children = children;
  }
  return {
    type,
    props: finalProps,
    __k: null,
    __: null,
    __b: 0,
    __e: null,
    __c: null,
    __v: -1,
    __i: -1,
    __u: 0,
  };
}

// @ts-ignore - required for JSX transform
globalThis.React = { createElement };

declare namespace JSX {
  interface IntrinsicElements {
    div: Record<string, unknown>;
  }
}

Deno.serve(async (req: Request) => {
  const hello = <div>Hello</div>;
  return new Response(
    JSON.stringify(hello),
    { status: 200, headers: { "Content-Type": "application/json" } },
  );
});
