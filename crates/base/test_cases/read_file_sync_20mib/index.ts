// NOTE: Just defined to prevent the JsRuntime leave from the event loop
Deno.serve(() => {/* do nothing */});

// Use path relative to project root (matching static_patterns)
let buf_in_ext_mem = Deno.readFileSync("./test_cases/read_file_sync_20mib/20mib.bin") as Uint8Array;
console.log(buf_in_ext_mem.length); // to prevent optimization
