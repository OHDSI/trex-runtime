// Extension to provide stub OS functionality for Edge Runtime
// NOTE: op_system_memory_info is now provided by vendor/deno_os
// The JavaScript module provides stub implementations for various OS calls

deno_core::extension!(os, esm_entry_point = "ext:os/os.js", esm = ["os.js"]);
