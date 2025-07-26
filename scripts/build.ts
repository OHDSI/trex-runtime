
  
const function_path2 = `${Deno.cwd()}`
console.log(function_path2)

// Check for required Docker tag environment variable
const dockerTag = Deno.env.get("TREX_DOCKER_TAG");
if (!dockerTag) {
    console.error("Error: TREX_DOCKER_TAG environment variable is required");
    Deno.exit(1);
}

async function build(fn) {
    return await Promise.all(fn.map(async f => { 
        if(f.function) {
        const path = `${function_path2}${f.function}/index.ts`
        const out = `${function_path2}${f.function}/index.eszip`
        f.eszip = `${f.function}/index.eszip`
        var _args = ["bundle", "--entrypoint", path, "--output", out, "--decorator", "typescript_with_metadata"]
        if (f.imports) {
            const f_imports = `file://${function_path2}${f.imports}`;
            _args = _args.concat(["--import-map", `${function_path2}${f.imports}`])
            f.imports = f_imports;
        }
        // Construct Docker command arguments
        const dockerArgs = [
            "run", "--rm",
            "-v", `${Deno.cwd()}:${Deno.cwd()}`,
            "-w", Deno.cwd(),
            `ghcr.io/data2evidence/d2e-trex-base@${dockerTag}`,
            "/usr/src/trex",
            ..._args
        ];
        let cmd = new Deno.Command("docker", { args: dockerArgs });
        let { code, stdout, stderr } = await cmd.output();
        console.log("docker " + dockerArgs.join(" "))

        if(code != 0) {
            console.log(f);
            console.log(_args)
            console.log(new TextDecoder().decode(stdout));
            console.error(new TextDecoder().decode(stderr));
            Deno.exit(-1);
        }
        
        
    }
    return f}))
}

const pkg = JSON.parse(Deno.readTextFileSync(`${function_path2}/package.org.json`));
const x = await build(pkg.trex.functions.api);
const y = await build(pkg.trex.functions.init);
pkg.trex.functions.api = x
pkg.trex.functions.init = y
Deno.writeTextFileSync(`${function_path2}/package.json`, JSON.stringify(pkg, null, 2));
