// Verifies Deno OS APIs work and subprocess spawning is blocked

const gid = Deno.gid();
const uid = Deno.uid();
const hostname = Deno.hostname();
const loadavg = Deno.loadavg();
const osUptime = Deno.osUptime();
const osRelease = Deno.osRelease();
const systemMemoryInfo = Deno.systemMemoryInfo();
const consoleSize = Deno.consoleSize();
const networkInterfaces = Deno.networkInterfaces();

if (typeof gid !== 'number') {
  throw new Error(`Expected gid to be a number, got: ${typeof gid}`);
}
if (typeof uid !== 'number') {
  throw new Error(`Expected uid to be a number, got: ${typeof uid}`);
}

if (typeof osUptime !== 'number' || osUptime < 0) {
  throw new Error(`Expected osUptime to be a non-negative number, got: ${osUptime}`);
}

if (typeof osRelease !== 'string') {
  throw new Error(`Expected osRelease to be a string, got: ${typeof osRelease}`);
}

if (!Array.isArray(loadavg) || loadavg.length !== 3) {
  throw new Error(`Expected loadavg to be array of 3, got: ${JSON.stringify(loadavg)}`);
}

const memKeys = ['total', 'free', 'available', 'buffers', 'cached', 'swapTotal', 'swapFree'];
for (const key of memKeys) {
  if (!(key in systemMemoryInfo)) {
    throw new Error(`Expected systemMemoryInfo to have key "${key}"`);
  }
}

if (!('rows' in consoleSize) || !('columns' in consoleSize)) {
  throw new Error(`Expected consoleSize to have rows and columns, got: ${JSON.stringify(consoleSize)}`);
}

if (!Array.isArray(networkInterfaces)) {
  throw new Error(`Expected networkInterfaces to be an array, got: ${typeof networkInterfaces}`);
}

if (!Deno.version.deno || !Deno.version.v8 || !Deno.version.typescript) {
  throw new Error(`Expected Deno.version to have deno, v8, typescript, got: ${JSON.stringify(Deno.version)}`);
}

let commandBlocked = false;
try {
  const cmd = new Deno.Command('', {});
  cmd.outputSync();
} catch (e) {
  if (e.message.includes('Spawning subprocesses is not allowed')) {
    commandBlocked = true;
  } else {
    throw new Error(`Expected subprocess error message, got: ${e.message}`);
  }
}

if (!commandBlocked) {
  throw new Error('Expected Deno.Command to be blocked');
}

console.log('osOps test passed');
