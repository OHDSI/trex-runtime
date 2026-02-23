const eventManager = new globalThis.EventManager();

console.log("event manager running");

interface LogEntry {
  event_type: string;
  level: string;
  message: string;
}

const buffer: LogEntry[] = [];
const FLUSH_INTERVAL = 2000;
const FLUSH_SIZE = 100;

let pool: any = null;
let lastFlushErrorAt = 0;

try {
  const databaseUrl = Deno.env.get("DATABASE_URL");
  if (databaseUrl) {
    const pg = (await import("pg")).default;
    pool = new pg.Pool({ connectionString: databaseUrl, max: 2 });
  }
} catch (err) {
  console.error("Failed to initialize pg pool for event logging:", err);
}

async function flush() {
  if (buffer.length === 0 || !pool) return;
  const entries = buffer.splice(0, buffer.length);
  try {
    const values: any[] = [];
    const placeholders: string[] = [];
    for (let i = 0; i < entries.length; i++) {
      const offset = i * 3;
      placeholders.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`);
      values.push(entries[i].event_type, entries[i].level, entries[i].message);
    }
    await pool.query(
      `INSERT INTO trex.event_log (event_type, level, message) VALUES ${placeholders.join(", ")}`,
      values,
    );
  } catch (err) {
    const now = Date.now();
    if (now - lastFlushErrorAt > 30000) {
      console.error("Failed to flush event logs to database:", err);
      lastFlushErrorAt = now;
    }
  }
}

const flushTimer = setInterval(flush, FLUSH_INTERVAL);

for await (const data of eventManager) {
  if (data) {
    switch (data.event_type) {
      case "Log":
        if (data.event.level === "Error") {
          console.error(data.event.msg);
        } else {
          console.dir(data.event.msg, { depth: Infinity });
        }
        buffer.push({
          event_type: data.event_type,
          level: data.event.level || "Info",
          message: typeof data.event.msg === "string" ? data.event.msg : JSON.stringify(data.event.msg),
        });
        break;
      default:
        console.dir(data, { depth: Infinity });
        buffer.push({
          event_type: data.event_type || "Unknown",
          level: "Info",
          message: typeof data === "string" ? data : JSON.stringify(data),
        });
    }
    if (buffer.length >= FLUSH_SIZE) {
      await flush();
    }
  }
}

clearInterval(flushTimer);
await flush();

if (pool) {
  try {
    await pool.end();
  } catch {}
}

console.log("event manager exiting");
