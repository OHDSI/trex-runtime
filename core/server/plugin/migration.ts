declare const Trex: any;

export interface MigrationPluginInfo {
  pluginName: string;
  schema: string;
  database: string;
  migrationsPath: string;
}

const migrationRegistry: Map<string, MigrationPluginInfo> = new Map();

export function addMigrationPlugin(
  value: any,
  dir: string,
  shortName: string
) {
  const schema = value?.schema;
  if (!schema || typeof schema !== "string") {
    console.warn(
      `Plugin ${shortName} has invalid migration config (missing schema) — skipping`
    );
    return;
  }

  const database = value?.database || "_config";
  const migrationsPath = `${dir}/migrations`;

  migrationRegistry.set(shortName, {
    pluginName: shortName,
    schema,
    database,
    migrationsPath,
  });
  console.log(
    `Registered migration plugin ${shortName} (schema: ${schema}, database: ${database})`
  );
}

export function getMigrationPlugins(): MigrationPluginInfo[] {
  return Array.from(migrationRegistry.values());
}

import { escapeSql } from "../lib/sql.ts";

export async function runAllPluginMigrations(): Promise<void> {
  if (migrationRegistry.size === 0) {
    return;
  }

  console.log(
    `Running migrations for ${migrationRegistry.size} plugin(s)...`
  );

  const conn = new Trex.TrexDB("memory");

  for (const [name, info] of migrationRegistry) {
    try {
      const sql = `SELECT * FROM trex_migration_run_schema('${escapeSql(info.migrationsPath)}', '${escapeSql(info.schema)}', '${escapeSql(info.database)}')`;
      await conn.execute(sql, []);
      console.log(`Plugin ${name}: migrations applied successfully`);
    } catch (err) {
      console.error(`Plugin ${name}: migration failed:`, err);
    }
  }
}
