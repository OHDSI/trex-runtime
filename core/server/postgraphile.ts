import { postgraphile } from "postgraphile";
import { PostGraphileAmberPreset } from "postgraphile/presets/amber";
import { PgV4SimpleSubscriptionsPlugin } from "postgraphile/presets/v4";
import { makePgService } from "postgraphile/adaptors/pg";
import { PostGraphileConnectionFilterPreset } from "postgraphile-plugin-connection-filter";
import { BASE_PATH } from "./config.ts";
import { pluginOperationsPlugin } from "./graphql/plugin-operations.ts";

export function createPostGraphile(databaseUrl: string, schemas: string[]) {
  return postgraphile({
    extends: [PostGraphileAmberPreset, PostGraphileConnectionFilterPreset],
    plugins: [pluginOperationsPlugin, PgV4SimpleSubscriptionsPlugin],
    pgServices: [
      makePgService({
        connectionString: databaseUrl,
        schemas,
      }),
    ],
    grafserv: {
      graphqlPath: `${BASE_PATH}/graphql`,
      graphiqlPath: `${BASE_PATH}/graphiql`,
      graphiql: true,
    },
    grafast: {
      context(ctx: any) {
        const req = ctx.expressv4?.req || ctx.req || ctx.request;
        return {
          pgSettings: req?.pgSettings || {},
        };
      },
    },
  });
}
