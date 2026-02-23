import { betterAuth } from "better-auth";
import { admin, jwt, oidcProvider } from "better-auth/plugins";
import { Pool } from "pg";
import { BASE_PATH } from "./config.ts";

const databaseUrl = Deno.env.get("DATABASE_URL");
if (!databaseUrl) {
  throw new Error("DATABASE_URL environment variable is required");
}

const authSecret = Deno.env.get("BETTER_AUTH_SECRET");
if (!authSecret) {
  throw new Error("BETTER_AUTH_SECRET environment variable is required");
}

export const pool = new Pool({
  connectionString: databaseUrl,
  options: "-c search_path=trex,public",
});

function getBaseConfig() {
  return {
    database: pool,
    basePath: `${BASE_PATH}/api/auth`,
    secret: authSecret,
    baseURL: (() => {
      const envUrl = Deno.env.get("BETTER_AUTH_URL");
      if (envUrl) {
        try { const u = new URL(envUrl); return u.origin; } catch { return envUrl; }
      }
      return "http://localhost:8000";
    })(),
    trustedOrigins: (Deno.env.get("BETTER_AUTH_TRUSTED_ORIGINS") || "http://localhost:5173").split(","),

    emailAndPassword: {
      enabled: true,
      requireEmailVerification: false,
      sendResetPassword: async ({ user, url }: { user: any; url: string }) => {
        console.log(`[auth] Password reset requested for ${user.email}`);
      },
    },

    emailVerification: {
      sendVerificationEmail: async ({ user, url }: { user: any; url: string }) => {
        console.log(`[auth] Verification email requested for ${user.email}`);
      },
      sendOnSignUp: true,
    },

    account: {
      accountLinking: {
        enabled: true,
      },
    },

    session: {
      expiresIn: 7 * 24 * 60 * 60,
    },

    user: {
      additionalFields: {
        role: {
          type: "string" as const,
          defaultValue: "user",
        },
        deletedAt: {
          type: "string" as const,
          required: false,
        },
        mustChangePassword: {
          type: "boolean" as const,
          defaultValue: false,
        },
      },
    },

    databaseHooks: {
      user: {
        create: {
          after: async (user: any) => {
            const adminEmail = Deno.env.get("ADMIN_EMAIL");
            const client = await pool.connect();
            try {
              const result = await client.query(
                'SELECT COUNT(*)::int AS count FROM "user"'
              );
              const userCount = result.rows[0].count;
              const isFirstUser = userCount <= 1;
              const matchesAdminEmail =
                adminEmail && user.email === adminEmail;

              if (isFirstUser || matchesAdminEmail) {
                await client.query(
                  'UPDATE "user" SET role = $1 WHERE id = $2',
                  ["admin", user.id]
                );
                console.log(
                  `[auth] Assigned admin role to ${user.email} (${isFirstUser ? "first user" : "ADMIN_EMAIL match"})`
                );
              }
            } finally {
              client.release();
            }
          },
        },
      },
    },

    plugins: [
      admin({
        adminRoles: ["admin"],
        defaultRole: "user",
      }),
      jwt(),
      oidcProvider({
        loginPage: `${BASE_PATH}/login`,
        consentPage: `${BASE_PATH}/consent`,
      }),
    ],
  };
}

function buildEnvFallback(): Record<string, { clientId: string; clientSecret: string }> {
  const providers: Record<string, { clientId: string; clientSecret: string }> = {};

  if (Deno.env.get("GOOGLE_CLIENT_ID")) {
    providers.google = {
      clientId: Deno.env.get("GOOGLE_CLIENT_ID")!,
      clientSecret: Deno.env.get("GOOGLE_CLIENT_SECRET")!,
    };
  }
  if (Deno.env.get("GITHUB_CLIENT_ID")) {
    providers.github = {
      clientId: Deno.env.get("GITHUB_CLIENT_ID")!,
      clientSecret: Deno.env.get("GITHUB_CLIENT_SECRET")!,
    };
  }
  if (Deno.env.get("MICROSOFT_CLIENT_ID")) {
    providers.microsoft = {
      clientId: Deno.env.get("MICROSOFT_CLIENT_ID")!,
      clientSecret: Deno.env.get("MICROSOFT_CLIENT_SECRET")!,
    };
  }

  return providers;
}

async function loadProvidersFromDB(): Promise<Record<string, { clientId: string; clientSecret: string }> | null> {
  const client = await pool.connect();
  try {
    const tableCheck = await client.query(
      `SELECT 1 FROM information_schema.tables WHERE table_schema = 'trex' AND table_name = 'sso_provider' LIMIT 1`
    );
    if (tableCheck.rows.length === 0) {
      return null; // table doesn't exist yet — fall back to env
    }

    const result = await client.query(
      `SELECT id, "clientId", "clientSecret" FROM trex.sso_provider WHERE enabled = true`
    );

    if (result.rows.length === 0) {
      return null; // no DB providers — fall back to env
    }

    const providers: Record<string, { clientId: string; clientSecret: string }> = {};
    for (const row of result.rows) {
      providers[row.id] = {
        clientId: row.clientId,
        clientSecret: row.clientSecret,
      };
    }
    return providers;
  } finally {
    client.release();
  }
}

function createAuthInstance(socialProviders: Record<string, { clientId: string; clientSecret: string }>) {
  return betterAuth({
    ...getBaseConfig(),
    socialProviders,
  });
}

let _authInstance = createAuthInstance(buildEnvFallback());

// Proxy so consumers always get the latest instance
export const auth = {
  get handler() { return _authInstance.handler; },
  get api() { return _authInstance.api; },
};

export async function initAuthFromDB() {
  try {
    const dbProviders = await loadProvidersFromDB();
    const providers = dbProviders ?? buildEnvFallback();
    _authInstance = createAuthInstance(providers);
    const names = Object.keys(providers);
    console.log(`[auth] Initialized SSO providers: ${names.length > 0 ? names.join(", ") : "none"}`);
  } catch (err) {
    console.error("[auth] Failed to load SSO providers from DB, using env fallback:", err);
    _authInstance = createAuthInstance(buildEnvFallback());
  }
}

export async function reloadAuthProviders() {
  try {
    const dbProviders = await loadProvidersFromDB();
    const providers = dbProviders ?? buildEnvFallback();
    _authInstance = createAuthInstance(providers);
    const names = Object.keys(providers);
    console.log(`[auth] Reloaded SSO providers: ${names.length > 0 ? names.join(", ") : "none"}`);
  } catch (err) {
    console.error("[auth] Failed to reload SSO providers:", err);
    throw err;
  }
}
