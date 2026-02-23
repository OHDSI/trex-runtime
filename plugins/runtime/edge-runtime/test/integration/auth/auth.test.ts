import { describe, it, expect, beforeAll, afterAll } from "vitest";
import pg from "pg";
import {
  signUp,
  signIn,
  signOut,
  getSession,
  forgotPassword,
  resetPassword,
  updateUser,
  listSessions,
  revokeSession,
  BASE_URL,
} from "./helpers.js";

const DATABASE_URL =
  process.env.DATABASE_URL || "postgres://postgres:mypass@localhost:65432/testdb";

const RUN_ID = Date.now().toString(36);
const email = (label: string) => `test-${label}-${RUN_ID}@e2e.test`;
const PASSWORD = "TestPass12345";

let pool: pg.Pool;

// Shared state across suites
let adminCookies: string[] = [];
let userCookies: string[] = [];
let adminUserId: string;
let regularUserId: string;
let usedResetToken: string;

beforeAll(async () => {
  pool = new pg.Pool({
    connectionString: DATABASE_URL,
    options: "-c search_path=trex,public",
  });
  // Clean up any leftover test users from previous runs
  await pool.query(`DELETE FROM "user" WHERE email LIKE '%@e2e.test'`);
});

afterAll(async () => {
  // Clean up test users (cascades to sessions/accounts via FK)
  await pool.query(`DELETE FROM "user" WHERE email LIKE '%@e2e.test'`);
  await pool.end();
});

// ---------------------------------------------------------------------------
// Sign-up
// ---------------------------------------------------------------------------
describe("sign-up", () => {
  it("registers a new user", async () => {
    const res = await signUp(email("admin"), PASSWORD, "Admin User");
    expect(res.status).toBe(200);
    expect(res.data.user).toBeDefined();
    expect(res.data.user!.email).toBe(email("admin"));
    adminCookies = res.cookies;
    adminUserId = res.data.user!.id;
  });

  it("first user gets admin role", async () => {
    // The DB hook assigns admin to the first user
    const row = await pool.query(
      `SELECT role FROM "user" WHERE id = $1`,
      [adminUserId]
    );
    expect(row.rows[0].role).toBe("admin");
  });

  it("rejects duplicate email", async () => {
    const res = await signUp(email("admin"), PASSWORD, "Dup User");
    expect(res.status).not.toBe(200);
  });

  it("rejects missing required fields", async () => {
    const res = await signUp("", "", "");
    expect(res.status).not.toBe(200);
  });

  it("second user gets default role", async () => {
    const res = await signUp(email("regular"), PASSWORD, "Regular User");
    expect(res.status).toBe(200);
    regularUserId = res.data.user!.id;
    userCookies = res.cookies;

    const row = await pool.query(
      `SELECT role FROM "user" WHERE id = $1`,
      [regularUserId]
    );
    expect(row.rows[0].role).toBe("user");
  });
});

// ---------------------------------------------------------------------------
// Sign-in
// ---------------------------------------------------------------------------
describe("sign-in", () => {
  it("succeeds with valid credentials", async () => {
    const res = await signIn(email("admin"), PASSWORD);
    expect(res.status).toBe(200);
    expect(res.data.user).toBeDefined();
    expect(res.cookies.length).toBeGreaterThan(0);
    // Refresh admin cookies
    adminCookies = res.cookies;
  });

  it("fails with wrong password", async () => {
    const res = await signIn(email("admin"), "WrongPassword99");
    expect(res.status).not.toBe(200);
  });

  it("fails with non-existent email", async () => {
    const res = await signIn("no-such-user@e2e.test", PASSWORD);
    expect(res.status).not.toBe(200);
  });

  it("session cookie authenticates subsequent requests", async () => {
    const session = await getSession(adminCookies);
    expect(session.status).toBe(200);
    expect(session.data?.user).toBeDefined();
    expect(session.data?.user!.email).toBe(email("admin"));
  });
});

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------
describe("session", () => {
  it("returns session for authenticated user", async () => {
    const res = await getSession(adminCookies);
    expect(res.status).toBe(200);
    expect(res.data?.session).toBeDefined();
    expect(res.data?.user).toBeDefined();
  });

  it("returns null/error for unauthenticated request", async () => {
    const res = await getSession([]);
    // Better Auth returns 200 with null data or 401
    expect([200, 401]).toContain(res.status);
    if (res.status === 200) {
      expect(res.data?.user ?? null).toBeNull();
    }
  });

  it("rejects invalid cookie", async () => {
    const res = await getSession(["better-auth.session_token=bogus-token"]);
    expect([200, 401]).toContain(res.status);
    if (res.status === 200) {
      expect(res.data?.user ?? null).toBeNull();
    }
  });

  it("lists active sessions", async () => {
    const res = await listSessions(adminCookies);
    expect(res.status).toBe(200);
    expect(Array.isArray(res.data)).toBe(true);
    expect(res.data.length).toBeGreaterThanOrEqual(1);
  });

  it("revokes a session", async () => {
    // Sign in to create a new session to revoke
    const login = await signIn(email("regular"), PASSWORD);
    expect(login.status).toBe(200);
    const freshCookies = login.cookies;

    // List sessions and find the one we just created
    const sessions = await listSessions(freshCookies);
    expect(sessions.status).toBe(200);
    expect(sessions.data.length).toBeGreaterThanOrEqual(1);

    // Get the token from the session list (DB token, not the cookie)
    const sessionToRevoke = sessions.data[sessions.data.length - 1];
    const token = (sessionToRevoke as any).token;
    expect(token).toBeDefined();

    const res = await revokeSession(token, freshCookies);
    expect(res.status).toBe(200);

    // Verify session count decreased
    // Re-login to get valid cookies for checking
    const relogin = await signIn(email("regular"), PASSWORD);
    const updatedSessions = await listSessions(relogin.cookies);
    expect(updatedSessions.status).toBe(200);
    // The revoked session should no longer be in the list
    const revokedStillPresent = updatedSessions.data.some(
      (s: any) => s.token === token
    );
    expect(revokedStillPresent).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Sign-out
// ---------------------------------------------------------------------------
describe("sign-out", () => {
  it("invalidates the session", async () => {
    // Sign in fresh
    const login = await signIn(email("regular"), PASSWORD);
    expect(login.status).toBe(200);
    const cookies = login.cookies;

    const res = await signOut(cookies);
    expect(res.status).toBe(200);

    // Session should be gone
    const session = await getSession(cookies);
    expect([200, 401]).toContain(session.status);
    if (session.status === 200) {
      expect(session.data?.user ?? null).toBeNull();
    }
  });

  it("is a no-op without a session", async () => {
    const res = await signOut([]);
    // Should not error out
    expect([200, 401, 302]).toContain(res.status);
  });
});

// ---------------------------------------------------------------------------
// Password reset
// ---------------------------------------------------------------------------
describe("password-reset", () => {
  it("accepts forgot-password for existing email", async () => {
    const res = await forgotPassword(email("regular"));
    expect(res.status).toBe(200);
  });

  it("does not error for non-existent email", async () => {
    // Better Auth typically returns 200 to avoid email enumeration
    const res = await forgotPassword("nonexistent@e2e.test");
    expect(res.status).toBe(200);
  });

  it("resets password with valid token", async () => {
    // Trigger forgot-password to create a verification token
    await forgotPassword(email("regular"));

    // Better Auth stores identifier as "reset-password:{urlToken}"
    const result = await pool.query(
      `SELECT identifier FROM verification WHERE identifier LIKE 'reset-password:%' ORDER BY "createdAt" DESC LIMIT 1`
    );
    expect(result.rows.length).toBeGreaterThan(0);
    usedResetToken = result.rows[0].identifier.replace("reset-password:", "");

    const res = await resetPassword("NewPassword456", usedResetToken);
    expect(res.status).toBe(200);

    // Can sign in with new password
    const login = await signIn(email("regular"), "NewPassword456");
    expect(login.status).toBe(200);
    userCookies = login.cookies;
  });

  it("rejects reset with invalid token", async () => {
    const res = await resetPassword("SomePass78901", "invalid-token-value");
    expect(res.status).not.toBe(200);
  });

  it("rejects reuse of spent token", async () => {
    // Reuse the exact token consumed in the previous test
    expect(usedResetToken).toBeDefined();
    const res = await resetPassword("AnotherPass000", usedResetToken);
    // Should fail — token already used or expired
    expect(res.status).not.toBe(200);
  });
});

// ---------------------------------------------------------------------------
// Update user
// ---------------------------------------------------------------------------
describe("update-user", () => {
  it("updates the user name", async () => {
    // Re-login as regular user in case cookies expired
    const login = await signIn(email("regular"), "NewPassword456");
    expect(login.status).toBe(200);
    userCookies = login.cookies;

    const res = await updateUser({ name: "Updated Name" }, userCookies);
    expect(res.status).toBe(200);

    const session = await getSession(userCookies);
    expect(session.status).toBe(200);
    expect(session.data?.user!.name).toBe("Updated Name");
  });

  it("rejects unauthenticated update", async () => {
    const res = await updateUser({ name: "Hacker" }, []);
    expect(res.status).not.toBe(200);
  });
});

// ---------------------------------------------------------------------------
// Auth context (GraphQL + RLS)
// ---------------------------------------------------------------------------
describe.skipIf(!process.env.TEST_POSTGRAPHILE)("auth-context", () => {
  it("GraphQL succeeds with admin auth cookies", async () => {
    // Re-login as admin
    const login = await signIn(email("admin"), PASSWORD);
    expect(login.status).toBe(200);
    adminCookies = login.cookies;

    const res = await fetch(`${BASE_URL}/graphql`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: adminCookies.map((c) => c.split(";")[0]).join("; "),
      },
      body: JSON.stringify({
        query: "{ __schema { queryType { name } } }",
      }),
    });
    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.data.__schema.queryType.name).toBeDefined();
  });

  it("GraphQL query without auth gets empty/restricted results", async () => {
    // Query a table protected by RLS without auth cookies
    const res = await fetch(`${BASE_URL}/graphql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query: `{ allUsers { nodes { id email } } }`,
      }),
    });

    // Could be 200 with empty nodes or an error — depends on PostGraphile config
    const data = await res.json();
    if (data.data?.allUsers) {
      // RLS should filter out all rows when no user context is set
      expect(data.data.allUsers.nodes.length).toBe(0);
    } else {
      // Or it may return an error
      expect(data.errors).toBeDefined();
    }
  });
});
