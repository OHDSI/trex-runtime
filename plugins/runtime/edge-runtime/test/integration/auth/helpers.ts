const BASE_URL = process.env.TEST_SERVER_URL || "http://localhost:9000";
const AUTH_BASE = `${BASE_URL}/api/auth`;

export interface AuthResponse<T = unknown> {
  status: number;
  data: T;
  cookies: string[];
}

function extractCookies(headers: Headers): string[] {
  return headers.getSetCookie();
}

function cookieHeader(cookies: string[]): string {
  return cookies
    .map((c) => c.split(";")[0])
    .join("; ");
}

async function authFetch<T = unknown>(
  path: string,
  options: {
    method?: string;
    body?: Record<string, unknown>;
    cookies?: string[];
  } = {}
): Promise<AuthResponse<T>> {
  const { method = "POST", body, cookies } = options;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Origin: BASE_URL,
  };
  if (cookies?.length) {
    headers["Cookie"] = cookieHeader(cookies);
  }

  const res = await fetch(`${AUTH_BASE}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
    redirect: "manual",
  });

  let data: T;
  const text = await res.text();
  try {
    data = JSON.parse(text);
  } catch {
    data = text as unknown as T;
  }

  return {
    status: res.status,
    data,
    cookies: extractCookies(res.headers),
  };
}

export async function signUp(email: string, password: string, name: string) {
  return authFetch<{ user?: { id: string; email: string; role: string }; token?: string }>(
    "/sign-up/email",
    { body: { email, password, name } }
  );
}

export async function signIn(email: string, password: string) {
  return authFetch<{ user?: { id: string; email: string; role: string }; token?: string }>(
    "/sign-in/email",
    { body: { email, password } }
  );
}

export async function signOut(cookies: string[]) {
  return authFetch("/sign-out", { cookies });
}

export async function getSession(cookies: string[]) {
  return authFetch<{ user?: { id: string; email: string; role: string }; session?: { id: string } }>(
    "/get-session",
    { method: "GET", cookies }
  );
}

export async function forgotPassword(email: string) {
  return authFetch("/request-password-reset", { body: { email, redirectTo: "/reset-password" } });
}

export async function resetPassword(newPassword: string, token: string) {
  return authFetch("/reset-password", { body: { newPassword, token } });
}

export async function updateUser(
  fields: Record<string, unknown>,
  cookies: string[]
) {
  return authFetch("/update-user", { body: fields, cookies });
}

export async function listSessions(cookies: string[]) {
  return authFetch<{ id: string; userId: string }[]>("/list-sessions", {
    method: "GET",
    cookies,
  });
}

export async function revokeSession(sessionToken: string, cookies: string[]) {
  return authFetch("/revoke-session", { body: { token: sessionToken }, cookies });
}

export { BASE_URL };
