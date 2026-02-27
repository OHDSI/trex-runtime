import { useState, useEffect, useCallback } from "react";
import { toast } from "sonner";
import { authClient, useSession } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  MonitorIcon,
  SmartphoneIcon,
  LinkIcon,
  UnlinkIcon,
  ShieldIcon,
  Loader2Icon,
  KeyIcon,
  CopyIcon,
  CheckIcon,
} from "lucide-react";
import { BASE_PATH } from "@/lib/config";

interface SessionInfo {
  id: string;
  token: string;
  ipAddress: string | null;
  userAgent: string | null;
  createdAt: string;
  expiresAt: string;
}

interface AccountInfo {
  id: string;
  providerId: string;
  accountId: string;
  createdAt: string;
}

// Known social providers the system supports.
// Extend this list as new providers are configured in Better Auth.
const KNOWN_PROVIDERS = ["github", "google", "microsoft", "apple"] as const;

function parseUserAgent(ua: string | null): {
  browser: string;
  device: string;
} {
  if (!ua) return { browser: "Unknown", device: "Unknown" };

  let browser = "Unknown";
  if (ua.includes("Edg/") || ua.includes("Edge/")) browser = "Edge";
  else if (ua.includes("Chrome/")) browser = "Chrome";
  else if (ua.includes("Firefox/")) browser = "Firefox";
  else if (ua.includes("Safari/") && !ua.includes("Chrome/"))
    browser = "Safari";

  let device = "Desktop";
  if (/Mobile|Android|iPhone|iPad/i.test(ua)) device = "Mobile";

  return { browser, device };
}

function providerDisplayName(provider: string): string {
  const names: Record<string, string> = {
    github: "GitHub",
    google: "Google",
    microsoft: "Microsoft",
    apple: "Apple",
    credential: "Email & Password",
  };
  return names[provider] || provider.charAt(0).toUpperCase() + provider.slice(1);
}

function ProfileTab() {
  const { data: session, isPending } = useSession();
  const [name, setName] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (session?.user?.name) {
      setName(session.user.name);
    }
  }, [session]);

  async function handleSave() {
    setSaving(true);
    try {
      const result = await authClient.updateUser({ name });
      if (result.error) {
        toast.error(result.error.message || "Failed to update profile.");
        return;
      }
      toast.success("Profile updated.");
    } catch {
      toast.error("An unexpected error occurred.");
    } finally {
      setSaving(false);
    }
  }

  if (isPending) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!session) return null;

  const initials =
    session.user.name
      ?.split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase() || "?";

  return (
    <Card>
      <CardHeader>
        <CardTitle>Profile Information</CardTitle>
        <CardDescription>Update your display name and avatar</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-6">
          <div className="flex items-center gap-4">
            <Avatar className="h-16 w-16" size="lg">
              <AvatarImage src={session.user.image || undefined} />
              <AvatarFallback className="text-lg">{initials}</AvatarFallback>
            </Avatar>
            <div>
              <p className="font-medium">{session.user.name}</p>
              <p className="text-sm text-muted-foreground">
                {session.user.email}
              </p>
            </div>
          </div>

          <Separator />

          <div className="flex flex-col gap-4 max-w-md">
            <div className="flex flex-col gap-2">
              <Label htmlFor="profile-name">Display Name</Label>
              <Input
                id="profile-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Your name"
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="profile-email">Email</Label>
              <Input
                id="profile-email"
                value={session.user.email}
                disabled
                readOnly
              />
              <p className="text-xs text-muted-foreground">
                Email cannot be changed
              </p>
            </div>

            <Button
              onClick={handleSave}
              disabled={saving || name === session.user.name}
              className="w-fit"
            >
              {saving ? "Saving..." : "Save Changes"}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function SecurityTab() {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleChangePassword(e: React.FormEvent) {
    e.preventDefault();
    setError("");

    if (newPassword !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    setLoading(true);
    try {
      const result = await authClient.changePassword({
        currentPassword,
        newPassword,
        revokeOtherSessions: false,
      });

      if (result.error) {
        setError(result.error.message || "Failed to change password.");
        return;
      }

      toast.success("Password changed successfully.");
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
    } catch {
      setError("An unexpected error occurred.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Change Password</CardTitle>
        <CardDescription>Update your account password</CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleChangePassword} className="flex flex-col gap-4 max-w-md">
          <div className="flex flex-col gap-2">
            <Label htmlFor="current-password">Current Password</Label>
            <Input
              id="current-password"
              type="password"
              value={currentPassword}
              onChange={(e) => setCurrentPassword(e.target.value)}
              required
              autoComplete="current-password"
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="new-password">New Password</Label>
            <Input
              id="new-password"
              type="password"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              required
              minLength={8}
              autoComplete="new-password"
            />
          </div>
          <div className="flex flex-col gap-2">
            <Label htmlFor="confirm-password">Confirm New Password</Label>
            <Input
              id="confirm-password"
              type="password"
              value={confirmPassword}
              onChange={(e) => setConfirmPassword(e.target.value)}
              required
              minLength={8}
              autoComplete="new-password"
            />
          </div>

          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}

          <Button type="submit" className="w-fit" disabled={loading}>
            {loading ? "Changing..." : "Change Password"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

// --- Linked Accounts Tab (T045 + T047 lockout protection) ---

function LinkedAccountsTab() {
  const [accounts, setAccounts] = useState<AccountInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [unlinking, setUnlinking] = useState<string | null>(null);
  const [linking, setLinking] = useState<string | null>(null);

  const fetchAccounts = useCallback(async () => {
    setLoading(true);
    try {
      const result = await authClient.listAccounts();
      if (result.data) {
        setAccounts(result.data as unknown as AccountInfo[]);
      }
    } catch {
      toast.error("Failed to load linked accounts.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAccounts();
  }, [fetchAccounts]);

  // T047: Check if user has at least one other auth method before allowing unlink.
  // The user must retain at least one linked account (social or credential).
  const canUnlink = accounts.length > 1;

  async function handleUnlink(providerId: string) {
    setUnlinking(providerId);
    try {
      const result = await authClient.unlinkAccount({ providerId });
      if (result.error) {
        toast.error(
          result.error.message || "Failed to unlink account."
        );
        return;
      }
      toast.success(`${providerDisplayName(providerId)} account unlinked.`);
      fetchAccounts();
    } catch {
      toast.error("Failed to unlink account.");
    } finally {
      setUnlinking(null);
    }
  }

  async function handleLink(provider: string) {
    setLinking(provider);
    try {
      await authClient.signIn.social({
        provider: provider as "github" | "google" | "microsoft" | "apple",
        callbackURL: window.location.href,
      });
    } catch {
      toast.error(`Failed to link ${providerDisplayName(provider)} account.`);
      setLinking(null);
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const linkedProviders = accounts.map((a) => a.providerId);
  const unlinkedProviders = KNOWN_PROVIDERS.filter(
    (p) => !linkedProviders.includes(p)
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle>Linked Accounts</CardTitle>
        <CardDescription>
          Manage your connected authentication providers
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-4">
          {/* Linked accounts */}
          {accounts.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No linked accounts found.
            </p>
          ) : (
            accounts.map((account) => (
              <div
                key={account.id}
                className="flex items-center justify-between rounded-md border p-4"
              >
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                    <LinkIcon className="size-4" />
                  </div>
                  <div>
                    <p className="text-sm font-medium">
                      {providerDisplayName(account.providerId)}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Linked{" "}
                      {account.createdAt
                        ? new Date(account.createdAt).toLocaleDateString()
                        : ""}
                    </p>
                  </div>
                </div>
                {account.providerId !== "credential" && (
                  <div className="relative group">
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={!canUnlink || unlinking === account.providerId}
                      onClick={() => handleUnlink(account.providerId)}
                    >
                      <UnlinkIcon className="size-4 mr-1" />
                      {unlinking === account.providerId
                        ? "Unlinking..."
                        : "Unlink"}
                    </Button>
                    {!canUnlink && (
                      <div className="absolute bottom-full right-0 mb-2 hidden group-hover:block w-56 rounded-md bg-popover border p-2 shadow-md text-xs text-muted-foreground z-50">
                        Cannot unlink your only authentication method. Link
                        another provider or set a password first.
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))
          )}

          {/* Unlinked providers */}
          {unlinkedProviders.length > 0 && (
            <>
              <Separator />
              <div>
                <p className="text-sm font-medium mb-3">
                  Available Providers
                </p>
                <div className="flex flex-wrap gap-2">
                  {unlinkedProviders.map((provider) => (
                    <Button
                      key={provider}
                      variant="outline"
                      size="sm"
                      disabled={linking === provider}
                      onClick={() => handleLink(provider)}
                    >
                      <LinkIcon className="size-4 mr-1" />
                      {linking === provider
                        ? "Linking..."
                        : `Link ${providerDisplayName(provider)}`}
                    </Button>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// --- Sessions Tab (T046) ---

function SessionsTab() {
  const { data: currentSession } = useSession();
  const [sessions, setSessions] = useState<SessionInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [revoking, setRevoking] = useState<string | null>(null);

  const fetchSessions = useCallback(async () => {
    setLoading(true);
    try {
      const result = await authClient.listSessions();
      if (result.data) {
        setSessions(result.data as unknown as SessionInfo[]);
      }
    } catch {
      toast.error("Failed to load sessions.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchSessions();
  }, [fetchSessions]);

  async function handleRevoke(sessionToken: string) {
    setRevoking(sessionToken);
    try {
      const result = await authClient.revokeSession({ token: sessionToken });
      if (result.error) {
        toast.error(result.error.message || "Failed to revoke session.");
        return;
      }
      toast.success("Session revoked.");
      fetchSessions();
    } catch {
      toast.error("Failed to revoke session.");
    } finally {
      setRevoking(null);
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const currentToken = currentSession?.session?.token;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Active Sessions</CardTitle>
        <CardDescription>
          Manage your active sessions across devices ({sessions.length} total)
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-col gap-3">
          {sessions.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No active sessions found.
            </p>
          ) : (
            sessions.map((session) => {
              const { browser, device } = parseUserAgent(session.userAgent);
              const isCurrent = session.token === currentToken;
              const DeviceIcon =
                device === "Mobile" ? SmartphoneIcon : MonitorIcon;

              return (
                <div
                  key={session.id}
                  className="flex items-center justify-between rounded-md border p-4"
                >
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                      <DeviceIcon className="size-4" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <p className="text-sm font-medium">
                          {browser} on {device}
                        </p>
                        {isCurrent && (
                          <Badge variant="default" className="text-xs">
                            <ShieldIcon className="size-3 mr-0.5" />
                            Current
                          </Badge>
                        )}
                      </div>
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        {session.ipAddress && (
                          <span className="font-mono">
                            {session.ipAddress}
                          </span>
                        )}
                        <span>
                          Created{" "}
                          {new Date(session.createdAt).toLocaleDateString()}
                        </span>
                      </div>
                    </div>
                  </div>
                  {!isCurrent && (
                    <Button
                      variant="ghost"
                      size="sm"
                      disabled={revoking === session.token}
                      onClick={() => handleRevoke(session.token)}
                    >
                      {revoking === session.token ? "Revoking..." : "Revoke"}
                    </Button>
                  )}
                </div>
              );
            })
          )}
        </div>
      </CardContent>
    </Card>
  );
}

interface ApiKeyInfo {
  id: string;
  name: string;
  key_prefix: string;
  lastUsedAt: string | null;
  expiresAt: string | null;
  revokedAt: string | null;
  createdAt: string;
}

function getKeyStatus(key: ApiKeyInfo): { label: string; variant: "default" | "secondary" | "destructive" | "outline" } {
  if (key.revokedAt) return { label: "Revoked", variant: "destructive" };
  if (key.expiresAt && new Date(key.expiresAt) < new Date()) return { label: "Expired", variant: "secondary" };
  return { label: "Active", variant: "default" };
}

function ApiKeysTab() {
  const [keys, setKeys] = useState<ApiKeyInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [revoking, setRevoking] = useState<string | null>(null);
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [newKeyName, setNewKeyName] = useState("");
  const [newKeyExpiry, setNewKeyExpiry] = useState("");
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const fetchKeys = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${BASE_PATH}/api/api-keys`, { credentials: "include" });
      if (!res.ok) throw new Error("Failed to fetch keys");
      setKeys(await res.json());
    } catch {
      toast.error("Failed to load API keys.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchKeys();
  }, [fetchKeys]);

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    setCreating(true);
    try {
      const body: Record<string, string> = { name: newKeyName };
      if (newKeyExpiry) body.expiresAt = new Date(newKeyExpiry).toISOString();
      const res = await fetch(`${BASE_PATH}/api/api-keys`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.error || "Failed to create key");
      }
      const data = await res.json();
      setCreatedKey(data.key);
      setShowCreateDialog(false);
      setNewKeyName("");
      setNewKeyExpiry("");
      fetchKeys();
      toast.success("API key created.");
    } catch (err: any) {
      toast.error(err.message || "Failed to create API key.");
    } finally {
      setCreating(false);
    }
  }

  async function handleRevoke(id: string) {
    setRevoking(id);
    try {
      const res = await fetch(`${BASE_PATH}/api/api-keys/${id}`, {
        method: "DELETE",
        credentials: "include",
      });
      if (!res.ok) throw new Error("Failed to revoke key");
      toast.success("API key revoked.");
      fetchKeys();
    } catch {
      toast.error("Failed to revoke API key.");
    } finally {
      setRevoking(null);
    }
  }

  async function handleCopy() {
    if (!createdKey) return;
    await navigator.clipboard.writeText(createdKey);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2Icon className="size-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <>
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div>
            <CardTitle>API Keys</CardTitle>
            <CardDescription>
              Manage API keys for MCP server authentication
            </CardDescription>
          </div>
          <Button onClick={() => setShowCreateDialog(true)}>
            <KeyIcon className="size-4 mr-1" />
            Create API Key
          </Button>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col gap-3">
            {keys.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No API keys yet. Create one to get started with the MCP server.
              </p>
            ) : (
              keys.map((key) => {
                const status = getKeyStatus(key);
                const isActive = status.label === "Active";
                return (
                  <div
                    key={key.id}
                    className="flex items-center justify-between rounded-md border p-4"
                  >
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                        <KeyIcon className="size-4" />
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <p className="text-sm font-medium">{key.name}</p>
                          <Badge variant={status.variant} className="text-xs">
                            {status.label}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-2 text-xs text-muted-foreground">
                          <span className="font-mono">{key.key_prefix}...</span>
                          <span>
                            Created {new Date(key.createdAt).toLocaleDateString()}
                          </span>
                          <span>
                            Expires{" "}
                            {key.expiresAt
                              ? new Date(key.expiresAt).toLocaleDateString()
                              : "Never"}
                          </span>
                          <span>
                            Last used{" "}
                            {key.lastUsedAt
                              ? new Date(key.lastUsedAt).toLocaleDateString()
                              : "Never"}
                          </span>
                        </div>
                      </div>
                    </div>
                    {isActive && (
                      <Button
                        variant="ghost"
                        size="sm"
                        disabled={revoking === key.id}
                        onClick={() => handleRevoke(key.id)}
                      >
                        {revoking === key.id ? "Revoking..." : "Revoke"}
                      </Button>
                    )}
                  </div>
                );
              })
            )}
          </div>
        </CardContent>
      </Card>

      {/* Create key dialog */}
      <Dialog open={showCreateDialog} onOpenChange={setShowCreateDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create API Key</DialogTitle>
            <DialogDescription>
              Create a new API key for MCP server authentication.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleCreate} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="key-name">Name</Label>
              <Input
                id="key-name"
                value={newKeyName}
                onChange={(e) => setNewKeyName(e.target.value)}
                placeholder="e.g. My MCP Client"
                required
              />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="key-expiry">Expiration (optional)</Label>
              <Input
                id="key-expiry"
                type="date"
                value={newKeyExpiry}
                onChange={(e) => setNewKeyExpiry(e.target.value)}
                min={new Date().toISOString().split("T")[0]}
              />
            </div>
            <DialogFooter>
              <Button type="submit" disabled={creating || !newKeyName.trim()}>
                {creating ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* One-time key display dialog */}
      <Dialog open={!!createdKey} onOpenChange={(open) => { if (!open) setCreatedKey(null); }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>API Key Created</DialogTitle>
            <DialogDescription>
              Copy this key now — it won't be shown again.
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center gap-2">
            <Input
              value={createdKey || ""}
              readOnly
              className="font-mono text-sm"
            />
            <Button variant="outline" size="icon" onClick={handleCopy}>
              {copied ? <CheckIcon className="size-4" /> : <CopyIcon className="size-4" />}
            </Button>
          </div>
          <DialogFooter showCloseButton />
        </DialogContent>
      </Dialog>
    </>
  );
}

export function Profile() {
  const { data: session } = useSession();
  const isAdmin = (session?.user as any)?.role === "admin";

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold">Account Settings</h2>
        <p className="text-muted-foreground">
          Manage your profile, security, linked accounts, and sessions
        </p>
      </div>

      <Tabs defaultValue="profile">
        <TabsList>
          <TabsTrigger value="profile">Profile</TabsTrigger>
          <TabsTrigger value="security">Security</TabsTrigger>
          <TabsTrigger value="accounts">Linked Accounts</TabsTrigger>
          <TabsTrigger value="sessions">Sessions</TabsTrigger>
          {isAdmin && <TabsTrigger value="api-keys">API Keys</TabsTrigger>}
        </TabsList>

        <TabsContent value="profile" className="mt-4">
          <ProfileTab />
        </TabsContent>

        <TabsContent value="security" className="mt-4">
          <SecurityTab />
        </TabsContent>

        <TabsContent value="accounts" className="mt-4">
          <LinkedAccountsTab />
        </TabsContent>

        <TabsContent value="sessions" className="mt-4">
          <SessionsTab />
        </TabsContent>

        {isAdmin && (
          <TabsContent value="api-keys" className="mt-4">
            <ApiKeysTab />
          </TabsContent>
        )}
      </Tabs>
    </div>
  );
}
