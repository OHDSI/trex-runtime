import { useState, useEffect, type FormEvent } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  ArrowLeftIcon,
  CopyIcon,
  CheckIcon,
  PlusIcon,
  TrashIcon,
  KeyIcon,
} from "lucide-react";

const APP_DETAIL_QUERY = `
  query OauthApplicationById($id: UUID!) {
    oauthApplicationById(id: $id) {
      id
      name
      clientId
      type
      disabled
      trusted
      scope
      redirectUris
      createdAt
      updatedAt
      oauthAccessTokensByClientId {
        totalCount
      }
    }
  }
`;

const UPDATE_APP_MUTATION = `
  mutation UpdateOauthApplicationById($id: UUID!, $patch: OauthApplicationPatch!) {
    updateOauthApplicationById(input: { id: $id, patch: $patch }) {
      oauthApplication {
        id
        name
        redirectUris
        scope
        disabled
        trusted
      }
    }
  }
`;

const DELETE_APP_MUTATION = `
  mutation DeleteOauthApplicationById($id: UUID!) {
    deleteOauthApplicationById(input: { id: $id }) {
      deletedOauthApplicationNodeId
    }
  }
`;

const ROTATE_SECRET_MUTATION = `
  mutation RotateOauthApplicationSecret($id: UUID!) {
    rotateOauthApplicationSecret(input: { id: $id }) {
      oauthApplication {
        id
        clientSecret
      }
    }
  }
`;

interface OAuthAppDetail {
  id: string;
  name: string;
  clientId: string;
  type: string;
  disabled: boolean;
  trusted: boolean;
  scope: string | null;
  redirectUris: string;
  createdAt: string;
  updatedAt: string;
  oauthAccessTokensByClientId: {
    totalCount: number;
  };
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }

  return (
    <Button
      variant="ghost"
      size="icon-xs"
      onClick={(e) => {
        e.preventDefault();
        handleCopy();
      }}
    >
      {copied ? (
        <CheckIcon className="size-3" />
      ) : (
        <CopyIcon className="size-3" />
      )}
    </Button>
  );
}

function parseRedirectUris(raw: string): string[] {
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return raw ? [raw] : [];
  }
}

export function AppDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();

  const [result, reexecuteQuery] = useQuery({
    query: APP_DETAIL_QUERY,
    variables: { id },
    pause: !id,
  });

  const [, updateApp] = useMutation(UPDATE_APP_MUTATION);
  const [, deleteApp] = useMutation(DELETE_APP_MUTATION);
  const [, rotateSecret] = useMutation(ROTATE_SECRET_MUTATION);

  const app: OAuthAppDetail | null =
    result.data?.oauthApplicationById || null;

  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState("");
  const [editRedirectUris, setEditRedirectUris] = useState<string[]>([]);
  const [editScope, setEditScope] = useState("");
  const [saving, setSaving] = useState(false);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [rotateDialogOpen, setRotateDialogOpen] = useState(false);
  const [newSecret, setNewSecret] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [rotating, setRotating] = useState(false);

  useEffect(() => {
    if (app) {
      setEditName(app.name);
      setEditRedirectUris(parseRedirectUris(app.redirectUris));
      setEditScope(app.scope || "");
    }
  }, [app]);

  if (result.fetching) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin h-8 w-8 border-4 border-primary border-t-transparent rounded-full" />
      </div>
    );
  }

  if (result.error) {
    return (
      <div className="text-center py-20">
        <p className="text-destructive">
          Failed to load application: {result.error.message}
        </p>
      </div>
    );
  }

  if (!app) {
    return (
      <div className="text-center py-20">
        <p className="text-muted-foreground">Application not found.</p>
      </div>
    );
  }

  const redirectUris = parseRedirectUris(app.redirectUris);

  function startEditing() {
    if (!app) return;
    setEditName(app.name);
    setEditRedirectUris(parseRedirectUris(app.redirectUris));
    setEditScope(app.scope || "");
    setEditing(true);
  }

  function cancelEditing() {
    setEditing(false);
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    if (!app) return;
    setSaving(true);

    const filteredUris = editRedirectUris.filter(Boolean);
    if (filteredUris.length === 0) {
      toast.error("At least one redirect URI is required.");
      setSaving(false);
      return;
    }

    try {
      const res = await updateApp({
        id: app.id,
        patch: {
          name: editName,
          redirectUris: JSON.stringify(filteredUris),
          scope: editScope || null,
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Application updated.");
      setEditing(false);
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to update application.");
    } finally {
      setSaving(false);
    }
  }

  async function handleToggleDisabled() {
    try {
      const res = await updateApp({
        id: app!.id,
        patch: { disabled: !app!.disabled },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success(
        app!.disabled ? "Application enabled." : "Application disabled."
      );
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to update application status.");
    }
  }

  async function handleDelete() {
    setDeleting(true);
    try {
      const res = await deleteApp({ id: app!.id });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Application deleted.");
      navigate("/admin/apps");
    } catch {
      toast.error("Failed to delete application.");
    } finally {
      setDeleting(false);
    }
  }

  async function handleRotateSecret() {
    setRotating(true);
    try {
      const res = await rotateSecret({ id: app!.id });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      const secret =
        res.data?.rotateOauthApplicationSecret?.oauthApplication?.clientSecret;
      if (secret) {
        setNewSecret(secret);
        setRotateDialogOpen(false);
        toast.success("Client secret rotated.");
      }
    } catch {
      toast.error("Failed to rotate client secret.");
    } finally {
      setRotating(false);
    }
  }

  function addRedirectUri() {
    setEditRedirectUris((uris) => [...uris, ""]);
  }

  function removeRedirectUri(index: number) {
    setEditRedirectUris((uris) => uris.filter((_, i) => i !== index));
  }

  function updateRedirectUri(index: number, value: string) {
    setEditRedirectUris((uris) =>
      uris.map((uri, i) => (i === index ? value : uri))
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={() => navigate("/admin/apps")}
        >
          <ArrowLeftIcon />
        </Button>
        <div className="flex-1">
          <h2 className="text-2xl font-bold">{app.name}</h2>
          <p className="text-muted-foreground text-sm">
            Application details and configuration
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={app.disabled ? "secondary" : "default"}>
            {app.disabled ? "Disabled" : "Active"}
          </Badge>
          <Badge variant={app.type === "confidential" ? "default" : "outline"}>
            {app.type}
          </Badge>
          {app.trusted && <Badge variant="secondary">Trusted</Badge>}
        </div>
      </div>

      <Separator />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {/* Main Details */}
        <div className="lg:col-span-2 space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Application Details</CardTitle>
              <CardDescription>
                Core configuration for this OAuth application
              </CardDescription>
            </CardHeader>
            <CardContent>
              {editing ? (
                <form onSubmit={handleSave} className="flex flex-col gap-4">
                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-name">Name</Label>
                    <Input
                      id="edit-name"
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                      required
                    />
                  </div>

                  <div className="flex flex-col gap-2">
                    <Label>Redirect URIs</Label>
                    <div className="flex flex-col gap-2">
                      {editRedirectUris.map((uri, i) => (
                        <div key={i} className="flex items-center gap-2">
                          <Input
                            value={uri}
                            onChange={(e) =>
                              updateRedirectUri(i, e.target.value)
                            }
                            placeholder="https://example.com/callback"
                          />
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            onClick={() => removeRedirectUri(i)}
                            disabled={editRedirectUris.length <= 1}
                          >
                            <TrashIcon className="size-4" />
                          </Button>
                        </div>
                      ))}
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="w-fit"
                        onClick={addRedirectUri}
                      >
                        <PlusIcon />
                        Add URI
                      </Button>
                    </div>
                  </div>

                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-scope">Scopes</Label>
                    <Input
                      id="edit-scope"
                      value={editScope}
                      onChange={(e) => setEditScope(e.target.value)}
                      placeholder="openid profile email"
                    />
                    <p className="text-xs text-muted-foreground">
                      Space-separated list of allowed scopes
                    </p>
                  </div>

                  <div className="flex gap-2 justify-end">
                    <Button
                      type="button"
                      variant="outline"
                      onClick={cancelEditing}
                    >
                      Cancel
                    </Button>
                    <Button type="submit" disabled={saving}>
                      {saving ? "Saving..." : "Save Changes"}
                    </Button>
                  </div>
                </form>
              ) : (
                <div className="flex flex-col gap-4">
                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Name</Label>
                    <p className="text-sm">{app.name}</p>
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Client ID</Label>
                    <div className="flex items-center gap-2">
                      <code className="text-sm font-mono break-all">
                        {app.clientId}
                      </code>
                      <CopyButton text={app.clientId} />
                    </div>
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">
                      Redirect URIs
                    </Label>
                    {redirectUris.length > 0 ? (
                      <ul className="flex flex-col gap-1">
                        {redirectUris.map((uri, i) => (
                          <li key={i}>
                            <code className="text-sm font-mono break-all">
                              {uri}
                            </code>
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="text-sm text-muted-foreground">
                        No redirect URIs configured
                      </p>
                    )}
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Scopes</Label>
                    <p className="text-sm">
                      {app.scope || (
                        <span className="text-muted-foreground">
                          No scopes configured
                        </span>
                      )}
                    </p>
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Created</Label>
                    <p className="text-sm">
                      {new Date(app.createdAt).toLocaleString()}
                    </p>
                  </div>

                  {app.updatedAt && (
                    <div className="flex flex-col gap-1">
                      <Label className="text-muted-foreground">
                        Last Updated
                      </Label>
                      <p className="text-sm">
                        {new Date(app.updatedAt).toLocaleString()}
                      </p>
                    </div>
                  )}

                  <div className="flex gap-2 justify-end">
                    <Button variant="outline" onClick={startEditing}>
                      Edit
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Sidebar */}
        <div className="space-y-6">
          {/* Stats */}
          <Card>
            <CardHeader>
              <CardTitle>Statistics</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col gap-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">
                    Active Tokens
                  </span>
                  <span className="text-sm font-medium">
                    {app.oauthAccessTokensByClientId.totalCount}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Type</span>
                  <Badge
                    variant={
                      app.type === "confidential" ? "default" : "outline"
                    }
                  >
                    {app.type}
                  </Badge>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">
                    Trusted
                  </span>
                  <span className="text-sm font-medium">
                    {app.trusted ? "Yes" : "No"}
                  </span>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Actions */}
          <Card>
            <CardHeader>
              <CardTitle>Actions</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col gap-2">
                {app.type === "confidential" && (
                  <Button
                    variant="outline"
                    className="w-full justify-start"
                    onClick={() => setRotateDialogOpen(true)}
                  >
                    <KeyIcon />
                    Rotate Secret
                  </Button>
                )}
                <Button
                  variant="outline"
                  className="w-full justify-start"
                  onClick={handleToggleDisabled}
                >
                  {app.disabled ? "Enable Application" : "Disable Application"}
                </Button>
                <Button
                  variant="destructive"
                  className="w-full justify-start"
                  onClick={() => setDeleteDialogOpen(true)}
                >
                  <TrashIcon />
                  Delete Application
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Application</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete "{app.name}"? This action cannot
              be undone. All associated tokens and authorization grants will be
              revoked.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setDeleteDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleting}
            >
              {deleting ? "Deleting..." : "Delete Application"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Rotate Secret Confirmation Dialog */}
      <Dialog open={rotateDialogOpen} onOpenChange={setRotateDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rotate Client Secret</DialogTitle>
            <DialogDescription>
              This will generate a new client secret and invalidate the current
              one. Any applications using the old secret will stop working
              immediately.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setRotateDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button onClick={handleRotateSecret} disabled={rotating}>
              {rotating ? "Rotating..." : "Rotate Secret"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* New Secret Display Dialog */}
      <Dialog
        open={newSecret !== null}
        onOpenChange={(open) => {
          if (!open) setNewSecret(null);
        }}
      >
        <DialogContent showCloseButton={false}>
          <DialogHeader>
            <DialogTitle>New Client Secret</DialogTitle>
            <DialogDescription>
              Save this secret now. It will not be shown again.
            </DialogDescription>
          </DialogHeader>
          {newSecret && (
            <div className="flex flex-col gap-4">
              <div className="flex items-center gap-2 rounded-md bg-muted px-3 py-2">
                <code className="text-sm font-mono flex-1 break-all">
                  {newSecret}
                </code>
                <CopyButton text={newSecret} />
              </div>
              <div className="rounded-md border border-destructive/50 bg-destructive/10 p-3">
                <p className="text-sm text-destructive">
                  Make sure to copy the new client secret. You will not be able
                  to see it again.
                </p>
              </div>
            </div>
          )}
          <DialogFooter>
            <Button onClick={() => setNewSecret(null)}>
              I have saved the secret
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
