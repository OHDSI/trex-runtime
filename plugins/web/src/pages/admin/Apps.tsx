import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { CopyIcon, CheckIcon, PlusIcon } from "lucide-react";

const LIST_APPS_QUERY = `
  query ListOauthApplications($first: Int, $after: Cursor) {
    allOauthApplications(first: $first, after: $after, orderBy: [CREATED_AT_DESC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        id
        name
        clientId
        type
        disabled
        scope
        createdAt
      }
    }
  }
`;

const CREATE_APP_MUTATION = `
  mutation CreateOauthApplication($input: CreateOauthApplicationInput!) {
    createOauthApplication(input: $input) {
      oauthApplication {
        id
        name
        clientId
        clientSecret
        type
        scope
        createdAt
      }
    }
  }
`;

interface OAuthApp {
  id: string;
  name: string;
  clientId: string;
  type: string;
  disabled: boolean;
  scope: string | null;
  createdAt: string;
}

interface CreatedApp {
  clientId: string;
  clientSecret: string;
  name: string;
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
    <Button variant="ghost" size="icon-xs" onClick={handleCopy}>
      {copied ? (
        <CheckIcon className="size-3" />
      ) : (
        <CopyIcon className="size-3" />
      )}
    </Button>
  );
}

function truncateId(id: string, length = 12): string {
  if (id.length <= length) return id;
  return id.slice(0, length) + "...";
}

export function Apps() {
  const navigate = useNavigate();
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [registerOpen, setRegisterOpen] = useState(false);
  const [credentialsDialog, setCredentialsDialog] = useState<CreatedApp | null>(
    null
  );

  const [formName, setFormName] = useState("");
  const [formRedirectUris, setFormRedirectUris] = useState("");
  const [formType, setFormType] = useState<"confidential" | "public">(
    "confidential"
  );
  const [formScopes, setFormScopes] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const [result, reexecuteQuery] = useQuery({
    query: LIST_APPS_QUERY,
    variables: { first: 25, after: cursor },
  });

  const [, createApp] = useMutation(CREATE_APP_MUTATION);

  const apps: OAuthApp[] = result.data?.allOauthApplications?.nodes || [];
  const pageInfo = result.data?.allOauthApplications?.pageInfo;
  const totalCount = result.data?.allOauthApplications?.totalCount ?? 0;

  const filteredApps = search
    ? apps.filter(
        (app) =>
          app.name.toLowerCase().includes(search.toLowerCase()) ||
          app.clientId.toLowerCase().includes(search.toLowerCase())
      )
    : apps;

  function resetForm() {
    setFormName("");
    setFormRedirectUris("");
    setFormType("confidential");
    setFormScopes("");
  }

  async function handleCreate(e: FormEvent) {
    e.preventDefault();
    setSubmitting(true);

    const redirectUris = formRedirectUris
      .split("\n")
      .map((uri) => uri.trim())
      .filter(Boolean);

    if (redirectUris.length === 0) {
      toast.error("At least one redirect URI is required.");
      setSubmitting(false);
      return;
    }

    try {
      const res = await createApp({
        input: {
          oauthApplication: {
            name: formName,
            redirectUris: JSON.stringify(redirectUris),
            type: formType,
            scope: formScopes || null,
          },
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      const created = res.data?.createOauthApplication?.oauthApplication;
      if (created) {
        setRegisterOpen(false);
        resetForm();
        setCredentialsDialog({
          clientId: created.clientId,
          clientSecret: created.clientSecret,
          name: created.name,
        });
        reexecuteQuery({ requestPolicy: "network-only" });
        toast.success("Application registered.");
      }
    } catch {
      toast.error("Failed to register application.");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<OAuthApp>[] = [
    {
      header: "Name",
      accessorKey: "name",
    },
    {
      header: "Client ID",
      cell: (row) => (
        <div className="flex items-center gap-1">
          <code className="text-xs font-mono">
            {truncateId(row.clientId)}
          </code>
          <CopyButton text={row.clientId} />
        </div>
      ),
    },
    {
      header: "Type",
      cell: (row) => (
        <Badge variant={row.type === "confidential" ? "default" : "secondary"}>
          {row.type}
        </Badge>
      ),
    },
    {
      header: "Status",
      cell: (row) => (
        <Badge variant={row.disabled ? "secondary" : "default"}>
          {row.disabled ? "Disabled" : "Active"}
        </Badge>
      ),
    },
    {
      header: "Created",
      cell: (row) => new Date(row.createdAt).toLocaleDateString(),
    },
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-bold">Applications</h2>
        <p className="text-muted-foreground">
          OAuth applications ({totalCount} total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={filteredApps}
        loading={result.fetching}
        searchPlaceholder="Search applications..."
        searchValue={search}
        onSearchChange={setSearch}
        emptyMessage="No applications found."
        onRowClick={(row) => navigate(`/admin/apps/${row.id}`)}
        hasNextPage={pageInfo?.hasNextPage}
        hasPreviousPage={prevCursors.length > 0}
        onNextPage={() => {
          if (pageInfo?.endCursor) {
            setPrevCursors((p) => [...p, cursor || ""]);
            setCursor(pageInfo.endCursor);
          }
        }}
        onPreviousPage={() => {
          const prev = prevCursors[prevCursors.length - 1];
          setPrevCursors((p) => p.slice(0, -1));
          setCursor(prev || null);
        }}
        actions={
          <Button onClick={() => setRegisterOpen(true)}>
            <PlusIcon />
            Register Application
          </Button>
        }
      />

      {/* Register Application Dialog */}
      <Dialog
        open={registerOpen}
        onOpenChange={(open) => {
          setRegisterOpen(open);
          if (!open) resetForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Register Application</DialogTitle>
            <DialogDescription>
              Create a new OAuth application. You will receive a client ID and
              secret after registration.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleCreate} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="app-name">Application Name</Label>
              <Input
                id="app-name"
                placeholder="My Application"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="redirect-uris">Redirect URIs</Label>
              <textarea
                id="redirect-uris"
                className="border-input bg-transparent placeholder:text-muted-foreground flex min-h-[80px] w-full rounded-md border px-3 py-2 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none disabled:cursor-not-allowed disabled:opacity-50"
                placeholder={"https://example.com/callback\nhttps://example.com/auth/callback"}
                value={formRedirectUris}
                onChange={(e) => setFormRedirectUris(e.target.value)}
                required
              />
              <p className="text-xs text-muted-foreground">
                One URI per line
              </p>
            </div>

            <div className="flex flex-col gap-2">
              <Label>Application Type</Label>
              <div className="flex gap-4">
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    name="app-type"
                    value="confidential"
                    checked={formType === "confidential"}
                    onChange={() => setFormType("confidential")}
                    className="accent-primary"
                  />
                  Confidential
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    name="app-type"
                    value="public"
                    checked={formType === "public"}
                    onChange={() => setFormType("public")}
                    className="accent-primary"
                  />
                  Public
                </label>
              </div>
              <p className="text-xs text-muted-foreground">
                Confidential clients can securely store a client secret. Public
                clients (SPAs, mobile apps) cannot.
              </p>
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="scopes">Scopes</Label>
              <Input
                id="scopes"
                placeholder="openid profile email"
                value={formScopes}
                onChange={(e) => setFormScopes(e.target.value)}
              />
              <p className="text-xs text-muted-foreground">
                Space-separated list of allowed scopes
              </p>
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setRegisterOpen(false);
                  resetForm();
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Registering..." : "Register"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Credentials One-Time View Dialog */}
      <Dialog
        open={credentialsDialog !== null}
        onOpenChange={(open) => {
          if (!open) setCredentialsDialog(null);
        }}
      >
        <DialogContent showCloseButton={false}>
          <DialogHeader>
            <DialogTitle>Application Credentials</DialogTitle>
            <DialogDescription>
              Save these credentials now. The client secret will not be shown
              again.
            </DialogDescription>
          </DialogHeader>
          {credentialsDialog && (
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-2">
                <Label className="text-muted-foreground">
                  Application Name
                </Label>
                <p className="text-sm font-medium">
                  {credentialsDialog.name}
                </p>
              </div>
              <div className="flex flex-col gap-2">
                <Label className="text-muted-foreground">Client ID</Label>
                <div className="flex items-center gap-2 rounded-md bg-muted px-3 py-2">
                  <code className="text-sm font-mono flex-1 break-all">
                    {credentialsDialog.clientId}
                  </code>
                  <CopyButton text={credentialsDialog.clientId} />
                </div>
              </div>
              <div className="flex flex-col gap-2">
                <Label className="text-muted-foreground">Client Secret</Label>
                <div className="flex items-center gap-2 rounded-md bg-muted px-3 py-2">
                  <code className="text-sm font-mono flex-1 break-all">
                    {credentialsDialog.clientSecret}
                  </code>
                  <CopyButton text={credentialsDialog.clientSecret} />
                </div>
              </div>
              <div className="rounded-md border border-destructive/50 bg-destructive/10 p-3">
                <p className="text-sm text-destructive">
                  Make sure to copy the client secret. You will not be able to
                  see it again.
                </p>
              </div>
            </div>
          )}
          <DialogFooter>
            <Button onClick={() => setCredentialsDialog(null)}>
              I have saved the credentials
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
