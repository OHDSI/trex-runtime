import { useState, type FormEvent } from "react";
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
import { PlusIcon } from "lucide-react";

const LIST_SSO_PROVIDERS_QUERY = `
  query ListSsoProviders($first: Int, $after: Cursor) {
    allSsoProviders(first: $first, after: $after, orderBy: [PRIMARY_KEY_DESC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        nodeId: id
        id: rowId
        displayName
        clientId
        enabled
        createdAt
      }
    }
  }
`;

const SEARCH_SSO_PROVIDERS_QUERY = `
  query SearchSsoProviders($query: String!, $first: Int, $after: Cursor) {
    searchSsoProviders(query: $query, first: $first, after: $after) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        nodeId: id
        id: rowId
        displayName
        clientId
        enabled
        createdAt
      }
    }
  }
`;

const SAVE_SSO_PROVIDER_MUTATION = `
  mutation SaveSsoProvider(
    $pId: String!,
    $pDisplayName: String!,
    $pClientId: String!,
    $pClientSecret: String!,
    $pEnabled: Boolean
  ) {
    saveSsoProvider(
      input: {
        pId: $pId,
        pDisplayName: $pDisplayName,
        pClientId: $pClientId,
        pClientSecret: $pClientSecret,
        pEnabled: $pEnabled
      }
    ) {
      ssoProviderEdge {
        node {
          id
        }
      }
    }
  }
`;

const DELETE_SSO_PROVIDER_MUTATION = `
  mutation DeleteSsoProvider($id: String!) {
    deleteSsoProviderById(input: { id: $id }) {
      deletedSsoProviderNodeId
    }
  }
`;

const RELOAD_SSO_MUTATION = `
  mutation ReloadSsoProviders {
    reloadSsoProviders
  }
`;

const SUPPORTED_PROVIDERS = [
  { id: "google", label: "Google" },
  { id: "github", label: "GitHub" },
  { id: "microsoft", label: "Microsoft" },
  { id: "apple", label: "Apple" },
  { id: "discord", label: "Discord" },
  { id: "facebook", label: "Facebook" },
  { id: "twitter", label: "Twitter" },
  { id: "linkedin", label: "LinkedIn" },
  { id: "gitlab", label: "GitLab" },
  { id: "spotify", label: "Spotify" },
];

interface SsoProviderRow {
  nodeId: string;
  id: string;
  displayName: string;
  clientId: string;
  enabled: boolean;
  createdAt: string;
}

function maskSecret(value: string): string {
  if (value.length <= 8) return "****";
  return value.slice(0, 4) + "****" + value.slice(-4);
}

export function SsoProviders() {
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editing, setEditing] = useState<SsoProviderRow | null>(null);

  const [formProviderId, setFormProviderId] = useState("");
  const [formDisplayName, setFormDisplayName] = useState("");
  const [formClientId, setFormClientId] = useState("");
  const [formClientSecret, setFormClientSecret] = useState("");
  const [formEnabled, setFormEnabled] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const isSearching = search.length > 0;

  const [listResult, reexecuteList] = useQuery({
    query: LIST_SSO_PROVIDERS_QUERY,
    variables: { first: 25, after: cursor },
    pause: isSearching,
  });

  const [searchResult, reexecuteSearch] = useQuery({
    query: SEARCH_SSO_PROVIDERS_QUERY,
    variables: { query: search, first: 25, after: cursor },
    pause: !isSearching,
  });

  const [, saveSsoProvider] = useMutation(SAVE_SSO_PROVIDER_MUTATION);
  const [, deleteSsoProvider] = useMutation(DELETE_SSO_PROVIDER_MUTATION);
  const [, reloadSso] = useMutation(RELOAD_SSO_MUTATION);

  const result = isSearching ? searchResult : listResult;
  const connection = isSearching
    ? result.data?.searchSsoProviders
    : result.data?.allSsoProviders;

  const providers: SsoProviderRow[] = connection?.nodes || [];
  const pageInfo = connection?.pageInfo;
  const totalCount = connection?.totalCount ?? 0;

  function resetForm() {
    setFormProviderId("");
    setFormDisplayName("");
    setFormClientId("");
    setFormClientSecret("");
    setFormEnabled(false);
    setEditing(null);
  }

  function openAdd() {
    resetForm();
    setDialogOpen(true);
  }

  function openEdit(row: SsoProviderRow) {
    setEditing(row);
    setFormProviderId(row.id);
    setFormDisplayName(row.displayName);
    setFormClientId(row.clientId);
    setFormClientSecret("");
    setFormEnabled(row.enabled);
    setDialogOpen(true);
  }

  function refetch() {
    if (isSearching) {
      reexecuteSearch({ requestPolicy: "network-only" });
    } else {
      reexecuteList({ requestPolicy: "network-only" });
    }
  }

  async function triggerReload() {
    try {
      const res = await reloadSso({});
      if (res.error) {
        console.error("SSO reload failed:", res.error.message);
      }
    } catch (err) {
      console.error("SSO reload error:", err);
    }
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();

    if (!editing && !/^[a-z][a-z0-9_]*$/.test(formProviderId)) {
      toast.error("Provider ID must start with a lowercase letter and contain only lowercase letters, numbers, and underscores.");
      return;
    }

    setSubmitting(true);
    try {
      const res = await saveSsoProvider({
        pId: formProviderId,
        pDisplayName: formDisplayName,
        pClientId: formClientId,
        pClientSecret: formClientSecret,
        pEnabled: formEnabled,
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setDialogOpen(false);
      resetForm();
      refetch();
      await triggerReload();
      toast.success(editing ? "Provider updated." : "Provider added.");
    } catch {
      toast.error("Failed to save provider.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!editing) return;

    setSubmitting(true);
    try {
      const res = await deleteSsoProvider({ id: editing.nodeId });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setDialogOpen(false);
      resetForm();
      refetch();
      await triggerReload();
      toast.success("Provider deleted.");
    } catch {
      toast.error("Failed to delete provider.");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<SsoProviderRow>[] = [
    {
      header: "Provider ID",
      cell: (row) => <code className="text-xs font-mono">{row.id}</code>,
    },
    {
      header: "Display Name",
      accessorKey: "displayName",
    },
    {
      header: "Client ID",
      cell: (row) => (
        <code className="text-xs font-mono">{maskSecret(row.clientId)}</code>
      ),
    },
    {
      header: "Status",
      cell: (row) => (
        <Badge variant={row.enabled ? "default" : "secondary"}>
          {row.enabled ? "Enabled" : "Disabled"}
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
        <h2 className="text-2xl font-bold">SSO Providers</h2>
        <p className="text-muted-foreground">
          Configure single sign-on providers ({totalCount} total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={providers}
        loading={result.fetching}
        searchPlaceholder="Search providers..."
        searchValue={search}
        onSearchChange={(val) => {
          setSearch(val);
          setCursor(null);
          setPrevCursors([]);
        }}
        emptyMessage="No SSO providers configured."
        onRowClick={openEdit}
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
          <Button onClick={openAdd}>
            <PlusIcon />
            Add Provider
          </Button>
        }
      />

      <Dialog
        open={dialogOpen}
        onOpenChange={(open) => {
          setDialogOpen(open);
          if (!open) resetForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editing ? "Edit Provider" : "Add Provider"}
            </DialogTitle>
            <DialogDescription>
              {editing
                ? "Update the SSO provider configuration. Leave Client Secret blank to keep the existing value."
                : "Configure a new SSO provider for sign-in."}
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleSave} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="sso-provider-id">Provider</Label>
              {editing ? (
                <Input
                  id="sso-provider-id"
                  value={formProviderId}
                  disabled
                />
              ) : (
                <select
                  id="sso-provider-id"
                  className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
                  value={formProviderId}
                  onChange={(e) => {
                    setFormProviderId(e.target.value);
                    const match = SUPPORTED_PROVIDERS.find(
                      (p) => p.id === e.target.value
                    );
                    if (match) setFormDisplayName(match.label);
                  }}
                  required
                >
                  <option value="">Select a provider...</option>
                  {SUPPORTED_PROVIDERS.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.label}
                    </option>
                  ))}
                </select>
              )}
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="sso-display-name">Display Name</Label>
              <Input
                id="sso-display-name"
                placeholder="Google"
                value={formDisplayName}
                onChange={(e) => setFormDisplayName(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="sso-client-id">Client ID</Label>
              <Input
                id="sso-client-id"
                placeholder="your-client-id"
                value={formClientId}
                onChange={(e) => setFormClientId(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="sso-client-secret">Client Secret</Label>
              <Input
                id="sso-client-secret"
                type="password"
                placeholder={editing ? "Leave blank to keep existing" : "your-client-secret"}
                value={formClientSecret}
                onChange={(e) => setFormClientSecret(e.target.value)}
                required={!editing}
              />
            </div>

            <div className="flex items-center gap-2">
              <input
                id="sso-enabled"
                type="checkbox"
                checked={formEnabled}
                onChange={(e) => setFormEnabled(e.target.checked)}
                className="h-4 w-4 rounded border-input"
              />
              <Label htmlFor="sso-enabled" className="cursor-pointer">
                Enabled
              </Label>
            </div>

            <DialogFooter>
              {editing && (
                <Button
                  type="button"
                  variant="destructive"
                  onClick={handleDelete}
                  disabled={submitting}
                >
                  Delete
                </Button>
              )}
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setDialogOpen(false);
                  resetForm();
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Saving..." : "Save"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
