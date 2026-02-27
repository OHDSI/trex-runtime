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

const LIST_SUBSCRIPTIONS_QUERY = `
  query ListSubscriptions($first: Int, $after: Cursor) {
    allNotifySubscriptions(first: $first, after: $after, orderBy: [PRIMARY_KEY_DESC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        nodeId: id
        id: rowId
        name
        topic
        sourceTable
        events
        description
        enabled
        createdAt
      }
    }
  }
`;

const SEARCH_SUBSCRIPTIONS_QUERY = `
  query SearchSubscriptions($query: String!, $first: Int, $after: Cursor) {
    searchSubscriptions(query: $query, first: $first, after: $after) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        nodeId: id
        id: rowId
        name
        topic
        sourceTable
        events
        description
        enabled
        createdAt
      }
    }
  }
`;

const SAVE_SUBSCRIPTION_MUTATION = `
  mutation SaveSubscription(
    $pName: String!,
    $pTopic: String!,
    $pSourceTable: String!,
    $pEvents: [String]!,
    $pDescription: String,
    $pEnabled: Boolean
  ) {
    saveSubscription(
      input: {
        pName: $pName,
        pTopic: $pTopic,
        pSourceTable: $pSourceTable,
        pEvents: $pEvents,
        pDescription: $pDescription,
        pEnabled: $pEnabled
      }
    ) {
      notifySubscriptionEdge {
        node { id }
      }
    }
  }
`;

const DELETE_SUBSCRIPTION_MUTATION = `
  mutation RemoveSubscription($pName: String!) {
    removeSubscription(input: { pName: $pName }) {
      notifySubscriptionEdge {
        node { id }
      }
    }
  }
`;

const EVENT_OPTIONS = ["INSERT", "UPDATE", "DELETE"] as const;

interface SubscriptionRow {
  nodeId: string;
  id: string;
  name: string;
  topic: string;
  sourceTable: string;
  events: string[];
  description: string | null;
  enabled: boolean;
  createdAt: string;
}

export function Subscriptions() {
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editing, setEditing] = useState<SubscriptionRow | null>(null);

  const [formName, setFormName] = useState("");
  const [formTopic, setFormTopic] = useState("");
  const [formSourceTable, setFormSourceTable] = useState("");
  const [formEvents, setFormEvents] = useState<string[]>(["INSERT", "UPDATE"]);
  const [formDescription, setFormDescription] = useState("");
  const [formEnabled, setFormEnabled] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  const isSearching = search.length > 0;

  const [listResult, reexecuteList] = useQuery({
    query: LIST_SUBSCRIPTIONS_QUERY,
    variables: { first: 25, after: cursor },
    pause: isSearching,
  });

  const [searchResult, reexecuteSearch] = useQuery({
    query: SEARCH_SUBSCRIPTIONS_QUERY,
    variables: { query: search, first: 25, after: cursor },
    pause: !isSearching,
  });

  const [, saveSubscription] = useMutation(SAVE_SUBSCRIPTION_MUTATION);
  const [, deleteSubscription] = useMutation(DELETE_SUBSCRIPTION_MUTATION);

  const result = isSearching ? searchResult : listResult;
  const connection = isSearching
    ? result.data?.searchSubscriptions
    : result.data?.allNotifySubscriptions;

  const subscriptions: SubscriptionRow[] = connection?.nodes || [];
  const pageInfo = connection?.pageInfo;
  const totalCount = connection?.totalCount ?? 0;

  function resetForm() {
    setFormName("");
    setFormTopic("");
    setFormSourceTable("");
    setFormEvents(["INSERT", "UPDATE"]);
    setFormDescription("");
    setFormEnabled(true);
    setEditing(null);
  }

  function openAdd() {
    resetForm();
    setDialogOpen(true);
  }

  function openEdit(row: SubscriptionRow) {
    setEditing(row);
    setFormName(row.name);
    setFormTopic(row.topic);
    setFormSourceTable(row.sourceTable);
    setFormEvents([...row.events]);
    setFormDescription(row.description || "");
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

  function toggleEvent(event: string) {
    setFormEvents((prev) =>
      prev.includes(event)
        ? prev.filter((e) => e !== event)
        : [...prev, event]
    );
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();

    if (!/^[a-z][a-z0-9_]*$/.test(formName)) {
      toast.error(
        "Name must start with a lowercase letter and contain only lowercase letters, numbers, and underscores."
      );
      return;
    }

    if (formEvents.length === 0) {
      toast.error("Select at least one event type.");
      return;
    }

    setSubmitting(true);
    try {
      const res = await saveSubscription({
        pName: formName,
        pTopic: formTopic,
        pSourceTable: formSourceTable,
        pEvents: formEvents,
        pDescription: formDescription || null,
        pEnabled: formEnabled,
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setDialogOpen(false);
      resetForm();
      refetch();
      toast.success(editing ? "Subscription updated." : "Subscription created.");
    } catch {
      toast.error("Failed to save subscription.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleDelete() {
    if (!editing) return;

    setSubmitting(true);
    try {
      const res = await deleteSubscription({ pName: editing.name });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setDialogOpen(false);
      resetForm();
      refetch();
      toast.success("Subscription deleted.");
    } catch {
      toast.error("Failed to delete subscription.");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<SubscriptionRow>[] = [
    {
      header: "Name",
      cell: (row) => <code className="text-xs font-mono">{row.name}</code>,
    },
    {
      header: "Topic",
      cell: (row) => <code className="text-xs font-mono">{row.topic}</code>,
    },
    {
      header: "Source Table",
      cell: (row) => (
        <code className="text-xs font-mono">{row.sourceTable}</code>
      ),
    },
    {
      header: "Events",
      cell: (row) => (
        <div className="flex gap-1">
          {row.events.map((ev) => (
            <Badge key={ev} variant="outline" className="text-xs">
              {ev}
            </Badge>
          ))}
        </div>
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
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-bold">Subscriptions</h2>
        <p className="text-muted-foreground">
          Manage real-time GraphQL subscriptions via LISTEN/NOTIFY ({totalCount}{" "}
          total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={subscriptions}
        loading={result.fetching}
        searchPlaceholder="Search subscriptions..."
        searchValue={search}
        onSearchChange={(val) => {
          setSearch(val);
          setCursor(null);
          setPrevCursors([]);
        }}
        emptyMessage="No subscriptions configured."
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
            Add Subscription
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
              {editing ? "Edit Subscription" : "Add Subscription"}
            </DialogTitle>
            <DialogDescription>
              {editing
                ? "Update the subscription configuration. Saving will recreate the database trigger."
                : "Create a new LISTEN/NOTIFY subscription with a database trigger."}
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleSave} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="sub-name">Name</Label>
              <Input
                id="sub-name"
                placeholder="user_changes"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                disabled={!!editing}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="sub-topic">Topic</Label>
              <Input
                id="sub-topic"
                placeholder="user.changes"
                value={formTopic}
                onChange={(e) => setFormTopic(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="sub-source-table">Source Table</Label>
              <Input
                id="sub-source-table"
                placeholder='trex."user"'
                value={formSourceTable}
                onChange={(e) => setFormSourceTable(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label>Events</Label>
              <div className="flex gap-4">
                {EVENT_OPTIONS.map((event) => (
                  <label
                    key={event}
                    className="flex items-center gap-2 cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      checked={formEvents.includes(event)}
                      onChange={() => toggleEvent(event)}
                      className="h-4 w-4 rounded border-input"
                    />
                    <span className="text-sm">{event}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="sub-description">Description</Label>
              <Input
                id="sub-description"
                placeholder="Optional description"
                value={formDescription}
                onChange={(e) => setFormDescription(e.target.value)}
              />
            </div>

            <div className="flex items-center gap-2">
              <input
                id="sub-enabled"
                type="checkbox"
                checked={formEnabled}
                onChange={(e) => setFormEnabled(e.target.checked)}
                className="h-4 w-4 rounded border-input"
              />
              <Label htmlFor="sub-enabled" className="cursor-pointer">
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
