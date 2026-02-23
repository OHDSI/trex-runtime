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
import { PlusIcon } from "lucide-react";

const LIST_DASHBOARDS_QUERY = `
  query ListDashboards($first: Int, $after: Cursor) {
    allDashboards(first: $first, after: $after, orderBy: [PRIMARY_KEY_DESC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        nodeId: id
        id: rowId
        name
        language
        createdAt
        userByUserId { name }
      }
    }
  }
`;

const SEARCH_DASHBOARDS_QUERY = `
  query SearchDashboards($query: String!, $first: Int, $after: Cursor) {
    searchDashboards(query: $query, first: $first, after: $after) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        nodeId: id
        id: rowId
        name
        language
        createdAt
        userByUserId { name }
      }
    }
  }
`;

const CREATE_DASHBOARD_MUTATION = `
  mutation CreateDashboard($input: CreateDashboardInput!) {
    createDashboard(input: $input) {
      dashboard {
        rowId
      }
    }
  }
`;

interface DashboardRow {
  nodeId: string;
  id: string;
  name: string;
  language: string;
  createdAt: string;
  userByUserId: { name: string } | null;
}

export function Analytics() {
  const navigate = useNavigate();
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);

  const [formName, setFormName] = useState("");
  const [formLanguage, setFormLanguage] = useState<"python" | "r" | "markdown">("python");
  const [submitting, setSubmitting] = useState(false);

  const isSearching = search.length > 0;

  const [listResult] = useQuery({
    query: LIST_DASHBOARDS_QUERY,
    variables: { first: 25, after: cursor },
    pause: isSearching,
  });

  const [searchResult] = useQuery({
    query: SEARCH_DASHBOARDS_QUERY,
    variables: { query: search, first: 25, after: cursor },
    pause: !isSearching,
  });

  const [, createDashboard] = useMutation(CREATE_DASHBOARD_MUTATION);

  const result = isSearching ? searchResult : listResult;
  const connection = isSearching
    ? result.data?.searchDashboards
    : result.data?.allDashboards;

  const dashboards: DashboardRow[] = connection?.nodes || [];
  const pageInfo = connection?.pageInfo;
  const totalCount = connection?.totalCount ?? 0;

  function resetForm() {
    setFormName("");
    setFormLanguage("python");
  }

  async function handleCreate(e: FormEvent) {
    e.preventDefault();
    setSubmitting(true);

    try {
      const code =
        formLanguage === "markdown"
          ? `# ${formName}\n\n<grid cols="3">\n<bigvalue title="Metric A" value="1,234" />\n<bigvalue title="Metric B" value="567" />\n<bigvalue title="Metric C" value="$89" />\n</grid>\n\n## Trend\n\n<linechart x="month" y="value" data='[{"month":"Jan","value":10},{"month":"Feb","value":25},{"month":"Mar","value":18},{"month":"Apr","value":32}]' title="Monthly" />\n\n## Breakdown\n\n<barchart x="category" y="count" data='[{"category":"A","count":40},{"category":"B","count":25},{"category":"C","count":35}]' title="By Category" />\n`
          : undefined;

      const res = await createDashboard({
        input: {
          dashboard: {
            name: formName,
            language: formLanguage,
            ...(code && { code }),
          },
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      const newId = res.data?.createDashboard?.dashboard?.rowId;
      setDialogOpen(false);
      resetForm();

      if (newId) {
        navigate(`/admin/analytics/${newId}`);
      }

      toast.success("Dashboard created.");
    } catch {
      toast.error("Failed to create dashboard.");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<DashboardRow>[] = [
    {
      header: "Name",
      cell: (row) => <span className="font-medium">{row.name}</span>,
    },
    {
      header: "Language",
      cell: (row) => (
        <Badge variant="outline">
          {row.language === "python" ? "Python" : row.language === "r" ? "R" : "Markdown"}
        </Badge>
      ),
    },
    {
      header: "Owner",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">
          {row.userByUserId?.name ?? "-"}
        </span>
      ),
    },
    {
      header: "Created",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">
          {new Date(row.createdAt).toLocaleDateString()}
        </span>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-bold">Dashboards</h2>
        <p className="text-muted-foreground">
          Analytics dashboards ({totalCount} total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={dashboards}
        loading={result.fetching}
        searchPlaceholder="Search dashboards..."
        searchValue={search}
        onSearchChange={(val) => {
          setSearch(val);
          setCursor(null);
          setPrevCursors([]);
        }}
        emptyMessage="No dashboards yet."
        onRowClick={(row) => navigate(`/admin/analytics/${row.id}`)}
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
          <Button onClick={() => { resetForm(); setDialogOpen(true); }}>
            <PlusIcon />
            New Dashboard
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
            <DialogTitle>New Dashboard</DialogTitle>
            <DialogDescription>
              Create a new analytics dashboard.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleCreate} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="dash-name">Name</Label>
              <Input
                id="dash-name"
                placeholder="My Dashboard"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label>Language</Label>
              <div className="flex gap-4">
                {(["python", "r", "markdown"] as const).map((lang) => (
                  <label key={lang} className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      name="language"
                      checked={formLanguage === lang}
                      onChange={() => setFormLanguage(lang)}
                      className="h-4 w-4"
                    />
                    <span className="text-sm">
                      {lang === "python" ? "Python" : lang === "r" ? "R" : "Markdown"}
                    </span>
                  </label>
                ))}
              </div>
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => { setDialogOpen(false); resetForm(); }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Creating..." : "Create"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
