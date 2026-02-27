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

const LIST_DATABASES_QUERY = `
  query ListDatabases($first: Int, $after: Cursor) {
    allDatabases(first: $first, after: $after, orderBy: [CREATED_AT_DESC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        id
        host
        port
        databaseName
        dialect
        enabled
        createdAt
        databaseCredentialsByDatabaseId {
          totalCount
        }
      }
    }
  }
`;

const SEARCH_DATABASES_QUERY = `
  query SearchDatabases($query: String!, $first: Int, $after: Cursor) {
    searchDatabases(query: $query, first: $first, after: $after) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        id
        host
        port
        databaseName
        dialect
        enabled
        createdAt
        databaseCredentialsByDatabaseId {
          totalCount
        }
      }
    }
  }
`;

const CREATE_DATABASE_MUTATION = `
  mutation CreateDatabase($input: CreateDatabaseInput!) {
    createDatabase(input: $input) {
      database {
        id
      }
    }
  }
`;

interface DatabaseRow {
  id: string;
  host: string;
  port: number;
  databaseName: string;
  dialect: string;
  enabled: boolean;
  createdAt: string;
  databaseCredentialsByDatabaseId: {
    totalCount: number;
  };
}

export function Databases() {
  const navigate = useNavigate();
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [addOpen, setAddOpen] = useState(false);

  const [formId, setFormId] = useState("");
  const [formHost, setFormHost] = useState("");
  const [formPort, setFormPort] = useState("5432");
  const [formDbName, setFormDbName] = useState("");
  const [formDialect, setFormDialect] = useState("postgresql");
  const [submitting, setSubmitting] = useState(false);

  const isSearching = search.length > 0;

  const [listResult] = useQuery({
    query: LIST_DATABASES_QUERY,
    variables: { first: 25, after: cursor },
    pause: isSearching,
  });

  const [searchResult] = useQuery({
    query: SEARCH_DATABASES_QUERY,
    variables: { query: search, first: 25, after: cursor },
    pause: !isSearching,
  });

  const [, createDatabase] = useMutation(CREATE_DATABASE_MUTATION);

  const result = isSearching ? searchResult : listResult;
  const connection = isSearching
    ? result.data?.searchDatabases
    : result.data?.allDatabases;

  const databases: DatabaseRow[] = connection?.nodes || [];
  const pageInfo = connection?.pageInfo;
  const totalCount = connection?.totalCount ?? 0;

  function resetForm() {
    setFormId("");
    setFormHost("");
    setFormPort("5432");
    setFormDbName("");
    setFormDialect("postgresql");
  }

  async function handleCreate(e: FormEvent) {
    e.preventDefault();

    if (!/^[A-Za-z0-9_]+$/.test(formId)) {
      toast.error("ID must contain only letters, numbers, and underscores.");
      return;
    }

    setSubmitting(true);

    try {
      const res = await createDatabase({
        input: {
          database: {
            id: formId,
            host: formHost,
            port: parseInt(formPort, 10),
            databaseName: formDbName,
            dialect: formDialect,
          },
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setAddOpen(false);
      resetForm();
      toast.success("Database added.");
    } catch {
      toast.error("Failed to add database.");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<DatabaseRow>[] = [
    {
      header: "ID",
      cell: (row) => <code className="text-xs font-mono">{row.id}</code>,
    },
    {
      header: "Host:Port",
      cell: (row) => (
        <span className="text-sm">
          {row.host}:{row.port}
        </span>
      ),
    },
    {
      header: "Database Name",
      accessorKey: "databaseName",
    },
    {
      header: "Dialect",
      cell: (row) => <Badge variant="secondary">{row.dialect}</Badge>,
    },
    {
      header: "Credentials",
      cell: (row) => (
        <span className="text-sm">
          {row.databaseCredentialsByDatabaseId.totalCount}
        </span>
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
        <h2 className="text-2xl font-bold">Federation</h2>
        <p className="text-muted-foreground">
          External database connections ({totalCount} total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={databases}
        loading={result.fetching}
        searchPlaceholder="Search databases..."
        searchValue={search}
        onSearchChange={(val) => {
          setSearch(val);
          setCursor(null);
          setPrevCursors([]);
        }}
        emptyMessage="No databases found."
        onRowClick={(row) => navigate(`/admin/databases/${row.id}`)}
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
          <Button onClick={() => setAddOpen(true)}>
            <PlusIcon />
            Add Database
          </Button>
        }
      />

      {/* Add Database Dialog */}
      <Dialog
        open={addOpen}
        onOpenChange={(open) => {
          setAddOpen(open);
          if (!open) resetForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Database</DialogTitle>
            <DialogDescription>
              Register an external database connection.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleCreate} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="db-id">ID</Label>
              <Input
                id="db-id"
                placeholder="alpdev"
                value={formId}
                onChange={(e) => setFormId(e.target.value)}
                pattern="^[A-Za-z0-9_]+$"
                required
              />
              <p className="text-xs text-muted-foreground">
                Letters, numbers, and underscores only
              </p>
            </div>

            <div className="grid grid-cols-3 gap-4">
              <div className="col-span-2 flex flex-col gap-2">
                <Label htmlFor="db-host">Host</Label>
                <Input
                  id="db-host"
                  placeholder="localhost"
                  value={formHost}
                  onChange={(e) => setFormHost(e.target.value)}
                  required
                />
              </div>
              <div className="flex flex-col gap-2">
                <Label htmlFor="db-port">Port</Label>
                <Input
                  id="db-port"
                  type="number"
                  placeholder="5432"
                  value={formPort}
                  onChange={(e) => setFormPort(e.target.value)}
                  required
                />
              </div>
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="db-name">Database Name</Label>
              <Input
                id="db-name"
                placeholder="mydb"
                value={formDbName}
                onChange={(e) => setFormDbName(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="db-dialect">Dialect</Label>
              <select
                id="db-dialect"
                className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
                value={formDialect}
                onChange={(e) => setFormDialect(e.target.value)}
              >
                <option value="postgresql">PostgreSQL</option>
                <option value="mysql">MySQL</option>
                <option value="mssql">MSSQL</option>
                <option value="oracle">Oracle</option>
                <option value="duckdb">trexsql</option>
              </select>
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setAddOpen(false);
                  resetForm();
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Adding..." : "Add Database"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
