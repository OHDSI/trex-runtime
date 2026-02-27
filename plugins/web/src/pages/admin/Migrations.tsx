import { useEffect, useState } from "react";
import { useQuery, useMutation } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon, ChevronDownIcon, ChevronRightIcon } from "lucide-react";
import { toast } from "sonner";

const TREX_MIGRATIONS_QUERY = `
  query TrexMigrations {
    trexMigrations {
      pluginName
      schema
      database
      currentVersion
      totalMigrations
      appliedCount
      pendingCount
      migrations { version name status appliedOn checksum }
    }
  }
`;

const RUN_MIGRATIONS_MUTATION = `
  mutation RunPluginMigrations($pluginName: String) {
    runPluginMigrations(pluginName: $pluginName) {
      success
      error
      results { version name status }
    }
  }
`;

interface MigrationRow {
  version: number;
  name: string;
  status: string;
  appliedOn: string | null;
  checksum: string | null;
}

interface MigrationSummary {
  pluginName: string;
  schema: string;
  database: string;
  currentVersion: number | null;
  totalMigrations: number;
  appliedCount: number;
  pendingCount: number;
  migrations: MigrationRow[];
}

export function Migrations() {
  const [result, reexecute] = useQuery({ query: TREX_MIGRATIONS_QUERY });
  const [, runMigrations] = useMutation(RUN_MIGRATIONS_MUTATION);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const summaries: MigrationSummary[] = result.data?.trexMigrations || [];

  function refetch() {
    reexecute({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetch, 30000);
    return () => clearInterval(interval);
  }, []);

  function toggleExpand(pluginName: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(pluginName)) {
        next.delete(pluginName);
      } else {
        next.add(pluginName);
      }
      return next;
    });
  }

  async function handleRunMigrations() {
    const res = await runMigrations({});
    if (res.data?.runPluginMigrations?.success) {
      const results = res.data.runPluginMigrations.results || [];
      const applied = results.filter((r: any) => r.status === "applied").length;
      if (applied > 0) {
        toast.success(`${applied} migration(s) applied`);
      } else {
        toast.info("All migrations are up to date");
      }
      refetch();
    } else {
      toast.error(res.data?.runPluginMigrations?.error || "Migration failed");
    }
  }

  const totalPending = summaries.reduce((sum, s) => sum + s.pendingCount, 0);

  const columns: Column<MigrationSummary>[] = [
    {
      header: "",
      cell: (row) => (
        <button
          onClick={() => toggleExpand(row.pluginName)}
          className="p-1 hover:bg-accent rounded"
        >
          {expanded.has(row.pluginName) ? (
            <ChevronDownIcon className="h-4 w-4" />
          ) : (
            <ChevronRightIcon className="h-4 w-4" />
          )}
        </button>
      ),
    },
    {
      header: "Plugin",
      cell: (row) => (
        <span className="text-sm font-medium">{row.pluginName}</span>
      ),
    },
    {
      header: "Schema",
      cell: (row) => (
        <code className="text-xs font-mono">{row.schema}</code>
      ),
    },
    {
      header: "Database",
      cell: (row) => (
        <code className="text-xs font-mono">{row.database}</code>
      ),
    },
    {
      header: "Schema Version",
      cell: (row) => (
        <span className="text-sm">{row.currentVersion ?? "-"}</span>
      ),
    },
    {
      header: "Applied",
      cell: (row) => (
        <Badge variant="default">{row.appliedCount}</Badge>
      ),
    },
    {
      header: "Pending",
      cell: (row) => (
        <Badge variant={row.pendingCount > 0 ? "destructive" : "secondary"}>
          {row.pendingCount}
        </Badge>
      ),
    },
    {
      header: "Status",
      cell: (row) =>
        row.pendingCount > 0 ? (
          <Badge variant="outline">Pending</Badge>
        ) : (
          <Badge variant="default">Up to date</Badge>
        ),
    },
  ];

  const migrationColumns: Column<MigrationRow>[] = [
    {
      header: "Version",
      cell: (row) => <span className="text-sm font-mono">{row.version}</span>,
    },
    {
      header: "Name",
      cell: (row) => <span className="text-sm">{row.name}</span>,
    },
    {
      header: "Status",
      cell: (row) => (
        <Badge
          variant={
            row.status === "applied"
              ? "default"
              : row.status === "pending"
              ? "outline"
              : "destructive"
          }
        >
          {row.status}
        </Badge>
      ),
    },
    {
      header: "Applied On",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">
          {row.appliedOn || "-"}
        </span>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Migrations</h2>
          <p className="text-muted-foreground">
            {summaries.length} migration source{summaries.length === 1 ? "" : "s"}
            {totalPending > 0 && `, ${totalPending} pending`}
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={handleRunMigrations}>
            Run Migrations
          </Button>
          <Button variant="outline" onClick={refetch}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <DataTable
        columns={columns}
        data={summaries}
        loading={result.fetching}
        emptyMessage="No migration sources found."
      />

      {summaries
        .filter((s) => expanded.has(s.pluginName))
        .map((s) => (
          <div key={s.pluginName} className="pl-8">
            <h3 className="text-sm font-semibold mb-2">
              {s.pluginName} — individual migrations
            </h3>
            <DataTable
              columns={migrationColumns}
              data={s.migrations}
              loading={false}
              emptyMessage="No migrations."
            />
          </div>
        ))}
    </div>
  );
}
