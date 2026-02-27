import { useState, useEffect, useCallback, useRef } from "react";
import { useQuery } from "urql";
import { client } from "@/lib/graphql-client";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const EVENT_LOGS_QUERY = `
  query EventLogs($level: String, $limit: Int, $before: String) {
    eventLogs(level: $level, limit: $limit, before: $before) {
      id eventType level message createdAt
    }
  }
`;

interface EventLogRow {
  id: string;
  eventType: string;
  level: string;
  message: string;
  createdAt: string;
}

const LEVEL_OPTIONS = ["All", "Error", "Info", "Debug", "Warn"];

function levelBadgeVariant(level: string): "default" | "secondary" | "destructive" {
  switch (level) {
    case "Error":
      return "destructive";
    case "Warn":
      return "secondary";
    default:
      return "default";
  }
}

function formatTimestamp(iso: string): string {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

export function Logs() {
  const [levelFilter, setLevelFilter] = useState("All");
  const [allRows, setAllRows] = useState<EventLogRow[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const PAGE_SIZE = 100;
  const headIdRef = useRef<string | null>(null);

  const variables = {
    level: levelFilter === "All" ? null : levelFilter,
    limit: PAGE_SIZE,
    before: null as string | null,
  };

  const [result, reexecute] = useQuery({ query: EVENT_LOGS_QUERY, variables });

  // Initial load / filter change: replace all rows
  useEffect(() => {
    const rows: EventLogRow[] = result.data?.eventLogs || [];
    setAllRows(rows);
    setHasMore(rows.length >= PAGE_SIZE);
    if (rows.length > 0) {
      headIdRef.current = rows[0].id;
    }
  }, [result.data]);

  function refetch() {
    reexecute({ requestPolicy: "network-only" });
  }

  // Auto-refresh: prepend only newer entries
  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const res = await client
          .query(EVENT_LOGS_QUERY, {
            level: levelFilter === "All" ? null : levelFilter,
            limit: PAGE_SIZE,
            before: null,
          }, { requestPolicy: "network-only" })
          .toPromise();

        const fresh: EventLogRow[] = res.data?.eventLogs || [];
        if (fresh.length === 0) return;

        const currentHeadId = headIdRef.current;
        if (!currentHeadId) {
          setAllRows(fresh);
          setHasMore(fresh.length >= PAGE_SIZE);
          headIdRef.current = fresh[0].id;
          return;
        }

        const newEntries = fresh.filter((r) => {
          try { return BigInt(r.id) > BigInt(currentHeadId); } catch { return false; }
        });
        if (newEntries.length > 0) {
          setAllRows((prev) => [...newEntries, ...prev]);
          headIdRef.current = newEntries[0].id;
        }
      } catch {
      }
    }, 5000);
    return () => clearInterval(interval);
  }, [levelFilter]);

  useEffect(() => {
    headIdRef.current = null;
    refetch();
  }, [levelFilter]);

  const loadMore = useCallback(async () => {
    if (allRows.length === 0) return;
    const lastId = allRows[allRows.length - 1].id;
    const res = await client
      .query(EVENT_LOGS_QUERY, {
        level: levelFilter === "All" ? null : levelFilter,
        limit: PAGE_SIZE,
        before: lastId,
      }, { requestPolicy: "network-only" })
      .toPromise();

    const older: EventLogRow[] = res.data?.eventLogs || [];
    setAllRows((prev) => [...prev, ...older]);
    setHasMore(older.length >= PAGE_SIZE);
  }, [allRows, levelFilter]);

  const columns: Column<EventLogRow>[] = [
    {
      header: "Timestamp",
      cell: (row) => (
        <span className="text-sm text-muted-foreground whitespace-nowrap">
          {formatTimestamp(row.createdAt)}
        </span>
      ),
    },
    {
      header: "Level",
      cell: (row) => (
        <Badge variant={levelBadgeVariant(row.level)}>{row.level}</Badge>
      ),
    },
    {
      header: "Type",
      cell: (row) => (
        <span className="text-sm">{row.eventType}</span>
      ),
    },
    {
      header: "Message",
      cell: (row) => (
        <span
          className="text-sm truncate max-w-[500px] block"
          title={row.message}
        >
          {row.message}
        </span>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Logs</h2>
          <p className="text-muted-foreground">
            Runtime event logs.
          </p>
        </div>
        <div className="flex gap-2 items-center">
          <select
            className="border-input bg-transparent flex h-9 rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
            value={levelFilter}
            onChange={(e) => setLevelFilter(e.target.value)}
          >
            {LEVEL_OPTIONS.map((opt) => (
              <option key={opt} value={opt}>
                {opt === "All" ? "All Levels" : opt}
              </option>
            ))}
          </select>
          <Button variant="outline" onClick={refetch}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <DataTable
        columns={columns}
        data={allRows}
        loading={result.fetching && allRows.length === 0}
        emptyMessage="No log entries found."
      />

      {hasMore && (
        <div className="flex justify-center">
          <Button variant="outline" onClick={loadMore}>
            Load More
          </Button>
        </div>
      )}
    </div>
  );
}
