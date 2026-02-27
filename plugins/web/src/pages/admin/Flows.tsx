import { useEffect } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const REGISTERED_FLOWS_QUERY = `
  query RegisteredFlows {
    registeredFlows { name entrypoint image tags }
  }
`;

interface FlowRow {
  name: string;
  entrypoint: string;
  image: string;
  tags: string[];
}

export function Flows() {
  const [result, reexecute] = useQuery({ query: REGISTERED_FLOWS_QUERY });
  const flows: FlowRow[] = result.data?.registeredFlows || [];

  function refetch() {
    reexecute({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetch, 30000);
    return () => clearInterval(interval);
  }, []);

  const columns: Column<FlowRow>[] = [
    {
      header: "Flow Name",
      cell: (row) => <span className="text-sm font-medium">{row.name}</span>,
    },
    {
      header: "Entrypoint",
      cell: (row) => <code className="text-xs font-mono">{row.entrypoint}</code>,
    },
    {
      header: "Image",
      cell: (row) => <code className="text-xs font-mono">{row.image || "-"}</code>,
    },
    {
      header: "Tags",
      cell: (row) => (
        <div className="flex flex-wrap gap-1">
          {row.tags.length > 0
            ? row.tags.map((t) => (
                <Badge key={t} variant="secondary">{t}</Badge>
              ))
            : <span className="text-sm text-muted-foreground">-</span>}
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Flows</h2>
          <p className="text-muted-foreground">
            {flows.length} registered flow{flows.length === 1 ? "" : "s"}
          </p>
        </div>
        <Button variant="outline" onClick={refetch}>
          <RefreshCwIcon className="h-4 w-4" />
        </Button>
      </div>

      <DataTable
        columns={columns}
        data={flows}
        loading={result.fetching}
        emptyMessage="No flows registered. Flows are deployed when PREFECT_API_URL is configured and flow plugins are installed."
      />
    </div>
  );
}
