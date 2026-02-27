import { useEffect } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon } from "lucide-react";

const UI_PLUGINS_QUERY = `
  query UiPlugins {
    uiPluginRoutes { pluginName urlPrefix fsPath }
    uiPluginsJson
  }
`;

interface UiRouteRow {
  pluginName: string;
  urlPrefix: string;
  fsPath: string;
}

export function UiPlugins() {
  const [result, reexecute] = useQuery({ query: UI_PLUGINS_QUERY });
  const routes: UiRouteRow[] = result.data?.uiPluginRoutes || [];
  const pluginsJson = result.data?.uiPluginsJson;

  function refetch() {
    reexecute({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetch, 30000);
    return () => clearInterval(interval);
  }, []);

  const columns: Column<UiRouteRow>[] = [
    {
      header: "Plugin",
      cell: (row) => <span className="text-sm font-medium">{row.pluginName}</span>,
    },
    {
      header: "URL Prefix",
      cell: (row) => <code className="text-xs font-mono">{row.urlPrefix}</code>,
    },
    {
      header: "Filesystem Path",
      cell: (row) => (
        <code className="text-xs font-mono" title={row.fsPath}>
          {row.fsPath.length > 50 ? row.fsPath.slice(0, 50) + "\u2026" : row.fsPath}
        </code>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">UI Plugins</h2>
          <p className="text-muted-foreground">
            {routes.length} registered route{routes.length === 1 ? "" : "s"}
          </p>
        </div>
        <Button variant="outline" onClick={refetch}>
          <RefreshCwIcon className="h-4 w-4" />
        </Button>
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Registered UI Routes</h3>
        <DataTable
          columns={columns}
          data={routes}
          loading={result.fetching}
          emptyMessage="No UI plugin routes registered."
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">UI Plugins Config</h3>
        {result.fetching ? (
          <div className="flex justify-center py-8">
            <div className="animate-spin h-6 w-6 border-2 border-primary border-t-transparent rounded-full" />
          </div>
        ) : pluginsJson && Object.keys(pluginsJson).length > 0 ? (
          <pre className="rounded-md border bg-muted p-4 text-xs font-mono overflow-auto max-h-96">
            {JSON.stringify(pluginsJson, null, 2)}
          </pre>
        ) : (
          <p className="text-sm text-muted-foreground py-4">No UI plugins configuration.</p>
        )}
      </div>
    </div>
  );
}
