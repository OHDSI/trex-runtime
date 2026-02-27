import { useEffect } from "react";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { RefreshCwIcon, PlayIcon } from "lucide-react";

const TREX_EXTENSIONS_QUERY = `
  query TrexExtensions {
    trexExtensions { extensionName loaded installed extensionVersion description installPath }
  }
`;

const LOAD_EXTENSION_MUTATION = `
  mutation LoadExtension($extensionName: String!) {
    loadExtension(extensionName: $extensionName) { success message error }
  }
`;

interface ExtensionRow {
  extensionName: string;
  loaded: boolean;
  installed: boolean;
  extensionVersion: string | null;
  description: string | null;
  installPath: string | null;
}

export function Extensions() {
  const [extResult, reexecuteExts] = useQuery({ query: TREX_EXTENSIONS_QUERY });
  const [, loadExtension] = useMutation(LOAD_EXTENSION_MUTATION);
  const allExtensions: ExtensionRow[] = extResult.data?.trexExtensions || [];
  const extensions = allExtensions.filter((e) => e.installed || e.loaded);

  function refetch() {
    reexecuteExts({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetch, 30000);
    return () => clearInterval(interval);
  }, []);

  async function handleLoad(extensionName: string) {
    try {
      const res = await loadExtension({ extensionName });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.loadExtension;
      if (!data?.success) {
        toast.error(data?.error || "Failed to load extension");
        return;
      }
      toast.success(data.message || `Loaded ${extensionName}`);
      refetch();
    } catch (err: any) {
      toast.error(err.message || "Failed to load extension");
    }
  }

  const loadedCount = extensions.filter((e) => e.loaded).length;

  const columns: Column<ExtensionRow>[] = [
    {
      header: "Extension",
      cell: (row) => <span className="text-sm font-medium">{row.extensionName}</span>,
    },
    {
      header: "Version",
      cell: (row) => (
        <code className="text-xs font-mono">{row.extensionVersion || "-"}</code>
      ),
    },
    {
      header: "Loaded",
      cell: (row) => (
        <Badge variant={row.loaded ? "default" : "secondary"}>
          {row.loaded ? "Yes" : "No"}
        </Badge>
      ),
    },
    {
      header: "Description",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">{row.description || "-"}</span>
      ),
    },
    {
      header: "Actions",
      cell: (row) => (
        <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
          {row.installed && !row.loaded && (
            <Button variant="outline" size="sm" onClick={() => handleLoad(row.extensionName)}>
              <PlayIcon className="h-3 w-3 mr-1" />
              Load
            </Button>
          )}
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Extensions</h2>
          <p className="text-muted-foreground">
            {loadedCount} extension{loadedCount === 1 ? "" : "s"} loaded, {extensions.length} installed
          </p>
        </div>
        <Button variant="outline" onClick={refetch}>
          <RefreshCwIcon className="h-4 w-4" />
        </Button>
      </div>

      <DataTable
        columns={columns}
        data={extensions}
        loading={extResult.fetching}
        emptyMessage="No extensions found."
      />
    </div>
  );
}
