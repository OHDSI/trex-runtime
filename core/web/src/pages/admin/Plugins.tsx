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
import { PlusIcon, RefreshCwIcon, DownloadIcon, Trash2Icon, ArrowUpCircleIcon, LoaderIcon } from "lucide-react";

const AVAILABLE_PLUGINS = `
  query {
    availablePlugins {
      name
      packageName
      version
      activeVersion
      active
      installed
      pendingRestart
      description
      registryVersion
    }
  }
`;

const INSTALL_PLUGIN = `
  mutation InstallPlugin($packageSpec: String!) {
    installPlugin(packageSpec: $packageSpec) {
      success
      error
      results
    }
  }
`;

const UNINSTALL_PLUGIN = `
  mutation UninstallPlugin($packageName: String!) {
    uninstallPlugin(packageName: $packageName) {
      success
      error
    }
  }
`;

const UPDATE_PLUGIN = `
  mutation UpdatePlugin($packageName: String!, $version: String) {
    updatePlugin(packageName: $packageName, version: $version) {
      success
      error
      results
    }
  }
`;

interface PluginRow {
  name: string;
  packageName?: string;
  version: string | null;
  activeVersion: string | null;
  active: boolean;
  installed: boolean;
  pendingRestart: boolean;
  description?: string;
  registryVersion?: string;
}

type StatusInfo = { label: string; variant: "default" | "secondary" | "destructive" | "outline" };

function getStatus(row: PluginRow): StatusInfo {
  if (row.active && row.installed && !row.pendingRestart) {
    return { label: "Active", variant: "default" };
  }
  if (row.active && row.installed && row.pendingRestart) {
    return { label: "Pending Restart", variant: "secondary" };
  }
  if (row.installed && !row.active) {
    return { label: "Installed", variant: "secondary" };
  }
  if (!row.installed && row.active) {
    return { label: "Removed", variant: "destructive" };
  }
  return { label: "Available", variant: "outline" };
}

export function Plugins() {
  const [search, setSearch] = useState("");
  const [installOpen, setInstallOpen] = useState(false);
  const [installSpec, setInstallSpec] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [needsRestart, setNeedsRestart] = useState(false);
  const [busyPlugins, setBusyPlugins] = useState<Record<string, string>>({});

  const [result, reexecuteQuery] = useQuery({ query: AVAILABLE_PLUGINS });
  const [, installPlugin] = useMutation(INSTALL_PLUGIN);
  const [, uninstallPlugin] = useMutation(UNINSTALL_PLUGIN);
  const [, updatePlugin] = useMutation(UPDATE_PLUGIN);

  const plugins: PluginRow[] = result.data?.availablePlugins || [];
  const loading = result.fetching;

  function refetch() {
    reexecuteQuery({ requestPolicy: "network-only" });
  }

  function setBusy(name: string, action: string) {
    setBusyPlugins((prev) => ({ ...prev, [name]: action }));
  }

  function clearBusy(name: string) {
    setBusyPlugins((prev) => {
      const next = { ...prev };
      delete next[name];
      return next;
    });
  }

  const filtered = search
    ? plugins.filter((p) => (p.packageName || p.name).toLowerCase().includes(search.toLowerCase()))
    : plugins;

  async function handleInstall(e: FormEvent) {
    e.preventDefault();
    if (!installSpec.trim()) return;
    setSubmitting(true);
    try {
      const res = await installPlugin({ packageSpec: installSpec.trim() });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.installPlugin;
      if (!data?.success) {
        toast.error(data?.error || "Install failed");
        return;
      }
      toast.success(`Installed ${installSpec.trim()}`);
      setInstallOpen(false);
      setInstallSpec("");
      setNeedsRestart(true);
      refetch();
    } catch (err: any) {
      toast.error(err.message || "Install failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleUninstall(name: string) {
    setBusy(name, "Uninstalling...");
    try {
      const res = await uninstallPlugin({ packageName: name });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.uninstallPlugin;
      if (!data?.success) {
        toast.error(data?.error || "Uninstall failed");
        return;
      }
      toast.success(`Uninstalled ${name}`);
      setNeedsRestart(true);
      refetch();
    } catch (err: any) {
      toast.error(err.message || "Uninstall failed");
    } finally {
      clearBusy(name);
    }
  }

  async function handleUpdate(name: string, packageName?: string) {
    setBusy(name, "Updating...");
    try {
      const res = await updatePlugin({ packageName: packageName || name });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.updatePlugin;
      if (!data?.success) {
        toast.error(data?.error || "Update failed");
        return;
      }
      toast.success(`Updated ${name}`);
      setNeedsRestart(true);
      refetch();
    } catch (err: any) {
      toast.error(err.message || "Update failed");
    } finally {
      clearBusy(name);
    }
  }

  async function handleInstallFromRegistry(name: string, packageName?: string) {
    setBusy(name, "Installing...");
    try {
      const res = await installPlugin({ packageSpec: packageName || name });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.installPlugin;
      if (!data?.success) {
        toast.error(data?.error || "Install failed");
        return;
      }
      toast.success(`Installed ${packageName || name}`);
      setNeedsRestart(true);
      refetch();
    } catch (err: any) {
      toast.error(err.message || "Install failed");
    } finally {
      clearBusy(name);
    }
  }

  const columns: Column<PluginRow>[] = [
    {
      header: "Name",
      cell: (row) => (
        <div>
          <code className="text-xs font-mono">{row.packageName || row.name}</code>
          {row.description && (
            <p className="text-xs text-muted-foreground mt-0.5 max-w-xs truncate" title={row.description}>{row.description}</p>
          )}
        </div>
      ),
    },
    {
      header: "Version",
      cell: (row) => {
        if (!row.version) return <span className="text-sm">—</span>;
        const display = row.version.length > 16 ? row.version.slice(0, 16) + "..." : row.version;
        return <span className="text-sm" title={row.version}>{display}</span>;
      },
    },
    {
      header: "Registry Version",
      cell: (row) => {
        if (!row.registryVersion) return <span className="text-sm">—</span>;
        const display = row.registryVersion.length > 16 ? row.registryVersion.slice(0, 16) + "..." : row.registryVersion;
        return (
          <span className="text-sm" title={row.registryVersion}>
            {display}
            {row.installed && row.version && row.registryVersion !== row.version && (
              <Badge variant="outline" className="ml-1 text-[10px] px-1 py-0">new</Badge>
            )}
          </span>
        );
      },
    },
    {
      header: "Status",
      cell: (row) => {
        const busy = busyPlugins[row.name];
        if (busy) {
          return (
            <span className="flex items-center gap-1.5 text-sm text-muted-foreground">
              <LoaderIcon className="h-3 w-3 animate-spin" />
              {busy}
            </span>
          );
        }
        const status = getStatus(row);
        return <Badge variant={status.variant}>{status.label}</Badge>;
      },
    },
    {
      header: "Actions",
      cell: (row) => {
        const busy = !!busyPlugins[row.name];
        return (
          <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
            {!row.installed && row.registryVersion && (
              <Button variant="outline" size="sm" disabled={busy} onClick={() => handleInstallFromRegistry(row.name, row.packageName)}>
                <DownloadIcon className="h-3 w-3 mr-1" />
                Install
              </Button>
            )}
            {row.installed && row.registryVersion && row.registryVersion > (row.version || "") && (
              <Button variant="outline" size="sm" disabled={busy} onClick={() => handleUpdate(row.name, row.packageName)}>
                <ArrowUpCircleIcon className="h-3 w-3 mr-1" />
                Update
              </Button>
            )}
            {row.installed && (
              <Button variant="outline" size="sm" disabled={busy} onClick={() => handleUninstall(row.name)}>
                <Trash2Icon className="h-3 w-3 mr-1" />
                Uninstall
              </Button>
            )}
          </div>
        );
      },
    },
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-bold">Plugins</h2>
        <p className="text-muted-foreground">
          Manage installed plugins ({plugins.length} total)
        </p>
      </div>

      {needsRestart && (
        <div className="rounded-md border border-blue-200 bg-blue-50 dark:border-blue-900 dark:bg-blue-950 px-4 py-3 text-sm text-blue-800 dark:text-blue-200">
          A server restart is needed for plugin changes to take effect.
        </div>
      )}

      <DataTable
        columns={columns}
        data={filtered}
        loading={loading}
        searchPlaceholder="Filter plugins..."
        searchValue={search}
        onSearchChange={setSearch}
        emptyMessage="No plugins found."
        actions={
          <>
            <Button variant="outline" onClick={refetch}>
              <RefreshCwIcon className="h-4 w-4" />
            </Button>
            <Button onClick={() => setInstallOpen(true)}>
              <PlusIcon />
              Install Plugin
            </Button>
          </>
        }
      />

      <Dialog
        open={installOpen}
        onOpenChange={(open) => {
          setInstallOpen(open);
          if (!open) setInstallSpec("");
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Install Plugin</DialogTitle>
            <DialogDescription>
              Enter a package name or spec (e.g. @trex/etl or @trex/etl@1.0.0).
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleInstall} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="pkg-spec">Package</Label>
              <Input
                id="pkg-spec"
                placeholder="@trex/my-plugin@1.0.0"
                value={installSpec}
                onChange={(e) => setInstallSpec(e.target.value)}
                required
              />
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setInstallOpen(false);
                  setInstallSpec("");
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Installing..." : "Install"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
