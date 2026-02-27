import { useState, useEffect, type FormEvent } from "react";
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
import { Link } from "react-router-dom";
import { PlayIcon, SquareIcon, RefreshCwIcon, RotateCwIcon } from "lucide-react";

const TREX_NODES_QUERY = `
  query TrexNodes {
    trexNodes { nodeId nodeName gossipAddr dataNode status }
  }
`;

const TREX_SERVICES_QUERY = `
  query TrexServices {
    trexServices { nodeName serviceName host port status uptimeSeconds config }
  }
`;

const TREX_CLUSTER_STATUS_QUERY = `
  query TrexClusterStatus {
    trexClusterStatus { totalNodes activeQueries queuedQueries memoryUtilizationPct }
  }
`;

const START_SERVICE_MUTATION = `
  mutation StartService($extension: String!, $config: String!) {
    startService(extension: $extension, config: $config) { success message error }
  }
`;

const STOP_SERVICE_MUTATION = `
  mutation StopService($extension: String!) {
    stopService(extension: $extension) { success message error }
  }
`;

const RESTART_SERVICE_MUTATION = `
  mutation RestartService($extension: String!, $config: String!) {
    restartService(extension: $extension, config: $config) { success message error }
  }
`;

const ETL_PIPELINES_QUERY = `
  query EtlPipelines {
    etlPipelines { name state mode connection publication snapshot rowsReplicated lastActivity error }
  }
`;

const STOP_ETL_PIPELINE_MUTATION = `
  mutation StopEtlPipeline($name: String!) {
    stopEtlPipeline(name: $name) { success message error }
  }
`;

const KNOWN_EXTENSIONS = [
  { id: "flight", label: "Flight", defaultPort: "50051" },
  { id: "pgwire", label: "PGWire", defaultPort: "5432" },
  { id: "trexas", label: "Trexas", defaultPort: "8080" },
  { id: "chdb", label: "ChDB", defaultPort: "9000" },
  { id: "etl", label: "ETL", defaultPort: "8081" },
  { id: "distributed-scheduler", label: "Distributed Scheduler", defaultPort: "8082" },
  { id: "distributed-executor", label: "Distributed Executor", defaultPort: "8083" },
];

interface NodeRow {
  nodeId: string;
  nodeName: string;
  gossipAddr: string;
  dataNode: string;
  status: string;
}

interface ServiceRow {
  nodeName: string;
  serviceName: string;
  host: string;
  port: string;
  status: string;
  uptimeSeconds: string;
  config: string | null;
}

interface ClusterStatus {
  totalNodes: string;
  activeQueries: string;
  queuedQueries: string;
  memoryUtilizationPct: string;
}

interface EtlPipelineRow {
  name: string;
  state: string;
  mode: string;
  connection: string;
  publication: string;
  snapshot: string;
  rowsReplicated: string;
  lastActivity: string;
  error: string | null;
}

function statusBadgeVariant(status: string): "default" | "secondary" | "destructive" {
  const s = status.toLowerCase();
  if (s === "active" || s === "running") return "default";
  if (s === "stopped") return "secondary";
  return "destructive";
}

function formatUptime(seconds: string): string {
  const s = parseInt(seconds, 10);
  if (isNaN(s) || s < 0) return "-";
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return `${h}h ${m}m`;
}

export function Services() {
  const [startOpen, setStartOpen] = useState(false);
  const [formExtension, setFormExtension] = useState("");
  const [formHost, setFormHost] = useState("0.0.0.0");
  const [formPort, setFormPort] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const [nodesResult, reexecuteNodes] = useQuery({ query: TREX_NODES_QUERY });
  const [servicesResult, reexecuteServices] = useQuery({ query: TREX_SERVICES_QUERY });
  const [clusterResult, reexecuteCluster] = useQuery({ query: TREX_CLUSTER_STATUS_QUERY });

  const [etlResult, reexecuteEtl] = useQuery({ query: ETL_PIPELINES_QUERY });

  const [, startService] = useMutation(START_SERVICE_MUTATION);
  const [, stopService] = useMutation(STOP_SERVICE_MUTATION);
  const [, restartService] = useMutation(RESTART_SERVICE_MUTATION);
  const [, stopEtlPipeline] = useMutation(STOP_ETL_PIPELINE_MUTATION);

  const nodes: NodeRow[] = nodesResult.data?.trexNodes || [];
  const services: ServiceRow[] = servicesResult.data?.trexServices || [];
  const clusterStatus: ClusterStatus | null = clusterResult.data?.trexClusterStatus || null;
  const etlPipelines: EtlPipelineRow[] = etlResult.data?.etlPipelines || [];

  function refetchAll() {
    reexecuteNodes({ requestPolicy: "network-only" });
    reexecuteServices({ requestPolicy: "network-only" });
    reexecuteCluster({ requestPolicy: "network-only" });
    reexecuteEtl({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetchAll, 10000);
    return () => clearInterval(interval);
  }, []);

  async function handleStartService(e: FormEvent) {
    e.preventDefault();
    if (!formExtension) return;
    setSubmitting(true);
    try {
      const config = JSON.stringify({ host: formHost, port: formPort });
      const res = await startService({ extension: formExtension, config });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.startService;
      if (!data?.success) {
        toast.error(data?.error || "Failed to start service");
        return;
      }
      toast.success(data.message || `Started ${formExtension}`);
      setStartOpen(false);
      setFormExtension("");
      setFormHost("0.0.0.0");
      setFormPort("");
      refetchAll();
    } catch (err: any) {
      toast.error(err.message || "Failed to start service");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleStopService(serviceName: string) {
    try {
      const res = await stopService({ extension: serviceName });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.stopService;
      if (!data?.success) {
        toast.error(data?.error || "Failed to stop service");
        return;
      }
      toast.success(data.message || `Stopped ${serviceName}`);
      refetchAll();
    } catch (err: any) {
      toast.error(err.message || "Failed to stop service");
    }
  }

  async function handleRestartService(serviceName: string, config: string | null) {
    try {
      const res = await restartService({ extension: serviceName, config: config || "{}" });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.restartService;
      if (!data?.success) {
        toast.error(data?.error || "Failed to restart service");
        return;
      }
      toast.success(data.message || `Restarted ${serviceName}`);
      refetchAll();
    } catch (err: any) {
      toast.error(err.message || "Failed to restart service");
    }
  }

  async function handleStopEtl(pipelineName: string) {
    try {
      const res = await stopEtlPipeline({ name: pipelineName });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.stopEtlPipeline;
      if (!data?.success) {
        toast.error(data?.error || "Failed to stop pipeline");
        return;
      }
      toast.success(data.message || `Pipeline '${pipelineName}' stopped`);
      refetchAll();
    } catch (err: any) {
      toast.error(err.message || "Failed to stop pipeline");
    }
  }

  const etlColumns: Column<EtlPipelineRow>[] = [
    {
      header: "Name",
      cell: (row) => <code className="text-xs font-mono">{row.name}</code>,
    },
    {
      header: "State",
      cell: (row) => (
        <Badge variant={statusBadgeVariant(row.state)}>{row.state}</Badge>
      ),
    },
    {
      header: "Mode",
      cell: (row) => <Badge variant="secondary">{row.mode}</Badge>,
    },
    {
      header: "Rows Replicated",
      cell: (row) => (
        <span className="text-sm font-mono">{Number(row.rowsReplicated).toLocaleString()}</span>
      ),
    },
    {
      header: "Error",
      cell: (row) =>
        row.error ? (
          <span className="text-sm text-destructive truncate max-w-[200px] block" title={row.error}>
            {row.error}
          </span>
        ) : (
          <span className="text-sm text-muted-foreground">-</span>
        ),
    },
    {
      header: "Actions",
      cell: (row) => (
        <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
          {!["stopped", "error"].includes(row.state.toLowerCase()) && (
            <Button variant="outline" size="sm" onClick={() => handleStopEtl(row.name)}>
              <SquareIcon className="h-3 w-3 mr-1" />
              Stop
            </Button>
          )}
        </div>
      ),
    },
  ];

  const nodeColumns: Column<NodeRow>[] = [
    {
      header: "Node Name",
      cell: (row) => <code className="text-xs font-mono">{row.nodeName}</code>,
    },
    {
      header: "Gossip Address",
      cell: (row) => <span className="text-sm">{row.gossipAddr}</span>,
    },
    {
      header: "Data Node",
      cell: (row) => (
        <Badge variant={row.dataNode.toLowerCase() === "true" ? "default" : "secondary"}>
          {row.dataNode.toLowerCase() === "true" ? "Yes" : "No"}
        </Badge>
      ),
    },
    {
      header: "Status",
      cell: (row) => (
        <Badge variant={statusBadgeVariant(row.status)}>{row.status}</Badge>
      ),
    },
  ];

  const serviceColumns: Column<ServiceRow>[] = [
    {
      header: "Node",
      cell: (row) => <code className="text-xs font-mono">{row.nodeName}</code>,
    },
    {
      header: "Service",
      cell: (row) => <span className="text-sm font-medium">{row.serviceName}</span>,
    },
    {
      header: "Host:Port",
      cell: (row) => <code className="text-xs font-mono">{row.host}:{row.port}</code>,
    },
    {
      header: "Status",
      cell: (row) => (
        <Badge variant={statusBadgeVariant(row.status)}>{row.status}</Badge>
      ),
    },
    {
      header: "Uptime",
      cell: (row) => <span className="text-sm">{formatUptime(row.uptimeSeconds)}</span>,
    },
    {
      header: "Actions",
      cell: (row) => (
        <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
          {row.status.toLowerCase() === "running" && (
            <>
              <Button variant="outline" size="sm" onClick={() => handleRestartService(row.serviceName, row.config)}>
                <RotateCwIcon className="h-3 w-3 mr-1" />
                Restart
              </Button>
              <Button variant="outline" size="sm" onClick={() => handleStopService(row.serviceName)}>
                <SquareIcon className="h-3 w-3 mr-1" />
                Stop
              </Button>
            </>
          )}
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Services</h2>
          <p className="text-muted-foreground">
            {clusterStatus
              ? `${clusterStatus.totalNodes} node${clusterStatus.totalNodes === "1" ? "" : "s"}, ${clusterStatus.activeQueries} active quer${clusterStatus.activeQueries === "1" ? "y" : "ies"}, ${clusterStatus.queuedQueries} queued, ${clusterStatus.memoryUtilizationPct}% memory`
              : "Cluster nodes and services"}
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={refetchAll}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
          <Button onClick={() => setStartOpen(true)}>
            <PlayIcon className="h-4 w-4 mr-1" />
            Start Service
          </Button>
        </div>
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Nodes</h3>
        <DataTable
          columns={nodeColumns}
          data={nodes}
          loading={nodesResult.fetching}
          emptyMessage="No cluster nodes found. The db extension may not be running."
        />
      </div>

      <div className="space-y-2">
        <h3 className="text-lg font-semibold">Running Services</h3>
        <DataTable
          columns={serviceColumns}
          data={services}
          loading={servicesResult.fetching}
          emptyMessage="No services running."
        />
      </div>

      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold">ETL Pipelines</h3>
          <Link to="/admin/etl" className="text-sm text-muted-foreground hover:underline">
            View all
          </Link>
        </div>
        <DataTable
          columns={etlColumns}
          data={etlPipelines}
          loading={etlResult.fetching}
          emptyMessage="No ETL pipelines running."
        />
      </div>

      <Dialog
        open={startOpen}
        onOpenChange={(open) => {
          setStartOpen(open);
          if (!open) {
            setFormExtension("");
            setFormHost("0.0.0.0");
            setFormPort("");
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start Service</DialogTitle>
            <DialogDescription>
              Select an extension to start as a service on the cluster.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleStartService} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="svc-extension">Extension</Label>
              <select
                id="svc-extension"
                className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
                value={formExtension}
                onChange={(e) => {
                  setFormExtension(e.target.value);
                  const ext = KNOWN_EXTENSIONS.find((x) => x.id === e.target.value);
                  if (ext) setFormPort(ext.defaultPort);
                }}
                required
              >
                <option value="">Select an extension...</option>
                {KNOWN_EXTENSIONS.map((ext) => (
                  <option key={ext.id} value={ext.id}>
                    {ext.label}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="svc-host">Host</Label>
              <Input
                id="svc-host"
                placeholder="0.0.0.0"
                value={formHost}
                onChange={(e) => setFormHost(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="svc-port">Port</Label>
              <Input
                id="svc-port"
                type="number"
                placeholder="50051"
                value={formPort}
                onChange={(e) => setFormPort(e.target.value)}
                required
              />
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setStartOpen(false);
                  setFormExtension("");
                  setFormHost("0.0.0.0");
                  setFormPort("");
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Starting..." : "Start"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
