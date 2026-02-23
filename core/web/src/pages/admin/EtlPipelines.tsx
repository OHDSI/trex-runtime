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
import { PlayIcon, SquareIcon, RefreshCwIcon } from "lucide-react";

const ETL_PIPELINES_QUERY = `
  query EtlPipelines {
    etlPipelines { name state mode connection publication snapshot rowsReplicated lastActivity error }
  }
`;

const ALL_DATABASES_QUERY = `
  query AllDatabasesForEtl {
    allDatabases(first: 100) {
      nodes {
        id
        host
        port
        databaseName
        dialect
        databaseCredentialsByDatabaseId {
          totalCount
        }
      }
    }
  }
`;

const START_ETL_PIPELINE_MUTATION = `
  mutation StartEtlPipeline(
    $name: String!
    $databaseId: String!
    $mode: String!
    $publication: String
    $schema: String
    $batchSize: Int
    $batchTimeoutMs: Int
    $retryDelayMs: Int
    $retryMaxAttempts: Int
  ) {
    startEtlPipeline(
      name: $name
      databaseId: $databaseId
      mode: $mode
      publication: $publication
      schema: $schema
      batchSize: $batchSize
      batchTimeoutMs: $batchTimeoutMs
      retryDelayMs: $retryDelayMs
      retryMaxAttempts: $retryMaxAttempts
    ) { success message error }
  }
`;

const STOP_ETL_PIPELINE_MUTATION = `
  mutation StopEtlPipeline($name: String!) {
    stopEtlPipeline(name: $name) { success message error }
  }
`;

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

interface DatabaseOption {
  id: string;
  host: string;
  port: number;
  databaseName: string;
  dialect: string;
  databaseCredentialsByDatabaseId: { totalCount: number };
}

const MODE_OPTIONS = [
  { value: "copy_and_cdc", label: "Initial Copy + CDC" },
  { value: "cdc_only", label: "CDC Only" },
  { value: "copy_only", label: "Copy Only" },
];

function stateBadgeVariant(state: string): "default" | "secondary" | "destructive" {
  const s = state.toLowerCase();
  if (s === "streaming" || s === "snapshotting") return "default";
  if (s === "starting" || s === "stopped" || s === "stopping") return "secondary";
  return "destructive";
}

function modeBadgeVariant(mode: string): "default" | "secondary" | "destructive" {
  if (mode === "copy_and_cdc") return "default";
  if (mode === "cdc_only") return "secondary";
  return "secondary";
}

function modeLabel(mode: string): string {
  const opt = MODE_OPTIONS.find((m) => m.value === mode);
  return opt?.label || mode;
}

export function EtlPipelines() {
  const [startOpen, setStartOpen] = useState(false);
  const [formName, setFormName] = useState("");
  const [formDatabaseId, setFormDatabaseId] = useState("");
  const [formMode, setFormMode] = useState("copy_and_cdc");
  const [formPublication, setFormPublication] = useState("");
  const [formSchema, setFormSchema] = useState("public");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [formBatchSize, setFormBatchSize] = useState("");
  const [formBatchTimeout, setFormBatchTimeout] = useState("");
  const [formRetryDelay, setFormRetryDelay] = useState("");
  const [formRetryMax, setFormRetryMax] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const [pipelinesResult, reexecutePipelines] = useQuery({ query: ETL_PIPELINES_QUERY });
  const [dbResult] = useQuery({ query: ALL_DATABASES_QUERY });

  const [, startEtlPipeline] = useMutation(START_ETL_PIPELINE_MUTATION);
  const [, stopEtlPipeline] = useMutation(STOP_ETL_PIPELINE_MUTATION);

  const pipelines: EtlPipelineRow[] = pipelinesResult.data?.etlPipelines || [];
  const databases: DatabaseOption[] = (dbResult.data?.allDatabases?.nodes || []).filter(
    (db: DatabaseOption) => db.databaseCredentialsByDatabaseId.totalCount > 0
  );

  function refetchAll() {
    reexecutePipelines({ requestPolicy: "network-only" });
  }

  useEffect(() => {
    const interval = setInterval(refetchAll, 5000);
    return () => clearInterval(interval);
  }, []);

  function resetForm() {
    setFormName("");
    setFormDatabaseId("");
    setFormMode("copy_and_cdc");
    setFormPublication("");
    setFormSchema("public");
    setShowAdvanced(false);
    setFormBatchSize("");
    setFormBatchTimeout("");
    setFormRetryDelay("");
    setFormRetryMax("");
  }

  async function handleStart(e: FormEvent) {
    e.preventDefault();
    if (!formName || !formDatabaseId || !formMode) return;
    setSubmitting(true);
    try {
      const variables: Record<string, any> = {
        name: formName,
        databaseId: formDatabaseId,
        mode: formMode,
      };
      if (formMode === "copy_and_cdc" || formMode === "cdc_only") {
        variables.publication = formPublication;
      }
      if (formMode === "copy_only") {
        variables.schema = formSchema;
      }
      if (formBatchSize) variables.batchSize = parseInt(formBatchSize, 10);
      if (formBatchTimeout) variables.batchTimeoutMs = parseInt(formBatchTimeout, 10);
      if (formRetryDelay) variables.retryDelayMs = parseInt(formRetryDelay, 10);
      if (formRetryMax) variables.retryMaxAttempts = parseInt(formRetryMax, 10);

      const res = await startEtlPipeline(variables);
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.startEtlPipeline;
      if (!data?.success) {
        toast.error(data?.error || "Failed to start pipeline");
        return;
      }
      toast.success(data.message || `Pipeline '${formName}' started`);
      setStartOpen(false);
      resetForm();
      refetchAll();
    } catch (err: any) {
      toast.error(err.message || "Failed to start pipeline");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleStop(pipelineName: string) {
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

  const needsPublication = formMode === "copy_and_cdc" || formMode === "cdc_only";
  const needsSchema = formMode === "copy_only";

  const columns: Column<EtlPipelineRow>[] = [
    {
      header: "Name",
      cell: (row) => <code className="text-xs font-mono">{row.name}</code>,
    },
    {
      header: "State",
      cell: (row) => (
        <Badge variant={stateBadgeVariant(row.state)}>{row.state}</Badge>
      ),
    },
    {
      header: "Mode",
      cell: (row) => (
        <Badge variant={modeBadgeVariant(row.mode)}>{modeLabel(row.mode)}</Badge>
      ),
    },
    {
      header: "Publication / Schema",
      cell: (row) => (
        <span className="text-sm">{row.publication || "-"}</span>
      ),
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
            <Button variant="outline" size="sm" onClick={() => handleStop(row.name)}>
              <SquareIcon className="h-3 w-3 mr-1" />
              Stop
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
          <h2 className="text-2xl font-bold">Replication Pipelines</h2>
          <p className="text-muted-foreground">
            Manage data replication pipelines from PostgreSQL sources.
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={refetchAll}>
            <RefreshCwIcon className="h-4 w-4" />
          </Button>
          <Button onClick={() => setStartOpen(true)}>
            <PlayIcon className="h-4 w-4 mr-1" />
            Start Pipeline
          </Button>
        </div>
      </div>

      <DataTable
        columns={columns}
        data={pipelines}
        loading={pipelinesResult.fetching}
        emptyMessage="No replication pipelines running. Start a pipeline to replicate data from a PostgreSQL source."
      />

      <Dialog
        open={startOpen}
        onOpenChange={(open) => {
          setStartOpen(open);
          if (!open) resetForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start Replication Pipeline</DialogTitle>
            <DialogDescription>
              Configure and start a data replication pipeline from a PostgreSQL source.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleStart} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="etl-name">Pipeline Name</Label>
              <Input
                id="etl-name"
                placeholder="my-pipeline"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="etl-database">Source Database</Label>
              <select
                id="etl-database"
                className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
                value={formDatabaseId}
                onChange={(e) => setFormDatabaseId(e.target.value)}
                required
              >
                <option value="">Select a database...</option>
                {databases.map((db) => (
                  <option key={db.id} value={db.id}>
                    {db.host}:{db.port}/{db.databaseName}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="etl-mode">Mode</Label>
              <select
                id="etl-mode"
                className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
                value={formMode}
                onChange={(e) => setFormMode(e.target.value)}
                required
              >
                {MODE_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>

            {needsPublication && (
              <div className="flex flex-col gap-2">
                <Label htmlFor="etl-publication">Publication Name</Label>
                <Input
                  id="etl-publication"
                  placeholder="my_publication"
                  value={formPublication}
                  onChange={(e) => setFormPublication(e.target.value)}
                  required
                />
              </div>
            )}

            {needsSchema && (
              <div className="flex flex-col gap-2">
                <Label htmlFor="etl-schema">Schema</Label>
                <Input
                  id="etl-schema"
                  placeholder="public"
                  value={formSchema}
                  onChange={(e) => setFormSchema(e.target.value)}
                  required
                />
              </div>
            )}

            <details
              open={showAdvanced}
              onToggle={(e) => setShowAdvanced((e.target as HTMLDetailsElement).open)}
            >
              <summary className="text-sm text-muted-foreground cursor-pointer">
                Advanced Settings
              </summary>
              <div className="flex flex-col gap-3 mt-3">
                <div className="flex flex-col gap-2">
                  <Label htmlFor="etl-batch-size">Batch Size</Label>
                  <Input
                    id="etl-batch-size"
                    type="number"
                    placeholder="1000"
                    value={formBatchSize}
                    onChange={(e) => setFormBatchSize(e.target.value)}
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="etl-batch-timeout">Batch Timeout (ms)</Label>
                  <Input
                    id="etl-batch-timeout"
                    type="number"
                    placeholder="5000"
                    value={formBatchTimeout}
                    onChange={(e) => setFormBatchTimeout(e.target.value)}
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="etl-retry-delay">Retry Delay (ms)</Label>
                  <Input
                    id="etl-retry-delay"
                    type="number"
                    placeholder="10000"
                    value={formRetryDelay}
                    onChange={(e) => setFormRetryDelay(e.target.value)}
                  />
                </div>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="etl-retry-max">Max Retry Attempts</Label>
                  <Input
                    id="etl-retry-max"
                    type="number"
                    placeholder="5"
                    value={formRetryMax}
                    onChange={(e) => setFormRetryMax(e.target.value)}
                  />
                </div>
              </div>
            </details>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setStartOpen(false);
                  resetForm();
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Starting..." : "Start Pipeline"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
