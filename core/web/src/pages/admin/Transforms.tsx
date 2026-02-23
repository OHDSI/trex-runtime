import { useState, type FormEvent } from "react";
import { useQuery, useMutation } from "urql";
import { client } from "@/lib/graphql-client";
import { toast } from "sonner";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { RefreshCwIcon, PlayIcon } from "lucide-react";

const TRANSFORM_PROJECTS_QUERY = `
  query TransformProjects {
    transformProjects { pluginName projectPath }
  }
`;

const TREX_DATABASES_QUERY = `
  query TrexDatabasesForTransform {
    trexDatabases { databaseName databaseOid path internal type }
  }
`;

const TREX_SCHEMAS_QUERY = `
  query TrexSchemasForTransform {
    trexSchemas { databaseName schemaName schemaOid internal }
  }
`;

const TRANSFORM_COMPILE_QUERY = `
  query TransformCompile($pluginName: String!) {
    transformCompile(pluginName: $pluginName) { name materialized order status }
  }
`;

const TRANSFORM_RUN_MUTATION = `
  mutation TransformRun($pluginName: String!, $destDb: String!, $destSchema: String!, $sourceDb: String!, $sourceSchema: String!) {
    transformRun(pluginName: $pluginName, destDb: $destDb, destSchema: $destSchema, sourceDb: $sourceDb, sourceSchema: $sourceSchema) {
      name action materialized durationMs message
    }
  }
`;

const TRANSFORM_SEED_MUTATION = `
  mutation TransformSeed($pluginName: String!, $destDb: String!, $destSchema: String!) {
    transformSeed(pluginName: $pluginName, destDb: $destDb, destSchema: $destSchema) {
      name action rows message
    }
  }
`;

const TRANSFORM_TEST_MUTATION = `
  mutation TransformTest($pluginName: String!, $destDb: String!, $destSchema: String!, $sourceDb: String!, $sourceSchema: String!) {
    transformTest(pluginName: $pluginName, destDb: $destDb, destSchema: $destSchema, sourceDb: $sourceDb, sourceSchema: $sourceSchema) {
      name status rowsReturned
    }
  }
`;

const TRANSFORM_PLAN_QUERY = `
  query TransformPlan($pluginName: String!, $destDb: String!, $destSchema: String!, $sourceDb: String!, $sourceSchema: String!) {
    transformPlan(pluginName: $pluginName, destDb: $destDb, destSchema: $destSchema, sourceDb: $sourceDb, sourceSchema: $sourceSchema) {
      name action materialized reason
    }
  }
`;

const TRANSFORM_FRESHNESS_QUERY = `
  query TransformFreshness($pluginName: String!, $destDb: String!, $destSchema: String!) {
    transformFreshness(pluginName: $pluginName, destDb: $destDb, destSchema: $destSchema) {
      name status maxLoadedAt ageHours warnAfter errorAfter
    }
  }
`;

interface TransformProject {
  pluginName: string;
  projectPath: string;
}

interface TrexDatabase {
  databaseName: string;
  databaseOid: string;
  path: string | null;
  internal: boolean;
  type: string;
}

interface TrexSchema {
  databaseName: string;
  schemaName: string;
  schemaOid: string;
  internal: boolean;
}

const selectClass =
  "border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none";

function statusBadgeVariant(status: string): "default" | "secondary" | "destructive" {
  if (status === "pass" || status === "ok" || status === "no_change") return "default";
  if (status === "warn" || status === "create" || status === "update") return "secondary";
  return "destructive";
}

export function Transforms() {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedPlugin, setSelectedPlugin] = useState<TransformProject | null>(null);
  const [sourceDb, setSourceDb] = useState("");
  const [sourceSchema, setSourceSchema] = useState("");
  const [destDb, setDestDb] = useState("");
  const [destSchema, setDestSchema] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [results, setResults] = useState<any[] | null>(null);
  const [resultType, setResultType] = useState<string>("");

  const [projectsResult, reexecuteProjects] = useQuery({ query: TRANSFORM_PROJECTS_QUERY });
  const [dbResult] = useQuery({ query: TREX_DATABASES_QUERY });
  const [schemasResult] = useQuery({ query: TREX_SCHEMAS_QUERY });

  const [, runTransform] = useMutation(TRANSFORM_RUN_MUTATION);
  const [, seedTransform] = useMutation(TRANSFORM_SEED_MUTATION);
  const [, testTransform] = useMutation(TRANSFORM_TEST_MUTATION);

  const projects: TransformProject[] = projectsResult.data?.transformProjects || [];
  const databases: TrexDatabase[] = (dbResult.data?.trexDatabases || []).filter(
    (db: TrexDatabase) => !db.internal
  );
  const allSchemas: TrexSchema[] = schemasResult.data?.trexSchemas || [];

  const sourceSchemasFiltered = allSchemas.filter(
    (s) => s.databaseName === sourceDb && !s.internal
  );
  const destSchemasFiltered = allSchemas.filter(
    (s) => s.databaseName === destDb && !s.internal
  );

  function openDialog(plugin: TransformProject) {
    setSelectedPlugin(plugin);
    setResults(null);
    setResultType("");
    setDialogOpen(true);
  }

  function resetForm() {
    setSelectedPlugin(null);
    setSourceDb("");
    setSourceSchema("");
    setDestDb("");
    setDestSchema("");
    setResults(null);
    setResultType("");
  }

  async function handleCompile() {
    if (!selectedPlugin) return;
    setSubmitting(true);
    setResults(null);
    try {
      const res = await client
        .query(TRANSFORM_COMPILE_QUERY, { pluginName: selectedPlugin.pluginName })
        .toPromise();
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      setResults(res.data?.transformCompile || []);
      setResultType("compile");
    } catch (err: any) {
      toast.error(err.message || "Compile failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handlePlan() {
    if (!selectedPlugin || !destDb || !destSchema || !sourceDb || !sourceSchema) return;
    setSubmitting(true);
    setResults(null);
    try {
      const res = await client
        .query(TRANSFORM_PLAN_QUERY, {
          pluginName: selectedPlugin.pluginName,
          destDb,
          destSchema,
          sourceDb,
          sourceSchema,
        })
        .toPromise();
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      setResults(res.data?.transformPlan || []);
      setResultType("plan");
    } catch (err: any) {
      toast.error(err.message || "Plan failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleFreshness() {
    if (!selectedPlugin || !destDb || !destSchema) return;
    setSubmitting(true);
    setResults(null);
    try {
      const res = await client
        .query(TRANSFORM_FRESHNESS_QUERY, {
          pluginName: selectedPlugin.pluginName,
          destDb,
          destSchema,
        })
        .toPromise();
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      setResults(res.data?.transformFreshness || []);
      setResultType("freshness");
    } catch (err: any) {
      toast.error(err.message || "Freshness check failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleSeed(e: FormEvent) {
    e.preventDefault();
    if (!selectedPlugin || !destDb || !destSchema) return;
    setSubmitting(true);
    setResults(null);
    try {
      const res = await seedTransform({
        pluginName: selectedPlugin.pluginName,
        destDb,
        destSchema,
      });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.transformSeed;
      setResults(data || []);
      setResultType("seed");
      toast.success("Seed completed");
    } catch (err: any) {
      toast.error(err.message || "Seed failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleRun() {
    if (!selectedPlugin || !destDb || !destSchema || !sourceDb || !sourceSchema) return;
    setSubmitting(true);
    setResults(null);
    try {
      const res = await runTransform({
        pluginName: selectedPlugin.pluginName,
        destDb,
        destSchema,
        sourceDb,
        sourceSchema,
      });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.transformRun;
      setResults(data || []);
      setResultType("run");
      toast.success("Run completed");
    } catch (err: any) {
      toast.error(err.message || "Run failed");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleTest() {
    if (!selectedPlugin || !destDb || !destSchema || !sourceDb || !sourceSchema) return;
    setSubmitting(true);
    setResults(null);
    try {
      const res = await testTransform({
        pluginName: selectedPlugin.pluginName,
        destDb,
        destSchema,
        sourceDb,
        sourceSchema,
      });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.transformTest;
      setResults(data || []);
      setResultType("test");
      toast.success("Tests completed");
    } catch (err: any) {
      toast.error(err.message || "Test failed");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<TransformProject>[] = [
    {
      header: "Plugin Name",
      cell: (row) => <code className="text-xs font-mono">{row.pluginName}</code>,
    },
    {
      header: "Project Path",
      cell: (row) => <span className="text-sm text-muted-foreground">{row.projectPath}</span>,
    },
    {
      header: "Actions",
      cell: (row) => (
        <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
          <Button variant="outline" size="sm" onClick={() => openDialog(row)}>
            <PlayIcon className="h-3 w-3 mr-1" />
            Operations
          </Button>
        </div>
      ),
    },
  ];

  const hasDbConfig = destDb && destSchema;
  const hasSourceConfig = sourceDb && sourceSchema;

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-bold">Transforms</h2>
          <p className="text-muted-foreground">
            Manage transform projects — compile, plan, seed, run, and test SQL model pipelines.
          </p>
        </div>
        <Button variant="outline" onClick={() => reexecuteProjects({ requestPolicy: "network-only" })}>
          <RefreshCwIcon className="h-4 w-4" />
        </Button>
      </div>

      <DataTable
        columns={columns}
        data={projects}
        loading={projectsResult.fetching}
        emptyMessage="No transform plugins registered. Install a transform plugin to get started."
      />

      <Dialog
        open={dialogOpen}
        onOpenChange={(open) => {
          setDialogOpen(open);
          if (!open) resetForm();
        }}
      >
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>
              Transform: {selectedPlugin?.pluginName}
            </DialogTitle>
            <DialogDescription>
              Configure databases and run transform operations.
            </DialogDescription>
          </DialogHeader>

          <div className="flex flex-col gap-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="flex flex-col gap-2">
                <Label>Source Database</Label>
                <select
                  className={selectClass}
                  value={sourceDb}
                  onChange={(e) => {
                    setSourceDb(e.target.value);
                    setSourceSchema("");
                  }}
                >
                  <option value="">Select database...</option>
                  {databases.map((db) => (
                    <option key={db.databaseName} value={db.databaseName}>
                      {db.databaseName}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex flex-col gap-2">
                <Label>Source Schema</Label>
                <select
                  className={selectClass}
                  value={sourceSchema}
                  onChange={(e) => setSourceSchema(e.target.value)}
                  disabled={!sourceDb}
                >
                  <option value="">Select schema...</option>
                  {sourceSchemasFiltered.map((s) => (
                    <option key={s.schemaName} value={s.schemaName}>
                      {s.schemaName}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="flex flex-col gap-2">
                <Label>Destination Database</Label>
                <select
                  className={selectClass}
                  value={destDb}
                  onChange={(e) => {
                    setDestDb(e.target.value);
                    setDestSchema("");
                  }}
                >
                  <option value="">Select database...</option>
                  {databases.map((db) => (
                    <option key={db.databaseName} value={db.databaseName}>
                      {db.databaseName}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex flex-col gap-2">
                <Label>Destination Schema</Label>
                <select
                  className={selectClass}
                  value={destSchema}
                  onChange={(e) => setDestSchema(e.target.value)}
                  disabled={!destDb}
                >
                  <option value="">Select schema...</option>
                  {destSchemasFiltered.map((s) => (
                    <option key={s.schemaName} value={s.schemaName}>
                      {s.schemaName}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={submitting}
                onClick={handleCompile}
              >
                Compile
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={submitting || !hasDbConfig}
                onClick={handleSeed}
              >
                Seed
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={submitting || !hasDbConfig || !hasSourceConfig}
                onClick={handlePlan}
              >
                Plan
              </Button>
              <Button
                size="sm"
                disabled={submitting || !hasDbConfig || !hasSourceConfig}
                onClick={handleRun}
              >
                Run
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={submitting || !hasDbConfig || !hasSourceConfig}
                onClick={handleTest}
              >
                Test
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={submitting || !hasDbConfig}
                onClick={handleFreshness}
              >
                Freshness
              </Button>
            </div>

            {results && results.length > 0 && (
              <div className="rounded-md border">
                <div className="px-3 py-2 bg-muted/50 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  {resultType} results
                </div>
                <div className="divide-y">
                  {results.map((r: any, i: number) => (
                    <div key={i} className="px-3 py-2 flex items-center gap-3 text-sm">
                      <code className="font-mono text-xs">{r.name}</code>
                      {r.status && (
                        <Badge variant={statusBadgeVariant(r.status)}>{r.status}</Badge>
                      )}
                      {r.action && (
                        <Badge variant={statusBadgeVariant(r.action)}>{r.action}</Badge>
                      )}
                      {r.materialized && (
                        <span className="text-muted-foreground">{r.materialized}</span>
                      )}
                      {r.durationMs && r.durationMs !== "0" && (
                        <span className="text-muted-foreground">{r.durationMs}ms</span>
                      )}
                      {r.reason && (
                        <span className="text-muted-foreground">{r.reason}</span>
                      )}
                      {r.rowsReturned && (
                        <span className="text-muted-foreground">rows: {r.rowsReturned}</span>
                      )}
                      {r.message && r.message !== "ok" && (
                        <span className="text-muted-foreground truncate max-w-[200px]" title={r.message}>
                          {r.message}
                        </span>
                      )}
                      {r.ageHours !== undefined && (
                        <span className="text-muted-foreground">
                          {r.ageHours >= 0 ? `${r.ageHours.toFixed(1)}h` : "N/A"}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {results && results.length === 0 && (
              <div className="text-sm text-muted-foreground text-center py-4">
                No results returned.
              </div>
            )}
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setDialogOpen(false);
                resetForm();
              }}
            >
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
