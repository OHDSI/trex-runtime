import { useState, useEffect, type FormEvent } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  ArrowLeftIcon,
  PlusIcon,
  TrashIcon,
  PlugIcon,
  LoaderIcon,
} from "lucide-react";

const DATABASE_DETAIL_QUERY = `
  query DatabaseById($id: String!) {
    databaseById(id: $id) {
      id
      host
      port
      databaseName
      dialect
      description
      enabled
      vocabSchemas
      extra
      createdAt
      updatedAt
      databaseCredentialsByDatabaseId {
        totalCount
        nodes {
          id
          username
          userScope
          serviceScope
          createdAt
        }
      }
    }
  }
`;

const UPDATE_DATABASE_MUTATION = `
  mutation UpdateDatabaseById($id: String!, $patch: DatabasePatch!) {
    updateDatabaseById(input: { id: $id, patch: $patch }) {
      database {
        id
        host
        port
        databaseName
        dialect
        description
        enabled
        vocabSchemas
      }
    }
  }
`;

const DELETE_DATABASE_MUTATION = `
  mutation DeleteDatabaseById($id: String!) {
    deleteDatabaseById(input: { id: $id }) {
      deletedDatabaseNodeId
    }
  }
`;

const SAVE_CREDENTIAL_MUTATION = `
  mutation SaveDatabaseCredential(
    $pDatabaseId: String!,
    $pUsername: String!,
    $pPassword: String!,
    $pUserScope: String,
    $pServiceScope: String
  ) {
    saveDatabaseCredential(input: {
      pDatabaseId: $pDatabaseId,
      pUsername: $pUsername,
      pPassword: $pPassword,
      pUserScope: $pUserScope,
      pServiceScope: $pServiceScope
    }) {
      databaseCredential {
        id
        username
        userScope
        serviceScope
      }
    }
  }
`;

const DELETE_CREDENTIAL_MUTATION = `
  mutation DeleteDatabaseCredentialById($id: String!) {
    deleteDatabaseCredentialById(input: { id: $id }) {
      deletedDatabaseCredentialNodeId
    }
  }
`;

const TEST_CONNECTION_MUTATION = `
  mutation TestDbConnection($databaseId: String!) {
    testDatabaseConnection(databaseId: $databaseId) {
      success
      message
    }
  }
`;

interface DatabaseCredential {
  id: string;
  username: string;
  userScope: string | null;
  serviceScope: string | null;
  createdAt: string;
}

interface DatabaseDetail {
  id: string;
  host: string;
  port: number;
  databaseName: string;
  dialect: string;
  description: string | null;
  enabled: boolean;
  vocabSchemas: any;
  extra: any;
  createdAt: string;
  updatedAt: string;
  databaseCredentialsByDatabaseId: {
    totalCount: number;
    nodes: DatabaseCredential[];
  };
}

export function DatabaseDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();

  const [result, reexecuteQuery] = useQuery({
    query: DATABASE_DETAIL_QUERY,
    variables: { id },
    pause: !id,
  });

  const [, updateDatabase] = useMutation(UPDATE_DATABASE_MUTATION);
  const [, deleteDatabase] = useMutation(DELETE_DATABASE_MUTATION);
  const [, saveCredential] = useMutation(SAVE_CREDENTIAL_MUTATION);
  const [, deleteCredential] = useMutation(DELETE_CREDENTIAL_MUTATION);
  const [, testConnection] = useMutation(TEST_CONNECTION_MUTATION);

  const db: DatabaseDetail | null = result.data?.databaseById || null;

  const [editing, setEditing] = useState(false);
  const [editHost, setEditHost] = useState("");
  const [editPort, setEditPort] = useState("");
  const [editDbName, setEditDbName] = useState("");
  const [editDialect, setEditDialect] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [editVocabSchemas, setEditVocabSchemas] = useState("");
  const [saving, setSaving] = useState(false);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [addCredOpen, setAddCredOpen] = useState(false);
  const [credSubmitting, setCredSubmitting] = useState(false);
  const [testing, setTesting] = useState(false);

  const [credUsername, setCredUsername] = useState("");
  const [credPassword, setCredPassword] = useState("");
  const [credUserScope, setCredUserScope] = useState("");
  const [credServiceScope, setCredServiceScope] = useState("");

  useEffect(() => {
    if (db) {
      setEditHost(db.host);
      setEditPort(String(db.port));
      setEditDbName(db.databaseName);
      setEditDialect(db.dialect);
      setEditDescription(db.description || "");
      setEditVocabSchemas(
        db.vocabSchemas ? JSON.stringify(db.vocabSchemas) : ""
      );
    }
  }, [db]);

  if (result.fetching) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin h-8 w-8 border-4 border-primary border-t-transparent rounded-full" />
      </div>
    );
  }

  if (result.error) {
    return (
      <div className="text-center py-20">
        <p className="text-destructive">
          Failed to load database: {result.error.message}
        </p>
      </div>
    );
  }

  if (!db) {
    return (
      <div className="text-center py-20">
        <p className="text-muted-foreground">Database not found.</p>
      </div>
    );
  }

  const credentials = db.databaseCredentialsByDatabaseId.nodes;
  const credentialCount = db.databaseCredentialsByDatabaseId.totalCount;

  function startEditing() {
    if (!db) return;
    setEditHost(db.host);
    setEditPort(String(db.port));
    setEditDbName(db.databaseName);
    setEditDialect(db.dialect);
    setEditDescription(db.description || "");
    setEditVocabSchemas(
      db.vocabSchemas ? JSON.stringify(db.vocabSchemas) : ""
    );
    setEditing(true);
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    if (!db) return;
    setSaving(true);

    let vocabSchemas = null;
    if (editVocabSchemas.trim()) {
      try {
        vocabSchemas = JSON.parse(editVocabSchemas);
      } catch {
        toast.error("Invalid JSON for vocabSchemas.");
        setSaving(false);
        return;
      }
    }

    try {
      const res = await updateDatabase({
        id: db.id,
        patch: {
          host: editHost,
          port: parseInt(editPort, 10),
          databaseName: editDbName,
          dialect: editDialect,
          description: editDescription || null,
          vocabSchemas,
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Database updated.");
      setEditing(false);
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to update database.");
    } finally {
      setSaving(false);
    }
  }

  async function handleToggleEnabled() {
    try {
      const res = await updateDatabase({
        id: db!.id,
        patch: { enabled: !db!.enabled },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success(db!.enabled ? "Database disabled." : "Database enabled.");
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to update database status.");
    }
  }

  async function handleDelete() {
    setDeleting(true);
    try {
      const res = await deleteDatabase({ id: db!.id });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Database deleted.");
      navigate("/admin/databases");
    } catch {
      toast.error("Failed to delete database.");
    } finally {
      setDeleting(false);
    }
  }

  async function handleTestConnection() {
    setTesting(true);
    try {
      const res = await testConnection({ databaseId: db!.id });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      const data = res.data?.testDatabaseConnection;
      if (data?.success) {
        toast.success(data.message);
      } else {
        toast.error(data?.message || "Connection failed");
      }
    } catch {
      toast.error("Failed to test connection.");
    } finally {
      setTesting(false);
    }
  }

  function resetCredForm() {
    setCredUsername("");
    setCredPassword("");
    setCredUserScope("");
    setCredServiceScope("");
  }

  async function handleAddCredential(e: FormEvent) {
    e.preventDefault();
    setCredSubmitting(true);

    try {
      const res = await saveCredential({
        pDatabaseId: db!.id,
        pUsername: credUsername,
        pPassword: credPassword,
        pUserScope: credUserScope || null,
        pServiceScope: credServiceScope || null,
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setAddCredOpen(false);
      resetCredForm();
      toast.success("Credential added.");
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to add credential.");
    } finally {
      setCredSubmitting(false);
    }
  }

  async function handleDeleteCredential(credId: string) {
    if (!window.confirm("Are you sure you want to delete this credential? This cannot be undone.")) return;
    try {
      const res = await deleteCredential({ id: credId });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Credential deleted.");
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to delete credential.");
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={() => navigate("/admin/databases")}
        >
          <ArrowLeftIcon />
        </Button>
        <div className="flex-1">
          <h2 className="text-2xl font-bold">{db.id}</h2>
          <p className="text-muted-foreground text-sm">
            Database connection details and credentials
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={db.enabled ? "default" : "secondary"}>
            {db.enabled ? "Enabled" : "Disabled"}
          </Badge>
          <Badge variant="outline">{db.dialect}</Badge>
        </div>
      </div>

      <Separator />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {/* Left column */}
        <div className="lg:col-span-2 space-y-6">
          {/* Details card */}
          <Card>
            <CardHeader>
              <CardTitle>Connection Details</CardTitle>
              <CardDescription>
                Database connection parameters
              </CardDescription>
            </CardHeader>
            <CardContent>
              {editing ? (
                <form onSubmit={handleSave} className="flex flex-col gap-4">
                  <div className="grid grid-cols-3 gap-4">
                    <div className="col-span-2 flex flex-col gap-2">
                      <Label htmlFor="edit-host">Host</Label>
                      <Input
                        id="edit-host"
                        value={editHost}
                        onChange={(e) => setEditHost(e.target.value)}
                        required
                      />
                    </div>
                    <div className="flex flex-col gap-2">
                      <Label htmlFor="edit-port">Port</Label>
                      <Input
                        id="edit-port"
                        type="number"
                        value={editPort}
                        onChange={(e) => setEditPort(e.target.value)}
                        required
                      />
                    </div>
                  </div>

                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-dbname">Database Name</Label>
                    <Input
                      id="edit-dbname"
                      value={editDbName}
                      onChange={(e) => setEditDbName(e.target.value)}
                      required
                    />
                  </div>

                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-dialect">Dialect</Label>
                    <select
                      id="edit-dialect"
                      className="border-input bg-transparent flex h-9 w-full rounded-md border px-3 py-1 text-sm focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] outline-none"
                      value={editDialect}
                      onChange={(e) => setEditDialect(e.target.value)}
                    >
                      <option value="postgresql">PostgreSQL</option>
                      <option value="mysql">MySQL</option>
                      <option value="mssql">MSSQL</option>
                      <option value="oracle">Oracle</option>
                      <option value="duckdb">trexsql</option>
                    </select>
                  </div>

                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-description">Description</Label>
                    <Input
                      id="edit-description"
                      value={editDescription}
                      onChange={(e) => setEditDescription(e.target.value)}
                      placeholder="Optional description"
                    />
                  </div>

                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-vocab">Vocab Schemas (JSON)</Label>
                    <Input
                      id="edit-vocab"
                      value={editVocabSchemas}
                      onChange={(e) => setEditVocabSchemas(e.target.value)}
                      placeholder='["cdm", "vocab"]'
                    />
                  </div>

                  <div className="flex gap-2 justify-end">
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => setEditing(false)}
                    >
                      Cancel
                    </Button>
                    <Button type="submit" disabled={saving}>
                      {saving ? "Saving..." : "Save Changes"}
                    </Button>
                  </div>
                </form>
              ) : (
                <div className="flex flex-col gap-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="flex flex-col gap-1">
                      <Label className="text-muted-foreground">Host</Label>
                      <p className="text-sm">{db.host}</p>
                    </div>
                    <div className="flex flex-col gap-1">
                      <Label className="text-muted-foreground">Port</Label>
                      <p className="text-sm">{db.port}</p>
                    </div>
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">
                      Database Name
                    </Label>
                    <p className="text-sm">{db.databaseName}</p>
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Dialect</Label>
                    <p className="text-sm">{db.dialect}</p>
                  </div>

                  {db.description && (
                    <div className="flex flex-col gap-1">
                      <Label className="text-muted-foreground">
                        Description
                      </Label>
                      <p className="text-sm">{db.description}</p>
                    </div>
                  )}

                  {db.vocabSchemas && (
                    <div className="flex flex-col gap-1">
                      <Label className="text-muted-foreground">
                        Vocab Schemas
                      </Label>
                      <code className="text-sm font-mono">
                        {JSON.stringify(db.vocabSchemas)}
                      </code>
                    </div>
                  )}

                  <div className="flex gap-2 justify-end">
                    <Button variant="outline" onClick={startEditing}>
                      Edit
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Credentials card */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Credentials</CardTitle>
                  <CardDescription>
                    Database access credentials ({credentialCount})
                  </CardDescription>
                </div>
                <Button size="sm" onClick={() => setAddCredOpen(true)}>
                  <PlusIcon />
                  Add Credential
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {credentials.length === 0 ? (
                <p className="text-sm text-muted-foreground text-center py-4">
                  No credentials configured.
                </p>
              ) : (
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Username</TableHead>
                        <TableHead>User Scope</TableHead>
                        <TableHead>Service Scope</TableHead>
                        <TableHead>Created</TableHead>
                        <TableHead className="w-10" />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {credentials.map((cred) => (
                        <TableRow key={cred.id}>
                          <TableCell>
                            <code className="text-sm font-mono">
                              {cred.username}
                            </code>
                          </TableCell>
                          <TableCell>
                            {cred.userScope || (
                              <span className="text-muted-foreground">-</span>
                            )}
                          </TableCell>
                          <TableCell>
                            {cred.serviceScope || (
                              <span className="text-muted-foreground">-</span>
                            )}
                          </TableCell>
                          <TableCell>
                            {new Date(cred.createdAt).toLocaleDateString()}
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon-xs"
                              onClick={() => handleDeleteCredential(cred.id)}
                            >
                              <TrashIcon className="size-3" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        {/* Right column */}
        <div className="space-y-6">
          {/* Info card */}
          <Card>
            <CardHeader>
              <CardTitle>Info</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col gap-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Dialect</span>
                  <Badge variant="secondary">{db.dialect}</Badge>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">
                    Credentials
                  </span>
                  <span className="text-sm font-medium">{credentialCount}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Status</span>
                  <Badge variant={db.enabled ? "default" : "secondary"}>
                    {db.enabled ? "Enabled" : "Disabled"}
                  </Badge>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Created</span>
                  <span className="text-sm">
                    {new Date(db.createdAt).toLocaleDateString()}
                  </span>
                </div>
                {db.updatedAt && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">
                      Updated
                    </span>
                    <span className="text-sm">
                      {new Date(db.updatedAt).toLocaleDateString()}
                    </span>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Actions card */}
          <Card>
            <CardHeader>
              <CardTitle>Actions</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-col gap-2">
                <Button
                  variant="outline"
                  className="w-full justify-start"
                  onClick={handleTestConnection}
                  disabled={testing}
                >
                  {testing ? (
                    <LoaderIcon className="animate-spin" />
                  ) : (
                    <PlugIcon />
                  )}
                  {testing ? "Testing..." : "Test Connection"}
                </Button>
                <Button
                  variant="outline"
                  className="w-full justify-start"
                  onClick={handleToggleEnabled}
                >
                  {db.enabled ? "Disable Database" : "Enable Database"}
                </Button>
                <Button
                  variant="destructive"
                  className="w-full justify-start"
                  onClick={() => setDeleteDialogOpen(true)}
                >
                  <TrashIcon />
                  Delete Database
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Delete Confirmation Dialog */}
      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Database</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete "{db.id}"? This action cannot be
              undone. All associated credentials will also be deleted.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => setDeleteDialogOpen(false)}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDelete}
              disabled={deleting}
            >
              {deleting ? "Deleting..." : "Delete Database"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Add Credential Dialog */}
      <Dialog
        open={addCredOpen}
        onOpenChange={(open) => {
          setAddCredOpen(open);
          if (!open) resetCredForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Credential</DialogTitle>
            <DialogDescription>
              Add database access credentials. The password will be stored but
              never displayed again.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleAddCredential} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="cred-username">Username</Label>
              <Input
                id="cred-username"
                value={credUsername}
                onChange={(e) => setCredUsername(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="cred-password">Password</Label>
              <Input
                id="cred-password"
                type="password"
                value={credPassword}
                onChange={(e) => setCredPassword(e.target.value)}
                required
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="flex flex-col gap-2">
                <Label htmlFor="cred-user-scope">User Scope</Label>
                <Input
                  id="cred-user-scope"
                  value={credUserScope}
                  onChange={(e) => setCredUserScope(e.target.value)}
                  placeholder="Optional"
                />
              </div>
              <div className="flex flex-col gap-2">
                <Label htmlFor="cred-service-scope">Service Scope</Label>
                <Input
                  id="cred-service-scope"
                  value={credServiceScope}
                  onChange={(e) => setCredServiceScope(e.target.value)}
                  placeholder="Optional"
                />
              </div>
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setAddCredOpen(false);
                  resetCredForm();
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={credSubmitting}>
                {credSubmitting ? "Adding..." : "Add Credential"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
