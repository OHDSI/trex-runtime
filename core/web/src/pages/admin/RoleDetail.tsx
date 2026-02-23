import { useState, useEffect, type FormEvent } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import { ArrowLeftIcon, TrashIcon } from "lucide-react";

const ROLE_DETAIL_QUERY = `
  query RoleByRowId($id: String!) {
    roleByRowId(rowId: $id) {
      id
      name
      description
      createdAt
      updatedAt
      userRolesByRoleId {
        totalCount
        nodes {
          rowId
          createdAt
          userByUserId {
            rowId
            name
            email
          }
        }
      }
    }
  }
`;

const UPDATE_ROLE_MUTATION = `
  mutation UpdateRoleByRowId($id: String!, $patch: RolePatch!) {
    updateRoleByRowId(input: { rowId: $id, patch: $patch }) {
      role {
        id
        name
        description
      }
    }
  }
`;

const DELETE_ROLE_MUTATION = `
  mutation DeleteRoleByRowId($id: String!) {
    deleteRoleByRowId(input: { rowId: $id }) {
      deletedRoleNodeId
    }
  }
`;

const DELETE_USER_ROLE_MUTATION = `
  mutation DeleteUserRoleByRowId($id: String!) {
    deleteUserRoleByRowId(input: { rowId: $id }) {
      deletedUserRoleNodeId
    }
  }
`;

interface AssignedUser {
  rowId: string;
  createdAt: string;
  userByUserId: {
    rowId: string;
    name: string;
    email: string;
  };
}

interface RoleData {
  id: string;
  name: string;
  description: string | null;
  createdAt: string;
  updatedAt: string;
  userRolesByRoleId: {
    totalCount: number;
    nodes: AssignedUser[];
  };
}

export function RoleDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();

  const [result, reexecuteQuery] = useQuery({
    query: ROLE_DETAIL_QUERY,
    variables: { id },
    pause: !id,
  });

  const [, updateRole] = useMutation(UPDATE_ROLE_MUTATION);
  const [, deleteRole] = useMutation(DELETE_ROLE_MUTATION);
  const [, deleteUserRole] = useMutation(DELETE_USER_ROLE_MUTATION);

  const role: RoleData | null = result.data?.roleByRowId || null;

  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [saving, setSaving] = useState(false);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);

  useEffect(() => {
    if (role) {
      setEditName(role.name);
      setEditDescription(role.description || "");
    }
  }, [role]);

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
          Failed to load role: {result.error.message}
        </p>
      </div>
    );
  }

  if (!role) {
    return (
      <div className="text-center py-20">
        <p className="text-muted-foreground">Role not found.</p>
      </div>
    );
  }

  const assignedUsers = role.userRolesByRoleId.nodes;
  const userCount = role.userRolesByRoleId.totalCount;

  function startEditing() {
    if (!role) return;
    setEditName(role.name);
    setEditDescription(role.description || "");
    setEditing(true);
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    if (!role) return;
    setSaving(true);

    try {
      const res = await updateRole({
        id: role.id,
        patch: {
          name: editName,
          description: editDescription || null,
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Role updated.");
      setEditing(false);
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to update role.");
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete() {
    setDeleting(true);
    try {
      const res = await deleteRole({ id: role!.id });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Role deleted.");
      navigate("/admin/roles");
    } catch {
      toast.error("Failed to delete role.");
    } finally {
      setDeleting(false);
    }
  }

  async function handleRemoveUser(userRoleId: string) {
    try {
      const res = await deleteUserRole({ id: userRoleId });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("User removed from role.");
      reexecuteQuery({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to remove user.");
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={() => navigate("/admin/roles")}
        >
          <ArrowLeftIcon />
        </Button>
        <div className="flex-1">
          <h2 className="text-2xl font-bold">{role.name}</h2>
          <p className="text-muted-foreground text-sm">
            Application role details and assigned users
          </p>
        </div>
      </div>

      <Separator />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {/* Left column */}
        <div className="lg:col-span-2 space-y-6">
          {/* Details card */}
          <Card>
            <CardHeader>
              <CardTitle>Details</CardTitle>
              <CardDescription>Role name and description</CardDescription>
            </CardHeader>
            <CardContent>
              {editing ? (
                <form onSubmit={handleSave} className="flex flex-col gap-4">
                  <div className="flex flex-col gap-2">
                    <Label htmlFor="edit-name">Name</Label>
                    <Input
                      id="edit-name"
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                      required
                    />
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
                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Name</Label>
                    <p className="text-sm">{role.name}</p>
                  </div>

                  <div className="flex flex-col gap-1">
                    <Label className="text-muted-foreground">Description</Label>
                    <p className="text-sm">
                      {role.description || (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </p>
                  </div>

                  <div className="flex gap-2 justify-end">
                    <Button variant="outline" onClick={startEditing}>
                      Edit
                    </Button>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Assigned Users card */}
          <Card>
            <CardHeader>
              <CardTitle>Assigned Users</CardTitle>
              <CardDescription>
                Users with this role ({userCount})
              </CardDescription>
            </CardHeader>
            <CardContent>
              {assignedUsers.length === 0 ? (
                <p className="text-sm text-muted-foreground text-center py-4">
                  No users assigned to this role.
                </p>
              ) : (
                <div className="rounded-md border">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Name</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Assigned</TableHead>
                        <TableHead className="w-10" />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {assignedUsers.map((ur) => (
                        <TableRow key={ur.rowId}>
                          <TableCell>
                            <Link
                              to={`/admin/users/${ur.userByUserId.rowId}`}
                              className="text-sm font-medium hover:underline"
                            >
                              {ur.userByUserId.name}
                            </Link>
                          </TableCell>
                          <TableCell>
                            <span className="text-sm">
                              {ur.userByUserId.email}
                            </span>
                          </TableCell>
                          <TableCell>
                            {new Date(ur.createdAt).toLocaleDateString()}
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="icon-xs"
                              onClick={() => handleRemoveUser(ur.rowId)}
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
                  <span className="text-sm text-muted-foreground">
                    Assigned Users
                  </span>
                  <span className="text-sm font-medium">{userCount}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-muted-foreground">Created</span>
                  <span className="text-sm">
                    {new Date(role.createdAt).toLocaleDateString()}
                  </span>
                </div>
                {role.updatedAt && (
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">
                      Updated
                    </span>
                    <span className="text-sm">
                      {new Date(role.updatedAt).toLocaleDateString()}
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
                  variant="destructive"
                  className="w-full justify-start"
                  onClick={() => setDeleteDialogOpen(true)}
                >
                  <TrashIcon />
                  Delete Role
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
            <DialogTitle>Delete Role</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete "{role.name}"? This action cannot
              be undone. All user assignments for this role will also be removed.
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
              {deleting ? "Deleting..." : "Delete Role"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
