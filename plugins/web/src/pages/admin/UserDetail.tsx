import { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, gql } from "urql";
import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { toast } from "sonner";

interface Account {
  id: string;
  providerId: string;
  accountId: string;
  createdAt: string;
}

interface UserRoleNode {
  rowId: string;
  roleByRoleId: {
    rowId: string;
    name: string;
  };
}

interface AvailableRole {
  rowId: string;
  name: string;
}

interface UserDetailData {
  rowId: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  role: string;
  banned: boolean;
  banReason: string | null;
  deletedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

interface UserByIdResult {
  userByRowId: UserDetailData & {
    accountsByUserId: {
      nodes: Account[];
    };
    sessionsByUserId: {
      totalCount: number;
    };
    userRolesByUserId: {
      nodes: UserRoleNode[];
    };
  };
}

const USER_BY_ID_QUERY = gql`
  query UserByRowId($id: String!) {
    userByRowId(rowId: $id) {
      rowId
      name
      email
      emailVerified
      image
      role
      banned
      banReason
      deletedAt
      createdAt
      updatedAt
      accountsByUserId {
        nodes {
          id
          providerId
          accountId
          createdAt
        }
      }
      sessionsByUserId {
        totalCount
      }
      userRolesByUserId {
        nodes {
          rowId
          roleByRoleId {
            rowId
            name
          }
        }
      }
    }
  }
`;

const ALL_ROLES_QUERY = gql`
  query AllRolesForAssign {
    allRoles(orderBy: [NAME_ASC]) {
      nodes {
        rowId
        name
      }
    }
  }
`;

const CREATE_USER_ROLE_MUTATION = gql`
  mutation CreateUserRole($input: CreateUserRoleInput!) {
    createUserRole(input: $input) {
      userRole {
        id
      }
    }
  }
`;

const DELETE_USER_ROLE_MUTATION = gql`
  mutation DeleteUserRoleByRowId($id: String!) {
    deleteUserRoleByRowId(input: { rowId: $id }) {
      deletedUserRoleNodeId
    }
  }
`;

const AVAILABLE_ROLES = ["user", "admin"];

function formatDate(dateString: string): string {
  return new Date(dateString).toLocaleDateString(undefined, {
    year: "numeric",
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function getUserStatus(user: UserDetailData): {
  label: string;
  variant: "default" | "secondary" | "destructive" | "outline";
} {
  if (user.deletedAt) return { label: "Deleted", variant: "destructive" };
  if (user.banned) return { label: "Banned", variant: "destructive" };
  return { label: "Active", variant: "default" };
}

export function UserDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

  const [addRoleOpen, setAddRoleOpen] = useState(false);

  const [{ data, fetching, error }, reexecute] = useQuery<UserByIdResult>({
    query: USER_BY_ID_QUERY,
    variables: { id },
    pause: !id,
  });

  const [allRolesResult] = useQuery<{ allRoles: { nodes: AvailableRole[] } }>({
    query: ALL_ROLES_QUERY,
    pause: !addRoleOpen,
  });

  const [, createUserRole] = useMutation(CREATE_USER_ROLE_MUTATION);
  const [, deleteUserRole] = useMutation(DELETE_USER_ROLE_MUTATION);

  if (fetching) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="animate-spin h-8 w-8 border-4 border-primary border-t-transparent rounded-full" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-20">
        <h2 className="text-xl font-semibold text-destructive">Error loading user</h2>
        <p className="text-muted-foreground mt-2">{error.message}</p>
        <Button variant="outline" className="mt-4" onClick={() => navigate("/admin/users")}>
          Back to Users
        </Button>
      </div>
    );
  }

  const user = data?.userByRowId;
  if (!user) {
    return (
      <div className="text-center py-20">
        <h2 className="text-xl font-semibold">User not found</h2>
        <Button variant="outline" className="mt-4" onClick={() => navigate("/admin/users")}>
          Back to Users
        </Button>
      </div>
    );
  }

  const status = getUserStatus(user);
  const accounts = user.accountsByUserId.nodes;
  const sessionCount = user.sessionsByUserId.totalCount;
  const userRoles = user.userRolesByUserId.nodes;
  const assignedRoleIds = new Set(userRoles.map((ur) => ur.roleByRoleId.rowId));
  const availableRoles = (allRolesResult.data?.allRoles.nodes || []).filter(
    (r) => !assignedRoleIds.has(r.rowId)
  );

  async function handleAssignRole(roleId: string) {
    try {
      const res = await createUserRole({
        input: { userRole: { userId: user!.rowId, roleId } },
      });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      toast.success("Role assigned");
      setAddRoleOpen(false);
      reexecute({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to assign role");
    }
  }

  async function handleRemoveRole(userRoleId: string) {
    try {
      const res = await deleteUserRole({ id: userRoleId });
      if (res.error) {
        toast.error(res.error.message);
        return;
      }
      toast.success("Role removed");
      reexecute({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to remove role");
    }
  }

  async function handleRoleChange(newRole: string) {
    try {
      const res = await authClient.admin.setRole({ userId: user!.rowId, role: newRole as "user" | "admin" });
      if (res.error) {
        toast.error(res.error.message || "Failed to change role");
        return;
      }
      toast.success(`Role changed to ${newRole}`);
      reexecute({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to change role");
    }
  }

  async function handleBanToggle() {
    try {
      if (user!.banned) {
        const res = await authClient.admin.unbanUser({ userId: user!.rowId });
        if (res.error) {
          toast.error(res.error.message || "Failed to unban user");
          return;
        }
        toast.success("User unbanned");
      } else {
        const res = await authClient.admin.banUser({ userId: user!.rowId, banReason: "Banned by admin" });
        if (res.error) {
          toast.error(res.error.message || "Failed to ban user");
          return;
        }
        toast.success("User banned");
      }
      reexecute({ requestPolicy: "network-only" });
    } catch {
      toast.error("Failed to update ban status");
    }
  }

  async function handleSoftDelete() {
    try {
      const res = await authClient.admin.removeUser({ userId: user!.rowId });
      if (res.error) {
        toast.error(res.error.message || "Failed to delete user");
        return;
      }
      toast.success("User deleted");
      setDeleteDialogOpen(false);
      navigate("/admin/users");
    } catch {
      toast.error("Failed to delete user");
    }
  }

  async function handleRestore() {
    toast.error("Restore is not available via the admin API");
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="sm" onClick={() => navigate("/admin/users")}>
            &larr; Back
          </Button>
          <div>
            <h1 className="text-2xl font-bold tracking-tight">{user.name}</h1>
            <p className="text-muted-foreground">{user.email}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={status.variant}>{status.label}</Badge>
          <Badge variant={user.role === "admin" ? "default" : "secondary"}>
            {user.role}
          </Badge>
        </div>
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        {/* Profile Information */}
        <Card>
          <CardHeader>
            <CardTitle>Profile</CardTitle>
            <CardDescription>User account information</CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="space-y-3 text-sm">
              <div className="flex justify-between">
                <dt className="text-muted-foreground">ID</dt>
                <dd className="font-mono text-xs">{user.rowId}</dd>
              </div>
              <Separator />
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Name</dt>
                <dd>{user.name}</dd>
              </div>
              <Separator />
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Email</dt>
                <dd>{user.email}</dd>
              </div>
              <Separator />
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Email Verified</dt>
                <dd>
                  <Badge variant={user.emailVerified ? "default" : "outline"}>
                    {user.emailVerified ? "Verified" : "Unverified"}
                  </Badge>
                </dd>
              </div>
              <Separator />
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Active Sessions</dt>
                <dd>{sessionCount}</dd>
              </div>
              <Separator />
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Created</dt>
                <dd>{formatDate(user.createdAt)}</dd>
              </div>
              <Separator />
              <div className="flex justify-between">
                <dt className="text-muted-foreground">Updated</dt>
                <dd>{formatDate(user.updatedAt)}</dd>
              </div>
              {user.deletedAt && (
                <>
                  <Separator />
                  <div className="flex justify-between">
                    <dt className="text-muted-foreground">Deleted</dt>
                    <dd className="text-destructive">{formatDate(user.deletedAt)}</dd>
                  </div>
                </>
              )}
              {user.banned && user.banReason && (
                <>
                  <Separator />
                  <div className="flex justify-between">
                    <dt className="text-muted-foreground">Ban Reason</dt>
                    <dd className="text-destructive">{user.banReason}</dd>
                  </div>
                </>
              )}
            </dl>
          </CardContent>
        </Card>

        {/* Linked Providers */}
        <Card>
          <CardHeader>
            <CardTitle>Linked Providers</CardTitle>
            <CardDescription>
              {accounts.length === 0
                ? "No linked authentication providers"
                : `${accounts.length} linked provider${accounts.length === 1 ? "" : "s"}`}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {accounts.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                This user has no linked external providers.
              </p>
            ) : (
              <div className="space-y-3">
                {accounts.map((account) => (
                  <div
                    key={account.id}
                    className="flex items-center justify-between rounded-md border p-3"
                  >
                    <div>
                      <p className="text-sm font-medium capitalize">
                        {account.providerId}
                      </p>
                      <p className="text-xs text-muted-foreground font-mono">
                        {account.accountId}
                      </p>
                    </div>
                    <span className="text-xs text-muted-foreground">
                      {formatDate(account.createdAt)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Application Roles */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>Application Roles</CardTitle>
              <CardDescription>Roles assigned to this user</CardDescription>
            </div>
            <Button size="sm" variant="outline" onClick={() => setAddRoleOpen(true)}>
              Add Role
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {userRoles.length === 0 ? (
            <p className="text-sm text-muted-foreground">No application roles assigned.</p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {userRoles.map((ur) => (
                <Badge key={ur.rowId} variant="secondary" className="gap-1">
                  {ur.roleByRoleId.name}
                  <button
                    onClick={() => handleRemoveRole(ur.rowId)}
                    className="ml-1 rounded-full hover:bg-muted-foreground/20 p-0.5"
                  >
                    <span className="sr-only">Remove {ur.roleByRoleId.name}</span>
                    <svg className="size-3" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M3 3l6 6M9 3l-6 6" />
                    </svg>
                  </button>
                </Badge>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Add Role Dialog */}
      <Dialog open={addRoleOpen} onOpenChange={setAddRoleOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Application Role</DialogTitle>
            <DialogDescription>
              Select a role to assign to {user.name}.
            </DialogDescription>
          </DialogHeader>
          {availableRoles.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4 text-center">
              {allRolesResult.fetching ? "Loading roles..." : "No available roles to assign."}
            </p>
          ) : (
            <div className="flex flex-col gap-2 max-h-64 overflow-y-auto">
              {availableRoles.map((role) => (
                <Button
                  key={role.rowId}
                  variant="outline"
                  className="w-full justify-start"
                  onClick={() => handleAssignRole(role.rowId)}
                >
                  {role.name}
                </Button>
              ))}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setAddRoleOpen(false)}>
              Cancel
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Actions */}
      <Card>
        <CardHeader>
          <CardTitle>Actions</CardTitle>
          <CardDescription>Manage this user account</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-3">
            {/* Role Change */}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline">Change Role</Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent>
                <DropdownMenuLabel>Set Role</DropdownMenuLabel>
                <DropdownMenuSeparator />
                {AVAILABLE_ROLES.map((role) => (
                  <DropdownMenuItem
                    key={role}
                    onClick={() => handleRoleChange(role)}
                    disabled={role === user.role}
                  >
                    <span className="capitalize">{role}</span>
                    {role === user.role && (
                      <span className="ml-2 text-xs text-muted-foreground">(current)</span>
                    )}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuContent>
            </DropdownMenu>

            {/* Ban / Unban */}
            <Button
              variant={user.banned ? "outline" : "destructive"}
              onClick={handleBanToggle}
            >
              {user.banned ? "Unban User" : "Ban User"}
            </Button>

            {/* Delete / Restore */}
            {user.deletedAt ? (
              <Button variant="outline" onClick={handleRestore}>
                Restore User
              </Button>
            ) : (
              <>
                <Button
                  variant="destructive"
                  onClick={() => setDeleteDialogOpen(true)}
                >
                  Delete User
                </Button>

                <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Delete User</DialogTitle>
                      <DialogDescription>
                        Are you sure you want to delete{" "}
                        <span className="font-semibold">{user.name}</span> ({user.email})?
                        This is a soft delete and can be reversed.
                      </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                      <Button
                        variant="outline"
                        onClick={() => setDeleteDialogOpen(false)}
                      >
                        Cancel
                      </Button>
                      <Button variant="destructive" onClick={handleSoftDelete}>
                        Delete User
                      </Button>
                    </DialogFooter>
                  </DialogContent>
                </Dialog>
              </>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
