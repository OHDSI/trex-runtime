import { useState, useCallback, useMemo, useRef, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, gql } from "urql";
import { authClient } from "@/lib/auth-client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { DataTable, type Column } from "@/components/DataTable";
import { toast } from "sonner";

interface User {
  rowId: string;
  name: string;
  email: string;
  role: string;
  banned: boolean;
  deletedAt: string | null;
  createdAt: string;
}

interface UsersQueryResult {
  allUsers: {
    totalCount: number;
    nodes: User[];
    pageInfo: {
      hasNextPage: boolean;
      hasPreviousPage: boolean;
      endCursor: string | null;
      startCursor: string | null;
    };
  };
}

interface SearchUsersQueryResult {
  searchUsers: {
    totalCount: number;
    nodes: User[];
    pageInfo: {
      hasNextPage: boolean;
      hasPreviousPage: boolean;
      endCursor: string | null;
      startCursor: string | null;
    };
  };
}

const ALL_USERS_QUERY = gql`
  query AllUsers($first: Int, $after: Cursor) {
    allUsers(first: $first, after: $after) {
      totalCount
      nodes {
        rowId
        name
        email
        role
        banned
        deletedAt
        createdAt
      }
      pageInfo {
        hasNextPage
        hasPreviousPage
        endCursor
        startCursor
      }
    }
  }
`;

const SEARCH_USERS_QUERY = gql`
  query SearchUsers($search: String!, $first: Int, $after: Cursor) {
    searchUsers(query: $search, first: $first, after: $after) {
      totalCount
      nodes {
        rowId
        name
        email
        role
        banned
        deletedAt
        createdAt
      }
      pageInfo {
        hasNextPage
        hasPreviousPage
        endCursor
        startCursor
      }
    }
  }
`;

const PAGE_SIZE = 20;

function getUserStatus(user: User): { label: string; variant: "default" | "secondary" | "destructive" | "outline" } {
  if (user.deletedAt) return { label: "Deleted", variant: "destructive" };
  if (user.banned) return { label: "Banned", variant: "destructive" };
  return { label: "Active", variant: "default" };
}

function formatDate(dateString: string): string {
  return new Date(dateString).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

export function Users() {
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [cursorStack, setCursorStack] = useState<string[]>([]);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [newUserName, setNewUserName] = useState("");
  const [newUserEmail, setNewUserEmail] = useState("");
  const [newUserPassword, setNewUserPassword] = useState("");

  const isSearching = debouncedSearch.length > 0;

  const [usersResult] = useQuery<UsersQueryResult>({
    query: ALL_USERS_QUERY,
    variables: { first: PAGE_SIZE, after: cursor },
    pause: isSearching,
  });

  const [searchResult] = useQuery<SearchUsersQueryResult>({
    query: SEARCH_USERS_QUERY,
    variables: { search: debouncedSearch, first: PAGE_SIZE, after: cursor },
    pause: !isSearching,
  });

  const [creating, setCreating] = useState(false);

  const result = isSearching ? searchResult : usersResult;
  const queryData = isSearching
    ? searchResult.data?.searchUsers
    : usersResult.data?.allUsers;

  const users = queryData?.nodes ?? [];
  const pageInfo = queryData?.pageInfo;

  useEffect(() => {
    return () => { if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current); };
  }, []);

  const handleSearchChange = useCallback(
    (value: string) => {
      setSearch(value);
      setCursor(null);
      setCursorStack([]);
      if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current);
      debounceTimerRef.current = setTimeout(() => {
        setDebouncedSearch(value);
      }, 300);
    },
    []
  );

  function handleNextPage() {
    if (pageInfo?.endCursor) {
      setCursorStack((prev) => [...prev, cursor ?? ""]);
      setCursor(pageInfo.endCursor);
    }
  }

  function handlePreviousPage() {
    setCursorStack((prev) => {
      const stack = [...prev];
      const previousCursor = stack.pop();
      setCursor(previousCursor || null);
      return stack;
    });
  }

  async function handleCreateUser(e: React.FormEvent) {
    e.preventDefault();
    setCreating(true);
    try {
      const res = await authClient.admin.createUser({
        name: newUserName,
        email: newUserEmail,
        password: newUserPassword,
        role: "user",
      });

      if (res.error) {
        toast.error(res.error.message || "Failed to create user");
        return;
      }

      toast.success(`User ${newUserEmail} created successfully`);
      setDialogOpen(false);
      setNewUserName("");
      setNewUserEmail("");
      setNewUserPassword("");
    } catch {
      toast.error("Failed to create user");
    } finally {
      setCreating(false);
    }
  }

  const columns: Column<User>[] = useMemo(
    () => [
      { header: "Name", accessorKey: "name" },
      { header: "Email", accessorKey: "email" },
      {
        header: "Role",
        cell: (row) => (
          <Badge variant={row.role === "admin" ? "default" : "secondary"}>
            {row.role}
          </Badge>
        ),
      },
      {
        header: "Status",
        cell: (row) => {
          const status = getUserStatus(row);
          return <Badge variant={status.variant}>{status.label}</Badge>;
        },
      },
      {
        header: "Created",
        cell: (row) => formatDate(row.createdAt),
      },
    ],
    []
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Users</h1>
          <p className="text-muted-foreground">
            {queryData ? `${queryData.totalCount} total users` : "Manage user accounts"}
          </p>
        </div>
      </div>

      <DataTable
        columns={columns}
        data={users}
        loading={result.fetching}
        searchPlaceholder="Search users by name or email..."
        searchValue={search}
        onSearchChange={handleSearchChange}
        hasNextPage={pageInfo?.hasNextPage}
        hasPreviousPage={cursorStack.length > 0}
        onNextPage={handleNextPage}
        onPreviousPage={handlePreviousPage}
        onRowClick={(row) => navigate(`/admin/users/${row.rowId}`)}
        emptyMessage="No users found."
        actions={
          <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
            <DialogTrigger asChild>
              <Button>Create User</Button>
            </DialogTrigger>
            <DialogContent>
              <form onSubmit={handleCreateUser}>
                <DialogHeader>
                  <DialogTitle>Create User</DialogTitle>
                  <DialogDescription>
                    Create a new user account with email and password.
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4 py-4">
                  <div className="space-y-2">
                    <Label htmlFor="name">Name</Label>
                    <Input
                      id="name"
                      value={newUserName}
                      onChange={(e) => setNewUserName(e.target.value)}
                      placeholder="Full name"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="email">Email</Label>
                    <Input
                      id="email"
                      type="email"
                      value={newUserEmail}
                      onChange={(e) => setNewUserEmail(e.target.value)}
                      placeholder="user@example.com"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="password">Password</Label>
                    <Input
                      id="password"
                      type="password"
                      value={newUserPassword}
                      onChange={(e) => setNewUserPassword(e.target.value)}
                      placeholder="Minimum 8 characters"
                      minLength={8}
                      required
                    />
                  </div>
                </div>
                <DialogFooter>
                  <Button
                    type="submit"
                    disabled={creating}
                  >
                    {creating ? "Creating..." : "Create User"}
                  </Button>
                </DialogFooter>
              </form>
            </DialogContent>
          </Dialog>
        }
      />
    </div>
  );
}
