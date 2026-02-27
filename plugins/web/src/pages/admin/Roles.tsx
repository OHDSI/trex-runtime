import { useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { DataTable, type Column } from "@/components/DataTable";
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
import { PlusIcon } from "lucide-react";

const LIST_ROLES_QUERY = `
  query ListRoles($first: Int, $after: Cursor) {
    allRoles(first: $first, after: $after, orderBy: [NAME_ASC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        rowId
        name
        description
        createdAt
        userRolesByRoleId {
          totalCount
        }
      }
    }
  }
`;

const SEARCH_ROLES_QUERY = `
  query SearchRoles($query: String!, $first: Int, $after: Cursor) {
    searchRoles(query: $query, first: $first, after: $after) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        rowId
        name
        description
        createdAt
        userRolesByRoleId {
          totalCount
        }
      }
    }
  }
`;

const CREATE_ROLE_MUTATION = `
  mutation CreateRole($input: CreateRoleInput!) {
    createRole(input: $input) {
      role {
        id
      }
    }
  }
`;

interface RoleRow {
  rowId: string;
  name: string;
  description: string | null;
  createdAt: string;
  userRolesByRoleId: {
    totalCount: number;
  };
}

export function Roles() {
  const navigate = useNavigate();
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  const [addOpen, setAddOpen] = useState(false);

  const [formName, setFormName] = useState("");
  const [formDescription, setFormDescription] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const isSearching = search.length > 0;

  const [listResult] = useQuery({
    query: LIST_ROLES_QUERY,
    variables: { first: 25, after: cursor },
    pause: isSearching,
  });

  const [searchResult] = useQuery({
    query: SEARCH_ROLES_QUERY,
    variables: { query: search, first: 25, after: cursor },
    pause: !isSearching,
  });

  const [, createRole] = useMutation(CREATE_ROLE_MUTATION);

  const result = isSearching ? searchResult : listResult;
  const connection = isSearching
    ? result.data?.searchRoles
    : result.data?.allRoles;

  const roles: RoleRow[] = connection?.nodes || [];
  const pageInfo = connection?.pageInfo;
  const totalCount = connection?.totalCount ?? 0;

  function resetForm() {
    setFormName("");
    setFormDescription("");
  }

  async function handleCreate(e: FormEvent) {
    e.preventDefault();
    setSubmitting(true);

    try {
      const res = await createRole({
        input: {
          role: {
            name: formName,
            description: formDescription || null,
          },
        },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setAddOpen(false);
      resetForm();
      toast.success("Role created.");
    } catch {
      toast.error("Failed to create role.");
    } finally {
      setSubmitting(false);
    }
  }

  const columns: Column<RoleRow>[] = [
    {
      header: "Name",
      cell: (row) => <span className="font-medium">{row.name}</span>,
    },
    {
      header: "Description",
      cell: (row) => (
        <span className="text-sm text-muted-foreground">
          {row.description || "-"}
        </span>
      ),
    },
    {
      header: "Users",
      cell: (row) => (
        <span className="text-sm">{row.userRolesByRoleId.totalCount}</span>
      ),
    },
    {
      header: "Created",
      cell: (row) => new Date(row.createdAt).toLocaleDateString(),
    },
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-bold">Roles</h2>
        <p className="text-muted-foreground">
          Application roles ({totalCount} total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={roles}
        loading={result.fetching}
        searchPlaceholder="Search roles..."
        searchValue={search}
        onSearchChange={(val) => {
          setSearch(val);
          setCursor(null);
          setPrevCursors([]);
        }}
        emptyMessage="No roles found."
        onRowClick={(row) => navigate(`/admin/roles/${row.rowId}`)}
        hasNextPage={pageInfo?.hasNextPage}
        hasPreviousPage={prevCursors.length > 0}
        onNextPage={() => {
          if (pageInfo?.endCursor) {
            setPrevCursors((p) => [...p, cursor || ""]);
            setCursor(pageInfo.endCursor);
          }
        }}
        onPreviousPage={() => {
          const prev = prevCursors[prevCursors.length - 1];
          setPrevCursors((p) => p.slice(0, -1));
          setCursor(prev || null);
        }}
        actions={
          <Button onClick={() => setAddOpen(true)}>
            <PlusIcon />
            Add Role
          </Button>
        }
      />

      <Dialog
        open={addOpen}
        onOpenChange={(open) => {
          setAddOpen(open);
          if (!open) resetForm();
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Role</DialogTitle>
            <DialogDescription>
              Create a new application role.
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={handleCreate} className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <Label htmlFor="role-name">Name</Label>
              <Input
                id="role-name"
                placeholder="e.g. analyst"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="role-description">Description</Label>
              <Input
                id="role-description"
                placeholder="Optional description"
                value={formDescription}
                onChange={(e) => setFormDescription(e.target.value)}
              />
            </div>

            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => {
                  setAddOpen(false);
                  resetForm();
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submitting}>
                {submitting ? "Creating..." : "Add Role"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
