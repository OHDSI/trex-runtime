import { useState } from "react";
import { useQuery } from "urql";
import { DataTable, type Column } from "@/components/DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { authClient } from "@/lib/auth-client";
import { toast } from "sonner";

const LIST_SESSIONS_QUERY = `
  query ListSessions($first: Int, $after: Cursor, $filter: SessionFilter) {
    allSessions(first: $first, after: $after, filter: $filter, orderBy: [CREATED_AT_DESC]) {
      totalCount
      pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
      nodes {
        id
        userId
        ipAddress
        userAgent
        expiresAt
        createdAt
        userByUserId { id name email }
      }
    }
  }
`;

interface Session {
  id: string;
  userId: string;
  ipAddress: string | null;
  userAgent: string | null;
  expiresAt: string;
  createdAt: string;
  userByUserId: { id: string; name: string; email: string } | null;
}

function parseUserAgent(ua: string | null): string {
  if (!ua) return "Unknown";
  if (ua.includes("Chrome")) return "Chrome";
  if (ua.includes("Firefox")) return "Firefox";
  if (ua.includes("Safari")) return "Safari";
  if (ua.includes("Edge")) return "Edge";
  return ua.slice(0, 30) + "...";
}

export function Sessions() {
  const [cursor, setCursor] = useState<string | null>(null);
  const [prevCursors, setPrevCursors] = useState<string[]>([]);

  const [result] = useQuery({
    query: LIST_SESSIONS_QUERY,
    variables: { first: 25, after: cursor },
  });

  const sessions: Session[] =
    result.data?.allSessions?.nodes || [];
  const pageInfo = result.data?.allSessions?.pageInfo;

  const handleRevoke = async (sessionId: string) => {
    try {
      await authClient.admin.revokeUserSession({ sessionId });
      toast.success("Session revoked");
    } catch {
      toast.error("Failed to revoke session");
    }
  };

  const columns: Column<Session>[] = [
    {
      header: "User",
      cell: (row) => (
        <div>
          <div className="font-medium">{row.userByUserId?.name || "—"}</div>
          <div className="text-sm text-muted-foreground">
            {row.userByUserId?.email || row.userId}
          </div>
        </div>
      ),
    },
    {
      header: "IP",
      cell: (row) => (
        <span className="text-sm font-mono">{row.ipAddress || "—"}</span>
      ),
    },
    {
      header: "Browser",
      cell: (row) => parseUserAgent(row.userAgent),
    },
    {
      header: "Expires",
      cell: (row) => new Date(row.expiresAt).toLocaleDateString(),
    },
    {
      header: "Created",
      cell: (row) => new Date(row.createdAt).toLocaleDateString(),
    },
    {
      header: "Status",
      cell: (row) => {
        const expired = new Date(row.expiresAt) < new Date();
        return (
          <Badge variant={expired ? "secondary" : "default"}>
            {expired ? "Expired" : "Active"}
          </Badge>
        );
      },
    },
    {
      header: "",
      cell: (row) => (
        <Button
          variant="ghost"
          size="sm"
          onClick={(e) => {
            e.stopPropagation();
            handleRevoke(row.id);
          }}
        >
          Revoke
        </Button>
      ),
    },
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-bold">Sessions</h2>
        <p className="text-muted-foreground">
          Active user sessions ({result.data?.allSessions?.totalCount ?? 0} total)
        </p>
      </div>

      <DataTable
        columns={columns}
        data={sessions}
        loading={result.fetching}
        emptyMessage="No active sessions."
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
      />
    </div>
  );
}
