import { useState, useRef, useEffect, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation } from "urql";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ArrowLeftIcon, SaveIcon, TrashIcon } from "lucide-react";
import { BASE_PATH } from "@/lib/config";
import { MarkdownEditor } from "@/components/markdown/MarkdownEditor";

const DASHBOARD_QUERY = `
  query DashboardByRowId($id: String!) {
    dashboardByRowId(rowId: $id) {
      rowId
      name
      language
      code
      createdAt
      updatedAt
      userByUserId { name }
    }
  }
`;

const UPDATE_DASHBOARD_MUTATION = `
  mutation UpdateDashboardByRowId($id: String!, $patch: DashboardPatch!) {
    updateDashboardByRowId(input: { rowId: $id, patch: $patch }) {
      dashboard {
        rowId
        code
        updatedAt
      }
    }
  }
`;

const DELETE_DASHBOARD_MUTATION = `
  mutation DeleteDashboardByRowId($id: String!) {
    deleteDashboardByRowId(input: { rowId: $id }) {
      deletedDashboardNodeId
    }
  }
`;

interface DashboardData {
  rowId: string;
  name: string;
  language: string;
  code: string;
  createdAt: string;
  updatedAt: string;
  userByUserId: { name: string } | null;
}

export function AnalyticsDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const iframeRef = useRef<HTMLIFrameElement>(null);

  const [saving, setSaving] = useState(false);
  const [iframeReady, setIframeReady] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [markdownContent, setMarkdownContent] = useState("");
  const [markdownDirty, setMarkdownDirty] = useState(false);

  // Stable editor URL — set once from the initial query, not re-derived on
  // every render, so urql cache invalidation after save won't reload the iframe.
  const [editorUrl, setEditorUrl] = useState<string | null>(null);

  const [result] = useQuery({
    query: DASHBOARD_QUERY,
    variables: { id },
    pause: !id,
  });

  const [, updateDashboard] = useMutation(UPDATE_DASHBOARD_MUTATION);
  const [, deleteDashboard] = useMutation(DELETE_DASHBOARD_MUTATION);

  const dashboard: DashboardData | null = result.data?.dashboardByRowId || null;

  useEffect(() => {
    if (dashboard && dashboard.language === "markdown" && !markdownDirty) {
      setMarkdownContent(dashboard.code ?? "");
    }
  }, [dashboard, markdownDirty]);

  useEffect(() => {
    if (dashboard && dashboard.language !== "markdown" && !editorUrl) {
      const subpath = dashboard.language === "r" ? "r" : "py";
      const fragment = dashboard.code ? `#${encodeURIComponent(dashboard.code)}` : "";
      setEditorUrl(`${BASE_PATH}/shinylive/${subpath}/editor/${fragment}`);
    }
  }, [dashboard, editorUrl]);

  const handleSave = useCallback(async () => {
    if (!dashboard) return;

    setSaving(true);
    try {
      let code: string;

      if (dashboard.language === "markdown") {
        code = markdownContent;
      } else {
        if (!iframeRef.current) return;
        const hash = iframeRef.current.contentWindow?.location.hash || "";
        code = hash.startsWith("#") ? hash.slice(1) : hash;

        if (!code) {
          toast.error("Editor has not loaded yet. Please wait and try again.");
          return;
        }
      }

      const res = await updateDashboard({
        id: dashboard.rowId,
        patch: { code },
      });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      setMarkdownDirty(false);
      toast.success("Dashboard saved.");
    } catch {
      toast.error("Failed to save dashboard.");
    } finally {
      setSaving(false);
    }
  }, [dashboard, updateDashboard, markdownContent]);

  async function handleDelete() {
    if (!dashboard) return;
    setDeleting(true);

    try {
      const res = await deleteDashboard({ id: dashboard.rowId });

      if (res.error) {
        toast.error(res.error.message);
        return;
      }

      toast.success("Dashboard deleted.");
      navigate("/admin/analytics");
    } catch {
      toast.error("Failed to delete dashboard.");
    } finally {
      setDeleting(false);
    }
  }

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
          Failed to load dashboard: {result.error.message}
        </p>
      </div>
    );
  }

  if (!dashboard) {
    return (
      <div className="text-center py-20">
        <p className="text-muted-foreground">Dashboard not found.</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 h-full">
      <div className="flex items-center gap-4">
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={() => navigate("/admin/analytics")}
        >
          <ArrowLeftIcon />
        </Button>
        <div className="flex-1 min-w-0">
          <h2 className="text-2xl font-bold truncate">{dashboard.name}</h2>
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Badge variant="outline">
              {dashboard.language === "python"
                ? "Python"
                : dashboard.language === "r"
                  ? "R"
                  : "Markdown"}
            </Badge>
            <span>
              by {dashboard.userByUserId?.name ?? "Unknown"}
            </span>
          </div>
        </div>
        <div className="flex gap-2 shrink-0">
          <Button onClick={handleSave} disabled={saving || (dashboard.language !== "markdown" && !iframeReady)}>
            <SaveIcon />
            {saving ? "Saving..." : "Save"}
          </Button>
          <Button
            variant="destructive"
            size="icon"
            onClick={() => setDeleteDialogOpen(true)}
          >
            <TrashIcon />
          </Button>
        </div>
      </div>

      <Separator />

      {dashboard.language === "markdown" ? (
        <MarkdownEditor
          value={markdownContent}
          onChange={(val) => {
            setMarkdownContent(val);
            setMarkdownDirty(true);
          }}
        />
      ) : (
        editorUrl && (
          <iframe
            ref={iframeRef}
            src={editorUrl}
            title={`${dashboard.name} — Shinylive editor`}
            onLoad={() => setIframeReady(true)}
            className="w-full flex-1 rounded-md border"
            style={{ minHeight: "600px" }}
          />
        )
      )}

      <Dialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Dashboard</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete "{dashboard.name}"? This action
              cannot be undone.
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
              {deleting ? "Deleting..." : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
