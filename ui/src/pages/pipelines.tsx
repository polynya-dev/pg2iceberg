import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import type { PipelineInfo } from "@/lib/api";
import { listPipelines, deletePipeline, modeLabel } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Plus, MoreHorizontal, Trash2, Eye } from "lucide-react";
import { toast } from "sonner";

function statusVariant(
  status: string
): "default" | "secondary" | "destructive" | "outline" {
  switch (status) {
    case "running":
      return "default";
    case "error":
      return "destructive";
    case "stopped":
    case "stopping":
      return "secondary";
    default:
      return "outline";
  }
}

export function PipelinesPage() {
  const navigate = useNavigate();
  const [pipelines, setPipelines] = useState<PipelineInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await listPipelines();
      setPipelines(data || []);
    } catch {
      // API may not be ready yet
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 3000);
    return () => clearInterval(interval);
  }, [refresh]);

  async function handleDelete() {
    if (!deleteTarget) return;
    try {
      await deletePipeline(deleteTarget);
      toast.success(`Pipeline "${deleteTarget}" deleted`);
      setDeleteTarget(null);
      refresh();
    } catch (e) {
      toast.error(`Failed to delete: ${(e as Error).message}`);
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">Pipelines</h2>
          <p className="text-sm text-muted-foreground">
            Manage Postgres to Iceberg replication pipelines.
          </p>
        </div>
        <Button onClick={() => navigate("/pipelines/new")}>
          <Plus className="mr-1 h-4 w-4" />
          New Pipeline
        </Button>
      </div>

      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>ID</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Namespace</TableHead>
              <TableHead>Mode</TableHead>
              <TableHead>Tables</TableHead>
              <TableHead className="w-12" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading && pipelines.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-24 text-center">
                  <div className="flex items-center justify-center gap-2 text-muted-foreground">
                    Loading...
                  </div>
                </TableCell>
              </TableRow>
            ) : pipelines.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={7}
                  className="h-24 text-center text-muted-foreground"
                >
                  No pipelines yet. Create one to get started.
                </TableCell>
              </TableRow>
            ) : (
              pipelines.map((p) => {
                const tableCount = p.config.tables?.length ?? 0;

                return (
                  <TableRow
                    key={p.id}
                    className="cursor-pointer"
                    onClick={() => navigate(`/pipelines/${p.id}`)}
                  >
                    <TableCell className="font-medium">{p.id}</TableCell>
                    <TableCell>
                      <Badge variant={statusVariant(p.status)}>
                        {p.status}
                      </Badge>
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {p.config.source.postgres.host}:
                      {p.config.source.postgres.port}/
                      {p.config.source.postgres.database}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {p.config.sink.namespace}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{modeLabel(p.config.source.mode)}</Badge>
                    </TableCell>
                    <TableCell>{tableCount}</TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger
                          className="inline-flex h-8 w-8 items-center justify-center rounded-md hover:bg-accent"
                          onClick={(e) => e.stopPropagation()}
                        >
                          <MoreHorizontal className="h-4 w-4" />
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem
                            onClick={(e) => {
                              e.stopPropagation();
                              navigate(`/pipelines/${p.id}`);
                            }}
                          >
                            <Eye className="mr-2 h-4 w-4" />
                            View Details
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            className="text-destructive"
                            onClick={(e) => {
                              e.stopPropagation();
                              setDeleteTarget(p.id);
                            }}
                          >
                            <Trash2 className="mr-2 h-4 w-4" />
                            Delete
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </TableCell>
                  </TableRow>
                );
              })
            )}
          </TableBody>
        </Table>
      </div>

      <AlertDialog
        open={deleteTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete pipeline?</AlertDialogTitle>
            <AlertDialogDescription>
              This will stop the pipeline &quot;{deleteTarget}&quot; and remove
              it. This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleDelete}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
