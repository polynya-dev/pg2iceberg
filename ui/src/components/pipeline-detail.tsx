import { useState } from "react";
import type { PipelineInfo } from "@/lib/api";
import { deletePipeline, addTable, removeTable, modeLabel } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { ComponentProps } from "react";
import { Input } from "@/components/ui/input";
import { Separator } from "@/components/ui/separator";
import { Trash2, Plus, X } from "lucide-react";
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

interface PipelineDetailProps {
  pipeline: PipelineInfo;
  onRefresh: () => void;
  onDeleted: () => void;
}

export function PipelineDetail({
  pipeline,
  onRefresh,
  onDeleted,
}: PipelineDetailProps) {
  const [newTable, setNewTable] = useState("");
  const [addingTable, setAddingTable] = useState(false);

  const tables = (pipeline.config.tables ?? []).map((t) => t.name);

  async function handleDelete() {
    try {
      await deletePipeline(pipeline.id);
      toast.success(`Pipeline "${pipeline.id}" deleted`);
      onDeleted();
    } catch (e) {
      toast.error(`Failed to delete: ${(e as Error).message}`);
    }
  }

  async function handleAddTable() {
    if (!newTable.trim()) return;
    setAddingTable(true);
    try {
      await addTable(pipeline.id, newTable.trim());
      toast.success(`Table "${newTable.trim()}" added`);
      setNewTable("");
      onRefresh();
    } catch (e) {
      toast.error(`Failed to add table: ${(e as Error).message}`);
    } finally {
      setAddingTable(false);
    }
  }

  async function handleRemoveTable(table: string) {
    try {
      await removeTable(pipeline.id, table);
      toast.success(`Table "${table}" removed`);
      onRefresh();
    } catch (e) {
      toast.error(`Failed to remove table: ${(e as Error).message}`);
    }
  }

  const { source, sink } = pipeline.config;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-2xl font-semibold">{pipeline.id}</h2>
          <div className="mt-1 flex items-center gap-2">
            <Badge variant={statusVariant(pipeline.status)}>
              {pipeline.status}
            </Badge>
            {pipeline.error && (
              <span className="text-sm text-destructive">{pipeline.error}</span>
            )}
          </div>
        </div>
        <AlertDialog>
          <AlertDialogTrigger
            render={
              <Button variant="destructive" size="sm" /> as ComponentProps<typeof AlertDialogTrigger>["render"]
            }
          >
            <Trash2 className="mr-1 h-4 w-4" />
            Delete
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Delete pipeline?</AlertDialogTitle>
              <AlertDialogDescription>
                This will stop the pipeline &quot;{pipeline.id}&quot; and remove
                it. This action cannot be undone.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction onClick={handleDelete}>
                Delete
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </div>

      <Separator />

      <Tabs defaultValue="source">
        <div className="flex justify-end">
          <TabsList>
            <TabsTrigger value="source">Source</TabsTrigger>
            <TabsTrigger value="sink">Sink</TabsTrigger>
            <TabsTrigger value="tables">Tables ({tables.length})</TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="source">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Source</CardTitle>
              <CardDescription>PostgreSQL connection</CardDescription>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Host</span>
                <span className="font-mono">
                  {source.postgres.host}:{source.postgres.port}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Database</span>
                <span className="font-mono">{source.postgres.database}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">User</span>
                <span className="font-mono">{source.postgres.user}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Mode</span>
                <Badge variant="outline">{modeLabel(source.mode)}</Badge>
              </div>
              {source.mode === "logical" && source.logical && (
                <>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Publication</span>
                    <span className="font-mono">
                      {source.logical.publication_name}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Slot</span>
                    <span className="font-mono">
                      {source.logical.slot_name}
                    </span>
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sink">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Sink</CardTitle>
              <CardDescription>Iceberg destination</CardDescription>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Catalog</span>
                <span className="font-mono">{sink.catalog_uri}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Warehouse</span>
                <span className="font-mono">{sink.warehouse}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Namespace</span>
                <span className="font-mono">{sink.namespace}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">S3 Endpoint</span>
                <span className="font-mono">{sink.s3_endpoint}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Flush</span>
                <span className="font-mono">
                  {sink.flush_interval} / {sink.flush_rows} rows
                </span>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="tables">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Tables</CardTitle>
              <CardDescription>
                Tables being replicated ({tables.length})
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Table Name</TableHead>
                    <TableHead className="w-16" />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {tables.map((t) => (
                    <TableRow key={t}>
                      <TableCell className="font-mono">{t}</TableCell>
                      <TableCell>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-8 w-8"
                          onClick={() => handleRemoveTable(t)}
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                  {tables.length === 0 && (
                    <TableRow>
                      <TableCell
                        colSpan={2}
                        className="text-center text-muted-foreground"
                      >
                        No tables
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
              <div className="mt-4 flex gap-2">
                <Input
                  placeholder="public.table_name"
                  value={newTable}
                  onChange={(e) => setNewTable(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleAddTable()}
                />
                <Button
                  size="sm"
                  onClick={handleAddTable}
                  disabled={addingTable || !newTable.trim()}
                >
                  <Plus className="mr-1 h-4 w-4" />
                  Add
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
