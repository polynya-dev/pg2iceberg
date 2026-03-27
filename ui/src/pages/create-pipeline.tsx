import { useState } from "react";
import { useNavigate } from "react-router-dom";
import type { PipelineConfig } from "@/lib/api";
import { createPipeline, discoverTables } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { ArrowLeft, ArrowRight, ChevronRight, Loader2, PlugZap } from "lucide-react";
import { toast } from "sonner";

const defaultConfig: PipelineConfig = {
  tables: [],
  source: {
    mode: "logical",
    postgres: {
      host: "localhost",
      port: 5432,
      database: "",
      user: "postgres",
      password: "",
    },
    logical: {
      publication_name: "pg2iceberg_pub",
      slot_name: "pg2iceberg_slot",
    },
  },
  sink: {
    catalog_uri: "",
    warehouse: "s3://warehouse/",
    namespace: "default",
    s3_endpoint: "",
    s3_access_key: "",
    s3_secret_key: "",
    s3_region: "us-east-1",
    flush_interval: "10s",
    flush_rows: 1000,
  },
  state: {
    path: "/data/pg2iceberg-state.json",
  },
};

type Step = "source" | "sink";

export function CreatePipelinePage() {
  const navigate = useNavigate();
  const [step, setStep] = useState<Step>("source");
  const [id, setId] = useState("");
  const [config, setConfig] = useState<PipelineConfig>(
    structuredClone(defaultConfig)
  );
  const [creating, setCreating] = useState(false);
  const [checking, setChecking] = useState(false);
  const [connected, setConnected] = useState(false);
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());

  function updatePostgres(field: string, value: string | number) {
    setConnected(false);
    setConfig((prev) => ({
      ...prev,
      source: {
        ...prev.source,
        postgres: { ...prev.source.postgres, [field]: value },
      },
    }));
  }

  function updateLogical(field: string, value: string) {
    setConfig((prev) => ({
      ...prev,
      source: {
        ...prev.source,
        logical: { ...prev.source.logical!, [field]: value },
      },
    }));
  }

  function updateSink(field: string, value: string | number) {
    setConfig((prev) => ({
      ...prev,
      sink: { ...prev.sink, [field]: value },
    }));
  }

  function toggleTable(table: string) {
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(table)) {
        next.delete(table);
      } else {
        next.add(table);
      }
      return next;
    });
  }

  function toggleAll() {
    if (selectedTables.size === availableTables.length) {
      setSelectedTables(new Set());
    } else {
      setSelectedTables(new Set(availableTables));
    }
  }

  async function handleCheckConnection() {
    const { host, port, database, user, password } = config.source.postgres;
    if (!database) {
      toast.error("Database name is required");
      return;
    }
    setChecking(true);
    try {
      const result = await discoverTables({
        host,
        port,
        database,
        user,
        password,
      });
      setAvailableTables(result.tables);
      setSelectedTables(new Set(result.tables));
      setConnected(true);
      toast.success(`Connected — found ${result.tables.length} table(s)`);
    } catch (e) {
      toast.error(`Failed to connect: ${(e as Error).message}`);
      setConnected(false);
    } finally {
      setChecking(false);
    }
  }

  function handleNext() {
    if (!id.trim()) {
      toast.error("Pipeline ID is required");
      return;
    }
    if (selectedTables.size === 0) {
      toast.error("Select at least one table to replicate");
      return;
    }
    setStep("sink");
  }

  async function handleCreate() {
    setCreating(true);
    try {
      const finalConfig = structuredClone(config);
      finalConfig.tables = Array.from(selectedTables).map((name) => ({ name }));

      await createPipeline(id.trim(), finalConfig);
      toast.success(`Pipeline "${id.trim()}" created`);
      navigate(`/pipelines/${id.trim()}`);
    } catch (e) {
      toast.error(`Failed to create: ${(e as Error).message}`);
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <Button
        variant="ghost"
        size="sm"
        onClick={() => {
          if (step === "sink") {
            setStep("source");
          } else {
            navigate("/");
          }
        }}
      >
        <ArrowLeft className="mr-1 h-4 w-4" />
        {step === "sink" ? "Back to Source" : "Back to Pipelines"}
      </Button>

      <div>
        <h2 className="text-2xl font-semibold">Create Pipeline</h2>
        <p className="text-sm text-muted-foreground">
          {step === "source"
            ? "Configure the source PostgreSQL database."
            : "Configure the Iceberg destination."}
        </p>
      </div>

      {step === "source" && (
        <>
          {/* Pipeline ID */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Pipeline ID</CardTitle>
              <CardDescription>
                A unique identifier for this pipeline.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Input
                id="pipeline-id"
                placeholder="my-pipeline"
                value={id}
                onChange={(e) => setId(e.target.value)}
              />
            </CardContent>
          </Card>

          {/* Connection */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Source (PostgreSQL)</CardTitle>
              <CardDescription>
                Enter connection details and check the connection to continue.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="pg-host">Host</Label>
                  <Input
                    id="pg-host"
                    value={config.source.postgres.host}
                    onChange={(e) => updatePostgres("host", e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="pg-port">Port</Label>
                  <Input
                    id="pg-port"
                    type="number"
                    value={config.source.postgres.port}
                    onChange={(e) =>
                      updatePostgres("port", parseInt(e.target.value) || 5432)
                    }
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="pg-database">Database</Label>
                <Input
                  id="pg-database"
                  value={config.source.postgres.database}
                  onChange={(e) => updatePostgres("database", e.target.value)}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="pg-user">User</Label>
                  <Input
                    id="pg-user"
                    value={config.source.postgres.user}
                    onChange={(e) => updatePostgres("user", e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="pg-password">Password</Label>
                  <Input
                    id="pg-password"
                    type="password"
                    value={config.source.postgres.password}
                    onChange={(e) => updatePostgres("password", e.target.value)}
                  />
                </div>
              </div>

              <Button
                onClick={handleCheckConnection}
                disabled={checking || !config.source.postgres.database}
              >
                {checking ? (
                  <Loader2 className="mr-1 h-4 w-4 animate-spin" />
                ) : (
                  <PlugZap className="mr-1 h-4 w-4" />
                )}
                {checking ? "Checking..." : "Check Connection"}
              </Button>
            </CardContent>
          </Card>

          {connected && (
            <>
              {/* Tables */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Tables</CardTitle>
                  <CardDescription>
                    Select tables to replicate ({selectedTables.size} of{" "}
                    {availableTables.length} selected).
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="rounded-md border">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-12">
                            <Checkbox
                              checked={
                                availableTables.length > 0 &&
                                selectedTables.size === availableTables.length
                              }
                              onCheckedChange={toggleAll}
                            />
                          </TableHead>
                          <TableHead>Table Name</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {availableTables.map((t) => (
                          <TableRow
                            key={t}
                            className="cursor-pointer"
                            onClick={() => toggleTable(t)}
                          >
                            <TableCell>
                              <Checkbox
                                checked={selectedTables.has(t)}
                                onCheckedChange={() => toggleTable(t)}
                              />
                            </TableCell>
                            <TableCell className="font-mono">{t}</TableCell>
                          </TableRow>
                        ))}
                        {availableTables.length === 0 && (
                          <TableRow>
                            <TableCell
                              colSpan={2}
                              className="text-center text-muted-foreground"
                            >
                              No tables found
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              {/* Advanced Settings */}
              <Collapsible>
                <CollapsibleTrigger className="flex items-center gap-1 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors [&[data-panel-open]>svg]:rotate-90">
                  <ChevronRight className="h-4 w-4 transition-transform" />
                  Advanced Settings
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <Card className="mt-3">
                    <CardHeader>
                      <CardTitle className="text-base">
                        Replication Settings
                      </CardTitle>
                      <CardDescription>
                        Configure how data is replicated from PostgreSQL.
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="space-y-2">
                        <Label>Replication Mode</Label>
                        <Select
                          value={config.source.mode}
                          onValueChange={(v) => {
                            if (v)
                              setConfig((prev) => ({
                                ...prev,
                                source: { ...prev.source, mode: v },
                              }));
                          }}
                          items={[
                            { label: "Logical Replication", value: "logical" },
                            { label: "Query-based", value: "query" },
                          ]}
                        >
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="logical">
                              Logical Replication
                            </SelectItem>
                            <SelectItem value="query">Query-based</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>

                      {config.source.mode === "logical" && (
                        <div className="grid grid-cols-2 gap-4">
                          <div className="space-y-2">
                            <Label htmlFor="pub-name">Publication Name</Label>
                            <Input
                              id="pub-name"
                              value={
                                config.source.logical?.publication_name ?? ""
                              }
                              onChange={(e) =>
                                updateLogical(
                                  "publication_name",
                                  e.target.value
                                )
                              }
                            />
                          </div>
                          <div className="space-y-2">
                            <Label htmlFor="slot-name">Slot Name</Label>
                            <Input
                              id="slot-name"
                              value={config.source.logical?.slot_name ?? ""}
                              onChange={(e) =>
                                updateLogical("slot_name", e.target.value)
                              }
                            />
                          </div>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                </CollapsibleContent>
              </Collapsible>

              {/* Next */}
              <div className="flex justify-end gap-2">
                <Button variant="outline" onClick={() => navigate("/")}>
                  Cancel
                </Button>
                <Button onClick={handleNext}>
                  Next
                  <ArrowRight className="ml-1 h-4 w-4" />
                </Button>
              </div>
            </>
          )}
        </>
      )}

      {step === "sink" && (
        <>
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Sink (Iceberg)</CardTitle>
              <CardDescription>
                Configure the Iceberg destination.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label htmlFor="catalog-uri">Catalog URI</Label>
                <Input
                  id="catalog-uri"
                  placeholder="http://iceberg-rest:8181"
                  value={config.sink.catalog_uri}
                  onChange={(e) => updateSink("catalog_uri", e.target.value)}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="warehouse">Warehouse</Label>
                  <Input
                    id="warehouse"
                    value={config.sink.warehouse}
                    onChange={(e) => updateSink("warehouse", e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="namespace">Namespace</Label>
                  <Input
                    id="namespace"
                    value={config.sink.namespace}
                    onChange={(e) => updateSink("namespace", e.target.value)}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="s3-endpoint">S3 Endpoint</Label>
                <Input
                  id="s3-endpoint"
                  placeholder="http://minio:9000"
                  value={config.sink.s3_endpoint}
                  onChange={(e) => updateSink("s3_endpoint", e.target.value)}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="s3-access">S3 Access Key</Label>
                  <Input
                    id="s3-access"
                    value={config.sink.s3_access_key}
                    onChange={(e) =>
                      updateSink("s3_access_key", e.target.value)
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="s3-secret">S3 Secret Key</Label>
                  <Input
                    id="s3-secret"
                    type="password"
                    value={config.sink.s3_secret_key}
                    onChange={(e) =>
                      updateSink("s3_secret_key", e.target.value)
                    }
                  />
                </div>
              </div>

              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="s3-region">S3 Region</Label>
                  <Input
                    id="s3-region"
                    value={config.sink.s3_region}
                    onChange={(e) => updateSink("s3_region", e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="flush-interval">Flush Interval</Label>
                  <Input
                    id="flush-interval"
                    value={config.sink.flush_interval}
                    onChange={(e) =>
                      updateSink("flush_interval", e.target.value)
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="flush-rows">Flush Rows</Label>
                  <Input
                    id="flush-rows"
                    type="number"
                    value={config.sink.flush_rows}
                    onChange={(e) =>
                      updateSink(
                        "flush_rows",
                        parseInt(e.target.value) || 1000
                      )
                    }
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Actions */}
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setStep("source")}>
              Back
            </Button>
            <Button onClick={handleCreate} disabled={creating}>
              {creating ? "Creating..." : "Create Pipeline"}
            </Button>
          </div>
        </>
      )}
    </div>
  );
}
