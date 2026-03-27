export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export interface LogicalConfig {
  publication_name: string;
  slot_name: string;
}

export interface QueryConfig {
  poll_interval: string;
}

export interface TableConfig {
  name: string;
  skip_snapshot?: boolean;
  primary_key?: string[];
  watermark_column?: string;
  iceberg?: {
    partition?: string[];
  };
}

export interface SourceConfig {
  mode: string;
  postgres: PostgresConfig;
  logical?: LogicalConfig;
  query?: QueryConfig;
}

export interface SinkConfig {
  catalog_uri: string;
  warehouse: string;
  namespace: string;
  s3_endpoint: string;
  s3_access_key: string;
  s3_secret_key: string;
  s3_region: string;
  flush_interval: string;
  flush_rows: number;
  flush_bytes?: number;
}

export interface StateConfig {
  path: string;
}

export interface PipelineConfig {
  tables: TableConfig[];
  source: SourceConfig;
  sink: SinkConfig;
  state: StateConfig;
}

export type PipelineStatus =
  | "starting"
  | "running"
  | "stopping"
  | "stopped"
  | "error";

export interface PipelineInfo {
  id: string;
  status: PipelineStatus;
  error?: string;
  config: PipelineConfig;
}

export const modeLabels: Record<string, string> = {
  logical: "Logical Replication",
  query: "Query-based",
};

export function modeLabel(mode: string): string {
  return modeLabels[mode] ?? mode;
}

const BASE = "/api/v1";

async function request<T>(
  path: string,
  options?: RequestInit
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (res.status === 204) return undefined as T;
  const body = await res.json();
  if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
  return body as T;
}

export function listPipelines() {
  return request<PipelineInfo[]>("/pipelines");
}

export function getPipeline(id: string) {
  return request<PipelineInfo>(`/pipelines/${id}`);
}

export function createPipeline(id: string, config: PipelineConfig) {
  return request<PipelineInfo>("/pipelines", {
    method: "POST",
    body: JSON.stringify({ id, config }),
  });
}

export function deletePipeline(id: string) {
  return request<void>(`/pipelines/${id}`, { method: "DELETE" });
}

export function addTable(pipelineId: string, table: string) {
  return request<{ status: string; table: string }>(
    `/pipelines/${pipelineId}/tables`,
    {
      method: "POST",
      body: JSON.stringify({ table }),
    }
  );
}

export function removeTable(pipelineId: string, table: string) {
  return request<void>(`/pipelines/${pipelineId}/tables/${table}`, {
    method: "DELETE",
  });
}

export function discoverTables(postgres: PostgresConfig) {
  return request<{ tables: string[] }>("/discover-tables", {
    method: "POST",
    body: JSON.stringify(postgres),
  });
}
