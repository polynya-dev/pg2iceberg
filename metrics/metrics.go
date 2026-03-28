package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "pg2iceberg"

// Pipeline status gauge. Values: 0=stopped, 1=starting, 2=snapshotting, 3=running, 4=stopping, 5=error.
var PipelineStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "pipeline_status",
	Help:      "Current pipeline status (0=stopped, 1=starting, 2=snapshotting, 3=running, 4=stopping, 5=error).",
}, []string{"pipeline"})

// StatusToFloat maps pipeline status strings to numeric gauge values.
var StatusToFloat = map[string]float64{
	"stopped":      0,
	"starting":     1,
	"snapshotting": 2,
	"running":      3,
	"stopping":     4,
	"error":        5,
}

var PipelineUptimeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "pipeline_uptime_seconds",
	Help:      "Pipeline uptime in seconds.",
}, []string{"pipeline"})

// --- Replication ---

var ConfirmedLSN = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "confirmed_lsn",
	Help:      "Last confirmed replication LSN position.",
}, []string{"pipeline"})

var ReplicationLagBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "replication_lag_bytes",
	Help:      "Replication lag in bytes (current WAL position minus confirmed LSN).",
}, []string{"pipeline"})

var WALRetainedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "wal_retained_bytes",
	Help:      "WAL bytes retained by the replication slot.",
}, []string{"pipeline"})

// --- Event processing ---

var RowsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "rows_processed_total",
	Help:      "Total rows processed by operation type.",
}, []string{"pipeline", "table", "operation"})

var BytesProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "bytes_processed_total",
	Help:      "Total bytes processed (flushed to Iceberg).",
}, []string{"pipeline"})

var EventsBuffered = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "events_buffered",
	Help:      "Currently buffered events awaiting flush.",
}, []string{"pipeline"})

var BytesBuffered = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "bytes_buffered",
	Help:      "Estimated bytes currently buffered.",
}, []string{"pipeline"})

// --- Flush ---

var FlushDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Name:      "flush_duration_seconds",
	Help:      "Time taken to flush buffered data to Iceberg.",
	Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~200s
}, []string{"pipeline"})

var FlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "flush_total",
	Help:      "Total number of flush operations.",
}, []string{"pipeline"})

var FlushErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "flush_errors_total",
	Help:      "Total number of failed flush operations.",
}, []string{"pipeline"})

var FlushRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "flush_rows_total",
	Help:      "Total rows flushed to Iceberg.",
}, []string{"pipeline"})

// --- S3 ---

var S3OperationDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Name:      "s3_operation_duration_seconds",
	Help:      "Time taken for S3 operations.",
	Buckets:   prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms to ~20s
}, []string{"operation"})

var S3BytesUploadedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "s3_bytes_uploaded_total",
	Help:      "Total bytes uploaded to S3.",
})

var S3BytesDownloadedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "s3_bytes_downloaded_total",
	Help:      "Total bytes downloaded from S3.",
})

var S3ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "s3_errors_total",
	Help:      "Total S3 operation errors.",
}, []string{"operation"})

// --- Catalog ---

var CatalogOperationDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Name:      "catalog_operation_duration_seconds",
	Help:      "Time taken for Iceberg catalog operations.",
	Buckets:   prometheus.ExponentialBuckets(0.01, 2, 10), // 10ms to ~5s
}, []string{"operation"})

var CatalogErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "catalog_errors_total",
	Help:      "Total catalog operation errors.",
}, []string{"operation"})

// --- TOAST ---

var ToastLookupsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "toast_lookups_total",
	Help:      "Total TOAST resolution lookup queries.",
}, []string{"pipeline", "table"})

var ToastRowsResolvedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "toast_rows_resolved_total",
	Help:      "Total rows resolved via TOAST lookups.",
}, []string{"pipeline", "table"})

// --- Compaction ---

var CompactionDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Name:      "compaction_duration_seconds",
	Help:      "Time taken for compaction runs.",
	Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~500s
}, []string{"table"})

var CompactionRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "compaction_runs_total",
	Help:      "Total compaction runs.",
}, []string{"table", "status"})

var CompactionInputRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "compaction_input_rows_total",
	Help:      "Total input rows processed during compaction.",
}, []string{"table"})

var CompactionDeletedRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "compaction_deleted_rows_total",
	Help:      "Total rows removed during compaction.",
}, []string{"table"})

// --- Snapshot ---

var SnapshotRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "snapshot_rows_total",
	Help:      "Total rows captured during initial snapshot.",
}, []string{"pipeline", "table"})

var SnapshotTablesCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: namespace,
	Name:      "snapshot_tables_completed_total",
	Help:      "Number of tables that completed initial snapshot.",
}, []string{"pipeline"})

var SnapshotInProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "snapshot_in_progress",
	Help:      "Whether the pipeline is currently performing an initial snapshot (1=yes, 0=no).",
}, []string{"pipeline"})
