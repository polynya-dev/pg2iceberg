// Package snapshot implements CTID-range-chunked initial table snapshots
// that write directly to materialized Iceberg tables. Shared between logical
// and query replication modes.
package snapshot

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/utils"
)

var snapTracer = otel.Tracer("pg2iceberg/snapshot")

// TxFactory creates a new transaction for snapshot queries. Each table gets
// its own transaction so tables can be snapshotted in parallel. The caller
// controls isolation level and snapshot pinning inside the factory.
// The returned cleanup function must be called when the transaction is no
// longer needed (typically closes the underlying connection).
type TxFactory func(ctx context.Context) (pgx.Tx, func(context.Context), error)

// Table describes a table to be snapshotted.
type Table struct {
	Name   string
	Schema *postgres.TableSchema
}

// Deps holds the dependencies needed for direct-to-Iceberg snapshot writes.
type Deps struct {
	Catalog    iceberg.MetadataCache
	S3         iceberg.ObjectStorage
	SinkCfg    config.SinkConfig
	LogicalCfg config.LogicalConfig
	TableCfgs  []config.TableConfig
	Schemas    map[string]*postgres.TableSchema
	Store      pipeline.CheckpointStore
	PipelineID string
}

// Snapshotter performs chunked table copies using CTID range scans and writes
// directly to materialized Iceberg tables. Tables are snapshotted concurrently
// via a WorkerPool. S3 uploads within each commit use goroutines (I/O-bound,
// no pooling needed).
type Snapshotter struct {
	tables    []Table
	txFactory TxFactory
	pool      *utils.Pool // table-level concurrency (CPU-bound work)
	deps      Deps
	cpMu      sync.Mutex // serializes checkpoint saves across concurrent tables
}

// NewSnapshotter creates a Snapshotter for the given tables.
// Concurrency controls how many tables are snapshotted in parallel.
func NewSnapshotter(tables []Table, txFactory TxFactory, concurrency int, deps Deps) *Snapshotter {
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(tables) {
		concurrency = len(tables)
	}
	return &Snapshotter{
		tables:    tables,
		txFactory: txFactory,
		pool:      utils.NewPool(concurrency),
		deps:      deps,
	}
}

// Workers returns a snapshot of all worker statuses.
func (s *Snapshotter) Workers() []utils.Status {
	return s.pool.Workers()
}

// Run snapshots all tables using the worker pool, writing directly to
// materialized Iceberg tables. Each CTID chunk gets its own Iceberg commit
// and checkpoint update.
func (s *Snapshotter) Run(ctx context.Context) ([]utils.Result, error) {
	ctx, span := snapTracer.Start(ctx, "pg2iceberg.snapshot", trace.WithAttributes(
		attribute.Int("snapshot.table_count", len(s.tables)),
	))
	defer span.End()

	tasks := make([]utils.Task, len(s.tables))
	for i, tbl := range s.tables {
		tbl := tbl
		tasks[i] = utils.Task{
			Name: tbl.Name,
			Fn: func(ctx context.Context, progress *utils.Progress) error {
				return s.snapshotOneTable(ctx, tbl, progress)
			},
		}
	}

	results, err := s.pool.Run(ctx, tasks)
	for _, r := range results {
		if r.Err == nil {
			log.Printf("[snapshot] emitted %d rows for %s", r.Rows, r.Task)
		}
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return results, err
}

// snapshotOneTable copies all rows from a single table using CTID range chunks.
// Each chunk is: query PG → accumulate rows in memory → commit to Iceberg → checkpoint.
func (s *Snapshotter) snapshotOneTable(ctx context.Context, tbl Table, progress *utils.Progress) error {
	ctx, span := snapTracer.Start(ctx, "pg2iceberg.snapshot.table", trace.WithAttributes(
		attribute.String("table", tbl.Name),
	))
	defer span.End()

	tx, cleanup, err := s.txFactory(ctx)
	if err != nil {
		return fmt.Errorf("create tx for %s: %w", tbl.Name, err)
	}
	defer cleanup(ctx)
	defer tx.Rollback(ctx)

	// Disable idle timeout for long-running snapshot transactions.
	if _, err := tx.Exec(ctx, "SET idle_in_transaction_session_timeout = 0"); err != nil {
		return fmt.Errorf("set idle timeout for %s: %w", tbl.Name, err)
	}

	// Compute CTID chunks.
	chunkPages := s.deps.LogicalCfg.SnapshotChunkPagesOrDefault()
	chunks, err := ComputeChunks(ctx, tx, tbl.Name, chunkPages)
	if err != nil {
		return fmt.Errorf("compute chunks for %s: %w", tbl.Name, err)
	}

	// Load checkpoint to determine which chunks to skip.
	cp, err := s.deps.Store.Load(ctx, s.deps.PipelineID)
	if err != nil {
		return fmt.Errorf("load checkpoint for %s: %w", tbl.Name, err)
	}
	lastCompletedChunk := -1
	if cp.SnapshotChunks != nil {
		if idx, ok := cp.SnapshotChunks[tbl.Name]; ok {
			lastCompletedChunk = idx
		}
	}

	// Look up the materialized table's Iceberg name and partition spec.
	icebergName := postgres.TableToIceberg(tbl.Name)
	var partSpec *iceberg.PartitionSpec
	for _, tc := range s.deps.TableCfgs {
		if tc.Name == tbl.Name {
			partSpec, err = iceberg.BuildPartitionSpec(tc.Iceberg.Partition, tbl.Schema)
			if err != nil {
				return fmt.Errorf("build partition spec for %s: %w", tbl.Name, err)
			}
			break
		}
	}

	// Get schema ID from catalog.
	var schemaID int
	matTm, err := s.deps.Catalog.LoadTable(ctx, s.deps.SinkCfg.Namespace, icebergName)
	if err != nil {
		return fmt.Errorf("load materialized table %s: %w", icebergName, err)
	}
	if matTm != nil {
		schemaID = matTm.Metadata.CurrentSchemaID
	}

	// Create the snapshot writer.
	sw := iceberg.NewSnapshotWriter(iceberg.SnapshotWriterConfig{
		Namespace:   s.deps.SinkCfg.Namespace,
		IcebergName: icebergName,
		SrcSchema:   tbl.Schema,
		PartSpec:    partSpec,
		SchemaID:    schemaID,
		TargetSize:  s.deps.LogicalCfg.SnapshotTargetFileSizeOrDefault(),
	}, s.deps.Catalog, s.deps.S3)

	log.Printf("[snapshot] %s: %d chunks (pages=%d, chunk_size=%d), resuming after chunk %d",
		tbl.Name, len(chunks), chunkPages, chunkPages, lastCompletedChunk)

	for _, chunk := range chunks {
		if chunk.Index <= lastCompletedChunk {
			continue
		}

		start := time.Now()
		rowCount, err := s.snapshotChunk(ctx, tx, tbl, sw, chunk, progress)
		if err != nil {
			return fmt.Errorf("snapshot chunk %d for %s: %w", chunk.Index, tbl.Name, err)
		}

		// Checkpoint progress. For the last chunk, also mark the table
		// complete in a single Load+Save (avoids a redundant round-trip).
		isLastChunk := chunk.Index == chunks[len(chunks)-1].Index
		s.cpMu.Lock()
		cp, err = s.deps.Store.Load(ctx, s.deps.PipelineID)
		if err != nil {
			s.cpMu.Unlock()
			return fmt.Errorf("load checkpoint after chunk %d: %w", chunk.Index, err)
		}
		if isLastChunk {
			// Last chunk — mark table complete and clean up chunk tracker.
			if cp.SnapshotedTables == nil {
				cp.SnapshotedTables = make(map[string]bool)
			}
			cp.SnapshotedTables[tbl.Name] = true
			delete(cp.SnapshotChunks, tbl.Name)
		} else {
			// Intermediate chunk — record progress for crash recovery.
			if cp.SnapshotChunks == nil {
				cp.SnapshotChunks = make(map[string]int)
			}
			cp.SnapshotChunks[tbl.Name] = chunk.Index
		}
		if err := s.deps.Store.Save(ctx, s.deps.PipelineID, cp); err != nil {
			s.cpMu.Unlock()
			return fmt.Errorf("save checkpoint after chunk %d: %w", chunk.Index, err)
		}
		s.cpMu.Unlock()

		duration := time.Since(start)
		pipeline.SnapshotChunksCompleted.WithLabelValues(s.deps.PipelineID, tbl.Name).Inc()
		pipeline.SnapshotChunkDuration.WithLabelValues(s.deps.PipelineID, tbl.Name).Observe(duration.Seconds())
		log.Printf("[snapshot] %s chunk %d/%d: %d rows in %s",
			tbl.Name, chunk.Index+1, len(chunks), rowCount, duration.Truncate(time.Millisecond))
	}

	// If all chunks were skipped (crash recovery resumed past the last chunk),
	// ensure the table is still marked complete.
	s.cpMu.Lock()
	cp, err = s.deps.Store.Load(ctx, s.deps.PipelineID)
	if err != nil {
		s.cpMu.Unlock()
		return fmt.Errorf("load checkpoint for table complete: %w", err)
	}
	if cp.SnapshotedTables == nil || !cp.SnapshotedTables[tbl.Name] {
		if cp.SnapshotedTables == nil {
			cp.SnapshotedTables = make(map[string]bool)
		}
		cp.SnapshotedTables[tbl.Name] = true
		delete(cp.SnapshotChunks, tbl.Name)
		if err := s.deps.Store.Save(ctx, s.deps.PipelineID, cp); err != nil {
			s.cpMu.Unlock()
			return fmt.Errorf("save checkpoint table complete: %w", err)
		}
	}
	s.cpMu.Unlock()

	pipeline.SnapshotTablesCompleted.WithLabelValues(s.deps.PipelineID).Inc()
	return nil
}

// snapshotChunk queries a single CTID range, accumulates rows in memory,
// and commits to Iceberg.
func (s *Snapshotter) snapshotChunk(ctx context.Context, tx pgx.Tx, tbl Table, sw *iceberg.SnapshotWriter, chunk ChunkRange, progress *utils.Progress) (int64, error) {
	ctx, span := snapTracer.Start(ctx, "pg2iceberg.snapshot.chunk", trace.WithAttributes(
		attribute.String("table", tbl.Name),
		attribute.Int("chunk.index", chunk.Index),
	))
	defer span.End()

	query := ChunkQuery(tbl.Name, chunk)
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}

	descs := rows.FieldDescriptions()
	var rowCount int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}

		row := make(map[string]any, len(descs))
		for i, desc := range descs {
			row[string(desc.Name)] = PgValueToString(values[i])
		}

		if err := sw.AddRow(row); err != nil {
			rows.Close()
			return 0, fmt.Errorf("add row: %w", err)
		}
		rowCount++
		progress.Add(1)
		pipeline.SnapshotRowsTotal.WithLabelValues(s.deps.PipelineID, tbl.Name).Inc()
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows: %w", err)
	}

	// Commit accumulated rows to Iceberg (no-op if empty).
	if rowCount > 0 {
		if _, err := sw.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit: %w", err)
		}
	}

	return rowCount, nil
}

// PgValueToString converts a pgx native Go value to its text representation,
// matching the format produced by the WAL decoder (which always returns strings).
func PgValueToString(v any) any {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case string:
		return x
	case int16:
		return fmt.Sprintf("%d", x)
	case int32:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float32:
		return fmt.Sprintf("%g", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "t"
		}
		return "f"
	case time.Time:
		return x.Format("2006-01-02 15:04:05.999999-07")
	case pgtype.Numeric:
		if !x.Valid {
			return nil
		}
		intStr := x.Int.String()
		if x.Exp >= 0 {
			return intStr + strings.Repeat("0", int(x.Exp))
		}
		scale := int(-x.Exp)
		if len(intStr) <= scale {
			intStr = strings.Repeat("0", scale-len(intStr)+1) + intStr
		}
		pos := len(intStr) - scale
		return intStr[:pos] + "." + intStr[pos:]
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
