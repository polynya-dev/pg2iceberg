package logical

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var matTracer = otel.Tracer("pg2iceberg/materializer")

// Materializer reads staged change events from the Stream and applies them to
// the materialized (flattened) tables using Merge-on-Read: equality delete
// files mark old rows, new data files contain updated rows. A file index
// tracks PK→file mappings to resolve TOAST unchanged columns when needed.
type Materializer struct {
	cfg     config.SinkConfig
	catalog iceberg.MetadataCache
	s3      iceberg.ObjectStorage
	tables  map[string]*tableSink
	stream  *stream.Stream

	// Per-table TableWriter for the shared write path (serialize → upload → manifest → commit).
	tableWriters map[string]*iceberg.TableWriter
}

// InvalidateFileIndices is a no-op. The SnapshotWriter now seeds the FileIndex
// via MetadataStore during CommitTransaction, so the materializer's first cycle
// gets a fully-populated index without any S3 reads.
//
// Retained for interface compatibility with callers.
func (m *Materializer) InvalidateFileIndices() {}

// SyncTableWriter updates the TableWriter's schema after a schema evolution.
func (m *Materializer) SyncTableWriter(pgTable string) {
	ts, ok := m.tables[pgTable]
	if !ok {
		return
	}
	tw, ok := m.tableWriters[pgTable]
	if !ok {
		return
	}
	tw.UpdateSchema(ts.srcSchema, ts.matSchemaID)
}

func NewMaterializer(cfg config.SinkConfig, catalog iceberg.MetadataCache, s3 iceberg.ObjectStorage, tables map[string]*tableSink, str *stream.Stream) *Materializer {
	writers := make(map[string]*iceberg.TableWriter, len(tables))
	for pgTable, ts := range tables {
		writers[pgTable] = iceberg.NewTableWriter(iceberg.TableWriteConfig{
			Namespace:   cfg.Namespace,
			IcebergName: ts.icebergName,
			SrcSchema:   ts.srcSchema,
			PartSpec:    ts.partSpec,
			SchemaID:    ts.matSchemaID,
			TargetSize:  cfg.MaterializerTargetFileSizeOrDefault(),
			Concurrency: cfg.MaterializerConcurrencyOrDefault(),
		}, catalog, s3)
	}
	return &Materializer{
		cfg:          cfg,
		catalog:      catalog,
		s3:           s3,
		tables:       tables,
		stream:       str,
		tableWriters: writers,
	}
}

// MaterializeAll runs a single materialization pass for all tables.
// Called during graceful shutdown to ensure all flushed events are materialized.
func (m *Materializer) MaterializeAll(ctx context.Context) {
	if err := m.materializeCycle(ctx); err != nil {
		log.Printf("[materializer] final cycle (1) error: %v", err)
		// Retry — cursor was not advanced on failure.
		if err := m.materializeCycle(ctx); err != nil {
			log.Printf("[materializer] final cycle (2) error: %v", err)
		}
	}
}

// Run starts the materialization loop. Blocks until ctx is cancelled.
func (m *Materializer) Run(ctx context.Context) {
	interval := m.cfg.MaterializerDuration()
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.materializeCycle(ctx); err != nil {
				log.Printf("[materializer] cycle error: %v", err)
			}
		}
	}
}

// materializeCycle runs a single materialization cycle. For each table, it
// reads new log entries from the Stream, prepares the merge-on-read commit,
// then commits atomically. Recovery is automatic: if the cursor was not
// advanced, the next cycle re-reads the same entries.
func (m *Materializer) materializeCycle(ctx context.Context) error {
	ctx, span := matTracer.Start(ctx, "pg2iceberg.materialize")
	defer span.End()

	coord := m.stream.Coordinator()

	// Phase 1: Read new log entries per table and parse events.
	type tableWork struct {
		pgTable   string
		ts        *tableSink
		events    []MatEvent
		maxOffset int64
	}
	var work []tableWork

	for pgTable, ts := range m.tables {
		cursor, err := coord.GetCursor(ctx, pgTable)
		if err != nil {
			return fmt.Errorf("get cursor for %s: %w", pgTable, err)
		}

		entries, err := m.stream.Read(ctx, pgTable, cursor)
		if err != nil {
			return fmt.Errorf("read log for %s: %w", pgTable, err)
		}
		if len(entries) == 0 {
			continue
		}

		events, err := m.readEventsFromLog(ctx, entries, ts.srcSchema)
		if err != nil {
			return fmt.Errorf("read events for %s: %w", pgTable, err)
		}
		if len(events) == 0 {
			continue
		}

		maxOffset := entries[len(entries)-1].EndOffset
		work = append(work, tableWork{pgTable, ts, events, maxOffset})
	}

	if len(work) == 0 {
		return nil
	}

	// Phase 2: Prepare tables in parallel.
	type prepInput struct {
		pgTable string
		ts      *tableSink
		events  []MatEvent
	}
	results := make([]*preparedMaterialization, len(work))
	var prepErr error
	var prepErrTable string
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for i, w := range work {
		i, w := i, w
		g.Go(func() error {
			prep, err := m.prepareTable(gctx, w.pgTable, w.ts, w.events)
			if err != nil {
				mu.Lock()
				if prepErr == nil {
					prepErr = err
					prepErrTable = w.pgTable
				}
				mu.Unlock()
				return err
			}
			results[i] = prep
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		pipeline.MaterializerErrorsTotal.WithLabelValues(prepErrTable).Inc()
		err = fmt.Errorf("prepare %s: %w", prepErrTable, prepErr)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	var prepared []*preparedMaterialization
	for _, p := range results {
		if p != nil {
			prepared = append(prepared, p)
		}
	}
	if len(prepared) == 0 {
		return nil
	}

	// Phase 3: Atomic multi-table commit.
	commits := make([]iceberg.TableCommit, len(prepared))
	for i, p := range prepared {
		if p.prepared != nil {
			commits[i] = p.prepared.ToTableCommit()
		} else {
			commits[i] = iceberg.TableCommit{
				Table:             p.icebergName,
				CurrentSnapshotID: p.prevSnapID,
				Snapshot:          p.commit,
			}
		}
	}

	if err := m.catalog.CommitTransaction(ctx, m.cfg.Namespace, commits); err != nil {
		for _, p := range prepared {
			pipeline.MaterializerErrorsTotal.WithLabelValues(p.pgTable).Inc()
		}
		err = fmt.Errorf("commit materialized transaction: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Phase 4: Success — advance cursors and apply side effects.
	for _, w := range work {
		if err := coord.SetCursor(ctx, w.pgTable, w.maxOffset); err != nil {
			log.Printf("[materializer] WARNING: failed to advance cursor for %s: %v", w.pgTable, err)
		}
	}
	for _, p := range prepared {
		m.applyPostCommit(p)
	}

	if len(prepared) > 1 {
		log.Printf("[materializer] cycle complete: %d tables committed atomically", len(prepared))
	}

	// Phase 5: Compaction — rewrite small files if thresholds exceeded.
	cc := iceberg.CompactionConfig{
		DataFileThreshold:   m.cfg.CompactionDataFilesOrDefault(),
		DeleteFileThreshold: m.cfg.CompactionDeleteFilesOrDefault(),
	}
	for _, p := range prepared {
		tw := m.tableWriters[p.pgTable]
		ts := m.tables[p.pgTable]
		compacted, err := tw.Compact(ctx, ts.srcSchema.PK, cc)
		if err != nil {
			log.Printf("[materializer] compaction error for %s: %v", p.pgTable, err)
			continue
		}
		if compacted == nil {
			continue
		}
		err = m.catalog.CommitTransaction(ctx, m.cfg.Namespace, []iceberg.TableCommit{compacted.ToTableCommit()})
		if err != nil {
			log.Printf("[materializer] compaction commit error for %s: %v", p.pgTable, err)
			continue
		}
	}

	return nil
}

// MatEvent represents a parsed change event from a staged Parquet file.
type MatEvent struct {
	op            string // "I", "U", "D"
	lsn           int64
	seq           int64
	unchangedCols []string
	row           map[string]any // user columns only
}

// preparedMaterialization holds all artifacts produced by prepareTable,
// ready to be committed atomically with other tables.
type preparedMaterialization struct {
	pgTable     string
	ts          *tableSink
	icebergName string // ts.icebergName
	prevSnapID  int64  // materialized table snapshot before this commit
	commit      iceberg.SnapshotCommit
	prepared    *iceberg.PreparedCommit // for post-commit delegation to TableWriter

	// Deferred side effects.
	events         []MatEvent // retained for metrics (event count, max LSN)
	dataCount      int
	deleteCount    int
	deleteRowCount int64
	bucketCount    int

	// Timing phases for diagnostics.
	start  time.Time
	tDrain time.Duration
	tFold  time.Duration
	tToast time.Duration
}

// MaterializeTable reads new events for one table and applies them to the
// materialized table using Merge-on-Read. This is a convenience wrapper that
// reads from the stream, prepares, commits, and applies post-commit effects
// for a single table.
func (m *Materializer) MaterializeTable(ctx context.Context, pgTable string, ts *tableSink) error {
	coord := m.stream.Coordinator()

	cursor, err := coord.GetCursor(ctx, pgTable)
	if err != nil {
		return fmt.Errorf("get cursor: %w", err)
	}
	entries, err := m.stream.Read(ctx, pgTable, cursor)
	if err != nil {
		return fmt.Errorf("read log: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	events, err := m.readEventsFromLog(ctx, entries, ts.srcSchema)
	if err != nil {
		return err
	}

	prep, err := m.prepareTable(ctx, pgTable, ts, events)
	if err != nil {
		return err
	}
	if prep == nil {
		return nil
	}

	tc := iceberg.TableCommit{
		Table:             prep.icebergName,
		CurrentSnapshotID: prep.prevSnapID,
		Snapshot:          prep.commit,
	}
	if prep.prepared != nil {
		tc = prep.prepared.ToTableCommit()
	}
	err = m.catalog.CommitTransaction(ctx, m.cfg.Namespace, []iceberg.TableCommit{tc})
	if err != nil {
		return fmt.Errorf("commit materialized snapshot: %w", err)
	}

	maxOffset := entries[len(entries)-1].EndOffset
	if err := coord.SetCursor(ctx, pgTable, maxOffset); err != nil {
		log.Printf("[materializer] WARNING: failed to advance cursor for %s: %v", pgTable, err)
	}

	m.applyPostCommit(prep)
	return nil
}

// prepareTable reads new events for one table, folds them into final-state-per-PK,
// resolves TOAST columns, then delegates the write path (serialize → upload →
// manifest assembly) to the shared iceberg.TableWriter. Does NOT commit.
func (m *Materializer) prepareTable(ctx context.Context, pgTable string, ts *tableSink,
	events []MatEvent) (*preparedMaterialization, error) {

	ctx, span := matTracer.Start(ctx, "pg2iceberg.materialize.table", trace.WithAttributes(attribute.String("table", pgTable)))
	defer span.End()

	start := time.Now()
	ns := m.cfg.Namespace

	if len(events) == 0 {
		return nil, nil
	}

	pk := ts.srcSchema.PK
	if len(pk) == 0 {
		log.Printf("[materializer] skipping %s: no primary key", pgTable)
		return nil, nil
	}

	tw := m.tableWriters[pgTable]

	// Build/refresh file index for TOAST resolution and DELETE routing.
	if _, err := tw.BuildFileIndex(ctx, pk); err != nil {
		err = fmt.Errorf("build file index: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	tDrain := time.Since(start)

	// --- Flat fold: events → final state per PK ---
	type pkState struct {
		op            string
		row           map[string]any
		unchangedCols []string
	}
	state := make(map[string]*pkState, len(events)/2+1)
	for _, ev := range events {
		pkKey := iceberg.BuildPKKey(ev.row, pk)
		existing := state[pkKey]

		switch ev.op {
		case "I":
			state[pkKey] = &pkState{op: "I", row: ev.row}
		case "U":
			if existing != nil && len(ev.unchangedCols) > 0 {
				merged := make(map[string]any, len(ev.row))
				for k, v := range ev.row {
					merged[k] = v
				}
				for _, col := range ev.unchangedCols {
					if val, ok := existing.row[col]; ok {
						merged[col] = val
					}
				}
				state[pkKey] = &pkState{op: "U", row: merged}
			} else {
				state[pkKey] = &pkState{
					op:            "U",
					row:           ev.row,
					unchangedCols: ev.unchangedCols,
				}
			}
		case "D":
			state[pkKey] = &pkState{op: "D", row: ev.row}
		}
	}

	tFold := time.Since(start)

	// --- TOAST resolution ---
	var allUnresolvedPKs []string
	for k, v := range state {
		if v.op == "U" && len(v.unchangedCols) > 0 {
			allUnresolvedPKs = append(allUnresolvedPKs, k)
		}
	}

	fileIdx := m.catalog.FileIndex(ns, ts.icebergName)
	if len(allUnresolvedPKs) > 0 && fileIdx != nil {
		affectedFilePaths := fileIdx.AffectedFiles(allUnresolvedPKs)
		resolved := 0
		for path := range affectedFilePaths {
			dfKey, err := iceberg.KeyFromURI(path)
			if err != nil {
				err = fmt.Errorf("TOAST: parse URI %s: %w", path, err)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			data, err := iceberg.DownloadWithRetry(ctx, m.s3, dfKey)
			if err != nil {
				err = fmt.Errorf("TOAST: download %s: %w", path, err)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			rows, err := iceberg.ReadParquetRows(data, ts.srcSchema)
			if err != nil {
				err = fmt.Errorf("TOAST: read %s: %w", path, err)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			for _, row := range rows {
				pkKey := iceberg.BuildPKKey(row, pk)
				s, ok := state[pkKey]
				if !ok || s.op != "U" || len(s.unchangedCols) == 0 {
					continue
				}
				for _, col := range s.unchangedCols {
					if val, exists := row[col]; exists {
						s.row[col] = val
					}
				}
				s.unchangedCols = nil
				resolved++
			}
		}
		if resolved > 0 {
			log.Printf("[materializer] TOAST: resolved %d/%d rows for %s (%d files scanned)",
				resolved, len(allUnresolvedPKs), pgTable, len(affectedFilePaths))
		}
	}

	tToast := time.Since(start)

	// --- Convert folded state to RowState and delegate to TableWriter ---
	rowStates := make([]iceberg.RowState, 0, len(state))
	for _, s := range state {
		rowStates = append(rowStates, iceberg.RowState{
			Op:  s.op,
			Row: s.row,
		})
	}

	prepared, err := tw.Prepare(ctx, rowStates, pk)
	if err != nil {
		err = fmt.Errorf("prepare: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if prepared == nil {
		return nil, nil
	}

	return &preparedMaterialization{
		pgTable:        pgTable,
		ts:             ts,
		icebergName:    ts.icebergName,
		prevSnapID:     prepared.PrevSnapshotID,
		commit:         prepared.Commit,
		prepared:       prepared,
		events:         events,
		dataCount:      prepared.DataCount,
		deleteCount:    prepared.DeleteCount,
		deleteRowCount: prepared.DeleteRowCount,
		bucketCount:    prepared.BucketCount,
		start:          start,
		tDrain:         tDrain,
		tFold:          tFold,
		tToast:         tToast,
	}, nil
}

// applyPostCommit finalizes side effects after a successful catalog commit.
func (m *Materializer) applyPostCommit(prep *preparedMaterialization) {
	duration := time.Since(prep.start)

	var maxLSN int64
	for _, ev := range prep.events {
		if ev.lsn > maxLSN {
			maxLSN = ev.lsn
		}
	}
	if maxLSN > 0 {
		pipeline.MaterializerMaterializedLSN.WithLabelValues(prep.pgTable).Set(float64(maxLSN))
	}

	pipeline.MaterializerDurationSeconds.WithLabelValues(prep.pgTable).Observe(duration.Seconds())
	pipeline.MaterializerRunsTotal.WithLabelValues(prep.pgTable, "stream").Inc()
	pipeline.MaterializerEventsTotal.WithLabelValues(prep.pgTable).Add(float64(len(prep.events)))
	pipeline.MaterializerDataFilesWrittenTotal.WithLabelValues(prep.pgTable).Add(float64(prep.dataCount))
	pipeline.MaterializerDeleteFilesWrittenTotal.WithLabelValues(prep.pgTable).Add(float64(prep.deleteCount))
	pipeline.MaterializerDeleteRowsTotal.WithLabelValues(prep.pgTable).Add(float64(prep.deleteRowCount))

	log.Printf("[materializer] materialized %s: %d events (stream), %d buckets, %d data files, %d delete files (%.1fs) [drain=%.0fms fold=%.0fms toast=%.0fms]",
		prep.pgTable, len(prep.events), prep.bucketCount, prep.dataCount, prep.deleteCount, duration.Seconds(),
		float64(prep.tDrain.Milliseconds()), float64((prep.tFold-prep.tDrain).Milliseconds()),
		float64((prep.tToast-prep.tFold).Milliseconds()))
}

// readEventsFromLog downloads staged Parquet files referenced by log entries
// and parses them into MatEvent structs.
func (m *Materializer) readEventsFromLog(ctx context.Context, entries []stream.LogEntry, srcSchema *postgres.TableSchema) ([]MatEvent, error) {
	eventsSchema := iceberg.EventsTableSchema(srcSchema)
	var events []MatEvent

	for _, entry := range entries {
		data, err := m.stream.Download(ctx, entry.S3Path)
		if err != nil {
			return nil, fmt.Errorf("download staged file %s: %w", entry.S3Path, err)
		}

		rows, err := iceberg.ReadParquetRows(data, eventsSchema)
		if err != nil {
			return nil, fmt.Errorf("read staged file %s: %w", entry.S3Path, err)
		}

		for _, row := range rows {
			lsn, err := iceberg.ToInt64(row["_lsn"])
			if err != nil {
				return nil, fmt.Errorf("parse _lsn in %s: %w", entry.S3Path, err)
			}
			seq, err := iceberg.ToInt64(row["_seq"])
			if err != nil {
				return nil, fmt.Errorf("parse _seq in %s: %w", entry.S3Path, err)
			}
			ev := MatEvent{
				op:  fmt.Sprintf("%v", row["_op"]),
				lsn: lsn,
				seq: seq,
			}

			if uc, ok := row["_unchanged_cols"]; ok && uc != nil {
				ucStr := fmt.Sprintf("%v", uc)
				if ucStr != "" {
					ev.unchangedCols = strings.Split(ucStr, ",")
				}
			}

			ev.row = make(map[string]any, len(srcSchema.Columns))
			for _, col := range srcSchema.Columns {
				ev.row[col.Name] = row[col.Name]
			}

			events = append(events, ev)
		}
	}

	return events, nil
}
