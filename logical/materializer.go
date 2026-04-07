package logical

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var matTracer = otel.Tracer("pg2iceberg/materializer")

// ChangeEventBuffer is a thread-safe buffer for change events, shared between
// the Sink (producer) and the Materializer (consumer). Created before both and
// injected into each, avoiding a circular dependency.
type ChangeEventBuffer struct {
	events     map[string][]MatEvent
	snapshotID map[string]int64 // latest events table snapshot ID per table
	mu         sync.Mutex
}

// NewChangeEventBuffer creates a new empty event buffer.
func NewChangeEventBuffer() *ChangeEventBuffer {
	return &ChangeEventBuffer{
		events:     make(map[string][]MatEvent),
		snapshotID: make(map[string]int64),
	}
}

// PushEvents adds change events for a table. Called by the Sink after flush.
// snapshotID is the events table snapshot that produced these events.
func (b *ChangeEventBuffer) PushEvents(pgTable string, events []MatEvent, snapshotID int64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events[pgTable] = append(b.events[pgTable], events...)
	b.snapshotID[pgTable] = snapshotID
	pipeline.MaterializerBufferSize.WithLabelValues(pgTable).Set(float64(len(b.events[pgTable])))
}

// Drain removes and returns all buffered events for a table,
// along with the latest events snapshot ID.
func (b *ChangeEventBuffer) Drain(pgTable string) ([]MatEvent, int64) {
	if b == nil {
		return nil, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	events := b.events[pgTable]
	snapID := b.snapshotID[pgTable]
	delete(b.events, pgTable)
	delete(b.snapshotID, pgTable)
	pipeline.MaterializerBufferSize.WithLabelValues(pgTable).Set(0)
	return events, snapID
}

// DrainAll removes and returns all buffered events for all tables at once,
// along with the latest events snapshot ID per table. This ensures events
// from the same Sink flush are consumed atomically across tables.
func (b *ChangeEventBuffer) DrainAll() (map[string][]MatEvent, map[string]int64) {
	if b == nil {
		return nil, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	events := b.events
	snapIDs := b.snapshotID
	b.events = make(map[string][]MatEvent)
	b.snapshotID = make(map[string]int64)
	for pgTable := range events {
		pipeline.MaterializerBufferSize.WithLabelValues(pgTable).Set(0)
	}
	return events, snapIDs
}

// Materializer reads change events from events tables and applies them to
// the materialized (flattened) tables using Merge-on-Read: equality delete
// files mark old rows, new data files contain updated rows. A file index
// tracks PK→file mappings to resolve TOAST unchanged columns when needed.
type Materializer struct {
	cfg     config.SinkConfig
	catalog iceberg.Catalog
	s3      iceberg.ObjectStorage
	tables  map[string]*tableSink
	buf     *ChangeEventBuffer

	// Tracks the last processed events snapshot per table.
	// Keyed by PG table name.
	lastEventsSnapshot map[string]int64

	// Per-table TableWriter for the shared write path (serialize → upload → manifest → commit).
	tableWriters map[string]*iceberg.TableWriter
}

// InvalidateFileIndices forces all table writers to rebuild their file indices
// on the next materialization cycle. Called after the snapshot phase completes
// so the materializer picks up rows written directly to materialized tables.
func (m *Materializer) InvalidateFileIndices() {
	for _, tw := range m.tableWriters {
		if tw.FileIdx != nil {
			tw.FileIdx.SnapshotID = 0
		}
	}
}

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

func NewMaterializer(cfg config.SinkConfig, catalog iceberg.Catalog, s3 iceberg.ObjectStorage, tables map[string]*tableSink, buf *ChangeEventBuffer) *Materializer {
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
		cfg:                cfg,
		catalog:            catalog,
		s3:                 s3,
		tables:             tables,
		buf:                buf,
		lastEventsSnapshot: make(map[string]int64),
		tableWriters:       writers,
	}
}

// SetLastEventsSnapshot restores the checkpoint for a table (called on startup).
func (m *Materializer) SetLastEventsSnapshot(pgTable string, snapshotID int64) {
	m.lastEventsSnapshot[pgTable] = snapshotID
}

// LastEventsSnapshots returns the current checkpoint state for persistence.
func (m *Materializer) LastEventsSnapshots() map[string]int64 {
	return m.lastEventsSnapshot
}

// MaterializeAll runs a single materialization pass for all tables.
// Called during graceful shutdown to ensure all flushed events are materialized.
//
// The in-memory ChangeEventBuffer is drained and discarded so that
// MaterializeAll runs two materialization cycles to ensure all events are
// processed before shutdown. The first cycle processes any buffered events
// atomically. The buffer is then drained to force the second cycle through
// the S3 path, which reads event files directly from the Iceberg events
// table. This is necessary because the Run() goroutine may have drained the
// buffer but failed to materialize (e.g. context cancelled), leaving a gap.
// The events table on S3 is the authoritative record of all flushed events,
// so reading from it guarantees nothing is skipped.
func (m *Materializer) MaterializeAll(ctx context.Context) {
	// Cycle 1: process buffered events atomically.
	if err := m.materializeCycle(ctx); err != nil {
		log.Printf("[materializer] final cycle (buffer) error: %v", err)
	}
	// Discard remaining buffer to force S3 path.
	m.buf.DrainAll()
	// Cycle 2: catch anything via S3 fallback.
	if err := m.materializeCycle(ctx); err != nil {
		log.Printf("[materializer] final cycle (s3) error: %v", err)
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

// materializeCycle runs a single materialization cycle for all tables.
// It drains all buffered events atomically, prepares each table, then
// commits all prepared tables atomically via CommitTransaction.
func (m *Materializer) materializeCycle(ctx context.Context) error {
	ctx, span := matTracer.Start(ctx, "pg2iceberg.materialize")
	defer span.End()

	// Phase 1: Drain all tables atomically.
	allEvents, allSnapIDs := m.buf.DrainAll()

	// Phase 2: Prepare all tables.
	var prepared []*preparedMaterialization
	for pgTable, ts := range m.tables {
		prep, err := m.prepareTable(ctx, pgTable, ts, allEvents[pgTable], allSnapIDs[pgTable])
		if err != nil {
			// Abort entire cycle. Drained events are safe on S3 in the
			// events tables; next cycle recovers via the S3 fallback path.
			pipeline.MaterializerErrorsTotal.WithLabelValues(pgTable).Inc()
			err = fmt.Errorf("prepare %s: %w", pgTable, err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		if prep != nil {
			prepared = append(prepared, prep)
		}
	}

	if len(prepared) == 0 {
		return nil
	}

	// Phase 3: Atomic multi-table commit.
	commits := make([]iceberg.TableCommit, len(prepared))
	for i, p := range prepared {
		commits[i] = iceberg.TableCommit{
			Table:             p.icebergName,
			CurrentSnapshotID: p.prevSnapID,
			Snapshot:          p.commit,
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

	// Phase 4: Apply side effects only after successful commit.
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
		err = m.catalog.CommitSnapshot(ctx, m.cfg.Namespace, compacted.IcebergName, compacted.PrevSnapshotID, compacted.Commit)
		if err != nil {
			log.Printf("[materializer] compaction commit error for %s: %v", p.pgTable, err)
			continue
		}
		tw.ApplyPostCommit(compacted)
	}

	return nil
}

// MatEvent represents a parsed change event from the events table.
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
	fromBuffer     bool
	bufSnapID      int64         // for checkpoint update (buffer path)
	s3SnapID       int64         // for checkpoint update (S3 path)
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
// drains the buffer, prepares, commits, and applies post-commit effects for
// a single table.
func (m *Materializer) MaterializeTable(ctx context.Context, pgTable string, ts *tableSink) error {
	events, bufSnapID := m.buf.Drain(pgTable)
	prep, err := m.prepareTable(ctx, pgTable, ts, events, bufSnapID)
	if err != nil {
		return err
	}
	if prep == nil {
		return nil
	}

	err = m.catalog.CommitTransaction(ctx, m.cfg.Namespace, []iceberg.TableCommit{{
		Table:             prep.icebergName,
		CurrentSnapshotID: prep.prevSnapID,
		Snapshot:          prep.commit,
	}})
	if err != nil {
		return fmt.Errorf("commit materialized snapshot: %w", err)
	}

	m.applyPostCommit(prep)
	return nil
}

// prepareTable reads new events for one table, folds them into final-state-per-PK,
// resolves TOAST columns, then delegates the write path (serialize → upload →
// manifest assembly) to the shared iceberg.TableWriter. Does NOT commit.
//
// The caller is responsible for committing via CommitTransaction and then
// calling applyPostCommit to finalize side effects.
func (m *Materializer) prepareTable(ctx context.Context, pgTable string, ts *tableSink,
	bufEvents []MatEvent, bufSnapID int64) (*preparedMaterialization, error) {

	ctx, span := matTracer.Start(ctx, "pg2iceberg.materialize.table", trace.WithAttributes(attribute.String("table", pgTable)))
	defer span.End()

	start := time.Now()
	catalog := m.catalog
	s3 := m.s3
	ns := m.cfg.Namespace

	events := bufEvents
	fromBuffer := len(events) > 0

	var s3SnapID int64
	if !fromBuffer {
		// No in-memory events — fall back to S3 (crash recovery path).
		eventsTm, err := catalog.LoadTable(ctx, ns, ts.eventsIcebergName)
		if err != nil {
			err = fmt.Errorf("load events table: %w", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		if eventsTm == nil || eventsTm.Metadata.CurrentSnapshotID == 0 {
			return nil, nil // no events yet
		}

		currentSnapshotID := eventsTm.Metadata.CurrentSnapshotID
		lastProcessed := m.lastEventsSnapshot[pgTable]

		if currentSnapshotID == lastProcessed {
			return nil, nil // already up to date
		}

		newFiles, err := m.findNewEventFiles(ctx, s3, eventsTm, lastProcessed)
		if err != nil {
			err = fmt.Errorf("find new event files: %w", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		if len(newFiles) == 0 {
			m.lastEventsSnapshot[pgTable] = currentSnapshotID
			return nil, nil
		}

		events, err = m.readEvents(ctx, s3, newFiles, ts.srcSchema)
		if err != nil {
			err = fmt.Errorf("read events: %w", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, err
		}
		if len(events) == 0 {
			m.lastEventsSnapshot[pgTable] = currentSnapshotID
			return nil, nil
		}

		// Track for deferred checkpoint update (applied after commit).
		s3SnapID = currentSnapshotID
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

	if len(allUnresolvedPKs) > 0 && tw.FileIdx != nil {
		affectedFilePaths := tw.FileIdx.AffectedFiles(allUnresolvedPKs)
		resolved := 0
		for path := range affectedFilePaths {
			dfKey, err := iceberg.KeyFromURI(path)
			if err != nil {
				err = fmt.Errorf("TOAST: parse URI %s: %w", path, err)
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			data, err := iceberg.DownloadWithRetry(ctx, s3, dfKey)
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
		if fromBuffer && bufSnapID > 0 {
			m.lastEventsSnapshot[pgTable] = bufSnapID
		} else if s3SnapID > 0 {
			m.lastEventsSnapshot[pgTable] = s3SnapID
		}
		return nil, nil
	}

	return &preparedMaterialization{
		pgTable:     pgTable,
		ts:          ts,
		icebergName: ts.icebergName,
		prevSnapID:  prepared.PrevSnapshotID,
		commit:      prepared.Commit,
		prepared:    prepared,
		fromBuffer:  fromBuffer,
		bufSnapID:   bufSnapID,
		s3SnapID:    s3SnapID,
		events:      events,
		dataCount:   prepared.DataCount,
		deleteCount: prepared.DeleteCount,
		deleteRowCount: prepared.DeleteRowCount,
		bucketCount:    prepared.BucketCount,
		start:       start,
		tDrain:      tDrain,
		tFold:       tFold,
		tToast:      tToast,
	}, nil
}

// applyPostCommit finalizes side effects after a successful catalog commit.
// Must be called only after CommitTransaction succeeds.
func (m *Materializer) applyPostCommit(prep *preparedMaterialization) {
	// Delegate manifest cache + file index invalidation to the TableWriter.
	if tw, ok := m.tableWriters[prep.pgTable]; ok && prep.prepared != nil {
		tw.ApplyPostCommit(prep.prepared)
	}

	// Update checkpoint.
	if prep.fromBuffer && prep.bufSnapID > 0 {
		m.lastEventsSnapshot[prep.pgTable] = prep.bufSnapID
	} else if prep.s3SnapID > 0 {
		m.lastEventsSnapshot[prep.pgTable] = prep.s3SnapID
	}

	// Metrics + logging.
	source := "buffer"
	if !prep.fromBuffer {
		source = "s3"
	}
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
	pipeline.MaterializerRunsTotal.WithLabelValues(prep.pgTable, source).Inc()
	pipeline.MaterializerEventsTotal.WithLabelValues(prep.pgTable).Add(float64(len(prep.events)))
	pipeline.MaterializerDataFilesWrittenTotal.WithLabelValues(prep.pgTable).Add(float64(prep.dataCount))
	pipeline.MaterializerDeleteFilesWrittenTotal.WithLabelValues(prep.pgTable).Add(float64(prep.deleteCount))
	pipeline.MaterializerDeleteRowsTotal.WithLabelValues(prep.pgTable).Add(float64(prep.deleteRowCount))

	log.Printf("[materializer] materialized %s: %d events (%s), %d buckets, %d data files, %d delete files (%.1fs) [drain=%.0fms fold=%.0fms toast=%.0fms]",
		prep.pgTable, len(prep.events), source, prep.bucketCount, prep.dataCount, prep.deleteCount, duration.Seconds(),
		float64(prep.tDrain.Milliseconds()), float64((prep.tFold-prep.tDrain).Milliseconds()),
		float64((prep.tToast-prep.tFold).Milliseconds()))
}

// findNewEventFiles returns data files added to the events table since the
// given snapshot ID. If lastSnapshotID is 0, returns all data files.
func (m *Materializer) findNewEventFiles(ctx context.Context, s3 iceberg.ObjectStorage, tm *iceberg.TableMetadata, lastSnapshotID int64) ([]iceberg.DataFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := iceberg.KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := iceberg.DownloadWithRetry(ctx, s3, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	manifestInfos, err := iceberg.ReadManifestList(mlData)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}

	var dataFiles []iceberg.DataFileInfo
	for _, mfi := range manifestInfos {
		if mfi.Content != 0 {
			continue
		}
		// If we have a checkpoint, only read manifests added after it.
		if lastSnapshotID > 0 && mfi.SnapshotID <= lastSnapshotID {
			continue
		}

		mKey, err := iceberg.KeyFromURI(mfi.Path)
		if err != nil {
			return nil, fmt.Errorf("parse manifest URI %s: %w", mfi.Path, err)
		}
		mData, err := iceberg.DownloadWithRetry(ctx, s3, mKey)
		if err != nil {
			return nil, fmt.Errorf("download manifest %s: %w", mfi.Path, err)
		}
		entries, err := iceberg.ReadManifest(mData)
		if err != nil {
			return nil, fmt.Errorf("read manifest %s: %w", mfi.Path, err)
		}
		for _, e := range entries {
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			dataFiles = append(dataFiles, e.DataFile)
		}
	}
	return dataFiles, nil
}

// readEvents reads event parquet files and parses them into ChangeEvent structs.
func (m *Materializer) readEvents(ctx context.Context, s3 iceberg.ObjectStorage, files []iceberg.DataFileInfo, srcSchema *postgres.TableSchema) ([]MatEvent, error) {
	var events []MatEvent

	// Build the events schema to read the parquet files.
	eventsSchema := iceberg.EventsTableSchema(srcSchema)

	for _, df := range files {
		key, err := iceberg.KeyFromURI(df.Path)
		if err != nil {
			return nil, fmt.Errorf("parse event file URI %s: %w", df.Path, err)
		}
		data, err := iceberg.DownloadWithRetry(ctx, s3, key)
		if err != nil {
			return nil, fmt.Errorf("download event file %s: %w", df.Path, err)
		}

		rows, err := iceberg.ReadParquetRows(data, eventsSchema)
		if err != nil {
			return nil, fmt.Errorf("read event file %s: %w", df.Path, err)
		}

		for _, row := range rows {
			lsn, err := iceberg.ToInt64(row["_lsn"])
			if err != nil {
				return nil, fmt.Errorf("parse _lsn in %s: %w", df.Path, err)
			}
			seq, err := iceberg.ToInt64(row["_seq"])
			if err != nil {
				return nil, fmt.Errorf("parse _seq in %s: %w", df.Path, err)
			}
			ev := MatEvent{
				op:  fmt.Sprintf("%v", row["_op"]),
				lsn: lsn,
				seq: seq,
			}

			// Parse unchanged cols.
			if uc, ok := row["_unchanged_cols"]; ok && uc != nil {
				ucStr := fmt.Sprintf("%v", uc)
				if ucStr != "" {
					ev.unchangedCols = strings.Split(ucStr, ",")
				}
			}

			// Extract user columns only.
			ev.row = make(map[string]any, len(srcSchema.Columns))
			for _, col := range srcSchema.Columns {
				ev.row[col.Name] = row[col.Name]
			}

			events = append(events, ev)
		}
	}

	return events, nil
}

