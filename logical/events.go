package logical

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/utils"
	"golang.org/x/sync/errgroup"
)


// txBuffer holds events for a single in-flight PG transaction.
type txBuffer struct {
	xid       uint32
	events    []postgres.ChangeEvent
	tables    map[string]bool // which tables this tx touches
	committed bool
	commitTS  time.Time // PG commit timestamp
}

// EventBuffer receives change events from the sink after a successful flush.
// Used to pass events to downstream consumers without S3 round-trips.
type EventBuffer interface {
	PushEvents(pgTable string, events []MatEvent, snapshotID int64)
}

// Sink buffers MatEvents and periodically flushes them to Iceberg.
// It writes append-only change events to an events table per source table.
// A separate Materializer reads these events and produces flattened tables.
type Sink struct {
	cfg        config.SinkConfig
	tableCfgs  []config.TableConfig
	pipelineID string // for metrics labeling

	catalog iceberg.CatalogWithCache
	s3      iceberg.ObjectStorage

	// Per-table state: keyed by PG table name (e.g. "public.orders").
	// Each tableSink writes to the events table (e.g. "orders_events").
	tables map[string]*tableSink

	// Transaction tracking: buffers events per PG transaction so flushes
	// align to transaction boundaries and multi-table commits are atomic.
	openTxns      map[uint32]*txBuffer // XID -> in-flight tx
	committedTxns []*txBuffer          // txns that received Commit, in order

	// Monotonically increasing sequence counter for ordering events within a flush.
	seqCounter int64

	// Global seq tracking for audit continuity validation.
	// flushMinSeq/flushMaxSeq track the range assigned in the current batch
	// (across all tables). lastCommittedMaxSeq is the max _seq from the
	// previous successful commit. A value of -1 means no prior commit.
	flushMinSeq         int64
	flushMaxSeq         int64
	flushSeqSet         bool
	lastCommittedMaxSeq int64

	// Optional event buffer for pushing events after flush (avoids S3 round-trips).
	eventBuf EventBuffer

	// pendingDirectEvents holds MatEvents from non-transactional writes
	// (e.g., snapshot) that should be pushed to the event buffer after the
	// next successful Flush. Keyed by PG table name.
	pendingDirectEvents map[string][]MatEvent

	mu sync.Mutex
}

// partitionedWriter holds the data writer for a single partition of the events table.
type partitionedWriter struct {
	dataWriter *iceberg.RollingWriter
	partValues map[string]any // partition values for this partition (nil for unpartitioned)
}

type tableSink struct {
	// Source schema (user columns only).
	srcSchema *postgres.TableSchema
	// Events table schema (metadata + user columns, all user cols nullable).
	eventsSchema *postgres.TableSchema

	icebergName       string // materialized table name in Iceberg (e.g. "orders")
	eventsIcebergName string // events table name in Iceberg (e.g. "orders_events")
	partSpec          *iceberg.PartitionSpec // materialized table partition spec
	eventsPartSpec    *iceberg.PartitionSpec // events table partition spec
	targetSize        int64
	schemaID          int // current Iceberg schema ID (events table)
	matSchemaID       int // materialized table schema ID

	// Per-partition writers for the events table, keyed by partition key string ("" for unpartitioned).
	partitions map[string]*partitionedWriter
	totalRows  int

	// Per-table seq tracking for snapshot summary metadata.
	// These track the min/max _seq assigned to this table in the current
	// flush batch. Reset after each successful flush.
	flushMinSeq int64
	flushMaxSeq int64
	flushSeqSet bool // true once at least one event has been written this batch

}

// BuildSink creates a fully-wired Sink from config, constructing the default
// S3 and catalog clients. Use NewSink when you need to inject custom
// dependencies (e.g. in tests).
func BuildSink(cfg config.SinkConfig, tableCfgs []config.TableConfig, pipelineID string, eventBuf EventBuffer) (*Sink, *iceberg.IcebergClients, error) {
	clients, err := iceberg.NewClients(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create iceberg clients: %w", err)
	}

	return NewSink(cfg, tableCfgs, pipelineID, clients.S3, clients.Catalog, eventBuf), clients, nil
}

// NewSink creates a Sink with the given dependencies.
// eventBuf is optional — pass nil to disable in-memory event buffering.
func NewSink(cfg config.SinkConfig, tableCfgs []config.TableConfig, pipelineID string, s3 iceberg.ObjectStorage, catalog iceberg.CatalogWithCache, eventBuf EventBuffer) *Sink {
	return &Sink{
		cfg:                 cfg,
		tableCfgs:           tableCfgs,
		pipelineID:          pipelineID,
		catalog:             catalog,
		s3:                  s3,
		eventBuf:            eventBuf,
		tables:              make(map[string]*tableSink),
		openTxns:            make(map[uint32]*txBuffer),
		pendingDirectEvents: make(map[string][]MatEvent),
		lastCommittedMaxSeq: -1,
	}
}

// SetSeqCounter restores the sequence counter from a checkpoint so that _seq
// remains globally monotonic across pipeline restarts. Also sets
// lastCommittedMaxSeq so the first flush validates continuity correctly.
func (s *Sink) SetSeqCounter(seq int64) {
	s.seqCounter = seq
	s.lastCommittedMaxSeq = seq - 1
}

// SeqCounter returns the current sequence counter value (for checkpointing).
func (s *Sink) SeqCounter() int64 { return s.seqCounter }

// Close is a no-op. Retained for interface compatibility.
func (s *Sink) Close() {}

// SetS3 updates the S3 storage client. Used when vended credentials
// are initialized after Sink construction.
func (s *Sink) SetS3(s3 iceberg.ObjectStorage) { s.s3 = s3 }

// Catalog returns the catalog client.
func (s *Sink) Catalog() iceberg.CatalogWithCache { return s.catalog }

// S3 returns the S3 client.
func (s *Sink) S3() iceberg.ObjectStorage { return s.s3 }

// Tables returns the per-table state map (shared with the materializer).
func (s *Sink) Tables() map[string]*tableSink { return s.tables }

// RegisterTable sets up writers for a table and ensures both the events table
// and materialized table exist in the catalog. The sink writes to the events
// table; the materializer reads from it and writes to the materialized table.
func (s *Sink) RegisterTable(ctx context.Context, ts *postgres.TableSchema) error {
	icebergTable := pgTableToIceberg(ts.Table)
	eventsTable := iceberg.EventsTableName(icebergTable)

	// Build partition spec from config (for materialized table).
	var partExprs []string
	for _, tc := range s.tableCfgs {
		if tc.Name == ts.Table {
			partExprs = tc.Iceberg.Partition
			break
		}
	}
	partSpec, err := iceberg.BuildPartitionSpec(partExprs, ts)
	if err != nil {
		return fmt.Errorf("build partition spec: %w", err)
	}

	// Ensure namespace exists.
	if err := s.catalog.EnsureNamespace(ctx, s.cfg.Namespace); err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}

	// Create or load the materialized table (e.g. "orders").
	matTm, err := s.catalog.LoadTable(ctx, s.cfg.Namespace, icebergTable)
	if err != nil {
		return fmt.Errorf("load materialized table: %w", err)
	}
	var matSchemaID int
	if matTm == nil {
		var location string
		if iceberg.IsStorageURI(s.cfg.Warehouse) {
			location = fmt.Sprintf("%s%s.db/%s", s.cfg.Warehouse, s.cfg.Namespace, icebergTable)
		}
		matTm, err = s.catalog.CreateTable(ctx, s.cfg.Namespace, icebergTable, ts, location, partSpec)
		if err != nil {
			return fmt.Errorf("create materialized table: %w", err)
		}
		log.Printf("[sink] created materialized table %s.%s", s.cfg.Namespace, icebergTable)
		matSchemaID = matTm.Metadata.CurrentSchemaID
	} else {
		log.Printf("[sink] using existing materialized table %s.%s", s.cfg.Namespace, icebergTable)
		matSchemaID = matTm.Metadata.CurrentSchemaID
	}

	// Build events table schema and partition spec.
	eventsSchema := iceberg.EventsTableSchema(ts)
	eventsPartSpec, err := iceberg.BuildPartitionSpec(
		[]string{s.cfg.EventsPartitionOrDefault()}, eventsSchema)
	if err != nil {
		return fmt.Errorf("build events partition spec: %w", err)
	}

	eventsTm, err := s.catalog.LoadTable(ctx, s.cfg.Namespace, eventsTable)
	if err != nil {
		return fmt.Errorf("load events table: %w", err)
	}
	var eventsSchemaID int
	if eventsTm == nil {
		var location string
		if iceberg.IsStorageURI(s.cfg.Warehouse) {
			location = fmt.Sprintf("%s%s.db/%s", s.cfg.Warehouse, s.cfg.Namespace, eventsTable)
		}
		eventsTm, err = s.catalog.CreateTable(ctx, s.cfg.Namespace, eventsTable, eventsSchema, location, eventsPartSpec)
		if err != nil {
			return fmt.Errorf("create events table: %w", err)
		}
		log.Printf("[sink] created events table %s.%s (partition=%s)",
			s.cfg.Namespace, eventsTable, s.cfg.EventsPartitionOrDefault())
		eventsSchemaID = eventsTm.Metadata.CurrentSchemaID
	} else {
		log.Printf("[sink] using existing events table %s.%s", s.cfg.Namespace, eventsTable)
		eventsSchemaID = eventsTm.Metadata.CurrentSchemaID
	}

	targetSize := s.cfg.TargetFileSizeOrDefault()
	tSink := &tableSink{
		srcSchema:         ts,
		eventsSchema:      eventsSchema,
		icebergName:       icebergTable,
		eventsIcebergName: eventsTable,
		partSpec:          partSpec,
		eventsPartSpec:    eventsPartSpec,
		targetSize:        targetSize,
		schemaID:          eventsSchemaID,
		matSchemaID:       matSchemaID,
		partitions:        make(map[string]*partitionedWriter),
	}

	s.tables[ts.Table] = tSink
	return nil
}

// UnregisterTable removes a table from the sink. Any buffered data is discarded.
func (s *Sink) UnregisterTable(pgTable string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, pgTable)
}

// EvolveSchema applies a schema change to a table: updates the in-memory schema,
// evolves both the events and materialized Iceberg tables, and rebuilds writers.
// The caller must flush all buffered data before calling this.
func (s *Sink) EvolveSchema(ctx context.Context, pgTable string, change *postgres.SchemaChange) error {
	ts, ok := s.tables[pgTable]
	if !ok {
		return fmt.Errorf("unregistered table: %s", pgTable)
	}

	// Apply diff to the source schema, tracking whether anything
	// actually changes at the Iceberg level.
	icebergChanged := false
	nextFieldID := ts.srcSchema.MaxFieldID() + 1

	for _, col := range change.AddedColumns {
		ts.srcSchema.Columns = append(ts.srcSchema.Columns, postgres.Column{
			Name:       col.Name,
			PGType:     col.PGType,
			IsNullable: true,
			FieldID:    nextFieldID,
		})
		nextFieldID++
		icebergChanged = true
	}

	for _, dropped := range change.DroppedColumns {
		for i, col := range ts.srcSchema.Columns {
			if col.Name == dropped {
				ts.srcSchema.Columns = append(ts.srcSchema.Columns[:i], ts.srcSchema.Columns[i+1:]...)
				icebergChanged = true
				break
			}
		}
	}

	for _, tc := range change.TypeChanges {
		for i, col := range ts.srcSchema.Columns {
			if col.Name == tc.Name {
				oldIceberg, _ := col.IcebergType()
				newCol := postgres.Column{PGType: tc.NewType}
				newIceberg, truncated := newCol.IcebergType()
				if truncated {
					log.Printf("WARN: column %s.%s type %s truncated to Iceberg %s", pgTable, tc.Name, tc.NewType, newIceberg)
				}
				ts.srcSchema.Columns[i].PGType = tc.NewType
				if oldIceberg != newIceberg {
					icebergChanged = true
				}
				break
			}
		}
	}

	if icebergChanged {
		// Evolve materialized table.
		newMatSchemaID, err := s.catalog.EvolveSchema(ctx, s.cfg.Namespace, ts.icebergName, ts.matSchemaID, ts.srcSchema)
		if err != nil {
			return fmt.Errorf("catalog evolve materialized schema for %s: %w", pgTable, err)
		}
		ts.matSchemaID = newMatSchemaID

		// Rebuild events schema and evolve events table.
		ts.eventsSchema = iceberg.EventsTableSchema(ts.srcSchema)
		newEventsSchemaID, err := s.catalog.EvolveSchema(ctx, s.cfg.Namespace, ts.eventsIcebergName, ts.schemaID, ts.eventsSchema)
		if err != nil {
			return fmt.Errorf("catalog evolve events schema for %s: %w", pgTable, err)
		}
		ts.schemaID = newEventsSchemaID
	} else {
		log.Printf("[sink] schema change for %s is a no-op at Iceberg level, skipping catalog evolution", pgTable)
	}

	// Rebuild writers with the new events postgres.
	for key, pw := range ts.partitions {
		ts.partitions[key] = &partitionedWriter{
			dataWriter: iceberg.NewRollingDataWriter(ts.eventsSchema, ts.targetSize),
			partValues: pw.partValues,
		}
	}

	log.Printf("[sink] evolved schema for %s to schema-id %d (events), %d (materialized)",
		pgTable, ts.schemaID, ts.matSchemaID)
	return nil
}

// getEventsWriter returns the events writer for the given row's partition.
func (ts *tableSink) getEventsWriter(row map[string]any) *partitionedWriter {
	key := ""
	if ts.eventsPartSpec != nil && !ts.eventsPartSpec.IsUnpartitioned() {
		key = ts.eventsPartSpec.PartitionKey(row, ts.eventsSchema)
	}
	pw, ok := ts.partitions[key]
	if !ok {
		pw = &partitionedWriter{
			dataWriter: iceberg.NewRollingDataWriter(ts.eventsSchema, ts.targetSize),
		}
		ts.partitions[key] = pw
	}
	return pw
}

// Write buffers a postgres.ChangeEvent for the next flush. DML events are held in
// per-transaction buffers until the corresponding postgres.OpCommit arrives, so
// flushes always align to PG transaction boundaries.
func (s *Sink) Write(event postgres.ChangeEvent) error {
	switch event.Operation {
	case postgres.OpBegin:
		s.openTxns[event.TransactionID] = &txBuffer{
			xid:    event.TransactionID,
			tables: make(map[string]bool),
		}
		return nil
	case postgres.OpCommit:
		tx, ok := s.openTxns[event.TransactionID]
		if !ok {
			return nil // no tracked DML in this tx
		}
		tx.committed = true
		tx.commitTS = event.SourceTimestamp
		s.committedTxns = append(s.committedTxns, tx)
		delete(s.openTxns, event.TransactionID)
		return nil
	}

	// DML event — buffer in the transaction.
	if event.TransactionID != 0 {
		tx, ok := s.openTxns[event.TransactionID]
		if ok {
			tx.events = append(tx.events, event)
			tx.tables[event.Table] = true
			return nil
		}
	}
	// Events without a transaction ID (e.g., snapshot) are written directly.
	// Buffer for materializer — will be pushed after the next successful Flush.
	if s.eventBuf != nil {
		ce := s.toMatEvent(event, s.seqCounter)
		s.pendingDirectEvents[event.Table] = append(s.pendingDirectEvents[event.Table], ce)
	}
	return s.writeDirect(event)
}

// writeDirect writes a change event to the events table as an append-only row.
// No TOAST resolution, no equality deletes — just append the event with metadata.
func (s *Sink) writeDirect(event postgres.ChangeEvent) error {
	ts, ok := s.tables[event.Table]
	if !ok {
		return fmt.Errorf("unregistered table: %s", event.Table)
	}

	// Reuse the source event's map directly — the source allocates a new map
	// per event and doesn't retain references. Add metadata columns in-place
	// to avoid a per-row map allocation.
	var row map[string]any
	switch event.Operation {
	case postgres.OpInsert:
		row = event.After
		row["_op"] = "I"
	case postgres.OpUpdate:
		row = event.After
		row["_op"] = "U"
		if len(event.UnchangedCols) > 0 {
			row["_unchanged_cols"] = iceberg.UnchangedColsString(event.UnchangedCols)
		}
	case postgres.OpDelete:
		row = event.Before
		row["_op"] = "D"
	default:
		return nil // ignore other operations
	}

	row["_lsn"] = int64(event.LSN)
	row["_ts"] = event.SourceTimestamp
	row["_seq"] = s.seqCounter

	// Track per-table min/max _seq for snapshot summary metadata.
	if !ts.flushSeqSet {
		ts.flushMinSeq = s.seqCounter
		ts.flushMaxSeq = s.seqCounter
		ts.flushSeqSet = true
	} else if s.seqCounter > ts.flushMaxSeq {
		ts.flushMaxSeq = s.seqCounter
	}

	// Track global min/max _seq for continuity validation.
	if !s.flushSeqSet {
		s.flushMinSeq = s.seqCounter
		s.flushMaxSeq = s.seqCounter
		s.flushSeqSet = true
	} else if s.seqCounter > s.flushMaxSeq {
		s.flushMaxSeq = s.seqCounter
	}

	s.seqCounter++

	pw := ts.getEventsWriter(row)
	if err := pw.dataWriter.Add(row); err != nil {
		return err
	}
	ts.totalRows++
	return nil
}

// toMatEvent converts a postgres.ChangeEvent into a materializer postgres.ChangeEvent,
// using the given seq value. Reuses the source event's map directly — the source
// allocates a fresh map per event so no copy is needed. The map is shared with
// writeDirect (which adds metadata keys like _lsn, _op); the materializer only
// accesses user columns by name so the extra keys are harmless.
func (s *Sink) toMatEvent(event postgres.ChangeEvent, seq int64) MatEvent {
	ce := MatEvent{
		lsn:           int64(event.LSN),
		seq:           seq,
		unchangedCols: event.UnchangedCols,
	}

	switch event.Operation {
	case postgres.OpInsert:
		ce.op = "I"
		ce.row = event.After
	case postgres.OpUpdate:
		ce.op = "U"
		ce.row = event.After
	case postgres.OpDelete:
		ce.op = "D"
		ce.row = event.Before
	}
	return ce
}

// ShouldFlush checks if there are committed transactions ready to flush.
func (s *Sink) ShouldFlush() bool {
	return len(s.committedTxns) > 0
}

// TotalBuffered returns the total number of buffered events eligible for flush.
func (s *Sink) TotalBuffered() int {
	total := 0
	for _, tx := range s.committedTxns {
		total += len(tx.events)
	}
	return total
}

// TotalBufferedBytes returns the estimated buffered bytes across all tables,
// including both committed and in-flight data (for backpressure monitoring).
func (s *Sink) TotalBufferedBytes() int64 {
	var total int64
	for _, ts := range s.tables {
		for _, pw := range ts.partitions {
			total += pw.dataWriter.EstimatedBytes()
		}
	}
	for _, tx := range s.committedTxns {
		total += int64(len(tx.events)) * 128
	}
	for _, tx := range s.openTxns {
		total += int64(len(tx.events)) * 128
	}
	return total
}

// CheckBackpressure is a no-op in the events-based architecture.
// Retained for interface compatibility with pipeline.go.
func (s *Sink) CheckBackpressure(ctx context.Context) error {
	return nil
}

// Flush replays committed transactions into writers, then flushes all tables
// atomically: S3 uploads in parallel, followed by a single multi-table
// catalog commit.
func (s *Sink) Flush(ctx context.Context) error {
	hasPendingDirect := len(s.pendingDirectEvents) > 0
	if len(s.committedTxns) == 0 && !hasPendingDirect {
		return nil
	}

	// Snapshot seq state so we can restore it on failure. A failed flush
	// (Add error, S3 upload, catalog commit) must not advance seqCounter,
	// because doFlush retries will replay the same committedTxns — they
	// must receive the same _seq values to maintain continuity.
	savedSeqCounter := s.seqCounter
	savedFlushSeqSet := s.flushSeqSet
	savedFlushMinSeq := s.flushMinSeq
	savedFlushMaxSeq := s.flushMaxSeq
	type perTableSeqState struct {
		minSeq int64
		maxSeq int64
		set    bool
	}
	savedTableSeq := make(map[string]perTableSeqState, len(s.tables))
	for pgTable, ts := range s.tables {
		savedTableSeq[pgTable] = perTableSeqState{
			minSeq: ts.flushMinSeq,
			maxSeq: ts.flushMaxSeq,
			set:    ts.flushSeqSet,
		}
	}
	restoreSeqState := func() {
		s.seqCounter = savedSeqCounter
		s.flushSeqSet = savedFlushSeqSet
		s.flushMinSeq = savedFlushMinSeq
		s.flushMaxSeq = savedFlushMaxSeq
		for pgTable, saved := range savedTableSeq {
			if ts, ok := s.tables[pgTable]; ok {
				ts.flushMinSeq = saved.minSeq
				ts.flushMaxSeq = saved.maxSeq
				ts.flushSeqSet = saved.set
			}
		}
	}

	// Discard stale completed chunks from a previous failed flush.
	for _, ts := range s.tables {
		for _, pw := range ts.partitions {
			pw.dataWriter.DiscardCompleted()
		}
	}

	// Collect events per table for the materializer in-memory buffer.
	// Start with pending direct events (snapshot rows written via writeDirect).
	var bufferedEvents map[string][]MatEvent
	if s.eventBuf != nil {
		bufferedEvents = make(map[string][]MatEvent)
		for pgTable, events := range s.pendingDirectEvents {
			bufferedEvents[pgTable] = append(bufferedEvents[pgTable], events...)
		}
	}

	// Count expected DML events before writing so we can verify none were
	// silently dropped during the loop.
	var expectedEvents int64
	for _, tx := range s.committedTxns {
		for _, event := range tx.events {
			switch event.Operation {
			case postgres.OpInsert, postgres.OpUpdate, postgres.OpDelete:
				expectedEvents++
			}
		}
	}

	// Drain committed transactions into per-table writers.
	// seqCounter is a globally monotonic ordering key (NOT reset across flushes).
	// The materializer sorts events by _seq to determine application order.
	for _, tx := range s.committedTxns {
		for _, event := range tx.events {
			// Capture seq before writeDirect increments seqCounter.
			if s.eventBuf != nil {
				ce := s.toMatEvent(event, s.seqCounter)
				bufferedEvents[event.Table] = append(bufferedEvents[event.Table], ce)
			}
			if err := s.writeDirect(event); err != nil {
				restoreSeqState()
				return fmt.Errorf("replay tx %d: %w", tx.xid, err)
			}
		}
	}

	// Verify that every expected event was assigned a seq. The seq counter
	// should have advanced by exactly expectedEvents; any mismatch means
	// writeDirect silently skipped or double-counted an event.
	actualWritten := s.seqCounter - savedSeqCounter
	if actualWritten != expectedEvents {
		restoreSeqState()
		return fmt.Errorf("seq count mismatch: expected %d events to be written, "+
			"but seqCounter advanced by %d — possible silent event drop",
			expectedEvents, actualWritten)
	}

	// Validate global seq continuity before committing to Iceberg.
	// If lastCommittedMaxSeq >= 0 (not first-ever flush), the current batch's
	// min_seq must be exactly lastCommittedMaxSeq+1. A gap means events were lost.
	if s.flushSeqSet && s.lastCommittedMaxSeq >= 0 {
		expected := s.lastCommittedMaxSeq + 1
		if s.flushMinSeq != expected {
			restoreSeqState()
			return fmt.Errorf("seq continuity violation: expected min_seq=%d (last committed max_seq=%d), "+
				"got min_seq=%d; this indicates WAL events were dropped — refusing to commit",
				expected, s.lastCommittedMaxSeq, s.flushMinSeq)
		}
	}

	// Flush all tables: parallel S3 uploads + single atomic catalog commit.
	snapshotIDs, err := s.flushAllTables(ctx)
	if err != nil {
		restoreSeqState()
		return err
	}

	// Update global seq tracking after successful commit.
	if s.flushSeqSet {
		s.lastCommittedMaxSeq = s.flushMaxSeq
	}
	s.flushSeqSet = false

	// Push events to materializer buffer only after successful S3 commit.
	if s.eventBuf != nil {
		for pgTable, events := range bufferedEvents {
			s.eventBuf.PushEvents(pgTable, events, snapshotIDs[pgTable])
		}
	}

	// Clear committed transactions and pending direct events.
	s.committedTxns = nil
	s.pendingDirectEvents = make(map[string][]MatEvent)
	return nil
}

// preparedFlush holds everything needed to commit an events table after S3 writes complete.
type preparedFlush struct {
	pgTable    string
	ts         *tableSink
	snapshotID int64
	prevSnapID int64
	commit     iceberg.SnapshotCommit
}

// flushAllTables serializes, uploads, and commits events for all tables.
// Events are written to the events table (append-only, no deletes).
func (s *Sink) flushAllTables(ctx context.Context) (map[string]int64, error) {
	type tableInfo struct {
		pgTable string
		ts      *tableSink
	}
	var tablesToFlush []tableInfo
	for pgTable, ts := range s.tables {
		hasData := false
		for _, pw := range ts.partitions {
			if pw.dataWriter.Len() > 0 {
				hasData = true
				break
			}
		}
		if !hasData {
			continue
		}
		tablesToFlush = append(tablesToFlush, tableInfo{pgTable, ts})
	}

	if len(tablesToFlush) == 0 {
		return nil, nil
	}

	// Serialize + upload all event partitions in parallel.
	var mu sync.Mutex
	partResults := make(map[string][]uploadResult)

	var tasks []utils.Task
	for _, t := range tablesToFlush {
		basePath := fmt.Sprintf("%s.db/%s", s.cfg.Namespace, t.ts.eventsIcebergName)
		for partKey, pw := range t.ts.partitions {
			pw := pw
			pgTable := t.pgTable
			partKey := partKey

			// Compute partition path for the S3 key.
			var partPath string
			if t.ts.eventsPartSpec != nil && !t.ts.eventsPartSpec.IsUnpartitioned() && partKey != "" {
				partPath = partKey // partKey is already in "field=value" format
			}

			tasks = append(tasks, utils.Task{
				Name: pgTable,
				Fn: func(ctx context.Context, progress *utils.Progress) error {
					uploads, err := serializeEventsPartition(pw, basePath, partPath)
					if err != nil {
						return err
					}
					results, err := s.uploadFiles(ctx, uploads)
					if err != nil {
						return err
					}
					mu.Lock()
					partResults[pgTable] = append(partResults[pgTable], results...)
					mu.Unlock()
					return nil
				},
			})
		}
	}

	concurrency := runtime.NumCPU()
	if len(tasks) < concurrency {
		concurrency = len(tasks)
	}
	pool := utils.NewPool(concurrency)
	if _, err := pool.Run(ctx, tasks); err != nil {
		return nil, err
	}

	// Assemble metadata and commit all events tables.
	prepared := make([]*preparedFlush, 0, len(tablesToFlush))
	for _, t := range tablesToFlush {
		results := partResults[t.pgTable]
		pf, err := s.assembleEventsCommit(ctx, t.pgTable, t.ts, results)
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, pf)
	}

	tableCommits := make([]iceberg.TableCommit, len(prepared))
	for i, pf := range prepared {
		tableCommits[i] = iceberg.TableCommit{
			Table:             pf.ts.eventsIcebergName,
			CurrentSnapshotID: pf.prevSnapID,
			Snapshot:          pf.commit,
		}
	}

	if err := s.catalog.CommitTransaction(ctx, s.cfg.Namespace, tableCommits); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	// All commits succeeded — finalize writers.
	snapshotIDs := make(map[string]int64, len(prepared))
	for _, pf := range prepared {
		log.Printf("[sink] committed events snapshot %d for %s", pf.snapshotID, pf.pgTable)
		snapshotIDs[pf.pgTable] = pf.snapshotID
		for _, pw := range pf.ts.partitions {
			pw.dataWriter.Commit()
		}
		pf.ts.totalRows = 0
		pf.ts.flushSeqSet = false
	}

	return snapshotIDs, nil
}

// serializeEventsPartition flushes an events partition writer to parquet bytes.
func serializeEventsPartition(pw *partitionedWriter, basePath, partPath string) ([]pendingUpload, error) {
	var uploads []pendingUpload

	dataChunks, err := pw.dataWriter.FlushAll()
	if err != nil {
		return nil, fmt.Errorf("flush events: %w", err)
	}
	for i, chunk := range dataChunks {
		fileUUID := uuid.New().String()
		var key string
		if partPath != "" {
			key = fmt.Sprintf("%s/data/%s/%s-events-%d.parquet", basePath, partPath, fileUUID, i)
		} else {
			key = fmt.Sprintf("%s/data/%s-events-%d.parquet", basePath, fileUUID, i)
		}
		uploads = append(uploads, pendingUpload{
			key:         key,
			data:        chunk.Data,
			content:     0,
			recordCount: chunk.RowCount,
		})
	}

	return uploads, nil
}

// uploadFiles uploads a batch of pending files to S3 in parallel.
func (s *Sink) uploadFiles(ctx context.Context, uploads []pendingUpload) ([]uploadResult, error) {
	results := make([]uploadResult, len(uploads))
	g, gctx := errgroup.WithContext(ctx)
	for i, u := range uploads {
		i, u := i, u
		g.Go(func() error {
			uri, err := s.s3.Upload(gctx, u.key, u.data)
			if err != nil {
				return fmt.Errorf("upload %s: %w", u.key, err)
			}
			results[i] = uploadResult{uri: uri, pendingUpload: u}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// extractPartDir extracts the partition directory from a data file URI.
// E.g. "s3://bucket/.../data/_ts_day=2026-04-04/uuid.parquet" → "_ts_day=2026-04-04"
func extractPartDir(uri string) string {
	idx := strings.Index(uri, "/data/")
	if idx < 0 {
		return ""
	}
	afterData := uri[idx+len("/data/"):]
	slashIdx := strings.Index(afterData, "/")
	if slashIdx < 0 {
		return "" // unpartitioned file
	}
	return afterData[:slashIdx]
}

// pendingUpload holds the data needed to upload a single Parquet file to S3.
type pendingUpload struct {
	key             string
	data            []byte
	content         int // 0=data, 2=equality delete
	recordCount     int64
	equalityFieldIDs []int
	partitionValues map[string]any
}

// uploadResult holds the outcome of a completed upload.
type uploadResult struct {
	uri string
	pendingUpload
}

// assembleEventsCommit builds manifests, manifest list, and snapshot commit
// for an events table (append-only, no deletes).
func (s *Sink) assembleEventsCommit(ctx context.Context, pgTable string, ts *tableSink, results []uploadResult) (*preparedFlush, error) {
	now := time.Now()
	snapshotID := now.UnixMilli()
	basePath := fmt.Sprintf("%s.db/%s", s.cfg.Namespace, ts.eventsIcebergName)

	eventsPartitioned := ts.eventsPartSpec != nil && !ts.eventsPartSpec.IsUnpartitioned()

	var dataFiles []iceberg.DataFileInfo
	for _, r := range results {
		df := iceberg.DataFileInfo{
			Path:          r.uri,
			FileSizeBytes: int64(len(r.data)),
			RecordCount:   r.recordCount,
			Content:       0,
		}
		if eventsPartitioned {
			partPath := extractPartDir(r.uri)
			if partPath != "" {
				partValues := ts.eventsPartSpec.ParsePartitionPath(partPath, ts.eventsSchema)
				df.PartitionValues = ts.eventsPartSpec.PartitionAvroValue(partValues, ts.eventsSchema)
			}
		}
		dataFiles = append(dataFiles, df)
	}

	// Load current events table metadata.
	tm, err := s.catalog.LoadTable(ctx, s.cfg.Namespace, ts.eventsIcebergName)
	if err != nil {
		return nil, fmt.Errorf("load events table: %w", err)
	}

	seqNum := tm.Metadata.LastSequenceNumber + 1

	// Use cached manifests from the catalog cache instead of re-downloading from S3.
	// On cold start (cache empty), load from S3 if a manifest list exists.
	existingManifests := s.catalog.Manifests(s.cfg.Namespace, ts.eventsIcebergName)
	if existingManifests == nil {
		if ml := tm.CurrentManifestList(); ml != "" {
			mlKey, err := iceberg.KeyFromURI(ml)
			if err != nil {
				return nil, fmt.Errorf("parse manifest list URI: %w", err)
			}
			mlData, err := s.s3.Download(ctx, mlKey)
			if err != nil {
				return nil, fmt.Errorf("download manifest list: %w", err)
			}
			existingManifests, err = iceberg.ReadManifestList(mlData)
			if err != nil {
				return nil, fmt.Errorf("read manifest list: %w", err)
			}
			s.catalog.SetManifests(s.cfg.Namespace, ts.eventsIcebergName, existingManifests)
		}
	}

	// Write data manifest.
	var manifestInfos []iceberg.ManifestFileInfo
	if len(dataFiles) > 0 {
		entries := make([]iceberg.ManifestEntry, len(dataFiles))
		for i, df := range dataFiles {
			entries[i] = iceberg.ManifestEntry{
				Status:     1,
				SnapshotID: snapshotID,
				DataFile:   df,
			}
		}
		manifestBytes, err := iceberg.WriteManifest(ts.eventsSchema, entries, seqNum, 0, ts.eventsPartSpec)
		if err != nil {
			return nil, fmt.Errorf("write events manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-m0.avro", basePath, uuid.New().String())
		manifestURI, err := s.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return nil, fmt.Errorf("upload events manifest: %w", err)
		}

		var totalRows int64
		for _, df := range dataFiles {
			totalRows += df.RecordCount
		}
		manifestInfos = append(manifestInfos, iceberg.ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0,
			SnapshotID:     snapshotID,
			AddedFiles:     len(dataFiles),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	// Write manifest list (existing + new) and update catalog cache.
	allManifests := append(existingManifests, manifestInfos...)
	s.catalog.SetManifests(s.cfg.Namespace, ts.eventsIcebergName, allManifests)
	mlBytes, err := iceberg.WriteManifestList(allManifests)
	if err != nil {
		return nil, fmt.Errorf("write manifest list: %w", err)
	}

	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s.s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return nil, fmt.Errorf("upload manifest list: %w", err)
	}

	eventCount := 0
	for _, df := range dataFiles {
		eventCount += int(df.RecordCount)
	}
	log.Printf("[sink] prepared events snapshot %d for %s (seq=%d, events=%d, files=%d, seq_range=[%d,%d])",
		snapshotID, pgTable, seqNum, eventCount, len(dataFiles), ts.flushMinSeq, ts.flushMaxSeq)

	summary := map[string]string{
		"operation": "append",
	}
	if ts.flushSeqSet {
		summary["min_seq"] = fmt.Sprintf("%d", ts.flushMinSeq)
		summary["max_seq"] = fmt.Sprintf("%d", ts.flushMaxSeq)
		summary["event_count"] = fmt.Sprintf("%d", eventCount)
	}

	return &preparedFlush{
		pgTable:    pgTable,
		ts:         ts,
		snapshotID: snapshotID,
		prevSnapID: tm.Metadata.CurrentSnapshotID,
		commit: iceberg.SnapshotCommit{
			SnapshotID:       snapshotID,
			SequenceNumber:   seqNum,
			TimestampMs:      now.UnixMilli(),
			ManifestListPath: mlURI,
			SchemaID:         ts.schemaID,
			Summary:          summary,
		},
	}, nil
}


// pgTableToIceberg converts "public.orders" to "orders" for the Iceberg table name.
func pgTableToIceberg(pgTable string) string {
	return postgres.TableToIceberg(pgTable)
}
