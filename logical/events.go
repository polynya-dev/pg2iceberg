package logical

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/stream"
	"go.opentelemetry.io/otel"
)

var sinkTracer = otel.Tracer("pg2iceberg/sink")

// stagedSchema is the fixed Parquet schema for staged WAL files.
// Created once, shared across all tables — user columns are JSON in _data.
var stagedSchema = iceberg.StagedEventSchema()

// txBuffer holds events for a single in-flight PG transaction.
type txBuffer struct {
	xid       uint32
	events    []postgres.ChangeEvent
	tables    map[string]bool // which tables this tx touches
	committed bool
	commitTS  time.Time // PG commit timestamp
}

// Sink buffers change events and periodically flushes them to the Stream
// (S3 staged Parquet + PG coordination). A separate Materializer reads
// staged files and produces flattened Iceberg tables.
type Sink struct {
	cfg        config.SinkConfig
	tableCfgs  []config.TableConfig
	pipelineID string // for metrics labeling

	catalog iceberg.MetadataCache
	s3      iceberg.ObjectStorage
	stream  stream.Stream

	// Per-table state: keyed by PG table name (e.g. "public.orders").
	tables map[string]*tableSink

	// Transaction tracking: buffers events per PG transaction so flushes
	// align to transaction boundaries and multi-table commits are atomic.
	openTxns      map[uint32]*txBuffer // XID -> in-flight tx
	committedTxns []*txBuffer          // txns that received Commit, in order

	mu sync.Mutex
}

type tableSink struct {
	// Source schema (user columns only).
	srcSchema *postgres.TableSchema

	icebergName string                 // materialized table name in Iceberg (e.g. "orders")
	partSpec    *iceberg.PartitionSpec // materialized table partition spec
	matSchemaID int                    // materialized table schema ID
	targetSize  int64

	// Single rolling writer for staged Parquet files.
	// Uses the fixed stagedSchema (user data is JSON in _data).
	// Never rebuilt on schema evolution.
	writer    *iceberg.RollingWriter
	totalRows int

	// Per-flush accumulator of MatEvents, keyed by the batch index
	// within the current flush. Used to pass pre-parsed events to
	// the CachedStream so the materializer skips JSON decode.
	pendingEvents []MatEvent
}

// BuildSink creates a fully-wired Sink from config, constructing the default
// S3 and catalog clients. Use NewSink when you need to inject custom
// dependencies (e.g. in tests).
func BuildSink(cfg config.SinkConfig, tableCfgs []config.TableConfig, pipelineID string) (*Sink, *iceberg.IcebergClients, error) {
	clients, err := iceberg.NewClients(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create iceberg clients: %w", err)
	}

	return NewSink(cfg, tableCfgs, pipelineID, clients.S3, clients.Catalog), clients, nil
}

// NewSink creates a Sink with the given dependencies.
func NewSink(cfg config.SinkConfig, tableCfgs []config.TableConfig, pipelineID string, s3 iceberg.ObjectStorage, catalog iceberg.MetadataCache) *Sink {
	return &Sink{
		cfg:        cfg,
		tableCfgs:  tableCfgs,
		pipelineID: pipelineID,
		catalog:    catalog,
		s3:         s3,
		tables:     make(map[string]*tableSink),
		openTxns:   make(map[uint32]*txBuffer),
	}
}

// Close is a no-op. Retained for interface compatibility.
func (s *Sink) Close() {}

// SetS3 updates the S3 storage client. Used when vended credentials
// are initialized after Sink construction.
func (s *Sink) SetS3(s3 iceberg.ObjectStorage) { s.s3 = s3 }

// SetStream sets the stream used for staging events. Must be called before Start.
func (s *Sink) SetStream(str stream.Stream) { s.stream = str }

// Stream returns the stream.
func (s *Sink) Stream() stream.Stream { return s.stream }

// Catalog returns the catalog client.
func (s *Sink) Catalog() iceberg.MetadataCache { return s.catalog }

// S3 returns the S3 client.
func (s *Sink) S3() iceberg.ObjectStorage { return s.s3 }

// Tables returns the per-table state map (shared with the materializer).
func (s *Sink) Tables() map[string]*tableSink { return s.tables }

// RegisterTable sets up writers for a table and ensures the materialized table
// exists in the catalog. The sink writes staged Parquet files via the Stream;
// the materializer reads them and writes to the materialized Iceberg table.
func (s *Sink) RegisterTable(ctx context.Context, ts *postgres.TableSchema) error {
	icebergTable := pgTableToIceberg(ts.Table)

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

	// Ensure materializer cursor exists for this table (default group).
	if s.stream != nil {
		if err := s.stream.Coordinator().EnsureCursor(ctx, "default", ts.Table); err != nil {
			return fmt.Errorf("ensure cursor for %s: %w", ts.Table, err)
		}
	}

	targetSize := s.cfg.TargetFileSizeOrDefault()
	tSink := &tableSink{
		srcSchema:   ts,
		icebergName: icebergTable,
		partSpec:    partSpec,
		matSchemaID: matSchemaID,
		targetSize:  targetSize,
		writer:      iceberg.NewRollingDataWriter(stagedSchema, targetSize),
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

// EvolveSchema applies a schema change to a table: updates the in-memory schema
// and evolves the materialized Iceberg table. The staged Parquet writer uses a
// fixed schema (user data is JSON in _data), so no writer rebuild is needed.
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
	} else {
		log.Printf("[sink] schema change for %s is a no-op at Iceberg level, skipping catalog evolution", pgTable)
	}

	// No writer rebuild needed — staged files use a fixed schema with JSON _data.

	log.Printf("[sink] evolved schema for %s to schema-id %d (materialized)",
		pgTable, ts.matSchemaID)
	return nil
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
	return s.writeDirect(event)
}

// writeDirect converts a change event into a staged Parquet row (fixed schema
// with JSON _data) and accumulates a MatEvent for the CachedStream.
func (s *Sink) writeDirect(event postgres.ChangeEvent) error {
	ts, ok := s.tables[event.Table]
	if !ok {
		return fmt.Errorf("unregistered table: %s", event.Table)
	}

	var me MatEvent
	switch event.Operation {
	case postgres.OpInsert:
		me.op = "I"
		me.row = event.After
	case postgres.OpUpdate:
		me.op = "U"
		me.row = event.After
		me.unchangedCols = event.UnchangedCols
	case postgres.OpDelete:
		me.op = "D"
		me.row = event.Before
	default:
		return nil
	}
	me.lsn = int64(event.LSN)

	// JSON-encode user columns for the _data field.
	dataJSON, err := json.Marshal(me.row)
	if err != nil {
		return fmt.Errorf("marshal _data for %s: %w", event.Table, err)
	}

	// Build the fixed-schema Parquet row.
	row := map[string]any{
		"_op":  me.op,
		"_lsn": me.lsn,
		"_ts":  event.SourceTimestamp,
		"_data": string(dataJSON),
	}
	if len(me.unchangedCols) > 0 {
		row["_unchanged_cols"] = iceberg.UnchangedColsString(me.unchangedCols)
	}

	if err := ts.writer.Add(row); err != nil {
		return err
	}
	ts.pendingEvents = append(ts.pendingEvents, me)
	ts.totalRows++
	return nil
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
		total += ts.writer.EstimatedBytes()
	}
	for _, tx := range s.committedTxns {
		total += int64(len(tx.events)) * 128
	}
	for _, tx := range s.openTxns {
		total += int64(len(tx.events)) * 128
	}
	return total
}

// CheckBackpressure is a no-op.
func (s *Sink) CheckBackpressure(ctx context.Context) error {
	return nil
}

// Flush replays committed transactions into writers, then stages all data
// to S3 and registers in the Stream atomically.
func (s *Sink) Flush(ctx context.Context) error {
	if len(s.committedTxns) == 0 {
		return nil
	}

	// Reset writers and pending events for a clean slate.
	for _, ts := range s.tables {
		ts.writer.Reset()
		ts.pendingEvents = nil
	}

	// Drain committed transactions into per-table writers.
	for _, tx := range s.committedTxns {
		for _, event := range tx.events {
			if err := s.writeDirect(event); err != nil {
				return fmt.Errorf("replay tx %d: %w", tx.xid, err)
			}
		}
	}

	// Collect FileChunks + pre-parsed events into WriteBatches.
	var batches []stream.WriteBatch
	type flushedTable struct {
		pgTable string
		ts      *tableSink
	}
	var flushed []flushedTable

	for pgTable, ts := range s.tables {
		if ts.writer.Len() == 0 {
			continue
		}
		chunks, err := ts.writer.FlushAll()
		if err != nil {
			return fmt.Errorf("flush writer for %s: %w", pgTable, err)
		}

		// Distribute pending events across chunks proportionally.
		// Each chunk covers a contiguous slice of the pending events.
		eventIdx := 0
		for _, chunk := range chunks {
			count := int(chunk.RowCount)
			var chunkEvents []MatEvent
			if eventIdx+count <= len(ts.pendingEvents) {
				chunkEvents = make([]MatEvent, count)
				copy(chunkEvents, ts.pendingEvents[eventIdx:eventIdx+count])
			}
			eventIdx += count

			batches = append(batches, stream.WriteBatch{
				Table:       pgTable,
				Data:        chunk.Data,
				RecordCount: count,
				Events:      chunkEvents,
			})
		}
		flushed = append(flushed, flushedTable{pgTable, ts})
	}

	if len(batches) > 0 {
		if err := s.stream.Append(ctx, batches); err != nil {
			return fmt.Errorf("stream append: %w", err)
		}
	}

	// Staging succeeded — clear state.
	s.committedTxns = nil
	for _, ft := range flushed {
		ft.ts.writer.Commit()
		ft.ts.pendingEvents = nil
		ft.ts.totalRows = 0
		log.Printf("[sink] staged %s events to stream", ft.pgTable)
	}

	return nil
}

// pgTableToIceberg converts "public.orders" to "orders" for the Iceberg table name.
func pgTableToIceberg(pgTable string) string {
	return postgres.TableToIceberg(pgTable)
}
