package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/sink"
	"github.com/pg2iceberg/pg2iceberg/source"
	"github.com/pg2iceberg/pg2iceberg/state"
)

// Status represents the current state of a Pipeline.
type Status string

const (
	StatusStarting     Status = "starting"
	StatusSnapshotting Status = "snapshotting"
	StatusRunning      Status = "running"
	StatusStopping     Status = "stopping"
	StatusStopped      Status = "stopped"
	StatusError        Status = "error"
)

// Metrics holds pipeline metrics exposed via the /metrics endpoint.
type Metrics struct {
	Status        Status `json:"status"`
	BufferedRows  int    `json:"buffered_rows"`
	BufferedBytes int64  `json:"buffered_bytes"`
	RowsProcessed  int64  `json:"rows_processed"`
	BytesProcessed int64  `json:"bytes_processed"`
	LSN           uint64 `json:"lsn,omitempty"`
	LastFlushAt   string `json:"last_flush_at,omitempty"`
	Uptime        string `json:"uptime"`
}

// Pipeline encapsulates a single source-to-sink replication pipeline.
type Pipeline struct {
	id  string
	cfg *config.Config

	status Status
	err    error

	src     source.Source
	snk     *sink.Sink
	store   state.CheckpointStore
	schemas map[string]*schema.TableSchema

	startedAt     time.Time
	lastFlushAt   time.Time
	rowsProcessed  int64
	bytesProcessed int64

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex
}

// NewPipeline creates a Pipeline but does not start it.
func NewPipeline(id string, cfg *config.Config) *Pipeline {
	return &Pipeline{
		id:     id,
		cfg:    cfg,
		status: StatusStopped,
		done:   make(chan struct{}),
	}
}

// WithSink overrides the sink for testing. Must be called before Start.
func (p *Pipeline) WithSink(snk *sink.Sink) {
	p.snk = snk
}

// WithCheckpointStore overrides the checkpoint store for testing. Must be called before Start.
func (p *Pipeline) WithCheckpointStore(store state.CheckpointStore) {
	p.store = store
}

// ID returns the pipeline identifier.
func (p *Pipeline) ID() string { return p.id }

// Config returns the pipeline configuration.
func (p *Pipeline) Config() *config.Config { return p.cfg }

// Source returns the pipeline's source (for testing/inspection).
func (p *Pipeline) Source() source.Source { return p.src }

// Status returns the current status and last error (if any).
func (p *Pipeline) Status() (Status, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status, p.err
}

// Done returns a channel that is closed when the pipeline exits.
func (p *Pipeline) Done() <-chan struct{} { return p.done }

// Metrics returns a snapshot of the pipeline's current metrics.
func (p *Pipeline) Metrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	m := Metrics{
		Status:        p.status,
		RowsProcessed:  p.rowsProcessed,
		BytesProcessed: p.bytesProcessed,
	}

	if p.snk != nil {
		m.BufferedRows = p.snk.TotalBuffered()
		m.BufferedBytes = p.snk.TotalBufferedBytes()
	}

	if ls, ok := p.src.(*source.LogicalSource); ok {
		m.LSN = ls.FlushedLSN()
	}

	if !p.lastFlushAt.IsZero() {
		m.LastFlushAt = p.lastFlushAt.Format(time.RFC3339)
	}

	if !p.startedAt.IsZero() {
		m.Uptime = time.Since(p.startedAt).Truncate(time.Second).String()
	}

	return m
}

// Start initializes and runs the pipeline. It performs setup synchronously
// (schema discovery, sink registration) and then spawns the event loop
// in a background goroutine. Returns an error only if initial setup fails.
func (p *Pipeline) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.status == StatusRunning || p.status == StatusStarting {
		p.mu.Unlock()
		return fmt.Errorf("pipeline %s already running", p.id)
	}
	p.status = StatusStarting
	p.err = nil
	p.done = make(chan struct{})
	p.mu.Unlock()

	pipeCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	if err := p.setup(pipeCtx); err != nil {
		cancel()
		p.setStatus(StatusError, err)
		close(p.done)
		return err
	}

	go p.run(pipeCtx)
	return nil
}

// Stop gracefully shuts down the pipeline.
func (p *Pipeline) Stop() error {
	p.mu.RLock()
	s := p.status
	p.mu.RUnlock()

	if s != StatusRunning && s != StatusStarting {
		return nil
	}

	p.setStatus(StatusStopping, nil)
	if p.cancel != nil {
		p.cancel()
	}

	<-p.done

	if p.src != nil {
		if err := p.src.Close(); err != nil {
			log.Printf("[pipeline:%s] source close error: %v", p.id, err)
		}
	}
	if p.store != nil {
		p.store.Close()
	}

	p.setStatus(StatusStopped, nil)
	return nil
}

func (p *Pipeline) setStatus(s Status, err error) {
	p.mu.Lock()
	p.status = s
	if err != nil {
		p.err = err
	}
	p.mu.Unlock()
	if v, ok := metrics.StatusToFloat[string(s)]; ok {
		metrics.PipelineStatus.WithLabelValues(p.id).Set(v)
	}
}

// setup performs all synchronous initialization: checkpoint, schema discovery,
// sink registration, compactor start, and source creation.
func (p *Pipeline) setup(ctx context.Context) error {
	// Load checkpoint (skip creation if already set via WithCheckpointStore).
	if p.store == nil {
		store, err := newCheckpointStore(ctx, p.cfg)
		if err != nil {
			return fmt.Errorf("create checkpoint store: %w", err)
		}
		p.store = store
	}
	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Discover schemas via a regular PG connection.
	pgConn, err := pgx.Connect(ctx, p.cfg.Source.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}

	p.schemas = make(map[string]*schema.TableSchema)
	for _, tc := range p.cfg.Tables {
		ts, err := schema.DiscoverSchema(ctx, pgConn, tc.Name)
		if err != nil {
			pgConn.Close(ctx)
			return fmt.Errorf("discover schema for %s: %w", tc.Name, err)
		}
		if p.cfg.Source.Mode == "query" && len(tc.PrimaryKey) > 0 {
			ts.PK = tc.PrimaryKey
		}
		p.schemas[tc.Name] = ts
		log.Printf("[pipeline:%s] discovered schema for %s: %d columns, pk=%v", p.id, tc.Name, len(ts.Columns), ts.PK)
	}
	pgConn.Close(ctx)

	// Initialize sink (skip if already set via WithSink).
	if p.snk == nil {
		p.snk, err = sink.NewSink(p.cfg.Sink, p.cfg.Source.Postgres, p.cfg.Tables, p.id)
		if err != nil {
			return fmt.Errorf("create sink: %w", err)
		}
	}

	for _, ts := range p.schemas {
		if err := p.snk.RegisterTable(ctx, ts); err != nil {
			return fmt.Errorf("register table %s: %w", ts.Table, err)
		}
	}

	// Start WAL lag monitor for logical mode.
	if p.cfg.Source.Mode == "logical" {
		go p.monitorWALLag(ctx)
	}

	// Start compactor if configured.
	compactionInterval := p.cfg.Sink.CompactionDuration()
	if compactionInterval > 0 {
		compactor := sink.NewCompactor(p.cfg.Sink, p.snk, p.schemas)
		go compactor.Run(ctx)
		log.Printf("[pipeline:%s] compactor started (interval=%s, target_size=%dMB, min_files=%d)",
			p.id, compactionInterval,
			p.cfg.Sink.CompactionTargetSizeOrDefault()/(1024*1024),
			p.cfg.Sink.CompactionMinFilesOrDefault())
	}

	// Initialize source.
	switch p.cfg.Source.Mode {
	case "query":
		qs := source.NewQuerySource(p.cfg.Source.Postgres, p.cfg.Source.Query, p.cfg.Tables)
		if cp.Mode == "query" && cp.Watermark != "" {
			t, err := time.Parse(time.RFC3339Nano, cp.Watermark)
			if err == nil {
				for _, tc := range p.cfg.Tables {
					qs.SetWatermark(tc.Name, t)
				}
				log.Printf("[pipeline:%s] restored query watermark: %v", p.id, t)
			}
		}
		p.src = qs

	case "logical":
		ls := source.NewLogicalSource(p.cfg.Source.Postgres, p.cfg.Source.Logical, p.cfg.Tables, p.id)
		if cp.Mode == "logical" && cp.LSN > 0 {
			ls.SetStartLSN(cp.LSN)
			log.Printf("[pipeline:%s] restored logical LSN: %d", p.id, cp.LSN)
		}
		ls.SetSnapshotComplete(cp.SnapshotComplete)
		ls.SetSnapshotedTables(cp.SnapshotedTables)
		if !cp.SnapshotComplete {
			p.setStatus(StatusSnapshotting, nil)
			metrics.SnapshotInProgress.WithLabelValues(p.id).Set(1)
		}
		p.src = ls

	default:
		return fmt.Errorf("unknown source mode: %s", p.cfg.Source.Mode)
	}

	return nil
}

// run is the main event loop, executed in a goroutine.
func (p *Pipeline) run(ctx context.Context) {
	defer close(p.done)
	defer p.snk.Close()

	p.mu.Lock()
	p.startedAt = time.Now()
	// Only transition to Running if not already Snapshotting (set in initSource).
	if p.status != StatusSnapshotting {
		p.status = StatusRunning
		metrics.PipelineStatus.WithLabelValues(p.id).Set(metrics.StatusToFloat["running"])
	}
	p.mu.Unlock()

	// Periodically update gauge-style metrics.
	metricsTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-metricsTicker.C:
				p.mu.RLock()
				if !p.startedAt.IsZero() {
					metrics.PipelineUptimeSeconds.WithLabelValues(p.id).Set(time.Since(p.startedAt).Seconds())
				}
				p.mu.RUnlock()
				metrics.EventsBuffered.WithLabelValues(p.id).Set(float64(p.snk.TotalBuffered()))
				metrics.BytesBuffered.WithLabelValues(p.id).Set(float64(p.snk.TotalBufferedBytes()))
				if ls, ok := p.src.(*source.LogicalSource); ok {
					metrics.ConfirmedLSN.WithLabelValues(p.id).Set(float64(ls.FlushedLSN()))
				}
			}
		}
	}()

	events := make(chan source.ChangeEvent, 1000)
	errCh := make(chan error, 1)

	go func() {
		errCh <- p.src.Capture(ctx, events)
	}()

	flushInterval := p.cfg.Sink.FlushDuration()
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flushRows := p.cfg.Sink.FlushRows
	flushBytes := p.cfg.Sink.FlushBytesOrDefault()

	log.Printf("[pipeline:%s] started (mode=%s, flush_interval=%s, flush_rows=%d, flush_bytes=%dMB, target_file_size=%dMB)",
		p.id, p.cfg.Source.Mode, flushInterval, flushRows,
		flushBytes/(1024*1024),
		p.cfg.Sink.TargetFileSizeOrDefault()/(1024*1024))

	for {
		select {
		case event, ok := <-events:
			if !ok {
				if err := p.flush(ctx); err != nil {
					p.setStatus(StatusError, err)
				}
				return
			}

			// Handle per-table snapshot completion: flush buffered rows and
			// checkpoint this table so crash recovery skips it.
			if event.Operation == source.OpSnapshotTableComplete {
				if err := p.flushSnapshotTable(ctx, event.Table); err != nil {
					log.Printf("[pipeline:%s] snapshot table %s flush error: %v", p.id, event.Table, err)
					p.setStatus(StatusError, err)
					return
				}
				continue
			}

			// Handle snapshot-complete marker: flush remaining snapshot rows
			// and persist the flag so we don't re-snapshot on restart.
			if event.Operation == source.OpSnapshotComplete {
				if err := p.flushSnapshotComplete(ctx); err != nil {
					log.Printf("[pipeline:%s] snapshot complete flush error: %v", p.id, err)
					p.setStatus(StatusError, err)
					return
				}
				continue
			}

			// Handle schema evolution: flush old data, evolve Iceberg schema,
			// rebuild writers, then resume with the new schema.
			if event.Operation == source.OpSchemaChange {
				if err := p.handleSchemaChange(ctx, event); err != nil {
					log.Printf("[pipeline:%s] schema change error: %v", p.id, err)
					p.setStatus(StatusError, err)
					return
				}
				continue
			}

			// Pass Begin/Commit through to sink for transaction tracking.
			if event.Operation == source.OpBegin || event.Operation == source.OpCommit {
				if err := p.snk.Write(event); err != nil {
					log.Printf("[pipeline:%s] write error: %v", p.id, err)
				}
				continue
			}

			if err := p.snk.Write(event); err != nil {
				log.Printf("[pipeline:%s] write error: %v", p.id, err)
				continue
			}
			p.rowsProcessed++
			metrics.RowsProcessedTotal.WithLabelValues(p.id, event.Table, event.Operation.String()).Inc()

			if p.snk.TotalBuffered() >= flushRows {
				if err := p.doFlush(ctx); err != nil {
					log.Printf("[pipeline:%s] flush error: %v", p.id, err)
				}
			}

			if p.snk.TotalBufferedBytes() >= flushBytes {
				if err := p.doFlush(ctx); err != nil {
					log.Printf("[pipeline:%s] flush error: %v", p.id, err)
				}
			}

		case <-flushTicker.C:
			if p.snk.ShouldFlush() {
				if err := p.doFlush(ctx); err != nil {
					log.Printf("[pipeline:%s] flush error: %v", p.id, err)
				}
			}

		case err := <-errCh:
			if p.snk.ShouldFlush() {
				flushedBytes := p.snk.TotalBufferedBytes()
				if flushErr := p.flush(ctx); flushErr != nil {
					log.Printf("[pipeline:%s] final flush error: %v", p.id, flushErr)
				} else {
					p.bytesProcessed += flushedBytes
				}
			}
			if err != nil && err != context.Canceled {
				p.setStatus(StatusError, err)
			}
			return
		}
	}
}

func (p *Pipeline) doFlush(ctx context.Context) error {
	if err := p.snk.CheckBackpressure(ctx); err != nil {
		return err
	}
	flushedRows := p.snk.TotalBuffered()
	flushedBytes := p.snk.TotalBufferedBytes()

	start := time.Now()
	err := p.flush(ctx)
	duration := time.Since(start).Seconds()

	metrics.FlushDurationSeconds.WithLabelValues(p.id).Observe(duration)
	metrics.FlushTotal.WithLabelValues(p.id).Inc()

	if err == nil {
		p.bytesProcessed += flushedBytes
		metrics.BytesProcessedTotal.WithLabelValues(p.id).Add(float64(flushedBytes))
		metrics.FlushRowsTotal.WithLabelValues(p.id).Add(float64(flushedRows))
	} else {
		metrics.FlushErrorsTotal.WithLabelValues(p.id).Inc()
	}
	return err
}

// flushSnapshotTable flushes buffered rows and marks a single table's snapshot
// as complete in the checkpoint. On crash recovery, this table will be skipped.
func (p *Pipeline) flushSnapshotTable(ctx context.Context, table string) error {
	flushedBytes := p.snk.TotalBufferedBytes()
	if err := p.snk.Flush(ctx); err != nil {
		return fmt.Errorf("snapshot table flush: %w", err)
	}
	p.bytesProcessed += flushedBytes

	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	cp.Mode = p.cfg.Source.Mode
	if cp.SnapshotedTables == nil {
		cp.SnapshotedTables = make(map[string]bool)
	}
	cp.SnapshotedTables[table] = true
	if ls, ok := p.src.(*source.LogicalSource); ok {
		cp.LSN = ls.ReceivedLSN()
		ls.SetFlushedLSN(cp.LSN)
	}

	if err := p.store.Save(p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	metrics.SnapshotTablesCompleted.WithLabelValues(p.id).Inc()
	log.Printf("[pipeline:%s] snapshot table %s complete, checkpoint saved", p.id, table)
	return nil
}

// flushSnapshotComplete flushes any buffered snapshot rows and saves the
// SnapshotComplete flag to the checkpoint so a restart won't re-snapshot.
func (p *Pipeline) flushSnapshotComplete(ctx context.Context) error {
	flushedBytes := p.snk.TotalBufferedBytes()
	if err := p.snk.Flush(ctx); err != nil {
		return fmt.Errorf("snapshot flush: %w", err)
	}
	p.bytesProcessed += flushedBytes

	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	cp.Mode = p.cfg.Source.Mode
	cp.SnapshotComplete = true
	cp.SnapshotedTables = nil // no longer needed once fully complete
	if ls, ok := p.src.(*source.LogicalSource); ok {
		cp.LSN = ls.ReceivedLSN()
		ls.SetFlushedLSN(cp.LSN)
	}

	if err := p.store.Save(p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	p.setStatus(StatusRunning, nil)
	metrics.SnapshotInProgress.WithLabelValues(p.id).Set(0)
	log.Printf("[pipeline:%s] initial snapshot complete, checkpoint saved", p.id)
	return nil
}

// handleSchemaChange flushes all buffered data with the old schema, then
// evolves the Iceberg table schema and rebuilds the Parquet writers.
func (p *Pipeline) handleSchemaChange(ctx context.Context, event source.ChangeEvent) error {
	sc := event.SchemaChange
	if sc == nil {
		return nil
	}

	// Defensive: warn if any open transaction touches this table.
	// PG DDL takes AccessExclusiveLock, so this should never happen.
	log.Printf("[pipeline:%s] schema change for %s: +%d columns, -%d columns, %d type changes",
		p.id, sc.Table, len(sc.AddedColumns), len(sc.DroppedColumns), len(sc.TypeChanges))

	// Flush all buffered data using the old schema.
	if p.snk.ShouldFlush() {
		if err := p.flush(ctx); err != nil {
			return fmt.Errorf("pre-evolution flush: %w", err)
		}
	}

	// Evolve the Iceberg table and rebuild writers.
	if err := p.snk.EvolveSchema(ctx, sc.Table, sc); err != nil {
		return fmt.Errorf("evolve schema: %w", err)
	}

	return nil
}

func (p *Pipeline) flush(ctx context.Context) error {
	if err := p.snk.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	p.mu.Lock()
	p.lastFlushAt = time.Now()
	p.mu.Unlock()

	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint for update: %w", err)
	}

	cp.Mode = p.cfg.Source.Mode
	switch p.cfg.Source.Mode {
	case "logical":
		if ls, ok := p.src.(*source.LogicalSource); ok {
			cp.LSN = ls.ReceivedLSN()
			ls.SetFlushedLSN(cp.LSN)
		}
	}

	if err := p.store.Save(p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

// AddTable dynamically adds a table to a running pipeline.
// For logical mode, the table must already be in the PG publication.
func (p *Pipeline) AddTable(ctx context.Context, tableName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.status != StatusRunning {
		return fmt.Errorf("pipeline not running")
	}

	if _, exists := p.schemas[tableName]; exists {
		return fmt.Errorf("table %s already registered", tableName)
	}

	// Discover schema via a fresh connection.
	pgConn, err := pgx.Connect(ctx, p.cfg.Source.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connect for schema discovery: %w", err)
	}
	defer pgConn.Close(ctx)

	ts, err := schema.DiscoverSchema(ctx, pgConn, tableName)
	if err != nil {
		return fmt.Errorf("discover schema for %s: %w", tableName, err)
	}

	// Register in sink (creates Iceberg table if needed).
	if err := p.snk.RegisterTable(ctx, ts); err != nil {
		return fmt.Errorf("register table %s: %w", tableName, err)
	}

	// Update source tracking.
	switch p.cfg.Source.Mode {
	case "logical":
		if ls, ok := p.src.(*source.LogicalSource); ok {
			ls.AddTable(tableName)
		}
	case "query":
		if qs, ok := p.src.(*source.QuerySource); ok {
			qs.AddTable(config.TableConfig{Name: tableName})
		}
	}

	p.schemas[tableName] = ts
	log.Printf("[pipeline:%s] added table %s", p.id, tableName)
	return nil
}

// RemoveTable dynamically removes a table from a running pipeline.
func (p *Pipeline) RemoveTable(ctx context.Context, tableName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.status != StatusRunning {
		return fmt.Errorf("pipeline not running")
	}

	if _, exists := p.schemas[tableName]; !exists {
		return fmt.Errorf("table %s not registered", tableName)
	}

	// Update source tracking.
	switch p.cfg.Source.Mode {
	case "logical":
		if ls, ok := p.src.(*source.LogicalSource); ok {
			ls.RemoveTable(tableName)
		}
	case "query":
		if qs, ok := p.src.(*source.QuerySource); ok {
			qs.RemoveTable(tableName)
		}
	}

	p.snk.UnregisterTable(tableName)
	delete(p.schemas, tableName)
	log.Printf("[pipeline:%s] removed table %s", p.id, tableName)
	return nil
}

// monitorWALLag periodically queries PG for WAL position and replication slot lag.
func (p *Pipeline) monitorWALLag(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn, err := pgx.Connect(ctx, p.cfg.Source.Postgres.DSN())
			if err != nil {
				continue
			}

			// Query replication slot lag and WAL disk usage.
			var lagBytes int64
			var walSizeBytes int64
			err = conn.QueryRow(ctx, `
				SELECT COALESCE(pg_current_wal_lsn() - confirmed_flush_lsn, 0),
				       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
				FROM pg_replication_slots
				WHERE slot_name = $1
			`, p.cfg.Source.Logical.SlotName).Scan(&lagBytes, &walSizeBytes)
			conn.Close(ctx)

			if err != nil {
				continue
			}

			// Replication lag: how far behind pg2iceberg is from the WAL tip.
			metrics.ReplicationLagBytes.WithLabelValues(p.id).Set(float64(lagBytes))
			// WAL retained: bytes Postgres must keep on disk for this slot.
			// Uses restart_lsn (where PG would restart streaming), which is
			// older than confirmed_flush_lsn, so this is always >= lag.
			metrics.WALRetainedBytes.WithLabelValues(p.id).Set(float64(walSizeBytes))
		}
	}
}

func newCheckpointStore(ctx context.Context, cfg *config.Config) (state.CheckpointStore, error) {
	// Explicit file path: use file store (local dev).
	if cfg.State.Path != "" {
		return state.NewFileStore(cfg.State.Path), nil
	}

	// Explicit postgres URL, or fall back to source postgres.
	url := cfg.State.PostgresURL
	if url == "" {
		url = cfg.Source.Postgres.DSN()
	}
	return state.NewPgStore(ctx, url)
}

