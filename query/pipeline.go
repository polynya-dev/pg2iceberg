package query

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/snapshot"
)

// Pipeline is the query-mode replication pipeline. It polls PostgreSQL via
// watermark queries and writes directly to materialized Iceberg tables using
// the shared TableWriter.
type Pipeline struct {
	id  string
	cfg *config.Config

	poller  *Poller
	buffers map[string]*Buffer              // per-table PK dedup buffer
	writers map[string]*iceberg.TableWriter // per-table Iceberg writer
	schemas map[string]*postgres.TableSchema

	catalog iceberg.Catalog
	s3      iceberg.ObjectStorage
	store   pipeline.CheckpointStore

	status        pipeline.Status
	err           error
	startedAt     time.Time
	lastFlushAt   time.Time
	rowsProcessed int64

	snapshotNeeded bool // true if initial snapshot is required (no checkpoint)

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex
}

// BuildPipeline creates a fully-wired query Pipeline from config.
func BuildPipeline(ctx context.Context, id string, cfg *config.Config) (*Pipeline, error) {
	cpStore, err := pipeline.NewCheckpointStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create checkpoint store: %w", err)
	}

	clients, err := iceberg.NewClients(cfg.Sink)
	if err != nil {
		cpStore.Close()
		return nil, fmt.Errorf("create iceberg clients: %w", err)
	}

	// In vended credential mode, initialize storage from the catalog.
	if clients.S3 == nil && len(cfg.Tables) > 0 {
		firstTable := postgres.TableToIceberg(cfg.Tables[0].Name)
		if err := clients.EnsureStorage(ctx, cfg.Sink.Namespace, firstTable); err != nil {
			cpStore.Close()
			return nil, fmt.Errorf("ensure storage: %w", err)
		}
	}

	return &Pipeline{
		id:      id,
		cfg:     cfg,
		catalog: clients.Catalog,
		s3:      clients.S3,
		store:   cpStore,
		status:  pipeline.StatusStopped,
		done:    make(chan struct{}),
	}, nil
}

// NewPipeline creates a Pipeline with injected dependencies (for tests).
func NewPipeline(id string, cfg *config.Config, s3 iceberg.ObjectStorage, catalog iceberg.Catalog, store pipeline.CheckpointStore) *Pipeline {
	return &Pipeline{
		id:      id,
		cfg:     cfg,
		catalog: catalog,
		s3:      s3,
		store:   store,
		status:  pipeline.StatusStopped,
		done:    make(chan struct{}),
	}
}

// Start initializes and runs the pipeline. Setup is synchronous; the event
// loop runs in a background goroutine.
func (p *Pipeline) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	if err := p.setup(ctx); err != nil {
		cancel()
		close(p.done)
		return err
	}

	go p.run(ctx)
	return nil
}

// Stop signals the pipeline to shut down gracefully.
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// Done returns a channel that is closed when the pipeline exits.
func (p *Pipeline) Done() <-chan struct{} { return p.done }

// Status returns the current status and last error.
func (p *Pipeline) Status() (pipeline.Status, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status, p.err
}

// Metrics returns a snapshot of pipeline pipeline.
func (p *Pipeline) Metrics() pipeline.Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	m := pipeline.Metrics{
		Status:        p.status,
		RowsProcessed: p.rowsProcessed,
	}

	var totalBuffered int
	var totalBytes int64
	for _, buf := range p.buffers {
		totalBuffered += buf.Len()
		totalBytes += buf.EstimatedBytes()
	}
	m.BufferedRows = totalBuffered
	m.BufferedBytes = totalBytes

	if !p.lastFlushAt.IsZero() {
		m.LastFlushAt = p.lastFlushAt.Format(time.RFC3339)
	}
	if !p.startedAt.IsZero() {
		m.Uptime = time.Since(p.startedAt).Truncate(time.Second).String()
	}
	return m
}

func (p *Pipeline) setStatus(s pipeline.Status, err error) {
	p.mu.Lock()
	p.status = s
	p.err = err
	p.mu.Unlock()
}

func (p *Pipeline) setup(ctx context.Context) error {
	p.setStatus(pipeline.StatusStarting, nil)

	// Load checkpoint.
	cp, err := p.store.Load(ctx, p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Create poller and discover schemas.
	p.poller = NewPoller(p.cfg.Source.Postgres, p.cfg.Tables)
	if err := p.poller.Connect(ctx); err != nil {
		return fmt.Errorf("poller connect: %w", err)
	}
	p.schemas = p.poller.Schemas()

	// Restore watermarks from checkpoint or mark snapshot as needed.
	if cp.Mode == "query" {
		if cp.QueryWatermarks != nil {
			for table, wmStr := range cp.QueryWatermarks {
				t, err := time.Parse(time.RFC3339Nano, wmStr)
				if err == nil {
					p.poller.SetWatermark(table, t)
				}
			}
			log.Printf("[query:%s] restored per-table watermarks from checkpoint", p.id)
		} else if cp.Watermark != "" {
			// Backward compat: single watermark for all tables.
			t, err := time.Parse(time.RFC3339Nano, cp.Watermark)
			if err == nil {
				for _, tc := range p.cfg.Tables {
					p.poller.SetWatermark(tc.Name, t)
				}
				log.Printf("[query:%s] restored watermark (legacy): %v", p.id, t)
			}
		} else {
			// Query checkpoint exists but no watermarks — needs snapshot.
			p.snapshotNeeded = true
		}
	} else if cp.Mode == "" {
		// No checkpoint at all — needs initial snapshot.
		p.snapshotNeeded = true
	}

	// Ensure namespace exists.
	if err := p.catalog.EnsureNamespace(ctx, p.cfg.Sink.Namespace); err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}

	// Create/load materialized tables and TableWriters. No events table.
	p.buffers = make(map[string]*Buffer, len(p.schemas))
	p.writers = make(map[string]*iceberg.TableWriter, len(p.schemas))

	for pgTable, ts := range p.schemas {
		icebergName := pgTableToIceberg(pgTable)

		// Build partition spec from config.
		var partExprs []string
		for _, tc := range p.cfg.Tables {
			if tc.Name == pgTable {
				partExprs = tc.Iceberg.Partition
				break
			}
		}
		partSpec, err := iceberg.BuildPartitionSpec(partExprs, ts)
		if err != nil {
			return fmt.Errorf("build partition spec for %s: %w", pgTable, err)
		}

		// Create or load the materialized table.
		matTm, err := p.catalog.LoadTable(ctx, p.cfg.Sink.Namespace, icebergName)
		if err != nil {
			return fmt.Errorf("load table %s: %w", icebergName, err)
		}
		var schemaID int
		if matTm == nil {
			var location string
			if iceberg.IsStorageURI(p.cfg.Sink.Warehouse) {
				location = fmt.Sprintf("%s%s.db/%s", p.cfg.Sink.Warehouse, p.cfg.Sink.Namespace, icebergName)
			}
			matTm, err = p.catalog.CreateTable(ctx, p.cfg.Sink.Namespace, icebergName, ts, location, partSpec)
			if err != nil {
				return fmt.Errorf("create table %s: %w", icebergName, err)
			}
			log.Printf("[query:%s] created materialized table %s.%s", p.id, p.cfg.Sink.Namespace, icebergName)
			schemaID = matTm.Metadata.CurrentSchemaID
		} else {
			log.Printf("[query:%s] using existing materialized table %s.%s", p.id, p.cfg.Sink.Namespace, icebergName)
			schemaID = matTm.Metadata.CurrentSchemaID
		}

		p.writers[pgTable] = iceberg.NewTableWriter(iceberg.TableWriteConfig{
			Namespace:   p.cfg.Sink.Namespace,
			IcebergName: icebergName,
			SrcSchema:   ts,
			PartSpec:    partSpec,
			SchemaID:    schemaID,
			TargetSize:  p.cfg.Sink.MaterializerTargetFileSizeOrDefault(),
			Concurrency: p.cfg.Sink.MaterializerConcurrencyOrDefault(),
		}, p.catalog, p.s3)

		p.buffers[pgTable] = NewBuffer(ts.PK)
	}

	return nil
}

func (p *Pipeline) run(ctx context.Context) {
	defer close(p.done)
	defer p.poller.Close(ctx)

	p.mu.Lock()
	p.startedAt = time.Now()
	if p.snapshotNeeded {
		p.status = pipeline.StatusSnapshotting
		pipeline.PipelineStatus.WithLabelValues(p.id).Set(pipeline.StatusToFloat["snapshotting"])
		pipeline.SnapshotInProgress.WithLabelValues(p.id).Set(1)
	} else {
		p.status = pipeline.StatusRunning
		pipeline.PipelineStatus.WithLabelValues(p.id).Set(pipeline.StatusToFloat["running"])
	}
	p.mu.Unlock()

	// Initial snapshot: bulk-copy all existing rows via CTID chunks.
	if p.snapshotNeeded {
		if err := p.runSnapshot(ctx); err != nil {
			p.setStatus(pipeline.StatusError, fmt.Errorf("initial snapshot: %w", err))
			return
		}
		p.snapshotNeeded = false
		p.setStatus(pipeline.StatusRunning, nil)
		pipeline.SnapshotInProgress.WithLabelValues(p.id).Set(0)
	}

	pollInterval := p.cfg.Source.Query.PollDuration()
	pollTicker := time.NewTicker(pollInterval)
	defer pollTicker.Stop()

	flushInterval := p.cfg.Sink.FlushDuration()
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flushRows := p.cfg.Sink.FlushRows
	flushBytes := p.cfg.Sink.FlushBytesOrDefault()

	log.Printf("[query:%s] started (poll_interval=%s, flush_interval=%s, flush_rows=%d, flush_bytes=%dMB)",
		p.id, pollInterval, flushInterval, flushRows, flushBytes/(1024*1024))

	// Periodic gauge-style metrics.
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
					pipeline.PipelineUptimeSeconds.WithLabelValues(p.id).Set(time.Since(p.startedAt).Seconds())
				}
				p.mu.RUnlock()
				for table, wm := range p.poller.Watermarks() {
					if !wm.IsZero() {
						pipeline.QueryWatermarkLagSeconds.WithLabelValues(p.id, table).Set(time.Since(wm).Seconds())
					}
				}
			}
		}
	}()

	// Immediate first poll.
	if err := p.pollAndBuffer(ctx); err != nil {
		p.setStatus(pipeline.StatusError, fmt.Errorf("initial poll: %w", err))
		return
	}

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown: flush remaining data.
			if p.totalBuffered() > 0 {
				flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := p.flush(flushCtx); err != nil {
					log.Printf("[query:%s] final flush error: %v", p.id, err)
				}
				cancel()
			}
			p.setStatus(pipeline.StatusStopped, nil)
			return

		case <-pollTicker.C:
			if err := p.pollAndBuffer(ctx); err != nil {
				p.setStatus(pipeline.StatusError, fmt.Errorf("poll: %w", err))
				return
			}
			// Check row/byte flush thresholds after poll.
			if flushRows > 0 && p.totalBuffered() >= flushRows {
				if err := p.flush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("flush: %w", err))
					return
				}
			}
			if p.totalBufferedBytes() >= flushBytes {
				if err := p.flush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("flush: %w", err))
					return
				}
			}

		case <-flushTicker.C:
			if p.totalBuffered() > 0 {
				if err := p.flush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("flush: %w", err))
					return
				}
			}
		}
	}
}

func (p *Pipeline) pollAndBuffer(ctx context.Context) error {
	start := time.Now()
	results, err := p.poller.PollWithRetry(ctx)
	pipeline.QueryPollDurationSeconds.WithLabelValues(p.id).Observe(time.Since(start).Seconds())
	pipeline.QueryPollTotal.WithLabelValues(p.id).Inc()
	if err != nil {
		return err
	}
	for _, r := range results {
		buf := p.buffers[r.Table]
		if buf == nil {
			continue
		}
		for _, row := range r.Rows {
			buf.Upsert(row)
		}
		p.rowsProcessed += int64(len(r.Rows))
		pipeline.RowsProcessedTotal.WithLabelValues(p.id, r.Table, "INSERT").Add(float64(len(r.Rows)))
		pipeline.QueryPollRowsTotal.WithLabelValues(p.id, r.Table).Add(float64(len(r.Rows)))
		pipeline.QueryBufferRows.WithLabelValues(p.id, r.Table).Set(float64(buf.Len()))
	}
	return nil
}

func (p *Pipeline) flush(ctx context.Context) error {
	start := time.Now()

	// Prepare commits for all tables with buffered data.
	var commits []iceberg.TableCommit
	type tablePrep struct {
		pgTable  string
		prepared *iceberg.PreparedCommit
	}
	var preps []tablePrep

	for pgTable, buf := range p.buffers {
		if buf.Len() == 0 {
			continue
		}
		rows := buf.Drain()
		pipeline.QueryBufferRows.WithLabelValues(p.id, pgTable).Set(0)
		tw := p.writers[pgTable]
		ts := p.schemas[pgTable]

		prepared, err := tw.Prepare(ctx, rows, ts.PK)
		if err != nil {
			pipeline.QueryFlushErrorsTotal.WithLabelValues(p.id).Inc()
			return fmt.Errorf("prepare %s: %w", pgTable, err)
		}
		if prepared == nil {
			continue
		}
		commits = append(commits, prepared.ToTableCommit())
		preps = append(preps, tablePrep{pgTable: pgTable, prepared: prepared})
	}

	if len(commits) == 0 {
		return nil
	}

	// Atomic multi-table commit.
	if err := p.catalog.CommitTransaction(ctx, p.cfg.Sink.Namespace, commits); err != nil {
		pipeline.QueryFlushErrorsTotal.WithLabelValues(p.id).Inc()
		return fmt.Errorf("commit: %w", err)
	}

	pipeline.QueryFlushTotal.WithLabelValues(p.id).Inc()
	pipeline.QueryFlushDurationSeconds.WithLabelValues(p.id).Observe(time.Since(start).Seconds())

	// Post-commit: update manifest caches + emit per-table metrics.
	for _, tp := range preps {
		p.writers[tp.pgTable].ApplyPostCommit(tp.prepared)
		pipeline.QueryDataFilesWrittenTotal.WithLabelValues(p.id, tp.pgTable).Add(float64(tp.prepared.DataCount))
		pipeline.QueryDeleteFilesWrittenTotal.WithLabelValues(p.id, tp.pgTable).Add(float64(tp.prepared.DeleteCount))
		log.Printf("[query:%s] flushed %s: %d data files, %d delete files",
			p.id, tp.pgTable, tp.prepared.DataCount, tp.prepared.DeleteCount)
	}

	// Compaction: rewrite small files if thresholds exceeded.
	cc := iceberg.CompactionConfig{
		DataFileThreshold:   p.cfg.Sink.CompactionDataFilesOrDefault(),
		DeleteFileThreshold: p.cfg.Sink.CompactionDeleteFilesOrDefault(),
	}
	for _, tp := range preps {
		tw := p.writers[tp.pgTable]
		ts := p.schemas[tp.pgTable]
		compacted, err := tw.Compact(ctx, ts.PK, cc)
		if err != nil {
			log.Printf("[query:%s] compaction error for %s: %v", p.id, tp.pgTable, err)
			continue
		}
		if compacted == nil {
			continue
		}
		err = p.catalog.CommitSnapshot(ctx, p.cfg.Sink.Namespace, compacted.IcebergName, compacted.PrevSnapshotID, compacted.Commit)
		if err != nil {
			log.Printf("[query:%s] compaction commit error for %s: %v", p.id, tp.pgTable, err)
			continue
		}
		tw.ApplyPostCommit(compacted)
	}

	// Checkpoint watermarks.
	p.mu.Lock()
	p.lastFlushAt = time.Now()
	p.mu.Unlock()

	cp, err := p.store.Load(ctx, p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	cp.Mode = "query"
	cp.QueryWatermarks = make(map[string]string, len(p.poller.Watermarks()))
	for table, wm := range p.poller.Watermarks() {
		cp.QueryWatermarks[table] = wm.Format(time.RFC3339Nano)
	}
	if err := p.store.Save(ctx, p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

func (p *Pipeline) totalBuffered() int {
	total := 0
	for _, buf := range p.buffers {
		total += buf.Len()
	}
	return total
}

func (p *Pipeline) totalBufferedBytes() int64 {
	var total int64
	for _, buf := range p.buffers {
		total += buf.EstimatedBytes()
	}
	return total
}

func pgTableToIceberg(pgTable string) string {
	return postgres.TableToIceberg(pgTable)
}

// runSnapshot performs the initial snapshot for all tables using CTID chunking.
// Before snapshotting, it captures MAX(watermark_column) per table as a fence.
// After the snapshot, the watermark is set to the fence value so the poller
// starts from there — catching any rows modified during the snapshot.
func (p *Pipeline) runSnapshot(ctx context.Context) error {
	log.Printf("[query:%s] starting initial snapshot", p.id)

	// Step 1: Capture watermark fence per table.
	fenceConn, err := pgx.Connect(ctx, p.cfg.Source.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("fence connect: %w", err)
	}
	defer fenceConn.Close(ctx)

	fences := make(map[string]time.Time)
	for _, tc := range p.cfg.Tables {
		if tc.WatermarkColumn == "" {
			continue
		}
		quotedTable := pgx.Identifier(strings.Split(tc.Name, ".")).Sanitize()
		quotedCol := pgx.Identifier{tc.WatermarkColumn}.Sanitize()
		var maxWM *time.Time
		err := fenceConn.QueryRow(ctx,
			fmt.Sprintf("SELECT MAX(%s) FROM %s", quotedCol, quotedTable),
		).Scan(&maxWM)
		if err != nil {
			return fmt.Errorf("fence query for %s: %w", tc.Name, err)
		}
		if maxWM != nil {
			fences[tc.Name] = *maxWM
			log.Printf("[query:%s] snapshot fence for %s: %v", p.id, tc.Name, *maxWM)
		}
	}
	fenceConn.Close(ctx)

	// Step 2: Build snapshot tables and run.
	var tables []snapshot.Table
	for _, tc := range p.cfg.Tables {
		ts := p.schemas[tc.Name]
		if ts == nil {
			continue
		}
		tables = append(tables, snapshot.Table{Name: tc.Name, Schema: ts})
	}

	if len(tables) == 0 {
		return nil
	}

	dsn := p.cfg.Source.Postgres.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("snapshot connect: %w", err)
		}
		cleanup := func(ctx context.Context) { conn.Close(ctx) }
		tx, err := conn.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, fmt.Errorf("begin snapshot tx: %w", err)
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, fmt.Errorf("set isolation level: %w", err)
		}
		return tx, cleanup, nil
	}

	deps := snapshot.Deps{
		Catalog:    p.catalog,
		S3:         p.s3,
		SinkCfg:    p.cfg.Sink,
		LogicalCfg: p.cfg.Source.Logical,
		TableCfgs:  p.cfg.Tables,
		Schemas:    p.schemas,
		Store:      p.store,
		PipelineID: p.id,
	}

	concurrency := p.cfg.Source.Logical.SnapshotConcurrencyOrDefault()
	snap := snapshot.NewSnapshotter(tables, txFactory, concurrency, deps)
	if _, err := snap.Run(ctx); err != nil {
		return err
	}

	// Step 3: Set watermarks to fence values and checkpoint.
	for table, fence := range fences {
		p.poller.SetWatermark(table, fence)
	}

	cp, err := p.store.Load(ctx, p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint after snapshot: %w", err)
	}
	cp.Mode = "query"
	cp.SnapshotComplete = true
	cp.SnapshotedTables = nil
	cp.SnapshotChunks = nil
	cp.QueryWatermarks = make(map[string]string, len(fences))
	for table, fence := range fences {
		cp.QueryWatermarks[table] = fence.Format(time.RFC3339Nano)
	}
	if err := p.store.Save(ctx, p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint after snapshot: %w", err)
	}

	log.Printf("[query:%s] initial snapshot complete, watermarks set to fence values", p.id)
	return nil
}
