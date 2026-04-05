package logical

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/sink"
	"github.com/pg2iceberg/pg2iceberg/utils"
)

// Pipeline encapsulates the logical replication pipeline: WAL capture →
// events table → materializer → materialized table.
type Pipeline struct {
	id  string
	cfg *config.Config

	status pipeline.Status
	err    error

	src     *LogicalSource
	snk     *sink.Sink
	store   pipeline.CheckpointStore
	schemas map[string]*schema.TableSchema

	startedAt      time.Time
	lastFlushAt    time.Time
	rowsProcessed  int64
	bytesProcessed int64
	lastWrittenLSN uint64

	materializer *sink.Materializer
	eventBuf     *sink.ChangeEventBuffer

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex
}

// BuildPipeline creates a fully-wired logical Pipeline from config.
func BuildPipeline(ctx context.Context, id string, cfg *config.Config) (*Pipeline, error) {
	cpStore, err := pipeline.NewCheckpointStore(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create checkpoint store: %w", err)
	}

	eventBuf := sink.NewChangeEventBuffer()
	snk, err := sink.BuildSink(cfg.Sink, cfg.Tables, id, eventBuf)
	if err != nil {
		cpStore.Close()
		return nil, fmt.Errorf("create sink: %w", err)
	}

	return &Pipeline{
		id:       id,
		cfg:      cfg,
		snk:      snk,
		store:    cpStore,
		eventBuf: eventBuf,
		status:   pipeline.StatusStopped,
		done:     make(chan struct{}),
	}, nil
}

// NewPipeline creates a Pipeline with injected dependencies (for tests).
func NewPipeline(id string, cfg *config.Config, snk *sink.Sink, store pipeline.CheckpointStore) *Pipeline {
	return &Pipeline{
		id:     id,
		cfg:    cfg,
		snk:    snk,
		store:  store,
		status: pipeline.StatusStopped,
		done:   make(chan struct{}),
	}
}

// SetEventBuf sets the change event buffer. Must be called before Start.
func (p *Pipeline) SetEventBuf(buf *sink.ChangeEventBuffer) { p.eventBuf = buf }

// Config returns the pipeline configuration.
func (p *Pipeline) Config() *config.Config { return p.cfg }

// ID returns the pipeline identifier.
func (p *Pipeline) ID() string { return p.id }

// Source returns the pipeline's source (for testing/inspection).
func (p *Pipeline) Source() *LogicalSource { return p.src }

// Status returns the current status and last error.
func (p *Pipeline) Status() (pipeline.Status, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status, p.err
}

// Done returns a channel that is closed when the pipeline exits.
func (p *Pipeline) Done() <-chan struct{} { return p.done }

// Metrics returns a snapshot of pipeline metrics.
func (p *Pipeline) Metrics() pipeline.Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	m := pipeline.Metrics{
		Status:         p.status,
		RowsProcessed:  p.rowsProcessed,
		BytesProcessed: p.bytesProcessed,
	}

	if p.snk != nil {
		m.BufferedRows = p.snk.TotalBuffered()
		m.BufferedBytes = p.snk.TotalBufferedBytes()
	}

	if p.src != nil {
		m.LSN = p.src.FlushedLSN()
	}

	if !p.lastFlushAt.IsZero() {
		m.LastFlushAt = p.lastFlushAt.Format(time.RFC3339)
	}

	if !p.startedAt.IsZero() {
		m.Uptime = time.Since(p.startedAt).Truncate(time.Second).String()
	}

	return m
}

// Start initializes and runs the pipeline.
func (p *Pipeline) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.status == pipeline.StatusRunning || p.status == pipeline.StatusStarting {
		p.mu.Unlock()
		return fmt.Errorf("pipeline %s already running", p.id)
	}
	p.status = pipeline.StatusStarting
	p.err = nil
	p.done = make(chan struct{})
	p.mu.Unlock()

	pipeCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	if err := p.setup(pipeCtx); err != nil {
		cancel()
		p.setStatus(pipeline.StatusError, err)
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

	if s != pipeline.StatusRunning && s != pipeline.StatusStarting {
		return nil
	}

	p.setStatus(pipeline.StatusStopping, nil)
	if p.cancel != nil {
		p.cancel()
	}

	<-p.done

	if p.src != nil {
		if err := p.src.Close(); err != nil {
			log.Printf("[logical:%s] source close error: %v", p.id, err)
		}
	}
	if p.store != nil {
		p.store.Close()
	}

	p.setStatus(pipeline.StatusStopped, nil)
	return nil
}

func (p *Pipeline) setStatus(s pipeline.Status, err error) {
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

func (p *Pipeline) setup(ctx context.Context) error {
	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Discover schemas.
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
		p.schemas[tc.Name] = ts
		log.Printf("[logical:%s] discovered schema for %s: %d columns, pk=%v", p.id, tc.Name, len(ts.Columns), ts.PK)
	}
	pgConn.Close(ctx)

	for _, ts := range p.schemas {
		if err := p.snk.RegisterTable(ctx, ts); err != nil {
			return fmt.Errorf("register table %s: %w", ts.Table, err)
		}
	}

	// Start WAL lag monitor.
	go p.monitorWALLag(ctx)

	// Start materializer.
	materializer := sink.NewMaterializer(p.cfg.Sink, p.snk.Catalog(), p.snk.S3(), p.snk.Tables(), p.eventBuf)
	if cp.MaterializerSnapshots != nil {
		for pgTable, snapID := range cp.MaterializerSnapshots {
			materializer.SetLastEventsSnapshot(pgTable, snapID)
		}
	}
	go materializer.Run(ctx)
	p.materializer = materializer
	log.Printf("[logical:%s] materializer started (interval=%s)", p.id, p.cfg.Sink.MaterializerDuration())

	// Initialize logical source.
	p.src = NewLogicalSource(p.cfg.Source.Postgres, p.cfg.Source.Logical, p.cfg.Tables, p.id)
	if cp.Mode == "logical" && cp.LSN > 0 {
		p.src.SetStartLSN(cp.LSN)
		log.Printf("[logical:%s] restored LSN: %d", p.id, cp.LSN)
	}
	p.src.SetSnapshotComplete(cp.SnapshotComplete)
	p.src.SetSnapshotedTables(cp.SnapshotedTables)
	if !cp.SnapshotComplete {
		p.setStatus(pipeline.StatusSnapshotting, nil)
		metrics.SnapshotInProgress.WithLabelValues(p.id).Set(1)
	}

	return nil
}

func (p *Pipeline) run(ctx context.Context) {
	defer close(p.done)
	defer p.snk.Close()

	p.mu.Lock()
	p.startedAt = time.Now()
	if p.status != pipeline.StatusSnapshotting {
		p.status = pipeline.StatusRunning
		metrics.PipelineStatus.WithLabelValues(p.id).Set(metrics.StatusToFloat["running"])
	}
	p.mu.Unlock()

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
					metrics.PipelineUptimeSeconds.WithLabelValues(p.id).Set(time.Since(p.startedAt).Seconds())
				}
				p.mu.RUnlock()
				metrics.EventsBuffered.WithLabelValues(p.id).Set(float64(p.snk.TotalBuffered()))
				metrics.BytesBuffered.WithLabelValues(p.id).Set(float64(p.snk.TotalBufferedBytes()))
				if p.src != nil {
					metrics.ConfirmedLSN.WithLabelValues(p.id).Set(float64(p.src.FlushedLSN()))
				}
			}
		}
	}()

	events := make(chan ChangeEvent, 1000)
	errCh := make(chan error, 1)

	go func() {
		errCh <- p.src.Capture(ctx, events)
	}()

	flushInterval := p.cfg.Sink.FlushDuration()
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flushRows := p.cfg.Sink.FlushRows
	flushBytes := p.cfg.Sink.FlushBytesOrDefault()

	log.Printf("[logical:%s] started (flush_interval=%s, flush_rows=%d, flush_bytes=%dMB, target_file_size=%dMB)",
		p.id, flushInterval, flushRows,
		flushBytes/(1024*1024),
		p.cfg.Sink.TargetFileSizeOrDefault()/(1024*1024))

	for {
		select {
		case event, ok := <-events:
			if !ok {
				if err := p.flush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, err)
				}
				return
			}

			if event.Operation == OpSnapshotTableComplete {
				if err := p.flushSnapshotTable(ctx, event.Table); err != nil {
					log.Printf("[logical:%s] snapshot table %s flush error: %v", p.id, event.Table, err)
					p.setStatus(pipeline.StatusError, err)
					return
				}
				continue
			}

			if event.Operation == OpSnapshotComplete {
				if err := p.flushSnapshotComplete(ctx); err != nil {
					log.Printf("[logical:%s] snapshot complete flush error: %v", p.id, err)
					p.setStatus(pipeline.StatusError, err)
					return
				}
				continue
			}

			if event.Operation == OpSchemaChange {
				if err := p.handleSchemaChange(ctx, event); err != nil {
					log.Printf("[logical:%s] schema change error: %v", p.id, err)
					p.setStatus(pipeline.StatusError, err)
					return
				}
				continue
			}

			if event.Operation == OpBegin || event.Operation == OpCommit {
				if err := p.snk.Write(event); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("write error: %w", err))
					return
				}
				if event.LSN > p.lastWrittenLSN {
					p.lastWrittenLSN = event.LSN
				}
				continue
			}

			if err := p.snk.Write(event); err != nil {
				p.setStatus(pipeline.StatusError, fmt.Errorf("write error: %w", err))
				return
			}
			if event.LSN > p.lastWrittenLSN {
				p.lastWrittenLSN = event.LSN
			}
			p.rowsProcessed++
			metrics.RowsProcessedTotal.WithLabelValues(p.id, event.Table, event.Operation.String()).Inc()

			if p.snk.TotalBuffered() >= flushRows {
				if err := p.doFlush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("flush failed after retries: %w", err))
					return
				}
			}

			if p.snk.TotalBufferedBytes() >= flushBytes {
				if err := p.doFlush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("flush failed after retries: %w", err))
					return
				}
			}

		case <-flushTicker.C:
			if p.snk.ShouldFlush() {
				if err := p.doFlush(ctx); err != nil {
					p.setStatus(pipeline.StatusError, fmt.Errorf("flush failed after retries: %w", err))
					return
				}
			}

		case err := <-errCh:
			var drainErr error
			for {
				select {
				case event, ok := <-events:
					if !ok {
						goto drained
					}
					if event.Operation == OpBegin || event.Operation == OpCommit {
						if writeErr := p.snk.Write(event); writeErr != nil {
							drainErr = writeErr
							goto drained
						}
						if event.LSN > p.lastWrittenLSN {
							p.lastWrittenLSN = event.LSN
						}
					} else if event.Operation != OpSnapshotTableComplete &&
						event.Operation != OpSnapshotComplete &&
						event.Operation != OpSchemaChange {
						if writeErr := p.snk.Write(event); writeErr != nil {
							drainErr = writeErr
							goto drained
						}
						if event.LSN > p.lastWrittenLSN {
							p.lastWrittenLSN = event.LSN
						}
					}
				default:
					goto drained
				}
			}
		drained:
			if drainErr != nil {
				p.setStatus(pipeline.StatusError, fmt.Errorf("drain write error: %w", drainErr))
				return
			}
			if p.snk.ShouldFlush() {
				flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
				flushedBytes := p.snk.TotalBufferedBytes()
				if flushErr := p.flush(flushCtx); flushErr != nil {
					flushCancel()
					p.setStatus(pipeline.StatusError, fmt.Errorf("final flush error: %w", flushErr))
					return
				}
				p.bytesProcessed += flushedBytes
				flushCancel()
			}
			if p.materializer != nil {
				matCtx, matCancel := context.WithTimeout(context.Background(), 60*time.Second)
				p.materializer.MaterializeAll(matCtx)
				matCancel()
				log.Printf("[logical:%s] final materialization complete", p.id)
			}
			if err != nil && err != context.Canceled {
				p.setStatus(pipeline.StatusError, err)
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
	err := utils.Do(ctx, 5, 100*time.Millisecond, 10*time.Second, func() error {
		return p.flush(ctx)
	})
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

	cp.Mode = "logical"
	if cp.SnapshotedTables == nil {
		cp.SnapshotedTables = make(map[string]bool)
	}
	cp.SnapshotedTables[table] = true
	cp.LSN = p.lastWrittenLSN
	p.src.SetFlushedLSN(cp.LSN)

	if err := p.store.Save(p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	metrics.SnapshotTablesCompleted.WithLabelValues(p.id).Inc()
	log.Printf("[logical:%s] snapshot table %s complete, checkpoint saved", p.id, table)
	return nil
}

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

	cp.Mode = "logical"
	cp.SnapshotComplete = true
	cp.SnapshotedTables = nil
	cp.LSN = p.lastWrittenLSN
	p.src.SetFlushedLSN(cp.LSN)

	if err := p.store.Save(p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	p.setStatus(pipeline.StatusRunning, nil)
	metrics.SnapshotInProgress.WithLabelValues(p.id).Set(0)
	log.Printf("[logical:%s] initial snapshot complete, checkpoint saved", p.id)
	return nil
}

func (p *Pipeline) handleSchemaChange(ctx context.Context, event ChangeEvent) error {
	sc := event.SchemaChange
	if sc == nil {
		return nil
	}

	log.Printf("[logical:%s] schema change for %s: +%d columns, -%d columns, %d type changes",
		p.id, sc.Table, len(sc.AddedColumns), len(sc.DroppedColumns), len(sc.TypeChanges))

	if p.snk.ShouldFlush() {
		if err := p.flush(ctx); err != nil {
			return fmt.Errorf("pre-evolution flush: %w", err)
		}
	}

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

	cp.Mode = "logical"
	cp.LSN = p.lastWrittenLSN
	p.src.SetFlushedLSN(cp.LSN)

	if p.materializer != nil {
		cp.MaterializerSnapshots = p.materializer.LastEventsSnapshots()
	}

	if err := p.store.Save(p.id, cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

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

			metrics.ReplicationLagBytes.WithLabelValues(p.id).Set(float64(lagBytes))
			metrics.WALRetainedBytes.WithLabelValues(p.id).Set(float64(walSizeBytes))
		}
	}
}
