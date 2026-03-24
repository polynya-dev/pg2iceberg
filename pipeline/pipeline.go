package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hasyimibhar/pg2iceberg/config"
	"github.com/hasyimibhar/pg2iceberg/schema"
	"github.com/hasyimibhar/pg2iceberg/sink"
	"github.com/hasyimibhar/pg2iceberg/source"
	"github.com/hasyimibhar/pg2iceberg/state"
	"github.com/jackc/pgx/v5"
)

// Status represents the current state of a Pipeline.
type Status string

const (
	StatusStarting Status = "starting"
	StatusRunning  Status = "running"
	StatusStopping Status = "stopping"
	StatusStopped  Status = "stopped"
	StatusError    Status = "error"
)

// Pipeline encapsulates a single source-to-sink replication pipeline.
type Pipeline struct {
	id  string
	cfg *config.Config

	status Status
	err    error

	src     source.Source
	snk     *sink.Sink
	store   *state.Store
	schemas map[string]*schema.TableSchema

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

// ID returns the pipeline identifier.
func (p *Pipeline) ID() string { return p.id }

// Config returns the pipeline configuration.
func (p *Pipeline) Config() *config.Config { return p.cfg }

// Status returns the current status and last error (if any).
func (p *Pipeline) Status() (Status, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status, p.err
}

// Done returns a channel that is closed when the pipeline exits.
func (p *Pipeline) Done() <-chan struct{} { return p.done }

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
}

// setup performs all synchronous initialization: checkpoint, schema discovery,
// sink registration, compactor start, and source creation.
func (p *Pipeline) setup(ctx context.Context) error {
	// Load checkpoint.
	p.store = state.NewStore(p.cfg.State.Path)
	cp, err := p.store.Load()
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Discover schemas via a regular PG connection.
	pgConn, err := pgx.Connect(ctx, p.cfg.Source.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}

	tables := getTables(p.cfg)
	p.schemas = make(map[string]*schema.TableSchema)
	for _, table := range tables {
		ts, err := schema.DiscoverSchema(ctx, pgConn, table)
		if err != nil {
			pgConn.Close(ctx)
			return fmt.Errorf("discover schema for %s: %w", table, err)
		}
		if p.cfg.Source.Mode == "query" {
			for _, qt := range p.cfg.Source.Query.Tables {
				if qt.Name == table && len(qt.PrimaryKey) > 0 {
					ts.PK = qt.PrimaryKey
				}
			}
		}
		p.schemas[table] = ts
		log.Printf("[pipeline:%s] discovered schema for %s: %d columns, pk=%v", p.id, table, len(ts.Columns), ts.PK)
	}
	pgConn.Close(ctx)

	// Initialize sink.
	p.snk, err = sink.NewSink(p.cfg.Sink)
	if err != nil {
		return fmt.Errorf("create sink: %w", err)
	}

	for _, ts := range p.schemas {
		if err := p.snk.RegisterTable(ctx, ts); err != nil {
			return fmt.Errorf("register table %s: %w", ts.Table, err)
		}
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
		qs := source.NewQuerySource(p.cfg.Source.Postgres, p.cfg.Source.Query)
		if cp.Mode == "query" && cp.Watermark != "" {
			t, err := time.Parse(time.RFC3339Nano, cp.Watermark)
			if err == nil {
				for _, qt := range p.cfg.Source.Query.Tables {
					qs.SetWatermark(qt.Name, t)
				}
				log.Printf("[pipeline:%s] restored query watermark: %v", p.id, t)
			}
		}
		p.src = qs

	case "logical":
		ls := source.NewLogicalSource(p.cfg.Source.Postgres, p.cfg.Source.Logical)
		if cp.Mode == "logical" && cp.LSN > 0 {
			ls.SetStartLSN(cp.LSN)
			log.Printf("[pipeline:%s] restored logical LSN: %d", p.id, cp.LSN)
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

	p.setStatus(StatusRunning, nil)

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

			if err := p.snk.Write(event); err != nil {
				log.Printf("[pipeline:%s] write error: %v", p.id, err)
				continue
			}

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
				if flushErr := p.flush(ctx); flushErr != nil {
					log.Printf("[pipeline:%s] final flush error: %v", p.id, flushErr)
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
	return p.flush(ctx)
}

func (p *Pipeline) flush(ctx context.Context) error {
	if err := p.snk.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	cp, err := p.store.Load()
	if err != nil {
		return fmt.Errorf("load checkpoint for update: %w", err)
	}

	cp.Mode = p.cfg.Source.Mode
	switch p.cfg.Source.Mode {
	case "logical":
		if ls, ok := p.src.(*source.LogicalSource); ok {
			cp.LSN = ls.ConfirmedLSN()
		}
	}

	if err := p.store.Save(cp); err != nil {
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
			qs.AddTable(config.QueryTableConfig{Name: tableName})
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

func getTables(cfg *config.Config) []string {
	switch cfg.Source.Mode {
	case "query":
		tables := make([]string, len(cfg.Source.Query.Tables))
		for i, t := range cfg.Source.Query.Tables {
			tables[i] = t.Name
		}
		return tables
	case "logical":
		return cfg.Source.Logical.Tables
	default:
		return nil
	}
}
