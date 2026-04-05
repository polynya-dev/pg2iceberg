package query

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/schema"
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
	schemas map[string]*schema.TableSchema

	catalog iceberg.Catalog
	s3      iceberg.ObjectStorage
	store   pipeline.CheckpointStore

	status        pipeline.Status
	err           error
	startedAt     time.Time
	lastFlushAt   time.Time
	rowsProcessed int64

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

	var httpClient *http.Client
	if cfg.Sink.CatalogAuth == "sigv4" {
		transport, err := iceberg.NewSigV4Transport(cfg.Sink.S3Region)
		if err != nil {
			cpStore.Close()
			return nil, fmt.Errorf("create sigv4 transport: %w", err)
		}
		httpClient = &http.Client{Transport: transport}
	}
	catalog := iceberg.NewCatalogClient(cfg.Sink.CatalogURI, httpClient)

	s3Client, err := iceberg.NewS3Client(cfg.Sink.S3Endpoint, cfg.Sink.S3AccessKey, cfg.Sink.S3SecretKey, cfg.Sink.S3Region, cfg.Sink.Warehouse)
	if err != nil {
		cpStore.Close()
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &Pipeline{
		id:      id,
		cfg:     cfg,
		catalog: catalog,
		s3:      s3Client,
		store:   cpStore,
		status:  pipeline.StatusStopped,
		done:    make(chan struct{}),
	}, nil
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

// Metrics returns a snapshot of pipeline metrics.
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
	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Create poller and discover schemas.
	p.poller = NewPoller(p.cfg.Source.Postgres, p.cfg.Tables)
	if err := p.poller.Connect(ctx); err != nil {
		return fmt.Errorf("poller connect: %w", err)
	}
	p.schemas = p.poller.Schemas()

	// Restore watermarks from checkpoint.
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
		}
	}

	// Ensure namespace exists.
	if err := p.catalog.EnsureNamespace(p.cfg.Sink.Namespace); err != nil {
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
		matTm, err := p.catalog.LoadTable(p.cfg.Sink.Namespace, icebergName)
		if err != nil {
			return fmt.Errorf("load table %s: %w", icebergName, err)
		}
		var schemaID int
		if matTm == nil {
			location := fmt.Sprintf("%s%s.db/%s", p.cfg.Sink.Warehouse, p.cfg.Sink.Namespace, icebergName)
			matTm, err = p.catalog.CreateTable(p.cfg.Sink.Namespace, icebergName, ts, location, partSpec)
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
	p.status = pipeline.StatusRunning
	metrics.PipelineStatus.WithLabelValues(p.id).Set(metrics.StatusToFloat["running"])
	p.mu.Unlock()

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
	results, err := p.poller.PollWithRetry(ctx)
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
		metrics.RowsProcessedTotal.WithLabelValues(p.id, r.Table, "INSERT").Add(float64(len(r.Rows)))
	}
	return nil
}

func (p *Pipeline) flush(ctx context.Context) error {
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
		tw := p.writers[pgTable]
		ts := p.schemas[pgTable]

		prepared, err := tw.Prepare(ctx, rows, ts.PK)
		if err != nil {
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
	if err := p.catalog.CommitTransaction(p.cfg.Sink.Namespace, commits); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Post-commit: update manifest caches.
	for _, tp := range preps {
		p.writers[tp.pgTable].ApplyPostCommit(tp.prepared)
		log.Printf("[query:%s] flushed %s: %d data files, %d delete files",
			p.id, tp.pgTable, tp.prepared.DataCount, tp.prepared.DeleteCount)
	}

	// Checkpoint watermarks.
	p.mu.Lock()
	p.lastFlushAt = time.Now()
	p.mu.Unlock()

	cp, err := p.store.Load(p.id)
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	cp.Mode = "query"
	cp.QueryWatermarks = make(map[string]string, len(p.poller.Watermarks()))
	for table, wm := range p.poller.Watermarks() {
		cp.QueryWatermarks[table] = wm.Format(time.RFC3339Nano)
	}
	if err := p.store.Save(p.id, cp); err != nil {
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

// pgTableToIceberg converts "public.orders" to "orders" for the Iceberg table name.
func pgTableToIceberg(pgTable string) string {
	parts := make([]string, 0, 2)
	for _, p := range splitDot(pgTable) {
		parts = append(parts, p)
	}
	if len(parts) > 1 {
		return parts[len(parts)-1]
	}
	return pgTable
}

func splitDot(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}
