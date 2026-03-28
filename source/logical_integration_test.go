//go:build integration

package source_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/sink"
	"github.com/pg2iceberg/pg2iceberg/source"
	"github.com/pg2iceberg/pg2iceberg/state"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// TestPipeline_FlushedLSN_OnlyAdvancesAfterFlush runs a real pipeline with
// Postgres logical replication and a real sink (backed by in-memory storage
// and catalog stubs). It verifies that:
//  1. Receiving WAL events does NOT advance flushedLSN
//  2. After pipeline flush (triggered by row threshold), flushedLSN advances
//  3. PG's confirmed_flush_lsn only advances after flushedLSN does
func TestPipeline_FlushedLSN_OnlyAdvancesAfterFlush(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	// Create table and publication.
	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_, err = conn.Exec(ctx, `
		CREATE TABLE test_events (
			id SERIAL PRIMARY KEY,
			value INTEGER NOT NULL
		);
		CREATE PUBLICATION test_pub FOR TABLE test_events;
	`)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	conn.Close(ctx)

	sinkCfg := config.SinkConfig{
		FlushInterval: "1h",    // disable time-based flush
		FlushRows:     15,      // flush after 15 data rows
		FlushBytes:    1 << 30, // effectively disabled
		Namespace:     "test_ns",
		Warehouse:     "s3://test-bucket/",
	}

	cfg := &config.Config{
		Tables: []config.TableConfig{
			{Name: "public.test_events"},
		},
		Source: config.SourceConfig{
			Mode:     "logical",
			Postgres: pgCfg,
			Logical: config.LogicalConfig{
				PublicationName: "test_pub",
				SlotName:        "test_slot",
			},
		},
		Sink: sinkCfg,
	}

	mem := newMemStorage()
	cat := newMemCatalog()

	snk := sink.NewSink(sinkCfg, pgCfg, cfg.Tables, "test", mem, cat)

	p := pipeline.NewPipeline("test", cfg, snk, state.NewMemStore())

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	defer func() {
		cancel()
		<-p.Done()
	}()

	// Wait for pipeline to be running (snapshot complete).
	waitForStatus(t, p, pipeline.StatusRunning, 30*time.Second)

	ls, ok := p.Source().(*source.LogicalSource)
	if !ok {
		t.Fatal("pipeline source is not *LogicalSource")
	}

	initialFlushed := ls.FlushedLSN()
	t.Logf("initial: flushedLSN=%d receivedLSN=%d", initialFlushed, ls.ReceivedLSN())

	// Record flushedLSN after snapshot completes.
	flushedAfterSnapshot := ls.FlushedLSN()

	// --- Phase 1: Insert 5 rows (below flush threshold of 15) ---
	conn, err = pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer conn.Close(ctx)

	for i := 0; i < 5; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", i); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Wait for receivedLSN to advance (events received but not flushed).
	waitFor(t, 15*time.Second, func() bool { return ls.ReceivedLSN() > flushedAfterSnapshot })
	t.Logf("phase 1: receivedLSN=%d flushedLSN=%d", ls.ReceivedLSN(), ls.FlushedLSN())

	// === Assertion 1: receivedLSN advanced, flushedLSN did NOT move past snapshot ===
	if ls.FlushedLSN() != flushedAfterSnapshot {
		t.Errorf("flushedLSN should NOT advance on receive: got %d, want %d", ls.FlushedLSN(), flushedAfterSnapshot)
	}

	// Wait for standby update so PG's confirmed_flush_lsn reflects flushedLSN.
	time.Sleep(12 * time.Second)

	// === Assertion 2: PG confirmed_flush_lsn is behind current WAL ===
	var confirmedBefore string
	err = conn.QueryRow(ctx, `
		SELECT confirmed_flush_lsn::text
		FROM pg_replication_slots WHERE slot_name = 'test_slot'
	`).Scan(&confirmedBefore)
	if err != nil {
		t.Fatalf("query confirmed_flush_lsn: %v", err)
	}

	var lagBeforeFlush int64
	err = conn.QueryRow(ctx, `
		SELECT pg_current_wal_lsn() - confirmed_flush_lsn
		FROM pg_replication_slots WHERE slot_name = 'test_slot'
	`).Scan(&lagBeforeFlush)
	if err != nil {
		t.Fatalf("query lag: %v", err)
	}
	t.Logf("PG confirmed_flush_lsn=%s lag=%d bytes", confirmedBefore, lagBeforeFlush)
	if lagBeforeFlush == 0 {
		t.Error("WAL lag should be > 0 before data flush")
	}

	// --- Phase 2: Insert enough rows to cross threshold and trigger flush ---
	for i := 5; i < 20; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", i); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Wait for flushedLSN to advance (flush triggered by row threshold).
	waitFor(t, 30*time.Second, func() bool { return ls.FlushedLSN() > flushedAfterSnapshot })
	t.Logf("phase 2: flushedLSN=%d receivedLSN=%d", ls.FlushedLSN(), ls.ReceivedLSN())

	// === Assertion 3: flushedLSN advanced after flush ===
	if ls.FlushedLSN() <= flushedAfterSnapshot {
		t.Errorf("flushedLSN should have advanced after flush: got %d, initial %d", ls.FlushedLSN(), flushedAfterSnapshot)
	}

	// Wait for standby update to propagate.
	time.Sleep(12 * time.Second)

	// === Assertion 4: PG confirmed_flush_lsn advanced ===
	var confirmedAfter string
	err = conn.QueryRow(ctx, `
		SELECT confirmed_flush_lsn::text
		FROM pg_replication_slots WHERE slot_name = 'test_slot'
	`).Scan(&confirmedAfter)
	if err != nil {
		t.Fatalf("query confirmed_flush_lsn after flush: %v", err)
	}
	t.Logf("PG confirmed_flush_lsn: before=%s after=%s", confirmedBefore, confirmedAfter)

	if confirmedAfter == confirmedBefore {
		t.Errorf("PG confirmed_flush_lsn should have advanced after data flush: before=%s after=%s", confirmedBefore, confirmedAfter)
	}

	// === Assertion 5: Verify data was written to in-memory storage ===
	mem.mu.Lock()
	fileCount := len(mem.files)
	mem.mu.Unlock()
	if fileCount == 0 {
		t.Error("expected files in in-memory storage after flush")
	}
	t.Logf("in-memory storage has %d files", fileCount)
}

func waitForStatus(t *testing.T, p *pipeline.Pipeline, target pipeline.Status, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		s, _ := p.Status()
		if s == target {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for status %s, current %s", target, s)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if condition() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for condition")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func startPostgres(t *testing.T, ctx context.Context) (pgCfg config.PostgresConfig, cleanup func()) {
	t.Helper()

	ctr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=4",
			"-c", "max_wal_senders=4",
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	return config.PostgresConfig{
		Host:     host,
		Port:     port.Int(),
		Database: "testdb",
		User:     "postgres",
		Password: "postgres",
	}, func() { ctr.Terminate(ctx) }
}

// memStorage is an in-memory ObjectStorage implementation for testing.
type memStorage struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemStorage() *memStorage {
	return &memStorage{files: make(map[string][]byte)}
}

func (m *memStorage) Upload(_ context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.files[key] = cp
	return fmt.Sprintf("s3://test-bucket/%s", key), nil
}

func (m *memStorage) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return data, nil
}

// memCatalog is an in-memory Catalog implementation for testing.
type memCatalog struct {
	mu     sync.Mutex
	tables map[string]*sink.TableMetadata // keyed by "ns.table"
	nextID int64
}

func newMemCatalog() *memCatalog {
	return &memCatalog{
		tables: make(map[string]*sink.TableMetadata),
		nextID: 1,
	}
}

func (c *memCatalog) EnsureNamespace(ns string) error { return nil }

func (c *memCatalog) LoadTable(ns, table string) (*sink.TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := c.tables[ns+"."+table]
	return tm, nil
}

func (c *memCatalog) CreateTable(ns, table string, ts *schema.TableSchema, location string, partSpec *sink.PartitionSpec) (*sink.TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &sink.TableMetadata{}
	tm.Metadata.FormatVersion = 2
	tm.Metadata.Location = location
	c.tables[ns+"."+table] = tm
	return tm, nil
}

func (c *memCatalog) CommitSnapshot(ns, table string, currentSnapshotID int64, snapshot sink.SnapshotCommit) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := ns + "." + table
	tm := c.tables[key]
	if tm == nil {
		return fmt.Errorf("table not found: %s", key)
	}
	tm.Metadata.CurrentSnapshotID = snapshot.SnapshotID
	tm.Metadata.LastSequenceNumber = snapshot.SequenceNumber
	snap := struct {
		SnapshotID     int64             `json:"snapshot-id"`
		TimestampMs    int64             `json:"timestamp-ms"`
		ManifestList   string            `json:"manifest-list"`
		Summary        map[string]string `json:"summary"`
		SchemaID       int               `json:"schema-id"`
		SequenceNumber int64             `json:"sequence-number"`
	}{
		SnapshotID:     snapshot.SnapshotID,
		TimestampMs:    snapshot.TimestampMs,
		ManifestList:   snapshot.ManifestListPath,
		Summary:        snapshot.Summary,
		SchemaID:       snapshot.SchemaID,
		SequenceNumber: snapshot.SequenceNumber,
	}
	tm.Metadata.Snapshots = append(tm.Metadata.Snapshots, snap)
	return nil
}

func (c *memCatalog) CommitTransaction(ns string, commits []sink.TableCommit) error {
	for _, tc := range commits {
		if err := c.CommitSnapshot(ns, tc.Table, tc.CurrentSnapshotID, tc.Snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (c *memCatalog) EvolveSchema(ns, table string, currentSchemaID int, newSchema *schema.TableSchema) (int, error) {
	return currentSchemaID + 1, nil
}

// flushCounter wraps a *sink.Sink and counts flushes for test assertions.
type flushCounter struct {
	count atomic.Int64
}

func (f *flushCounter) FlushCount() int64 { return f.count.Load() }
