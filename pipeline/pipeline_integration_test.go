//go:build integration

package pipeline_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	toxiclient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/sink"
	"github.com/pg2iceberg/pg2iceberg/source"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
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

	snk := sink.NewSink(sinkCfg, cfg.Tables, "test", mem, cat, nil)

	p := pipeline.NewPipeline("test", cfg, snk, pipeline.NewMemCheckpointStore())

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

	// Send standby now so PG's confirmed_flush_lsn reflects flushedLSN.
	if err := ls.SendStandbyNow(ctx); err != nil {
		t.Fatalf("send standby: %v", err)
	}

	// Wait for PG to process the standby update.
	var confirmedBefore string
	waitFor(t, 5*time.Second, func() bool {
		_ = conn.QueryRow(ctx, `
			SELECT confirmed_flush_lsn::text
			FROM pg_replication_slots WHERE slot_name = 'test_slot'
		`).Scan(&confirmedBefore)
		return confirmedBefore != ""
	})

	// === Assertion 2: PG confirmed_flush_lsn is behind current WAL ===

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

	// Send standby now so PG's confirmed_flush_lsn reflects flushedLSN.
	if err := ls.SendStandbyNow(ctx); err != nil {
		t.Fatalf("send standby: %v", err)
	}

	// Wait for PG to advance confirmed_flush_lsn past the pre-flush value.
	var confirmedAfter string
	waitFor(t, 5*time.Second, func() bool {
		_ = conn.QueryRow(ctx, `
			SELECT confirmed_flush_lsn::text
			FROM pg_replication_slots WHERE slot_name = 'test_slot'
		`).Scan(&confirmedAfter)
		return confirmedAfter != confirmedBefore
	})

	// === Assertion 4: PG confirmed_flush_lsn advanced ===
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

// TestPipeline_FlushedLSN_DoesNotIncludeUnflushedEvents verifies that when new
// WAL events arrive during a flush, flushedLSN only covers what was actually
// flushed to Iceberg — not events received after the flush started.
//
// The race: Capture goroutine advances receivedLSN while flush() is running.
// pipeline.flush() then reads ReceivedLSN() which now includes unflushed data,
// and incorrectly sets flushedLSN to that value. If the process crashes before
// the next flush, those events are lost because PG thinks they were confirmed.
func TestPipeline_FlushedLSN_DoesNotIncludeUnflushedEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

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
		FlushRows:     15,      // flush after 15 rows
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

	// Use a gate storage backend: blocks Upload until the test signals proceed.
	// This lets us deterministically inject new WAL events while flush is running.
	mem := newGatedStorage()
	cat := newMemCatalog()

	snk := sink.NewSink(sinkCfg, cfg.Tables, "test", mem, cat, nil)
	p := pipeline.NewPipeline("test", cfg, snk, pipeline.NewMemCheckpointStore())

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	defer func() {
		cancel()
		<-p.Done()
	}()

	waitForStatus(t, p, pipeline.StatusRunning, 30*time.Second)

	ls, ok := p.Source().(*source.LogicalSource)
	if !ok {
		t.Fatal("pipeline source is not *LogicalSource")
	}

	flushedAfterSnapshot := ls.FlushedLSN()

	// Arm the gate now that the pipeline is running (snapshot phase uploads pass freely).
	mem.Arm()

	// Insert 16 rows to trigger flush. The threshold is 15, but it's checked
	// after DML events (not Commit), so the 16th Insert is what sees
	// TotalBuffered >= 15 (15 previously committed single-row txns).
	conn, err = pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer conn.Close(ctx)

	for i := 0; i < 16; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", i); err != nil {
			t.Fatalf("insert batch 1: %v", err)
		}
	}

	// Step 1: Wait for the flush to start (Upload is called, storage blocks).
	t.Log("waiting for flush to start (upload blocked)...")
	select {
	case <-mem.uploading:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for upload to start")
	}

	// Step 2: While flush is blocked, insert more rows. The Capture goroutine
	// receives these and advances receivedLSN, but they are NOT in the flush.
	for i := 100; i < 110; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", i); err != nil {
			t.Fatalf("insert batch 2: %v", err)
		}
	}

	// Step 3: Wait for receivedLSN to advance past the snapshot (second batch received).
	receivedBeforeRelease := flushedAfterSnapshot
	waitFor(t, 15*time.Second, func() bool {
		receivedBeforeRelease = ls.ReceivedLSN()
		return receivedBeforeRelease > flushedAfterSnapshot
	})
	t.Logf("receivedLSN advanced to %d while flush is blocked", receivedBeforeRelease)

	// Step 4: Release the upload — flush completes, pipeline sets flushedLSN.
	close(mem.proceed)

	// Wait for flushedLSN to advance (flush completes).
	waitFor(t, 30*time.Second, func() bool { return ls.FlushedLSN() > flushedAfterSnapshot })

	flushedAfterFirstFlush := ls.FlushedLSN()
	receivedAfterFirstFlush := ls.ReceivedLSN()

	t.Logf("after first flush: flushedLSN=%d receivedLSN=%d", flushedAfterFirstFlush, receivedAfterFirstFlush)

	// === Key assertion ===
	// The first flush only covered the first 16 rows. The second batch (10 rows)
	// was received during the flush but NOT flushed to Iceberg. Therefore
	// flushedLSN must be strictly less than receivedLSN.
	//
	// BUG: pipeline.flush() reads ReceivedLSN() after Flush() completes, so
	// flushedLSN includes the second batch's LSN — data that was never flushed.
	if flushedAfterFirstFlush >= receivedAfterFirstFlush {
		t.Errorf("flushedLSN (%d) should be < receivedLSN (%d): "+
			"events received during flush should NOT be included in flushedLSN",
			flushedAfterFirstFlush, receivedAfterFirstFlush)
	}
}

// TestPipeline_FlushRetry_NoDuplicateData verifies that when a flush fails at
// the catalog commit stage and is retried, the retry does not produce duplicate
// data in Iceberg.
//
// The bug: RollingWriter.FlushAll() is non-destructive — serialized chunks stay
// in the `completed` slice until Commit() is called. If Flush() fails after
// FlushAll (e.g., catalog error), the next Flush() replays committedTxns into
// writers AND FlushAll appends new chunks to the stale `completed` list.
// The retry then uploads both old and new chunks, doubling the data.
func TestPipeline_FlushRetry_NoDuplicateData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

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
	cat := newFailOnceCatalog()

	snk := sink.NewSink(sinkCfg, cfg.Tables, "test", mem, cat, nil)
	p := pipeline.NewPipeline("test", cfg, snk, pipeline.NewMemCheckpointStore())

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	defer func() {
		cancel()
		<-p.Done()
	}()

	waitForStatus(t, p, pipeline.StatusRunning, 30*time.Second)

	ls, ok := p.Source().(*source.LogicalSource)
	if !ok {
		t.Fatal("pipeline source is not *LogicalSource")
	}
	flushedAfterSnapshot := ls.FlushedLSN()

	conn, err = pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer conn.Close(ctx)

	// Insert 16 rows to trigger flush. The threshold is 15, checked after DML
	// events. The 16th Insert sees TotalBuffered() = 15 (from 15 committed
	// single-row txns) and triggers the flush. The catalog will reject it.
	for i := 0; i < 16; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", i); err != nil {
			t.Fatalf("insert batch 1: %v", err)
		}
	}

	// Wait for the first catalog commit to fail.
	t.Log("waiting for first flush attempt (catalog failure)...")
	select {
	case <-cat.failedCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for first catalog commit")
	}
	t.Log("first flush failed as expected")

	// Insert 1 more row. Its Insert DML event triggers the threshold check
	// again (TotalBuffered >= 15 because committedTxns wasn't cleared after
	// the failed flush). This causes a retry flush that should succeed.
	if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", 100); err != nil {
		t.Fatalf("insert trigger row: %v", err)
	}

	// Wait for flushedLSN to advance (second flush succeeds).
	waitFor(t, 30*time.Second, func() bool { return ls.FlushedLSN() > flushedAfterSnapshot })
	t.Logf("second flush succeeded: flushedLSN=%d", ls.FlushedLSN())

	// Count data parquet files uploaded to S3.
	// The first (failed) attempt uploads 1 data file (orphaned in S3).
	// The second (successful) attempt should upload 1 data file.
	//
	// BUG: The second attempt's FlushAll returns stale `completed` chunks from
	// the first attempt PLUS new chunks from the replay, producing 2 data files
	// in the retry (3 total). This means the committed Iceberg snapshot
	// references duplicate data.
	mem.mu.Lock()
	var dataFileCount int
	var dataFileKeys []string
	for key := range mem.files {
		if strings.Contains(key, "-data-") && strings.HasSuffix(key, ".parquet") {
			dataFileCount++
			dataFileKeys = append(dataFileKeys, key)
		}
	}
	mem.mu.Unlock()

	t.Logf("data parquet files in S3: %d", dataFileCount)
	for _, k := range dataFileKeys {
		t.Logf("  %s", k)
	}

	// Expected: 2 data files (1 orphan from failed attempt + 1 from successful retry).
	// With the bug: 3 data files (1 orphan + 2 from retry including stale duplicate).
	if dataFileCount > 2 {
		t.Errorf("expected at most 2 data parquet files, got %d: "+
			"flush retry re-uploaded stale completed chunks, producing duplicate data",
			dataFileCount)
	}
}

// gatedStorage wraps memStorage and blocks Upload calls until the test signals
// proceed, allowing deterministic control of flush timing. The gate must be
// armed explicitly via Arm() — uploads before arming pass through freely.
type gatedStorage struct {
	*memStorage
	armed     atomic.Bool
	uploading chan struct{} // closed when first gated Upload is called
	proceed   chan struct{} // test closes this to unblock Upload
	once      sync.Once
}

func newGatedStorage() *gatedStorage {
	return &gatedStorage{
		memStorage: newMemStorage(),
		uploading:  make(chan struct{}),
		proceed:    make(chan struct{}),
	}
}

// Arm activates the gate. Subsequent Upload calls will block until proceed is closed.
func (s *gatedStorage) Arm() { s.armed.Store(true) }

func (s *gatedStorage) Upload(ctx context.Context, key string, data []byte) (string, error) {
	if s.armed.Load() {
		s.once.Do(func() { close(s.uploading) })
		<-s.proceed
	}
	return s.memStorage.Upload(ctx, key, data)
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

func (m *memStorage) DownloadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return data[offset:end], nil
}

func (m *memStorage) StatObject(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	if !ok {
		return 0, fmt.Errorf("not found: %s", key)
	}
	return int64(len(data)), nil
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

// failOnceCatalog wraps memCatalog and fails the first CommitTransaction call.
// This simulates a transient catalog failure to test flush retry behavior.
type failOnceCatalog struct {
	*memCatalog
	commitCalls atomic.Int32
	failedCh    chan struct{} // closed when first commit fails
	once        sync.Once
}

func newFailOnceCatalog() *failOnceCatalog {
	return &failOnceCatalog{
		memCatalog: newMemCatalog(),
		failedCh:   make(chan struct{}),
	}
}

func (c *failOnceCatalog) CommitTransaction(ns string, commits []sink.TableCommit) error {
	n := c.commitCalls.Add(1)
	if n == 1 {
		c.once.Do(func() { close(c.failedCh) })
		return fmt.Errorf("simulated catalog failure")
	}
	return c.memCatalog.CommitTransaction(ns, commits)
}

// --- Retry test doubles and helpers ---

// failNTimesStorage wraps memStorage and fails the first N Upload calls.
type failNTimesStorage struct {
	*memStorage
	failCount   int
	uploadCalls atomic.Int32
	failedCh    chan struct{}
	once        sync.Once
}

func newFailNTimesStorage(n int) *failNTimesStorage {
	return &failNTimesStorage{
		memStorage: newMemStorage(),
		failCount:  n,
		failedCh:   make(chan struct{}),
	}
}

func (s *failNTimesStorage) Upload(ctx context.Context, key string, data []byte) (string, error) {
	call := int(s.uploadCalls.Add(1))
	if call <= s.failCount {
		if call == s.failCount {
			s.once.Do(func() { close(s.failedCh) })
		}
		return "", fmt.Errorf("simulated S3 upload failure (attempt %d)", call)
	}
	return s.memStorage.Upload(ctx, key, data)
}

// failNTimesCatalog wraps memCatalog and fails the first N CommitTransaction calls.
type failNTimesCatalog struct {
	*memCatalog
	failCount   int
	commitCalls atomic.Int32
	failedCh    chan struct{}
	once        sync.Once
}

func newFailNTimesCatalog(n int) *failNTimesCatalog {
	return &failNTimesCatalog{
		memCatalog: newMemCatalog(),
		failCount:  n,
		failedCh:   make(chan struct{}),
	}
}

func (c *failNTimesCatalog) CommitTransaction(ns string, commits []sink.TableCommit) error {
	call := int(c.commitCalls.Add(1))
	if call <= c.failCount {
		if call == c.failCount {
			c.once.Do(func() { close(c.failedCh) })
		}
		return fmt.Errorf("simulated catalog failure (attempt %d)", call)
	}
	return c.memCatalog.CommitTransaction(ns, commits)
}

func newTestPipelineCfg(pgCfg config.PostgresConfig, slotName string) (*config.Config, config.SinkConfig) {
	sinkCfg := config.SinkConfig{
		FlushInterval: "1h",
		FlushRows:     15,
		FlushBytes:    1 << 30,
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
				SlotName:        slotName,
			},
		},
		Sink: sinkCfg,
	}
	return cfg, sinkCfg
}

func setupTestTable(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
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
}

func insertRows(t *testing.T, ctx context.Context, dsn string, start, count int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for insert: %v", err)
	}
	defer conn.Close(ctx)
	for i := start; i < start+count; i++ {
		if _, err := conn.Exec(ctx, "INSERT INTO test_events (value) VALUES ($1)", i); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
}

// --- Retry tests ---

// TestRetry_S3UploadFailure verifies that when S3 uploads fail transiently
// during flush, the retry logic in doFlush recovers and the pipeline continues.
func TestRetry_S3UploadFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()
	setupTestTable(t, ctx, pgCfg.DSN())

	cfg, sinkCfg := newTestPipelineCfg(pgCfg, "test_slot_s3")

	mem := newFailNTimesStorage(2)
	cat := newMemCatalog()
	snk := sink.NewSink(sinkCfg, cfg.Tables, "test", mem, cat, nil)
	p := pipeline.NewPipeline("test", cfg, snk, pipeline.NewMemCheckpointStore())

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	defer func() { cancel(); <-p.Done() }()

	waitForStatus(t, p, pipeline.StatusRunning, 30*time.Second)
	ls := p.Source().(*source.LogicalSource)
	lsnAfterSnapshot := ls.FlushedLSN()

	insertRows(t, ctx, pgCfg.DSN(), 0, 16)

	select {
	case <-mem.failedCh:
		t.Log("S3 upload failures occurred as expected")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for S3 upload failures")
	}

	waitFor(t, 30*time.Second, func() bool { return ls.FlushedLSN() > lsnAfterSnapshot })
	t.Logf("flushedLSN advanced: %d -> %d", lsnAfterSnapshot, ls.FlushedLSN())

	status, pErr := p.Status()
	if status != pipeline.StatusRunning {
		t.Errorf("expected StatusRunning, got %s (err=%v)", status, pErr)
	}
}

// TestRetry_CatalogCommitFailure verifies that when the catalog rejects
// commit requests transiently, the retry logic recovers.
func TestRetry_CatalogCommitFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()
	setupTestTable(t, ctx, pgCfg.DSN())

	cfg, sinkCfg := newTestPipelineCfg(pgCfg, "test_slot_cat")

	mem := newMemStorage()
	cat := newFailNTimesCatalog(3)
	snk := sink.NewSink(sinkCfg, cfg.Tables, "test", mem, cat, nil)
	p := pipeline.NewPipeline("test", cfg, snk, pipeline.NewMemCheckpointStore())

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	defer func() { cancel(); <-p.Done() }()

	waitForStatus(t, p, pipeline.StatusRunning, 30*time.Second)
	ls := p.Source().(*source.LogicalSource)
	lsnAfterSnapshot := ls.FlushedLSN()

	insertRows(t, ctx, pgCfg.DSN(), 0, 16)

	select {
	case <-cat.failedCh:
		t.Log("catalog commit failures occurred as expected")
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for catalog failures")
	}

	waitFor(t, 30*time.Second, func() bool { return ls.FlushedLSN() > lsnAfterSnapshot })
	t.Logf("flushedLSN advanced: %d -> %d", lsnAfterSnapshot, ls.FlushedLSN())

	status, pErr := p.Status()
	if status != pipeline.StatusRunning {
		t.Errorf("expected StatusRunning, got %s (err=%v)", status, pErr)
	}

	totalCalls := int(cat.commitCalls.Load())
	t.Logf("total CommitTransaction calls: %d (expected >= 4)", totalCalls)
	if totalCalls < 4 {
		t.Errorf("expected at least 4 commit calls (3 failed + 1 success), got %d", totalCalls)
	}
}

// TestRetry_Toxiproxy_NetworkBlip verifies that the pipeline survives
// network degradation between itself and Postgres.
//
// Setup: Postgres <-> Toxiproxy <-> Pipeline
//
// A latency toxic is injected on the proxy, adding 2s of delay to all packets.
// Despite the degradation, the pipeline continues streaming and successfully
// flushes data. After the toxic is removed, normal operation resumes.
func TestRetry_Toxiproxy_NetworkBlip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	defer net.Remove(ctx)

	pgCtr, err := tcpostgres.Run(ctx,
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
		network.WithNetwork([]string{"postgres"}, net),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer pgCtr.Terminate(ctx)

	toxiCtr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/shopify/toxiproxy:2.12.0",
			ExposedPorts: []string{"8474/tcp", "15432/tcp"},
			Networks:     []string{net.Name},
			WaitingFor:   wait.ForHTTP("/version").WithPort("8474"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start toxiproxy: %v", err)
	}
	defer toxiCtr.Terminate(ctx)

	toxiHost, _ := toxiCtr.Host(ctx)
	toxiAPIPort, _ := toxiCtr.MappedPort(ctx, "8474")
	toxiClient := toxiclient.NewClient(fmt.Sprintf("%s:%s", toxiHost, toxiAPIPort.Port()))

	proxy, err := toxiClient.CreateProxy("pg", "0.0.0.0:15432", "postgres:5432")
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}

	toxiPGPort, _ := toxiCtr.MappedPort(ctx, "15432")
	pgCfg := config.PostgresConfig{
		Host:     toxiHost,
		Port:     toxiPGPort.Int(),
		Database: "testdb",
		User:     "postgres",
		Password: "postgres",
	}

	setupTestTable(t, ctx, pgCfg.DSN())

	cfg, sinkCfg := newTestPipelineCfg(pgCfg, "test_slot_toxi")
	sinkCfg.FlushRows = 20
	cfg.Sink = sinkCfg

	mem := newMemStorage()
	cat := newMemCatalog()
	snk := sink.NewSink(sinkCfg, cfg.Tables, "test", mem, cat, nil)
	p := pipeline.NewPipeline("test", cfg, snk, pipeline.NewMemCheckpointStore())

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}
	defer func() { cancel(); <-p.Done() }()

	waitForStatus(t, p, pipeline.StatusRunning, 30*time.Second)
	ls := p.Source().(*source.LogicalSource)

	// Phase 1: Confirm streaming works.
	pgDirectHost, _ := pgCtr.Host(ctx)
	pgDirectPort, _ := pgCtr.MappedPort(ctx, "5432")
	directDSN := fmt.Sprintf("postgres://postgres:postgres@%s:%s/testdb", pgDirectHost, pgDirectPort.Port())
	insertRows(t, ctx, directDSN, 0, 5)
	waitFor(t, 10*time.Second, func() bool { return ls.ReceivedLSN() > 0 })
	t.Logf("phase 1: streaming works, receivedLSN=%d", ls.ReceivedLSN())

	// Phase 2: Inject 2s latency on downstream (PG -> pipeline).
	t.Log("injecting latency toxic (2s)...")
	_, err = proxy.AddToxic("lag", "latency", "downstream", 1, toxiclient.Attributes{
		"latency": 2000,
	})
	if err != nil {
		t.Fatalf("add toxic: %v", err)
	}
	time.Sleep(1 * time.Second)

	// Phase 3: Insert rows under latency to trigger flush.
	lsnBefore := ls.FlushedLSN()
	t.Log("inserting rows under latency...")
	insertRows(t, ctx, directDSN, 5, 16)

	// Phase 4: Verify flush succeeds despite latency.
	flushStart := time.Now()
	waitFor(t, 30*time.Second, func() bool { return ls.FlushedLSN() > lsnBefore })
	flushDuration := time.Since(flushStart)
	t.Logf("phase 4: flushed under latency in %s, flushedLSN=%d (was %d)",
		flushDuration.Truncate(time.Millisecond), ls.FlushedLSN(), lsnBefore)

	if flushDuration < 1*time.Second {
		t.Errorf("flush completed too fast (%s) — toxic may not have been active", flushDuration)
	}

	// Phase 5: Remove toxic, verify pipeline is healthy.
	t.Log("removing toxic...")
	if err := proxy.RemoveToxic("lag"); err != nil {
		t.Fatalf("remove toxic: %v", err)
	}

	status, pErr := p.Status()
	if status != pipeline.StatusRunning {
		t.Errorf("expected StatusRunning, got %s (err=%v)", status, pErr)
	}

	mem.mu.Lock()
	fileCount := len(mem.files)
	mem.mu.Unlock()
	if fileCount == 0 {
		t.Error("expected files in storage after flush, got 0")
	}
	t.Logf("storage has %d files after recovery", fileCount)
}
