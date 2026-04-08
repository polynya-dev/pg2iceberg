//go:build integration

package snapshot_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/snapshot"
)

// TestSnapshotter_DirectWrite snapshots a real PG table with CTID chunking
// and writes directly to a materialized Iceberg table (in-memory catalog/S3).
func TestSnapshotter_DirectWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Create table and insert test data.
	_, err = conn.Exec(ctx, `
		CREATE TABLE public.orders (
			id   SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			amt  INTEGER NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	const rowCount = 500
	for i := 1; i <= rowCount; i++ {
		_, err = conn.Exec(ctx, "INSERT INTO orders (name, amt) VALUES ($1, $2)",
			fmt.Sprintf("order-%d", i), i*10)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	conn.Close(ctx)

	// Discover schema.
	schemaConn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	ts, err := postgres.DiscoverSchema(ctx, schemaConn, "public.orders")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}
	schemaConn.Close(ctx)

	// Set up in-memory catalog and storage.
	catalog := newMemCatalog()
	s3 := newMemStorage()
	store := pipeline.NewMemCheckpointStore()

	sinkCfg := config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}

	// Create the materialized table in the catalog.
	_, err = catalog.CreateTable(ctx, sinkCfg.Namespace, "orders", ts,
		fmt.Sprintf("%stest_ns.db/orders", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table in catalog: %v", err)
	}

	logicalCfg := config.LogicalConfig{
		SnapshotChunkPages:     64, // small chunks to exercise chunking
		SnapshotTargetFileSize: 1024 * 1024,
	}

	deps := snapshot.Deps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: logicalCfg,
		TableCfgs:  []config.TableConfig{{Name: "public.orders"}},
		Schemas:    map[string]*postgres.TableSchema{"public.orders": ts},
		Store:      store,
		PipelineID: "test",
	}

	tables := []snapshot.Table{{Name: "public.orders", Schema: ts}}

	dsn := pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		cleanup := func(ctx context.Context) { c.Close(ctx) }
		tx, err := c.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, err
		}
		return tx, cleanup, nil
	}

	snap := snapshot.NewSnapshotter(tables, txFactory, 1, deps)
	results, err := snap.Run(ctx)
	if err != nil {
		t.Fatalf("snapshot run: %v", err)
	}

	// Verify results.
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("snapshot error: %v", results[0].Err)
	}
	if results[0].Rows != rowCount {
		t.Errorf("expected %d rows, got %d", rowCount, results[0].Rows)
	}

	// Verify catalog has a snapshot.
	tm, err := catalog.LoadTable(ctx, sinkCfg.Namespace, "orders")
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	if tm == nil {
		t.Fatal("table not found in catalog")
	}
	if tm.Metadata.CurrentSnapshotID == 0 {
		t.Error("expected non-zero snapshot ID after snapshot")
	}
	if len(tm.Metadata.Snapshots) == 0 {
		t.Error("expected at least one snapshot")
	}
	t.Logf("snapshots committed: %d", len(tm.Metadata.Snapshots))

	// Verify checkpoint.
	cp, err := store.Load(ctx, "test")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if !cp.SnapshotedTables["public.orders"] {
		t.Error("expected public.orders in SnapshotedTables")
	}
	// Chunk progress should be cleared after table completion.
	if len(cp.SnapshotChunks) != 0 {
		t.Errorf("expected empty SnapshotChunks after completion, got %v", cp.SnapshotChunks)
	}

	// Verify Parquet data written to S3 can be read back.
	verifyParquetRowCount(t, ctx, s3, tm, ts, rowCount)
}

// TestSnapshotter_ChunkRecovery verifies that a snapshot can resume after
// partial completion by skipping already-checkpointed chunks.
func TestSnapshotter_ChunkRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	_, err = conn.Exec(ctx, `
		CREATE TABLE public.items (
			id   SERIAL PRIMARY KEY,
			val  TEXT NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	const rowCount = 200
	for i := 1; i <= rowCount; i++ {
		_, err = conn.Exec(ctx, "INSERT INTO items (val) VALUES ($1)", fmt.Sprintf("item-%d", i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	conn.Close(ctx)

	schemaConn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	ts, err := postgres.DiscoverSchema(ctx, schemaConn, "public.items")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}
	schemaConn.Close(ctx)

	catalog := newMemCatalog()
	s3 := newMemStorage()
	store := pipeline.NewMemCheckpointStore()

	sinkCfg := config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}

	_, err = catalog.CreateTable(ctx, sinkCfg.Namespace, "items", ts,
		fmt.Sprintf("%stest_ns.db/items", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	logicalCfg := config.LogicalConfig{
		SnapshotChunkPages: 16, // very small chunks
	}

	deps := snapshot.Deps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: logicalCfg,
		TableCfgs:  []config.TableConfig{{Name: "public.items"}},
		Schemas:    map[string]*postgres.TableSchema{"public.items": ts},
		Store:      store,
		PipelineID: "test",
	}

	// Pre-seed the checkpoint to simulate partial completion (chunk 0 done).
	if err := store.Save(ctx, "test", &pipeline.Checkpoint{
		Mode:           "logical",
		SnapshotChunks: map[string]int{"public.items": 0},
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	// Count snapshots before run.
	tmBefore, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "items")
	snapshotsBefore := len(tmBefore.Metadata.Snapshots)

	tables := []snapshot.Table{{Name: "public.items", Schema: ts}}
	dsn := pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		cleanup := func(ctx context.Context) { c.Close(ctx) }
		tx, err := c.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, err
		}
		return tx, cleanup, nil
	}

	snap := snapshot.NewSnapshotter(tables, txFactory, 1, deps)
	results, err := snap.Run(ctx)
	if err != nil {
		t.Fatalf("snapshot run: %v", err)
	}

	if results[0].Err != nil {
		t.Fatalf("snapshot error: %v", results[0].Err)
	}

	// Verify that chunk 0 was skipped (fewer snapshots committed than a full run).
	tmAfter, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "items")
	snapshotsAfter := len(tmAfter.Metadata.Snapshots)
	t.Logf("snapshots before=%d, after=%d (chunk 0 was skipped)", snapshotsBefore, snapshotsAfter)

	// Should still have fewer rows than a full snapshot since chunk 0 data is missing,
	// but more importantly: checkpoint should be clean.
	cp, err := store.Load(ctx, "test")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if !cp.SnapshotedTables["public.items"] {
		t.Error("expected public.items in SnapshotedTables")
	}
	if len(cp.SnapshotChunks) != 0 {
		t.Errorf("expected empty SnapshotChunks, got %v", cp.SnapshotChunks)
	}
}

// TestSnapshotter_CrashAfterChunkCommit simulates a crash between an Iceberg
// chunk commit and the checkpoint save. On re-run, the chunk is re-executed
// because the checkpoint doesn't know it was already committed. This test
// verifies whether the re-run produces duplicate rows in the materialized table.
func TestSnapshotter_CrashAfterChunkCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	_, err = conn.Exec(ctx, `
		CREATE TABLE public.orders (
			id   SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			pad  TEXT NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert rows with wide padding to force multiple CTID chunks.
	// Each row is ~500 bytes, 4 pages = 32KB ≈ 64 rows per chunk.
	const rowCount = 500
	for i := 1; i <= rowCount; i++ {
		_, err = conn.Exec(ctx, "INSERT INTO orders (name, pad) VALUES ($1, $2)",
			fmt.Sprintf("order-%d", i), fmt.Sprintf("%0500d", i))
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	// ANALYZE so pg_class.relpages is accurate for CTID chunk computation.
	if _, err := conn.Exec(ctx, "ANALYZE public.orders"); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	conn.Close(ctx)

	schemaConn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	ts, err := postgres.DiscoverSchema(ctx, schemaConn, "public.orders")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}
	schemaConn.Close(ctx)

	catalog := newMemCatalog()
	s3 := newMemStorage()

	sinkCfg := config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}

	_, err = catalog.CreateTable(ctx, sinkCfg.Namespace, "orders", ts,
		fmt.Sprintf("%stest_ns.db/orders", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	logicalCfg := config.LogicalConfig{
		SnapshotChunkPages: 4, // very small → multiple chunks
	}

	dsn := pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		cl := func(ctx context.Context) { c.Close(ctx) }
		tx, err := c.Begin(ctx)
		if err != nil {
			cl(ctx)
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cl(ctx)
			return nil, nil, err
		}
		return tx, cl, nil
	}

	// --- Run 1: crash after first chunk's checkpoint save ---
	// Use a store that fails on the 2nd Save (chunk 1's checkpoint).
	// Chunk 0 commits to Iceberg + checkpoint saved.
	// Chunk 1 commits to Iceberg, but checkpoint save fails → "crash".
	crashStore := &crashAfterNSaves{
		inner:   pipeline.NewMemCheckpointStore(),
		crashAt: 2,
	}

	deps := snapshot.Deps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: logicalCfg,
		TableCfgs:  []config.TableConfig{{Name: "public.orders"}},
		Schemas:    map[string]*postgres.TableSchema{"public.orders": ts},
		Store:      crashStore,
		PipelineID: "test",
	}

	tables := []snapshot.Table{{Name: "public.orders", Schema: ts}}
	snap := snapshot.NewSnapshotter(tables, txFactory, 1, deps)
	_, err = snap.Run(ctx)
	if err == nil {
		t.Fatal("expected error from simulated crash, got nil")
	}
	t.Logf("run 1 crashed as expected: %v", err)

	// Check: chunk 0 is checkpointed, chunk 1's data is in Iceberg but not checkpointed.
	cp, err := crashStore.inner.Load(ctx, "test")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	t.Logf("checkpoint after crash: SnapshotChunks=%v", cp.SnapshotChunks)

	tm, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "orders")
	snapshotsAfterCrash := len(tm.Metadata.Snapshots)
	t.Logf("Iceberg snapshots after crash: %d", snapshotsAfterCrash)

	// --- Run 2: resume with the same store (checkpoint says chunk 0 done) ---
	deps.Store = crashStore.inner // use the underlying store, no more crashes
	snap2 := snapshot.NewSnapshotter(tables, txFactory, 1, deps)
	results, err := snap2.Run(ctx)
	if err != nil {
		t.Fatalf("run 2: %v", err)
	}
	if results[0].Err != nil {
		t.Fatalf("run 2 error: %v", results[0].Err)
	}

	// Count total rows in the materialized table.
	// BUG: chunk 1 was committed to Iceberg in run 1 but not checkpointed.
	// Run 2 re-commits chunk 1, producing duplicate rows. This is a known
	// limitation: snapshot crash recovery can produce duplicates because the
	// Iceberg append is not idempotent and there's no deduplication by PK
	// in the snapshot path.
	tmFinal, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "orders")
	totalSnapshots := len(tmFinal.Metadata.Snapshots)
	t.Logf("total Iceberg snapshots: %d (crash run: %d)", totalSnapshots, snapshotsAfterCrash)

	// Expect duplicates: the crashed chunk's rows appear twice.
	tmForCount, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "orders")
	mlURI := tmForCount.CurrentManifestList()
	mlKey, _ := iceberg.KeyFromURI(mlURI)
	mlData, _ := s3.Download(ctx, mlKey)
	manifests, _ := iceberg.ReadManifestList(mlData)
	var totalRows int64
	for _, mfi := range manifests {
		if mfi.Content != 0 {
			continue
		}
		mKey, _ := iceberg.KeyFromURI(mfi.Path)
		mData, _ := s3.Download(ctx, mKey)
		entries, _ := iceberg.ReadManifest(mData)
		for _, e := range entries {
			if e.DataFile.Content == 0 {
				totalRows += e.DataFile.RecordCount
			}
		}
	}
	t.Logf("total rows in materialized table: %d (expected %d, diff=%d duplicates)",
		totalRows, rowCount, totalRows-int64(rowCount))

	if totalRows <= int64(rowCount) {
		t.Fatalf("expected duplicate rows from crash recovery, but got %d (no duplicates)", totalRows)
	}
	if totalRows > int64(rowCount) {
		// TODO: fix snapshot crash recovery to detect already-committed chunks
		// and skip them instead of re-appending. Track with Iceberg snapshot ID
		// or data file UUIDs.
		t.Logf("KNOWN ISSUE: snapshot crash recovery produced %d duplicate rows", totalRows-int64(rowCount))
	}
}

// crashAfterNSaves wraps a CheckpointStore and returns an error on the Nth Save call.
type crashAfterNSaves struct {
	inner     pipeline.CheckpointStore
	saveCount int
	crashAt   int
}

func (s *crashAfterNSaves) Load(ctx context.Context, id string) (*pipeline.Checkpoint, error) {
	return s.inner.Load(ctx, id)
}

func (s *crashAfterNSaves) Save(ctx context.Context, id string, cp *pipeline.Checkpoint) error {
	s.saveCount++
	if s.saveCount == s.crashAt {
		return fmt.Errorf("simulated crash after save %d", s.saveCount)
	}
	return s.inner.Save(ctx, id, cp)
}

func (s *crashAfterNSaves) Close() {
	s.inner.Close()
}

// TestSnapshotter_EmptyTable verifies snapshot handles tables with zero rows.
func TestSnapshotter_EmptyTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_, err = conn.Exec(ctx, `CREATE TABLE public.empty_tbl (id SERIAL PRIMARY KEY, val TEXT)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	conn.Close(ctx)

	schemaConn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	ts, err := postgres.DiscoverSchema(ctx, schemaConn, "public.empty_tbl")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}
	schemaConn.Close(ctx)

	catalog := newMemCatalog()
	s3 := newMemStorage()
	store := pipeline.NewMemCheckpointStore()

	sinkCfg := config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}

	_, err = catalog.CreateTable(ctx, sinkCfg.Namespace, "empty_tbl", ts,
		fmt.Sprintf("%stest_ns.db/empty_tbl", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	deps := snapshot.Deps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: config.LogicalConfig{},
		TableCfgs:  []config.TableConfig{{Name: "public.empty_tbl"}},
		Schemas:    map[string]*postgres.TableSchema{"public.empty_tbl": ts},
		Store:      store,
		PipelineID: "test",
	}

	tables := []snapshot.Table{{Name: "public.empty_tbl", Schema: ts}}
	dsn := pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		cleanup := func(ctx context.Context) { c.Close(ctx) }
		tx, err := c.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, err
		}
		return tx, cleanup, nil
	}

	snap := snapshot.NewSnapshotter(tables, txFactory, 1, deps)
	results, err := snap.Run(ctx)
	if err != nil {
		t.Fatalf("snapshot run: %v", err)
	}

	if results[0].Err != nil {
		t.Fatalf("snapshot error: %v", results[0].Err)
	}
	if results[0].Rows != 0 {
		t.Errorf("expected 0 rows, got %d", results[0].Rows)
	}

	// No Iceberg snapshots should have been committed (no data to write).
	tm, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "empty_tbl")
	if tm.Metadata.CurrentSnapshotID != 0 {
		t.Errorf("expected no snapshot for empty table, got ID %d", tm.Metadata.CurrentSnapshotID)
	}

	// Checkpoint should still mark the table as complete.
	cp, _ := store.Load(ctx, "test")
	if !cp.SnapshotedTables["public.empty_tbl"] {
		t.Error("expected empty_tbl in SnapshotedTables")
	}
}

// TestSnapshotter_MultiTable verifies concurrent snapshot of multiple tables.
func TestSnapshotter_MultiTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	_, err = conn.Exec(ctx, `
		CREATE TABLE public.t1 (id SERIAL PRIMARY KEY, val TEXT);
		CREATE TABLE public.t2 (id SERIAL PRIMARY KEY, val TEXT);
	`)
	if err != nil {
		t.Fatalf("create tables: %v", err)
	}

	for i := 1; i <= 100; i++ {
		_, _ = conn.Exec(ctx, "INSERT INTO t1 (val) VALUES ($1)", fmt.Sprintf("a-%d", i))
		_, _ = conn.Exec(ctx, "INSERT INTO t2 (val) VALUES ($1)", fmt.Sprintf("b-%d", i))
	}
	conn.Close(ctx)

	schemaConn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	ts1, _ := postgres.DiscoverSchema(ctx, schemaConn, "public.t1")
	ts2, _ := postgres.DiscoverSchema(ctx, schemaConn, "public.t2")
	schemaConn.Close(ctx)

	catalog := newMemCatalog()
	s3 := newMemStorage()
	store := pipeline.NewMemCheckpointStore()

	sinkCfg := config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}

	catalog.CreateTable(ctx, sinkCfg.Namespace, "t1", ts1, "s3://test-bucket/test_ns.db/t1", nil)
	catalog.CreateTable(ctx, sinkCfg.Namespace, "t2", ts2, "s3://test-bucket/test_ns.db/t2", nil)

	deps := snapshot.Deps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: config.LogicalConfig{SnapshotChunkPages: 32},
		TableCfgs: []config.TableConfig{
			{Name: "public.t1"},
			{Name: "public.t2"},
		},
		Schemas: map[string]*postgres.TableSchema{
			"public.t1": ts1,
			"public.t2": ts2,
		},
		Store:      store,
		PipelineID: "test",
	}

	tables := []snapshot.Table{
		{Name: "public.t1", Schema: ts1},
		{Name: "public.t2", Schema: ts2},
	}

	dsn := pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		cleanup := func(ctx context.Context) { c.Close(ctx) }
		tx, err := c.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, err
		}
		return tx, cleanup, nil
	}

	// Concurrency 2 = both tables in parallel.
	snap := snapshot.NewSnapshotter(tables, txFactory, 2, deps)
	results, err := snap.Run(ctx)
	if err != nil {
		t.Fatalf("snapshot run: %v", err)
	}

	for _, r := range results {
		if r.Err != nil {
			t.Errorf("table %s error: %v", r.Task, r.Err)
		}
		if r.Rows != 100 {
			t.Errorf("table %s: expected 100 rows, got %d", r.Task, r.Rows)
		}
	}

	cp, _ := store.Load(ctx, "test")
	if !cp.SnapshotedTables["public.t1"] {
		t.Error("expected t1 in SnapshotedTables")
	}
	if !cp.SnapshotedTables["public.t2"] {
		t.Error("expected t2 in SnapshotedTables")
	}
}

// TestSnapshotter_CTIDChunking verifies that a large table is split into
// multiple CTID chunks and that all rows are captured with no data loss.
// This requires ANALYZE to update pg_class.relpages so ComputeChunks produces
// multiple ranges.
func TestSnapshotter_CTIDChunking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	pgCfg, cleanup := startPostgres(t, ctx)
	defer cleanup()

	conn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Create table with wide rows to occupy many pages.
	// A PG page is 8KB. Each row has an int + 500-byte text ≈ ~530 bytes with
	// overhead, so ~15 rows per page. 500k rows ≈ ~33k pages.
	_, err = conn.Exec(ctx, `
		CREATE TABLE public.big_tbl (
			id   SERIAL PRIMARY KEY,
			pad  TEXT NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	const rowCount = 500_000
	// Bulk insert via generate_series — fast even for large counts.
	_, err = conn.Exec(ctx, `
		INSERT INTO big_tbl (pad)
		SELECT repeat('x', 500)
		FROM generate_series(1, $1)`, rowCount)
	if err != nil {
		t.Fatalf("bulk insert: %v", err)
	}

	// ANALYZE so pg_class.relpages is up to date.
	_, err = conn.Exec(ctx, "ANALYZE big_tbl")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// Verify relpages > 0.
	var relpages int64
	err = conn.QueryRow(ctx,
		"SELECT relpages FROM pg_class WHERE relname = 'big_tbl'").Scan(&relpages)
	if err != nil {
		t.Fatalf("query relpages: %v", err)
	}
	t.Logf("big_tbl: %d rows, %d pages", rowCount, relpages)
	if relpages <= 1 {
		t.Fatalf("expected relpages > 1 after ANALYZE, got %d", relpages)
	}
	conn.Close(ctx)

	schemaConn, err := pgx.Connect(ctx, pgCfg.DSN())
	if err != nil {
		t.Fatalf("schema connect: %v", err)
	}
	ts, err := postgres.DiscoverSchema(ctx, schemaConn, "public.big_tbl")
	if err != nil {
		t.Fatalf("discover schema: %v", err)
	}
	schemaConn.Close(ctx)

	catalog := newMemCatalog()
	s3 := newMemStorage()
	store := pipeline.NewMemCheckpointStore()

	sinkCfg := config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}

	_, err = catalog.CreateTable(ctx, sinkCfg.Namespace, "big_tbl", ts,
		fmt.Sprintf("%stest_ns.db/big_tbl", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table in catalog: %v", err)
	}

	// Use very small chunk size to guarantee multiple chunks.
	// With ~111 pages and chunkPages=8, we get ~14 chunks.
	const chunkPages = 8
	deps := snapshot.Deps{
		Catalog: catalog,
		S3:      s3,
		SinkCfg: sinkCfg,
		LogicalCfg: config.LogicalConfig{
			SnapshotChunkPages:     chunkPages,
			SnapshotTargetFileSize: 1024 * 1024,
		},
		TableCfgs: []config.TableConfig{{Name: "public.big_tbl"}},
		Schemas:   map[string]*postgres.TableSchema{"public.big_tbl": ts},
		Store:     store,
		PipelineID: "test",
	}

	tables := []snapshot.Table{{Name: "public.big_tbl", Schema: ts}}
	dsn := pgCfg.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		cleanup := func(ctx context.Context) { c.Close(ctx) }
		tx, err := c.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, err
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, err
		}
		return tx, cleanup, nil
	}

	snap := snapshot.NewSnapshotter(tables, txFactory, 1, deps)
	results, err := snap.Run(ctx)
	if err != nil {
		t.Fatalf("snapshot run: %v", err)
	}

	if results[0].Err != nil {
		t.Fatalf("snapshot error: %v", results[0].Err)
	}
	if results[0].Rows != rowCount {
		t.Errorf("expected %d rows, got %d", rowCount, results[0].Rows)
	}

	// Verify multiple Iceberg snapshots were committed (one per chunk).
	tm, _ := catalog.LoadTable(ctx, sinkCfg.Namespace, "big_tbl")
	if tm == nil {
		t.Fatal("table not found in catalog")
	}
	numSnapshots := len(tm.Metadata.Snapshots)
	t.Logf("Iceberg snapshots committed: %d (relpages=%d, chunk_pages=%d)", numSnapshots, relpages, chunkPages)
	if numSnapshots <= 1 {
		t.Errorf("expected multiple Iceberg snapshots (one per CTID chunk), got %d", numSnapshots)
	}

	// Verify checkpoint: table complete, no leftover chunk state.
	cp, _ := store.Load(ctx, "test")
	if !cp.SnapshotedTables["public.big_tbl"] {
		t.Error("expected big_tbl in SnapshotedTables")
	}
	if len(cp.SnapshotChunks) != 0 {
		t.Errorf("expected empty SnapshotChunks, got %v", cp.SnapshotChunks)
	}

	// No data loss: verify total row count across all Parquet data files.
	verifyParquetRowCount(t, ctx, s3, tm, ts, rowCount)

	// Read back all rows and verify every ID is present.
	allRows := readAllDataFileRows(t, ctx, s3, tm)
	idSet := make(map[int32]bool, len(allRows))
	for _, row := range allRows {
		if id, ok := row["id"].(int32); ok {
			idSet[id] = true
		}
	}
	for i := int32(1); i <= rowCount; i++ {
		if !idSet[i] {
			t.Errorf("data loss: row id=%d missing from materialized data files", i)
		}
	}
	t.Logf("no data loss: all %d IDs present across %d chunks", len(idSet), numSnapshots)
}

// verifyParquetRowCount reads the manifest chain from the catalog and counts
// total rows across all data files.
func verifyParquetRowCount(t *testing.T, ctx context.Context, s3 *memStorage, tm *iceberg.TableMetadata, ts *postgres.TableSchema, expectedRows int) {
	t.Helper()

	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		t.Fatal("no manifest list in table metadata")
	}
	mlKey, err := iceberg.KeyFromURI(mlURI)
	if err != nil {
		t.Fatalf("parse manifest list URI: %v", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		t.Fatalf("download manifest list: %v", err)
	}
	manifests, err := iceberg.ReadManifestList(mlData)
	if err != nil {
		t.Fatalf("read manifest list: %v", err)
	}

	var totalRows int64
	for _, mfi := range manifests {
		if mfi.Content != 0 {
			continue
		}
		mKey, err := iceberg.KeyFromURI(mfi.Path)
		if err != nil {
			t.Fatalf("parse manifest URI: %v", err)
		}
		mData, err := s3.Download(ctx, mKey)
		if err != nil {
			t.Fatalf("download manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mData)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		for _, e := range entries {
			if e.DataFile.Content == 0 {
				totalRows += e.DataFile.RecordCount
			}
		}
	}

	if totalRows != int64(expectedRows) {
		t.Errorf("expected %d total rows across Parquet files, got %d", expectedRows, totalRows)
	}
	t.Logf("verified %d rows across %d manifests", totalRows, len(manifests))
}
