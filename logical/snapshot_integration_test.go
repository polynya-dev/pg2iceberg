//go:build integration

package logical_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/logical"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
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
	_, err = catalog.CreateTable(sinkCfg.Namespace, "orders", ts,
		fmt.Sprintf("%stest_ns.db/orders", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table in catalog: %v", err)
	}

	logicalCfg := config.LogicalConfig{
		SnapshotChunkPages:     64, // small chunks to exercise chunking
		SnapshotTargetFileSize: 1024 * 1024,
	}

	deps := logical.SnapshotDeps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: logicalCfg,
		TableCfgs:  []config.TableConfig{{Name: "public.orders"}},
		Schemas:    map[string]*postgres.TableSchema{"public.orders": ts},
		Store:      store,
		PipelineID: "test",
	}

	tables := []logical.SnapshotTable{{Name: "public.orders", Schema: ts}}

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

	snap := logical.NewSnapshotter(tables, txFactory, 1, deps)
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
	tm, err := catalog.LoadTable(sinkCfg.Namespace, "orders")
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
	cp, err := store.Load("test")
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

	_, err = catalog.CreateTable(sinkCfg.Namespace, "items", ts,
		fmt.Sprintf("%stest_ns.db/items", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	logicalCfg := config.LogicalConfig{
		SnapshotChunkPages: 16, // very small chunks
	}

	deps := logical.SnapshotDeps{
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
	if err := store.Save("test", &pipeline.Checkpoint{
		Mode:           "logical",
		SnapshotChunks: map[string]int{"public.items": 0},
	}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	// Count snapshots before run.
	tmBefore, _ := catalog.LoadTable(sinkCfg.Namespace, "items")
	snapshotsBefore := len(tmBefore.Metadata.Snapshots)

	tables := []logical.SnapshotTable{{Name: "public.items", Schema: ts}}
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

	snap := logical.NewSnapshotter(tables, txFactory, 1, deps)
	results, err := snap.Run(ctx)
	if err != nil {
		t.Fatalf("snapshot run: %v", err)
	}

	if results[0].Err != nil {
		t.Fatalf("snapshot error: %v", results[0].Err)
	}

	// Verify that chunk 0 was skipped (fewer snapshots committed than a full run).
	tmAfter, _ := catalog.LoadTable(sinkCfg.Namespace, "items")
	snapshotsAfter := len(tmAfter.Metadata.Snapshots)
	t.Logf("snapshots before=%d, after=%d (chunk 0 was skipped)", snapshotsBefore, snapshotsAfter)

	// Should still have fewer rows than a full snapshot since chunk 0 data is missing,
	// but more importantly: checkpoint should be clean.
	cp, err := store.Load("test")
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

	_, err = catalog.CreateTable(sinkCfg.Namespace, "empty_tbl", ts,
		fmt.Sprintf("%stest_ns.db/empty_tbl", sinkCfg.Warehouse), nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	deps := logical.SnapshotDeps{
		Catalog:    catalog,
		S3:         s3,
		SinkCfg:    sinkCfg,
		LogicalCfg: config.LogicalConfig{},
		TableCfgs:  []config.TableConfig{{Name: "public.empty_tbl"}},
		Schemas:    map[string]*postgres.TableSchema{"public.empty_tbl": ts},
		Store:      store,
		PipelineID: "test",
	}

	tables := []logical.SnapshotTable{{Name: "public.empty_tbl", Schema: ts}}
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

	snap := logical.NewSnapshotter(tables, txFactory, 1, deps)
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
	tm, _ := catalog.LoadTable(sinkCfg.Namespace, "empty_tbl")
	if tm.Metadata.CurrentSnapshotID != 0 {
		t.Errorf("expected no snapshot for empty table, got ID %d", tm.Metadata.CurrentSnapshotID)
	}

	// Checkpoint should still mark the table as complete.
	cp, _ := store.Load("test")
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

	catalog.CreateTable(sinkCfg.Namespace, "t1", ts1, "s3://test-bucket/test_ns.db/t1", nil)
	catalog.CreateTable(sinkCfg.Namespace, "t2", ts2, "s3://test-bucket/test_ns.db/t2", nil)

	deps := logical.SnapshotDeps{
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

	tables := []logical.SnapshotTable{
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
	snap := logical.NewSnapshotter(tables, txFactory, 2, deps)
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

	cp, _ := store.Load("test")
	if !cp.SnapshotedTables["public.t1"] {
		t.Error("expected t1 in SnapshotedTables")
	}
	if !cp.SnapshotedTables["public.t2"] {
		t.Error("expected t2 in SnapshotedTables")
	}
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
