//go:build integration

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPostgres(t *testing.T, ctx context.Context) (dsn string, cleanup func()) {
	t.Helper()

	ctr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=logical",
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

	dsn = "postgres://postgres:postgres@" + host + ":" + port.Port() + "/testdb?sslmode=disable"
	return dsn, func() { ctr.Terminate(ctx) }
}

func TestPgCheckpointStore_SaveAndLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	store, err := NewPgCheckpointStore(ctx, dsn)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	// Save a checkpoint with all fields populated.
	cp := &Checkpoint{
		Mode:             "logical",
		LSN:              12345,
		SnapshotComplete: true,
		SnapshotedTables: map[string]bool{
			"public.orders": true,
			"public.users":  true,
		},
		SnapshotChunks: map[string]int{
			"public.orders": 5,
		},
	}

	if err := store.Save(ctx, "test-pipeline", cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Load it back and verify all fields.
	loaded, err := store.Load(ctx, "test-pipeline")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded.Version != CheckpointVersion {
		t.Errorf("Version: got %d, want %d", loaded.Version, CheckpointVersion)
	}
	if loaded.Mode != "logical" {
		t.Errorf("Mode: got %q, want %q", loaded.Mode, "logical")
	}
	if loaded.LSN != 12345 {
		t.Errorf("LSN: got %d, want %d", loaded.LSN, 12345)
	}
	if !loaded.SnapshotComplete {
		t.Error("SnapshotComplete: got false, want true")
	}
	if loaded.Revision != 1 {
		t.Errorf("Revision: got %d, want %d", loaded.Revision, 1)
	}
	if loaded.Checksum == "" {
		t.Error("Checksum: got empty, want non-empty")
	}

	// Maps.
	if len(loaded.SnapshotedTables) != 2 || !loaded.SnapshotedTables["public.orders"] {
		t.Errorf("SnapshotedTables: got %v", loaded.SnapshotedTables)
	}
	if loaded.SnapshotChunks["public.orders"] != 5 {
		t.Errorf("SnapshotChunks: got %v", loaded.SnapshotChunks)
	}
}

func TestPgCheckpointStore_LoadFreshReturnsZero(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	store, err := NewPgCheckpointStore(ctx, dsn)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	loaded, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Mode != "" {
		t.Errorf("expected zero checkpoint, got Mode=%q", loaded.Mode)
	}
	if loaded.LSN != 0 {
		t.Errorf("expected LSN=0, got %d", loaded.LSN)
	}
}

func TestPgCheckpointStore_UpdatePreservesFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	store, err := NewPgCheckpointStore(ctx, dsn)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	// First save.
	cp := &Checkpoint{Mode: "logical", LSN: 1000}
	if err := store.Save(ctx, "p1", cp); err != nil {
		t.Fatalf("save 1: %v", err)
	}

	// Load, modify, save again.
	loaded, err := store.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	loaded.LSN = 2000
	if err := store.Save(ctx, "p1", loaded); err != nil {
		t.Fatalf("save 2: %v", err)
	}

	// Verify updated values.
	final, err := store.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("load 2: %v", err)
	}
	if final.LSN != 2000 {
		t.Errorf("LSN: got %d, want 2000", final.LSN)
	}
	if final.Revision != 2 {
		t.Errorf("Revision: got %d, want 2", final.Revision)
	}
}

func TestPgCheckpointStore_ConcurrentUpdateDetected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	store1, err := NewPgCheckpointStore(ctx, dsn)
	if err != nil {
		t.Fatalf("create store1: %v", err)
	}
	defer store1.Close()

	store2, err := NewPgCheckpointStore(ctx, dsn)
	if err != nil {
		t.Fatalf("create store2: %v", err)
	}
	defer store2.Close()

	// Instance 1 saves.
	cp := &Checkpoint{Mode: "logical", LSN: 1000}
	if err := store1.Save(ctx, "p1", cp); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Both load same revision.
	loaded1, err := store1.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("load 1: %v", err)
	}
	loaded2, err := store2.Load(ctx, "p1")
	if err != nil {
		t.Fatalf("load 2: %v", err)
	}

	// Instance 1 saves (advances revision).
	loaded1.LSN = 2000
	if err := store1.Save(ctx, "p1", loaded1); err != nil {
		t.Fatalf("save from instance 1: %v", err)
	}

	// Instance 2 tries to save with stale revision.
	loaded2.LSN = 3000
	err = store2.Save(ctx, "p1", loaded2)
	if err != ErrConcurrentUpdate {
		t.Fatalf("expected ErrConcurrentUpdate, got: %v", err)
	}
}

func TestPgCheckpointStore_MultiplePipelines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dsn, cleanup := startPostgres(t, ctx)
	defer cleanup()

	store, err := NewPgCheckpointStore(ctx, dsn)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	defer store.Close()

	// Save two different pipelines.
	cp1 := &Checkpoint{Mode: "logical", LSN: 100}
	cp2 := &Checkpoint{Mode: "query", Watermark: "2026-01-01T00:00:00Z"}

	if err := store.Save(ctx, "pipeline-a", cp1); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := store.Save(ctx, "pipeline-b", cp2); err != nil {
		t.Fatalf("save b: %v", err)
	}

	// Load each and verify isolation.
	loadedA, err := store.Load(ctx, "pipeline-a")
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	loadedB, err := store.Load(ctx, "pipeline-b")
	if err != nil {
		t.Fatalf("load b: %v", err)
	}

	if loadedA.Mode != "logical" || loadedA.LSN != 100 {
		t.Errorf("pipeline-a: got Mode=%q LSN=%d", loadedA.Mode, loadedA.LSN)
	}
	if loadedB.Mode != "query" || loadedB.Watermark != "2026-01-01T00:00:00Z" {
		t.Errorf("pipeline-b: got Mode=%q Watermark=%q", loadedB.Mode, loadedB.Watermark)
	}
}
