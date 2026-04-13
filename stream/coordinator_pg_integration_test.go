//go:build integration

package stream

import (
	"context"
	"fmt"
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

func newTestCoordinator(t *testing.T, ctx context.Context) *PgCoordinator {
	t.Helper()

	dsn, cleanup := startPostgres(t, ctx)
	t.Cleanup(cleanup)

	coord, err := NewPgCoordinator(ctx, dsn)
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	t.Cleanup(func() { coord.Close() })

	if err := coord.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return coord
}

func TestPgCoordinator_MigrateIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)

	// Second migrate should be a no-op.
	if err := coord.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestPgCoordinator_ClaimOffsets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)

	// First claim: single table.
	entries, err := coord.ClaimOffsets(ctx, []LogAppend{
		{Table: "public.orders", S3Path: "staged/file1.parquet", RecordCount: 10, ByteSize: 1000},
	})
	if err != nil {
		t.Fatalf("claim 1: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].StartOffset != 0 || entries[0].EndOffset != 10 {
		t.Errorf("claim 1: got [%d, %d), want [0, 10)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Second claim: contiguous offsets.
	entries, err = coord.ClaimOffsets(ctx, []LogAppend{
		{Table: "public.orders", S3Path: "staged/file2.parquet", RecordCount: 5, ByteSize: 500},
	})
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if entries[0].StartOffset != 10 || entries[0].EndOffset != 15 {
		t.Errorf("claim 2: got [%d, %d), want [10, 15)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Multi-table atomic claim.
	entries, err = coord.ClaimOffsets(ctx, []LogAppend{
		{Table: "public.orders", S3Path: "staged/file3.parquet", RecordCount: 3, ByteSize: 300},
		{Table: "public.users", S3Path: "staged/file4.parquet", RecordCount: 7, ByteSize: 700},
		{Table: "public.orders", S3Path: "staged/file5.parquet", RecordCount: 2, ByteSize: 200},
	})
	if err != nil {
		t.Fatalf("claim 3: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	// orders: previous end was 15, batch claims 3+2=5 → [15,18) and [18,20)
	if entries[0].StartOffset != 15 || entries[0].EndOffset != 18 {
		t.Errorf("claim 3[0] orders: got [%d, %d), want [15, 18)", entries[0].StartOffset, entries[0].EndOffset)
	}
	if entries[2].StartOffset != 18 || entries[2].EndOffset != 20 {
		t.Errorf("claim 3[2] orders: got [%d, %d), want [18, 20)", entries[2].StartOffset, entries[2].EndOffset)
	}
	// users: first claim → [0,7)
	if entries[1].StartOffset != 0 || entries[1].EndOffset != 7 {
		t.Errorf("claim 3[1] users: got [%d, %d), want [0, 7)", entries[1].StartOffset, entries[1].EndOffset)
	}

	// Empty claim is a no-op.
	entries, err = coord.ClaimOffsets(ctx, nil)
	if err != nil {
		t.Fatalf("empty claim: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for empty claim, got %d", len(entries))
	}
}

func TestPgCoordinator_ReadLog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)

	// Seed 3 entries.
	for i := 0; i < 3; i++ {
		_, err := coord.ClaimOffsets(ctx, []LogAppend{
			{Table: "public.orders", S3Path: fmt.Sprintf("staged/file%d.parquet", i), RecordCount: 10, ByteSize: 1000},
		})
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Read all.
	entries, err := coord.ReadLog(ctx, "public.orders", -1)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	// Verify ordering.
	for i := 1; i < len(entries); i++ {
		if entries[i].EndOffset <= entries[i-1].EndOffset {
			t.Errorf("not ordered: entry %d end=%d <= entry %d end=%d",
				i, entries[i].EndOffset, i-1, entries[i-1].EndOffset)
		}
	}
	// Verify created_at is populated.
	if entries[0].CreatedAt.IsZero() {
		t.Error("created_at is zero")
	}

	// Read after offset 10 — skip first entry.
	partial, err := coord.ReadLog(ctx, "public.orders", 10)
	if err != nil {
		t.Fatalf("read partial: %v", err)
	}
	if len(partial) != 2 {
		t.Fatalf("expected 2 entries after offset 10, got %d", len(partial))
	}

	// Read for non-existent table.
	empty, err := coord.ReadLog(ctx, "public.nonexistent", -1)
	if err != nil {
		t.Fatalf("read nonexistent: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 entries for nonexistent table, got %d", len(empty))
	}
}

func TestPgCoordinator_TruncateLog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)

	// Seed 3 entries: [0,10), [10,20), [20,30).
	for i := 0; i < 3; i++ {
		_, err := coord.ClaimOffsets(ctx, []LogAppend{
			{Table: "public.orders", S3Path: fmt.Sprintf("staged/file%d.parquet", i), RecordCount: 10, ByteSize: 1000},
		})
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// Truncate entries with end_offset <= 20.
	paths, err := coord.TruncateLog(ctx, "public.orders", 20)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 deleted paths, got %d", len(paths))
	}

	// Only 1 entry remains.
	entries, err := coord.ReadLog(ctx, "public.orders", -1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after truncate, got %d", len(entries))
	}
	if entries[0].StartOffset != 20 || entries[0].EndOffset != 30 {
		t.Errorf("remaining: got [%d, %d), want [20, 30)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Truncate nothing.
	paths, err = coord.TruncateLog(ctx, "public.orders", 0)
	if err != nil {
		t.Fatalf("truncate nothing: %v", err)
	}
	if len(paths) != 0 {
		t.Errorf("expected 0 deleted paths, got %d", len(paths))
	}
}

func TestPgCoordinator_Cursor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)

	// No cursor yet.
	offset, err := coord.GetCursor(ctx, "default", "public.orders")
	if err != nil {
		t.Fatalf("get cursor: %v", err)
	}
	if offset != -1 {
		t.Errorf("expected -1 for missing cursor, got %d", offset)
	}

	// Ensure cursor.
	if err := coord.EnsureCursor(ctx, "default", "public.orders"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	offset, err = coord.GetCursor(ctx, "default", "public.orders")
	if err != nil {
		t.Fatalf("get after ensure: %v", err)
	}
	if offset != -1 {
		t.Errorf("expected -1 after ensure, got %d", offset)
	}

	// Ensure is idempotent.
	if err := coord.EnsureCursor(ctx, "default", "public.orders"); err != nil {
		t.Fatalf("ensure idempotent: %v", err)
	}

	// Set cursor.
	if err := coord.SetCursor(ctx, "default", "public.orders", 42); err != nil {
		t.Fatalf("set: %v", err)
	}
	offset, err = coord.GetCursor(ctx, "default", "public.orders")
	if err != nil {
		t.Fatalf("get after set: %v", err)
	}
	if offset != 42 {
		t.Errorf("expected 42 after set, got %d", offset)
	}

	// Different tables are independent.
	if err := coord.EnsureCursor(ctx, "default", "public.users"); err != nil {
		t.Fatalf("ensure users: %v", err)
	}
	offset, err = coord.GetCursor(ctx, "default", "public.users")
	if err != nil {
		t.Fatalf("get users: %v", err)
	}
	if offset != -1 {
		t.Errorf("expected -1 for users, got %d", offset)
	}
}

func TestPgCoordinator_Lock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)
	ttl := 30 * time.Second

	// Worker A acquires.
	acquired, err := coord.TryLock(ctx, "orders", "worker-a", ttl)
	if err != nil {
		t.Fatalf("try lock a: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-a to acquire")
	}

	// Worker B blocked.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", ttl)
	if err != nil {
		t.Fatalf("try lock b: %v", err)
	}
	if acquired {
		t.Fatal("expected worker-b to be blocked")
	}

	// Worker A renews.
	renewed, err := coord.RenewLock(ctx, "orders", "worker-a", ttl)
	if err != nil {
		t.Fatalf("renew a: %v", err)
	}
	if !renewed {
		t.Fatal("expected worker-a renew to succeed")
	}

	// Worker B cannot renew.
	renewed, err = coord.RenewLock(ctx, "orders", "worker-b", ttl)
	if err != nil {
		t.Fatalf("renew b: %v", err)
	}
	if renewed {
		t.Fatal("expected worker-b renew to fail")
	}

	// Worker A releases.
	if err := coord.ReleaseLock(ctx, "orders", "worker-a"); err != nil {
		t.Fatalf("release a: %v", err)
	}

	// Worker B acquires.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", ttl)
	if err != nil {
		t.Fatalf("try lock b after release: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-b to acquire after release")
	}
}

func TestPgCoordinator_LockExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)

	// Worker A acquires with 1-second TTL.
	acquired, err := coord.TryLock(ctx, "orders", "worker-a", 1*time.Second)
	if err != nil {
		t.Fatalf("try lock a: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-a to acquire")
	}

	// Worker B blocked immediately.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", 30*time.Second)
	if err != nil {
		t.Fatalf("try lock b (before expiry): %v", err)
	}
	if acquired {
		t.Fatal("expected worker-b to be blocked before expiry")
	}

	// Wait for expiry.
	time.Sleep(2 * time.Second)

	// Worker B can now reclaim the expired lock.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", 30*time.Second)
	if err != nil {
		t.Fatalf("try lock b (after expiry): %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-b to acquire expired lock")
	}

	// Worker A renew fails (lock was stolen).
	renewed, err := coord.RenewLock(ctx, "orders", "worker-a", 30*time.Second)
	if err != nil {
		t.Fatalf("renew a: %v", err)
	}
	if renewed {
		t.Fatal("expected worker-a renew to fail after lock stolen")
	}
}

func TestPgCoordinator_LockDifferentTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)
	ttl := 30 * time.Second

	// Worker A locks orders.
	acquired, err := coord.TryLock(ctx, "orders", "worker-a", ttl)
	if err != nil {
		t.Fatalf("lock orders: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire orders lock")
	}

	// Worker B locks users (different table — no conflict).
	acquired, err = coord.TryLock(ctx, "users", "worker-b", ttl)
	if err != nil {
		t.Fatalf("lock users: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire users lock (different table)")
	}
}

func TestPgCoordinator_FullCycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()
	coord := newTestCoordinator(t, ctx)
	table := "public.orders"

	// Setup: ensure cursor.
	if err := coord.EnsureCursor(ctx, "default", table); err != nil {
		t.Fatalf("ensure cursor: %v", err)
	}

	// Simulate WAL writer: append 3 batches.
	for i := 0; i < 3; i++ {
		_, err := coord.ClaimOffsets(ctx, []LogAppend{
			{Table: table, S3Path: fmt.Sprintf("staged/batch%d.parquet", i), RecordCount: 10, ByteSize: 1000},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Simulate materializer cycle 1: read from cursor, process, advance.
	cursor, _ := coord.GetCursor(ctx, "default", table)
	entries, err := coord.ReadLog(ctx, table, cursor)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Process first 2 entries, advance cursor.
	maxOffset := entries[1].EndOffset // 20
	if err := coord.SetCursor(ctx, "default", table, maxOffset); err != nil {
		t.Fatalf("set cursor: %v", err)
	}

	// Simulate materializer cycle 2: only sees entry 3.
	cursor, _ = coord.GetCursor(ctx, "default", table)
	entries, err = coord.ReadLog(ctx, table, cursor)
	if err != nil {
		t.Fatalf("read cycle 2: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry in cycle 2, got %d", len(entries))
	}
	if entries[0].StartOffset != 20 || entries[0].EndOffset != 30 {
		t.Errorf("cycle 2 entry: got [%d, %d), want [20, 30)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Advance cursor and truncate processed entries.
	if err := coord.SetCursor(ctx, "default", table, entries[0].EndOffset); err != nil {
		t.Fatalf("set cursor final: %v", err)
	}
	paths, err := coord.TruncateLog(ctx, table, 20)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if len(paths) != 2 {
		t.Errorf("expected 2 truncated paths, got %d", len(paths))
	}

	// Verify only 1 entry remains.
	remaining, err := coord.ReadLog(ctx, table, -1)
	if err != nil {
		t.Fatalf("read remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining entry, got %d", len(remaining))
	}
}
