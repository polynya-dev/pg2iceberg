package stream

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pg2iceberg/pg2iceberg/iceberg"
)

// memStorage is a minimal in-memory ObjectStorage for testing.
type memStorage struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemStorage() *memStorage {
	return &memStorage{files: make(map[string][]byte)}
}

func (m *memStorage) URIForKey(key string) string {
	return fmt.Sprintf("s3://test-bucket/%s", key)
}

func (m *memStorage) Upload(_ context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.files[key] = cp
	return m.URIForKey(key), nil
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

func (m *memStorage) ListObjects(_ context.Context, prefix string) ([]iceberg.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []iceberg.ObjectInfo
	for key := range m.files {
		if len(prefix) == 0 || strings.HasPrefix(key, prefix) {
			result = append(result, iceberg.ObjectInfo{Key: key})
		}
	}
	return result, nil
}

func (m *memStorage) DeleteObjects(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.files, k)
	}
	return nil
}

func (m *memStorage) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ks []string
	for k := range m.files {
		ks = append(ks, k)
	}
	return ks
}

// --- Stream tests ---

func TestStream_AppendAndRead(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	s3 := newMemStorage()
	str := NewStream(coord, s3, "test_ns")

	// Append a single batch.
	err := str.Append(ctx, []WriteBatch{
		{Table: "public.orders", Data: []byte("parquet-data-1"), RecordCount: 10},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Verify S3 file was uploaded.
	keys := s3.keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 S3 file, got %d", len(keys))
	}
	if !strings.HasPrefix(keys[0], "staged/public.orders/") {
		t.Errorf("unexpected S3 key: %s", keys[0])
	}
	if !strings.HasSuffix(keys[0], ".parquet") {
		t.Errorf("expected .parquet suffix: %s", keys[0])
	}

	// Verify log entry was created.
	entries, err := str.Read(ctx, "public.orders", -1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Table != "public.orders" {
		t.Errorf("table: got %q, want %q", e.Table, "public.orders")
	}
	if e.StartOffset != 0 || e.EndOffset != 10 {
		t.Errorf("offsets: got [%d, %d), want [0, 10)", e.StartOffset, e.EndOffset)
	}
	if e.RecordCount != 10 {
		t.Errorf("record count: got %d, want 10", e.RecordCount)
	}

	// Download the staged file and verify content.
	data, err := str.Download(ctx, e.S3Path)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if string(data) != "parquet-data-1" {
		t.Errorf("data: got %q, want %q", string(data), "parquet-data-1")
	}
}

func TestStream_AppendMultiTable(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	s3 := newMemStorage()
	str := NewStream(coord, s3, "test_ns")

	// Append batches for two tables atomically.
	err := str.Append(ctx, []WriteBatch{
		{Table: "public.orders", Data: []byte("orders-data"), RecordCount: 5},
		{Table: "public.users", Data: []byte("users-data"), RecordCount: 3},
		{Table: "public.orders", Data: []byte("orders-data-2"), RecordCount: 7},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Verify S3: 3 files.
	keys := s3.keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 S3 files, got %d", len(keys))
	}

	// Verify orders log: 2 entries, contiguous offsets [0,5) and [5,12).
	entries, err := str.Read(ctx, "public.orders", -1)
	if err != nil {
		t.Fatalf("read orders: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 orders entries, got %d", len(entries))
	}
	if entries[0].StartOffset != 0 || entries[0].EndOffset != 5 {
		t.Errorf("orders[0]: got [%d, %d), want [0, 5)", entries[0].StartOffset, entries[0].EndOffset)
	}
	if entries[1].StartOffset != 5 || entries[1].EndOffset != 12 {
		t.Errorf("orders[1]: got [%d, %d), want [5, 12)", entries[1].StartOffset, entries[1].EndOffset)
	}

	// Verify users log: 1 entry [0,3).
	entries, err = str.Read(ctx, "public.users", -1)
	if err != nil {
		t.Fatalf("read users: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 users entry, got %d", len(entries))
	}
	if entries[0].StartOffset != 0 || entries[0].EndOffset != 3 {
		t.Errorf("users[0]: got [%d, %d), want [0, 3)", entries[0].StartOffset, entries[0].EndOffset)
	}
}

func TestStream_ReadAfterOffset(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	s3 := newMemStorage()
	str := NewStream(coord, s3, "test_ns")

	// Append 3 batches to build up offsets.
	for i := 0; i < 3; i++ {
		err := str.Append(ctx, []WriteBatch{
			{Table: "public.orders", Data: []byte(fmt.Sprintf("batch-%d", i)), RecordCount: 10},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Read all entries.
	all, err := str.Read(ctx, "public.orders", -1)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}

	// Read after offset 10 (skip first entry [0,10)).
	partial, err := str.Read(ctx, "public.orders", 10)
	if err != nil {
		t.Fatalf("read partial: %v", err)
	}
	if len(partial) != 2 {
		t.Fatalf("expected 2 entries after offset 10, got %d", len(partial))
	}
	if partial[0].StartOffset != 10 {
		t.Errorf("first entry start: got %d, want 10", partial[0].StartOffset)
	}

	// Read after offset 30 (past all entries).
	empty, err := str.Read(ctx, "public.orders", 30)
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 entries after offset 30, got %d", len(empty))
	}
}

func TestStream_Truncate(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	s3 := newMemStorage()
	str := NewStream(coord, s3, "test_ns")

	// Append 3 batches.
	for i := 0; i < 3; i++ {
		err := str.Append(ctx, []WriteBatch{
			{Table: "public.orders", Data: []byte(fmt.Sprintf("batch-%d", i)), RecordCount: 10},
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// Verify 3 S3 files and 3 log entries.
	if len(s3.keys()) != 3 {
		t.Fatalf("expected 3 S3 files before truncate, got %d", len(s3.keys()))
	}

	// Truncate entries with end_offset <= 20 (first two entries).
	if err := str.Truncate(ctx, "public.orders", 20); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Verify only 1 log entry remains.
	entries, err := str.Read(ctx, "public.orders", -1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after truncate, got %d", len(entries))
	}
	if entries[0].StartOffset != 20 || entries[0].EndOffset != 30 {
		t.Errorf("remaining entry: got [%d, %d), want [20, 30)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Verify S3 files were cleaned up (2 deleted, 1 remaining).
	if len(s3.keys()) != 1 {
		t.Errorf("expected 1 S3 file after truncate, got %d", len(s3.keys()))
	}
}

func TestStream_CursorIntegration(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	s3 := newMemStorage()
	str := NewStream(coord, s3, "test_ns")

	table := "public.orders"

	// Ensure cursor exists.
	if err := coord.EnsureCursor(ctx, table); err != nil {
		t.Fatalf("ensure cursor: %v", err)
	}

	// Initial cursor is -1.
	cursor, err := coord.GetCursor(ctx, table)
	if err != nil {
		t.Fatalf("get cursor: %v", err)
	}
	if cursor != -1 {
		t.Fatalf("initial cursor: got %d, want -1", cursor)
	}

	// Append two batches.
	if err := str.Append(ctx, []WriteBatch{
		{Table: table, Data: []byte("batch-1"), RecordCount: 5},
	}); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := str.Append(ctx, []WriteBatch{
		{Table: table, Data: []byte("batch-2"), RecordCount: 3},
	}); err != nil {
		t.Fatalf("append 2: %v", err)
	}

	// Read from cursor (-1) — should see both entries.
	entries, err := str.Read(ctx, table, cursor)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Advance cursor to end of first batch.
	maxOffset := entries[0].EndOffset
	if err := coord.SetCursor(ctx, table, maxOffset); err != nil {
		t.Fatalf("set cursor: %v", err)
	}

	// Read from new cursor — should see only second entry.
	cursor, _ = coord.GetCursor(ctx, table)
	entries, err = str.Read(ctx, table, cursor)
	if err != nil {
		t.Fatalf("read after advance: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after cursor advance, got %d", len(entries))
	}
	if entries[0].StartOffset != 5 || entries[0].EndOffset != 8 {
		t.Errorf("entry: got [%d, %d), want [5, 8)", entries[0].StartOffset, entries[0].EndOffset)
	}
}

func TestStream_AppendEmpty(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	s3 := newMemStorage()
	str := NewStream(coord, s3, "test_ns")

	// Append with no batches is a no-op.
	if err := str.Append(ctx, nil); err != nil {
		t.Fatalf("append nil: %v", err)
	}
	if err := str.Append(ctx, []WriteBatch{}); err != nil {
		t.Fatalf("append empty: %v", err)
	}

	if len(s3.keys()) != 0 {
		t.Errorf("expected 0 S3 files, got %d", len(s3.keys()))
	}
}

func TestCoordinator_Lock(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()
	ttl := 30 * time.Second

	// Worker A acquires lock.
	acquired, err := coord.TryLock(ctx, "orders", "worker-a", ttl)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-a to acquire lock")
	}

	// Worker B fails to acquire same lock.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", ttl)
	if err != nil {
		t.Fatalf("try lock b: %v", err)
	}
	if acquired {
		t.Fatal("expected worker-b to fail acquiring lock")
	}

	// Worker A can renew.
	renewed, err := coord.RenewLock(ctx, "orders", "worker-a", ttl)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !renewed {
		t.Fatal("expected worker-a to renew lock")
	}

	// Worker B cannot renew (doesn't hold it).
	renewed, err = coord.RenewLock(ctx, "orders", "worker-b", ttl)
	if err != nil {
		t.Fatalf("renew b: %v", err)
	}
	if renewed {
		t.Fatal("expected worker-b renew to fail")
	}

	// Worker A releases.
	if err := coord.ReleaseLock(ctx, "orders", "worker-a"); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Worker B can now acquire.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", ttl)
	if err != nil {
		t.Fatalf("try lock b after release: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-b to acquire lock after release")
	}
}

func TestCoordinator_LockExpiry(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()

	// Worker A acquires lock with very short TTL.
	acquired, err := coord.TryLock(ctx, "orders", "worker-a", 1*time.Millisecond)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-a to acquire lock")
	}

	// Wait for expiry.
	time.Sleep(5 * time.Millisecond)

	// Worker B can now acquire the expired lock.
	acquired, err = coord.TryLock(ctx, "orders", "worker-b", 30*time.Second)
	if err != nil {
		t.Fatalf("try lock b: %v", err)
	}
	if !acquired {
		t.Fatal("expected worker-b to acquire expired lock")
	}

	// Worker A's renew should fail (lock was stolen).
	renewed, err := coord.RenewLock(ctx, "orders", "worker-a", 30*time.Second)
	if err != nil {
		t.Fatalf("renew a: %v", err)
	}
	if renewed {
		t.Fatal("expected worker-a renew to fail after expiry")
	}
}

func TestCoordinator_ClaimOffsetsContiguous(t *testing.T) {
	ctx := context.Background()
	coord := NewMemCoordinator()

	// First claim: 10 records for orders.
	entries, err := coord.ClaimOffsets(ctx, []LogAppend{
		{Table: "public.orders", S3Path: "file1.parquet", RecordCount: 10, ByteSize: 1000},
	})
	if err != nil {
		t.Fatalf("claim 1: %v", err)
	}
	if entries[0].StartOffset != 0 || entries[0].EndOffset != 10 {
		t.Errorf("claim 1: got [%d, %d), want [0, 10)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Second claim: 5 records for orders.
	entries, err = coord.ClaimOffsets(ctx, []LogAppend{
		{Table: "public.orders", S3Path: "file2.parquet", RecordCount: 5, ByteSize: 500},
	})
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}
	if entries[0].StartOffset != 10 || entries[0].EndOffset != 15 {
		t.Errorf("claim 2: got [%d, %d), want [10, 15)", entries[0].StartOffset, entries[0].EndOffset)
	}

	// Third claim: multi-table batch.
	entries, err = coord.ClaimOffsets(ctx, []LogAppend{
		{Table: "public.orders", S3Path: "file3.parquet", RecordCount: 3, ByteSize: 300},
		{Table: "public.users", S3Path: "file4.parquet", RecordCount: 7, ByteSize: 700},
		{Table: "public.orders", S3Path: "file5.parquet", RecordCount: 2, ByteSize: 200},
	})
	if err != nil {
		t.Fatalf("claim 3: %v", err)
	}
	// orders: previous end was 15, new batch claims 3+2=5, so [15,18) and [18,20).
	if entries[0].StartOffset != 15 || entries[0].EndOffset != 18 {
		t.Errorf("claim 3[0]: got [%d, %d), want [15, 18)", entries[0].StartOffset, entries[0].EndOffset)
	}
	if entries[2].StartOffset != 18 || entries[2].EndOffset != 20 {
		t.Errorf("claim 3[2]: got [%d, %d), want [18, 20)", entries[2].StartOffset, entries[2].EndOffset)
	}
	// users: first claim, [0,7).
	if entries[1].StartOffset != 0 || entries[1].EndOffset != 7 {
		t.Errorf("claim 3[1]: got [%d, %d), want [0, 7)", entries[1].StartOffset, entries[1].EndOffset)
	}
}
