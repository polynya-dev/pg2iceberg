package iceberg

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

// testSchema returns a simple schema for compaction tests.
func testSchema() *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: "public.orders",
		PK:    []string{"id"},
		Columns: []postgres.Column{
			{Name: "id", PGType: "integer", FieldID: 1, IsNullable: false},
			{Name: "name", PGType: "text", FieldID: 2, IsNullable: false},
			{Name: "price", PGType: "integer", FieldID: 3, IsNullable: false},
		},
	}
}

// writeTestData uses a TableWriter to write rows and commit, simulating
// multiple materializer cycles that create many small files.
func writeTestData(t *testing.T, ctx context.Context, tw *TableWriter, catalog *testCompactCatalog, s3 *testCompactStorage, ns string, batches [][]RowState, pk []string) {
	t.Helper()
	for _, batch := range batches {
		prepared, err := tw.Prepare(ctx, batch, pk)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if prepared == nil {
			continue
		}
		err = catalog.CommitTransaction(ctx, ns, []TableCommit{prepared.ToTableCommit()})
		if err != nil {
			t.Fatalf("CommitTransaction: %v", err)
		}
	}
}

// testCompactStorage is an in-memory ObjectStorage for testing.
type testCompactStorage struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newTestStorage() *testCompactStorage {
	return &testCompactStorage{files: make(map[string][]byte)}
}

func (m *testCompactStorage) URIForKey(key string) string {
	return fmt.Sprintf("s3://test-bucket/%s", key)
}

func (m *testCompactStorage) Upload(_ context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.files[key] = cp
	return m.URIForKey(key), nil
}

func (m *testCompactStorage) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return data, nil
}

func (m *testCompactStorage) DownloadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
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

func (m *testCompactStorage) StatObject(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	if !ok {
		return 0, fmt.Errorf("not found: %s", key)
	}
	return int64(len(data)), nil
}

func (m *testCompactStorage) ListObjects(_ context.Context, prefix string) ([]ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []ObjectInfo
	for key := range m.files {
		if len(prefix) == 0 || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, ObjectInfo{Key: key, LastModified: time.Now().Add(-1 * time.Hour)})
		}
	}
	return result, nil
}

func (m *testCompactStorage) DeleteObjects(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.files, k)
	}
	return nil
}

func (m *testCompactStorage) fileCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.files)
}

// testCompactCatalog is an in-memory MetadataCache for testing.
type testCompactCatalog struct {
	mu        sync.Mutex
	tables    map[string]*TableMetadata
	manifests map[string][]ManifestFileInfo
}

func newTestCatalog() *testCompactCatalog {
	return &testCompactCatalog{
		tables:    make(map[string]*TableMetadata),
		manifests: make(map[string][]ManifestFileInfo),
	}
}

func (c *testCompactCatalog) EnsureNamespace(_ context.Context, ns string) error { return nil }

func (c *testCompactCatalog) LoadTable(_ context.Context, ns, table string) (*TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tables[ns+"."+table], nil
}

func (c *testCompactCatalog) CreateTable(_ context.Context, ns, table string, ts *postgres.TableSchema, location string, partSpec *PartitionSpec) (*TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &TableMetadata{}
	tm.Metadata.FormatVersion = 2
	tm.Metadata.Location = location
	c.tables[ns+"."+table] = tm
	return tm, nil
}

func (c *testCompactCatalog) CommitSnapshot(_ context.Context, ns, table string, currentSnapshotID int64, snapshot SnapshotCommit) error {
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

func (c *testCompactCatalog) CommitTransaction(ctx context.Context, ns string, commits []TableCommit) error {
	for _, tc := range commits {
		if err := c.CommitSnapshot(ctx, ns, tc.Table, tc.CurrentSnapshotID, tc.Snapshot); err != nil {
			return err
		}
		if tc.NewManifests != nil {
			c.SetManifests(ns, tc.Table, tc.NewManifests)
		}
	}
	return nil
}

func (c *testCompactCatalog) EvolveSchema(_ context.Context, ns, table string, currentSchemaID int, newSchema *postgres.TableSchema) (int, error) {
	return currentSchemaID + 1, nil
}

func (c *testCompactCatalog) Manifests(ns, table string) []ManifestFileInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.manifests[ns+"."+table]
}

func (c *testCompactCatalog) SetManifests(ns, table string, manifests []ManifestFileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manifests[ns+"."+table] = manifests
}

func (c *testCompactCatalog) DataFiles(ns, table string) []DataFileInfo   { return nil }
func (c *testCompactCatalog) SetDataFiles(ns, table string, _ []DataFileInfo) {}
func (c *testCompactCatalog) FileIndex(ns, table string) *FileIndex       { return nil }
func (c *testCompactCatalog) SetFileIndex(ns, table string, _ *FileIndex) {}

func TestCompact_MergesSmallFiles(t *testing.T) {
	ctx := context.Background()
	ts := testSchema()
	s3 := newTestStorage()
	catalog := newTestCatalog()
	ns := "test_ns"

	// Create table.
	catalog.CreateTable(ctx, ns, "orders", ts, "s3://test-bucket/test_ns.db/orders", nil)

	tw := NewTableWriter(TableWriteConfig{
		Namespace:   ns,
		IcebergName: "orders",
		SrcSchema:   ts,
		TargetSize:  1024 * 1024, // 1MB — all test files will be small
		Concurrency: 2,
	}, catalog, s3)

	// Write 10 small batches (simulating 10 materializer cycles).
	var batches [][]RowState
	for i := 0; i < 10; i++ {
		batch := []RowState{
			{Op: "I", Row: map[string]any{"id": int32(i*3 + 1), "name": fmt.Sprintf("item-%d", i*3+1), "price": int32((i + 1) * 100)}},
			{Op: "I", Row: map[string]any{"id": int32(i*3 + 2), "name": fmt.Sprintf("item-%d", i*3+2), "price": int32((i + 1) * 200)}},
			{Op: "I", Row: map[string]any{"id": int32(i*3 + 3), "name": fmt.Sprintf("item-%d", i*3+3), "price": int32((i + 1) * 300)}},
		}
		batches = append(batches, batch)
	}
	writeTestData(t, ctx, tw, catalog, s3, ns, batches, ts.PK)

	// Verify we have many small files before compaction.
	tm, _ := catalog.LoadTable(ctx, ns, "orders")
	manifests, _ := ReadManifestList(mustDownload(t, ctx, s3, tm.CurrentManifestList()))
	var dataFilesBefore int
	for _, mfi := range manifests {
		if mfi.Content == 0 {
			dataFilesBefore += mfi.AddedFiles
		}
	}
	t.Logf("Before compaction: %d data files", dataFilesBefore)
	if dataFilesBefore < 5 {
		t.Fatalf("expected at least 5 data files before compaction, got %d", dataFilesBefore)
	}

	// Run compaction with low threshold.
	cc := CompactionConfig{DataFileThreshold: 3, DeleteFileThreshold: 1}
	prepared, err := tw.Compact(ctx, ts.PK, cc)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if prepared == nil {
		t.Fatal("expected compaction to run, got nil")
	}

	// Commit the compacted snapshot.
	err = catalog.CommitTransaction(ctx, ns, []TableCommit{prepared.ToTableCommit()})
	if err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	// Verify fewer files after compaction.
	tm, _ = catalog.LoadTable(ctx, ns, "orders")
	manifests, _ = ReadManifestList(mustDownload(t, ctx, s3, tm.CurrentManifestList()))
	var dataFilesAfter int
	var totalRows int64
	for _, mfi := range manifests {
		if mfi.Content == 0 {
			dataFilesAfter += mfi.AddedFiles
			totalRows += mfi.AddedRows
		}
	}
	t.Logf("After compaction: %d data files, %d total rows", dataFilesAfter, totalRows)

	if dataFilesAfter >= dataFilesBefore {
		t.Fatalf("compaction should reduce file count: before=%d after=%d", dataFilesBefore, dataFilesAfter)
	}
	if totalRows != 30 {
		t.Fatalf("expected 30 rows after compaction, got %d", totalRows)
	}
}

func TestCompact_AppliesDeletes_PreservesUpdates(t *testing.T) {
	ctx := context.Background()
	ts := testSchema()
	s3 := newTestStorage()
	catalog := newTestCatalog()
	ns := "test_ns"

	catalog.CreateTable(ctx, ns, "orders", ts, "s3://test-bucket/test_ns.db/orders", nil)

	tw := NewTableWriter(TableWriteConfig{
		Namespace:   ns,
		IcebergName: "orders",
		SrcSchema:   ts,
		TargetSize:  1024 * 1024,
		Concurrency: 2,
	}, catalog, s3)

	// Batch 1 (seq=1): Insert 5 rows.
	batch1 := []RowState{
		{Op: "I", Row: map[string]any{"id": int32(1), "name": "alice", "price": int32(100)}},
		{Op: "I", Row: map[string]any{"id": int32(2), "name": "bob", "price": int32(200)}},
		{Op: "I", Row: map[string]any{"id": int32(3), "name": "charlie", "price": int32(300)}},
		{Op: "I", Row: map[string]any{"id": int32(4), "name": "dave", "price": int32(400)}},
		{Op: "I", Row: map[string]any{"id": int32(5), "name": "eve", "price": int32(500)}},
	}

	// Batch 2 (seq=2): Update alice (delete+insert at same seq), delete charlie.
	// Per Iceberg spec: the delete at seq=2 only applies to data files with seq < 2.
	// The new alice data file also has seq=2, so the delete does NOT apply to it.
	// charlie's delete at seq=2 applies to batch1's data file at seq=1.
	batch2 := []RowState{
		{Op: "U", Row: map[string]any{"id": int32(1), "name": "alice-updated", "price": int32(150)}},
		{Op: "D", Row: map[string]any{"id": int32(3), "name": "charlie", "price": int32(300)}},
	}

	// Write in 6 small batches to exceed threshold.
	batches := [][]RowState{batch1, batch2}
	for i := 0; i < 4; i++ {
		batches = append(batches, []RowState{
			{Op: "I", Row: map[string]any{"id": int32(10 + i), "name": fmt.Sprintf("extra-%d", i), "price": int32(1000 + i)}},
		})
	}
	writeTestData(t, ctx, tw, catalog, s3, ns, batches, ts.PK)

	// Build file index for smart compaction.
	_, err := tw.BuildFileIndex(ctx, ts.PK)
	if err != nil {
		t.Fatalf("BuildFileIndex: %v", err)
	}

	// Count files before.
	tm, _ := catalog.LoadTable(ctx, ns, "orders")
	manifests, _ := ReadManifestList(mustDownload(t, ctx, s3, tm.CurrentManifestList()))
	var dataBefore, deleteBefore int
	for _, mfi := range manifests {
		if mfi.Content == 0 {
			dataBefore += mfi.AddedFiles
		} else {
			deleteBefore += mfi.AddedFiles
		}
	}
	t.Logf("Before: %d data files, %d delete files", dataBefore, deleteBefore)

	// Compact.
	cc := CompactionConfig{DataFileThreshold: 3, DeleteFileThreshold: 1}
	prepared, err := tw.Compact(ctx, ts.PK, cc)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if prepared == nil {
		t.Fatal("expected compaction to run")
	}

	err = catalog.CommitTransaction(ctx, ns, []TableCommit{prepared.ToTableCommit()})
	if err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	// Verify: no delete files after compaction.
	tm, _ = catalog.LoadTable(ctx, ns, "orders")
	manifests, _ = ReadManifestList(mustDownload(t, ctx, s3, tm.CurrentManifestList()))
	var dataAfter, deleteAfter int
	var totalRows int64
	for _, mfi := range manifests {
		if mfi.Content == 0 {
			dataAfter += mfi.AddedFiles
			totalRows += mfi.AddedRows
		} else {
			deleteAfter += mfi.AddedFiles
		}
	}
	t.Logf("After: %d data files, %d delete files, %d rows", dataAfter, deleteAfter, totalRows)

	if deleteAfter != 0 {
		t.Fatalf("expected 0 delete files after compaction, got %d", deleteAfter)
	}

	// Sequence-aware compaction expected result:
	// - alice (id=1): old row at seq=1 is deleted (delete seq=2 > data seq=1).
	//   New alice-updated at seq=2 survives (delete seq=2 is NOT > data seq=2).
	// - charlie (id=3): deleted (delete seq=2 > data seq=1). No re-insert.
	// - bob, dave, eve: untouched.
	// - 4 extras: untouched.
	// Total: alice-updated + bob + dave + eve + 4 extras = 8 rows
	expectedRows := int64(8)
	if totalRows != expectedRows {
		t.Fatalf("expected %d rows after compaction (updated alice must survive), got %d", expectedRows, totalRows)
	}

	// Read compacted data to verify alice-updated is actually present.
	allEntries := readAllDataRows(t, ctx, s3, tm)
	foundAliceUpdated := false
	foundCharlie := false
	foundOriginalAlice := false
	for _, row := range allEntries {
		name, _ := row["name"].(string)
		if name == "alice-updated" {
			foundAliceUpdated = true
		}
		if name == "alice" {
			foundOriginalAlice = true
		}
		if name == "charlie" {
			foundCharlie = true
		}
	}
	if !foundAliceUpdated {
		t.Fatal("DATA LOSS: alice-updated not found after compaction — update was incorrectly deleted")
	}
	if foundOriginalAlice {
		t.Fatal("original alice should have been deleted by equality delete")
	}
	if foundCharlie {
		t.Fatal("charlie should have been deleted")
	}
	t.Logf("Verified: alice-updated present, original alice removed, charlie removed")
}

// readAllDataRows reads all data files from the current snapshot and returns all rows.
func readAllDataRows(t *testing.T, ctx context.Context, s3 *testCompactStorage, tm *TableMetadata) []map[string]any {
	t.Helper()
	mlData := mustDownload(t, ctx, s3, tm.CurrentManifestList())
	manifests, err := ReadManifestList(mlData)
	if err != nil {
		t.Fatalf("ReadManifestList: %v", err)
	}

	ts := testSchema()
	var allRows []map[string]any
	for _, mfi := range manifests {
		if mfi.Content != 0 {
			continue
		}
		mData := mustDownload(t, ctx, s3, mfi.Path)
		entries, err := ReadManifest(mData)
		if err != nil {
			t.Fatalf("ReadManifest: %v", err)
		}
		for _, e := range entries {
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			key, _ := KeyFromURI(e.DataFile.Path)
			data, err := s3.Download(ctx, key)
			if err != nil {
				t.Fatalf("Download %s: %v", key, err)
			}
			rows, err := ReadParquetRows(data, ts)
			if err != nil {
				t.Fatalf("ReadParquetRows: %v", err)
			}
			allRows = append(allRows, rows...)
		}
	}
	return allRows
}

func TestCompact_BelowThreshold(t *testing.T) {
	ctx := context.Background()
	ts := testSchema()
	s3 := newTestStorage()
	catalog := newTestCatalog()
	ns := "test_ns"

	catalog.CreateTable(ctx, ns, "orders", ts, "s3://test-bucket/test_ns.db/orders", nil)

	tw := NewTableWriter(TableWriteConfig{
		Namespace:   ns,
		IcebergName: "orders",
		SrcSchema:   ts,
		TargetSize:  1024 * 1024,
		Concurrency: 2,
	}, catalog, s3)

	// Write just 2 batches — below threshold.
	batches := [][]RowState{
		{{Op: "I", Row: map[string]any{"id": int32(1), "name": "a", "price": int32(100)}}},
		{{Op: "I", Row: map[string]any{"id": int32(2), "name": "b", "price": int32(200)}}},
	}
	writeTestData(t, ctx, tw, catalog, s3, ns, batches, ts.PK)

	cc := CompactionConfig{DataFileThreshold: 20, DeleteFileThreshold: 10}
	prepared, err := tw.Compact(ctx, ts.PK, cc)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if prepared != nil {
		t.Fatal("expected nil (below threshold), got compaction result")
	}
}

func mustDownload(t *testing.T, ctx context.Context, s3 *testCompactStorage, uri string) []byte {
	t.Helper()
	key, err := KeyFromURI(uri)
	if err != nil {
		t.Fatalf("KeyFromURI(%s): %v", uri, err)
	}
	data, err := s3.Download(ctx, key)
	if err != nil {
		t.Fatalf("Download(%s): %v", key, err)
	}
	return data
}
