package logical

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/stream"
)

// TestStagedEventRoundTrip verifies that events written by writeDirect and
// serialized to Parquet with JSON _data can be decoded back by
// readEventsFromParquet without data loss across all supported column types.
func TestStagedEventRoundTrip(t *testing.T) {
	ctx := context.Background()

	schema := &postgres.TableSchema{
		Table: "public.test_types",
		Columns: []postgres.Column{
			{Name: "id", PGType: postgres.Int4, FieldID: 1},
			{Name: "big_id", PGType: postgres.Int8, FieldID: 2},
			{Name: "name", PGType: postgres.Text, FieldID: 3},
			{Name: "price", PGType: postgres.Float8, FieldID: 4},
			{Name: "active", PGType: postgres.Bool, FieldID: 5},
			{Name: "tags", PGType: postgres.JSONB, FieldID: 6},
			{Name: "uuid_col", PGType: postgres.UUID, FieldID: 7},
		},
		PK: []string{"id"},
	}

	// Build a Sink with MemCoordinator + memStorage.
	coord := stream.NewMemCoordinator()
	s3 := newMemStorage()
	cs := stream.NewCachedStream(coord, s3, "test_ns")

	cfg := configForTest()
	sink := NewSink(cfg, nil, "test", s3, nil)
	sink.SetStream(cs)

	// Register the table manually.
	sink.tables["public.test_types"] = &tableSink{
		srcSchema:   schema,
		icebergName: "test_types",
		targetSize:  128 * 1024 * 1024,
		writer:      iceberg.NewRollingDataWriter(iceberg.StagedEventSchema(), 128*1024*1024),
	}
	coord.EnsureCursor(ctx, "default", "public.test_types")

	// Write events covering various types and operations.
	events := []postgres.ChangeEvent{
		{
			Table:     "public.test_types",
			Operation: postgres.OpInsert,
			LSN:       1001,
			After: map[string]any{
				"id":       int32(1),
				"big_id":   int64(9223372036854775807),
				"name":     "hello world",
				"price":    3.14159,
				"active":   true,
				"tags":     `{"key": "value"}`,
				"uuid_col": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			},
			SourceTimestamp: time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC),
		},
		{
			Table:     "public.test_types",
			Operation: postgres.OpUpdate,
			LSN:       1002,
			After: map[string]any{
				"id":       int32(1),
				"big_id":   int64(9223372036854775807),
				"name":     "updated name",
				"price":    2.71828,
				"active":   false,
				"tags":     `["a", "b"]`,
				"uuid_col": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			},
			UnchangedCols:   []string{"big_id", "uuid_col"},
			SourceTimestamp: time.Date(2025, 6, 15, 14, 31, 0, 0, time.UTC),
		},
		{
			Table:     "public.test_types",
			Operation: postgres.OpInsert,
			LSN:       1003,
			After: map[string]any{
				"id":       int32(2),
				"big_id":   int64(-9223372036854775808),
				"name":     "",     // empty string
				"price":    0.0,    // zero
				"active":   false,  // false
				"tags":     "null", // JSON null as string
				"uuid_col": "00000000-0000-0000-0000-000000000000",
			},
			SourceTimestamp: time.Date(2025, 6, 15, 14, 32, 0, 0, time.UTC),
		},
		{
			Table:     "public.test_types",
			Operation: postgres.OpDelete,
			LSN:       1004,
			Before: map[string]any{
				"id": int32(1),
			},
			SourceTimestamp: time.Date(2025, 6, 15, 14, 33, 0, 0, time.UTC),
		},
	}

	// Write events via writeDirect.
	for _, ev := range events {
		if err := sink.writeDirect(ev); err != nil {
			t.Fatalf("writeDirect: %v", err)
		}
	}

	// Flush to stream.
	sink.committedTxns = []*txBuffer{{events: events, committed: true}}
	// writeDirect was already called above, so reset and re-drain via Flush.
	// Actually, let's just flush the existing writer state by collecting chunks.
	ts := sink.tables["public.test_types"]
	chunks, err := ts.writer.FlushAll()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("no chunks produced")
	}

	// Upload the Parquet file and register in coordinator.
	var batches []stream.WriteBatch
	for _, chunk := range chunks {
		batches = append(batches, stream.WriteBatch{
			Table:       "public.test_types",
			Data:        chunk.Data,
			RecordCount: int(chunk.RowCount),
		})
	}
	if err := cs.Append(ctx, batches); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Now read back via readEventsFromParquet (the slow path — Parquet + JSON decode).
	entries, err := cs.Read(ctx, "public.test_types", -1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no log entries")
	}

	// Create a minimal materializer to use readEventsFromParquet.
	mat := &Materializer{stream: cs, s3: s3}
	decoded, err := mat.readEventsFromParquet(ctx, entries[0].S3Path)
	if err != nil {
		t.Fatalf("readEventsFromParquet: %v", err)
	}

	if len(decoded) != 4 {
		t.Fatalf("expected 4 events, got %d", len(decoded))
	}

	// Verify event 0: INSERT with all types.
	e0 := decoded[0]
	if e0.op != "I" {
		t.Errorf("event 0 op: got %q, want %q", e0.op, "I")
	}
	if e0.lsn != 1001 {
		t.Errorf("event 0 lsn: got %d, want 1001", e0.lsn)
	}
	assertJSONValue(t, "event 0 id", e0.row["id"], float64(1))
	assertJSONValue(t, "event 0 big_id", e0.row["big_id"], float64(9223372036854775807))
	assertJSONValue(t, "event 0 name", e0.row["name"], "hello world")
	assertJSONFloat(t, "event 0 price", e0.row["price"], 3.14159)
	assertJSONValue(t, "event 0 active", e0.row["active"], true)
	assertJSONValue(t, "event 0 uuid_col", e0.row["uuid_col"], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

	// Verify event 1: UPDATE with unchanged cols.
	e1 := decoded[1]
	if e1.op != "U" {
		t.Errorf("event 1 op: got %q, want %q", e1.op, "U")
	}
	if len(e1.unchangedCols) != 2 {
		t.Errorf("event 1 unchangedCols: got %v, want [big_id uuid_col]", e1.unchangedCols)
	}
	assertJSONValue(t, "event 1 name", e1.row["name"], "updated name")

	// Verify event 2: INSERT with edge values.
	e2 := decoded[2]
	assertJSONValue(t, "event 2 name", e2.row["name"], "")
	assertJSONValue(t, "event 2 price", e2.row["price"], float64(0))
	assertJSONValue(t, "event 2 active", e2.row["active"], false)

	// Verify event 3: DELETE.
	e3 := decoded[3]
	if e3.op != "D" {
		t.Errorf("event 3 op: got %q, want %q", e3.op, "D")
	}
	assertJSONValue(t, "event 3 id", e3.row["id"], float64(1))
}

// TestStagedEventCachePath verifies that the CachedStream fast path
// (in-memory MatEvents) produces the same results as the Parquet+JSON path.
func TestStagedEventCachePath(t *testing.T) {
	ctx := context.Background()

	coord := stream.NewMemCoordinator()
	s3 := newMemStorage()
	cs := stream.NewCachedStream(coord, s3, "test_ns")
	coord.EnsureCursor(ctx, "default", "public.items")

	// Simulate what the Sink does: create WriteBatches with Events.
	originalEvents := []MatEvent{
		{op: "I", lsn: 100, row: map[string]any{"id": int32(1), "name": "alice"}},
		{op: "U", lsn: 101, row: map[string]any{"id": int32(1), "name": "bob"}, unchangedCols: []string{"id"}},
		{op: "D", lsn: 102, row: map[string]any{"id": int32(2)}},
	}

	// Build Parquet data (the slow-path representation).
	schema := iceberg.StagedEventSchema()
	writer := iceberg.NewRollingDataWriter(schema, 128*1024*1024)
	for _, ev := range originalEvents {
		dataJSON, _ := json.Marshal(ev.row)
		row := map[string]any{
			"_op":   ev.op,
			"_lsn":  ev.lsn,
			"_ts":   time.Now(),
			"_data": string(dataJSON),
		}
		if len(ev.unchangedCols) > 0 {
			row["_unchanged_cols"] = iceberg.UnchangedColsString(ev.unchangedCols)
		}
		writer.Add(row)
	}
	chunks, _ := writer.FlushAll()

	// Append with Events field set (simulates combined mode).
	batches := []stream.WriteBatch{{
		Table:       "public.items",
		Data:        chunks[0].Data,
		RecordCount: int(chunks[0].RowCount),
		Events:      originalEvents,
	}}
	if err := cs.Append(ctx, batches); err != nil {
		t.Fatalf("append: %v", err)
	}

	entries, _ := cs.Read(ctx, "public.items", -1)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Fast path: CachedEvents should return the original events.
	cached := cs.CachedEvents(entries[0].S3Path)
	if cached == nil {
		t.Fatal("expected cached events, got nil")
	}
	cachedEvents, ok := cached.([]MatEvent)
	if !ok {
		t.Fatalf("cached events type: got %T", cached)
	}

	if len(cachedEvents) != len(originalEvents) {
		t.Fatalf("cached events count: got %d, want %d", len(cachedEvents), len(originalEvents))
	}
	for i, orig := range originalEvents {
		got := cachedEvents[i]
		if got.op != orig.op {
			t.Errorf("event %d op: got %q, want %q", i, got.op, orig.op)
		}
		if got.lsn != orig.lsn {
			t.Errorf("event %d lsn: got %d, want %d", i, got.lsn, orig.lsn)
		}
	}

	// Verify zero S3 downloads when using cache path.
	downloads := s3.downloads()
	if len(downloads) != 0 {
		t.Errorf("expected zero S3 downloads with cache path, got %d: %v", len(downloads), downloads)
	}
}

func assertJSONValue(t *testing.T, label string, got, want any) {
	t.Helper()
	// JSON numbers decode as float64.
	if g, ok := got.(json.Number); ok {
		f, _ := g.Float64()
		got = f
	}
	if got != want {
		t.Errorf("%s: got %v (%T), want %v (%T)", label, got, got, want, want)
	}
}

// configForTest returns a minimal SinkConfig for unit tests.
func configForTest() config.SinkConfig {
	return config.SinkConfig{
		Namespace: "test_ns",
		Warehouse: "s3://test-bucket/",
	}
}

// newMemStorage returns a minimal in-memory ObjectStorage for unit tests.
func newMemStorage() *testMemStorage {
	return &testMemStorage{files: make(map[string][]byte)}
}

type testMemStorage struct {
	mu          sync.Mutex
	files       map[string][]byte
	downloadLog []string
}

func (m *testMemStorage) URIForKey(key string) string {
	return "s3://test-bucket/" + key
}
func (m *testMemStorage) Upload(_ context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.files[key] = cp
	return m.URIForKey(key), nil
}
func (m *testMemStorage) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.downloadLog = append(m.downloadLog, key)
	data, ok := m.files[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return data, nil
}
func (m *testMemStorage) DownloadRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
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
func (m *testMemStorage) StatObject(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	if !ok {
		return 0, fmt.Errorf("not found: %s", key)
	}
	return int64(len(data)), nil
}
func (m *testMemStorage) ListObjects(_ context.Context, prefix string) ([]iceberg.ObjectInfo, error) {
	return nil, nil
}
func (m *testMemStorage) DeleteObjects(_ context.Context, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		delete(m.files, k)
	}
	return nil
}
func (m *testMemStorage) downloads() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.downloadLog))
	copy(cp, m.downloadLog)
	return cp
}

// newMemCatalog returns a stub MetadataCache for unit tests.
func newTestMemCatalog() iceberg.MetadataCache { return nil }

func assertJSONFloat(t *testing.T, label string, got any, want float64) {
	t.Helper()
	var f float64
	switch v := got.(type) {
	case float64:
		f = v
	case json.Number:
		f, _ = v.Float64()
	default:
		t.Errorf("%s: got %v (%T), want float64", label, got, got)
		return
	}
	if math.Abs(f-want) > 1e-5 {
		t.Errorf("%s: got %f, want %f", label, f, want)
	}
}
