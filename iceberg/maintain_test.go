package iceberg

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// testMaintainCatalog implements MaintenanceCatalog for testing.
type testMaintainCatalog struct {
	*testCompactCatalog
}

func (c *testMaintainCatalog) RemoveSnapshots(_ context.Context, ns, table string, snapshotIDs []int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := ns + "." + table
	tm := c.tables[key]
	if tm == nil {
		return fmt.Errorf("table not found: %s", key)
	}
	remove := make(map[int64]bool)
	for _, id := range snapshotIDs {
		remove[id] = true
	}
	var kept []struct {
		SnapshotID     int64             `json:"snapshot-id"`
		TimestampMs    int64             `json:"timestamp-ms"`
		ManifestList   string            `json:"manifest-list"`
		Summary        map[string]string `json:"summary"`
		SchemaID       int               `json:"schema-id"`
		SequenceNumber int64             `json:"sequence-number"`
	}
	for _, s := range tm.Metadata.Snapshots {
		if !remove[s.SnapshotID] {
			kept = append(kept, s)
		}
	}
	tm.Metadata.Snapshots = kept
	return nil
}

// TestMaintainTable_PreservesMetadataFile verifies that orphan cleanup does not
// delete the active metadata.json file pointed to by TableMetadata.MetadataLocation.
func TestMaintainTable_PreservesMetadataFile(t *testing.T) {
	ctx := context.Background()
	ts := testSchema()
	s3 := newTestStorage()
	catalog := &testMaintainCatalog{newTestCatalog()}
	ns := "test_ns"

	// Create table + write some data so we have a snapshot.
	catalog.CreateTable(ctx, ns, "orders", ts, "s3://test-bucket/test_ns.db/orders", nil)

	tw := NewTableWriter(TableWriteConfig{
		Namespace:   ns,
		IcebergName: "orders",
		SrcSchema:   ts,
		TargetSize:  1024 * 1024,
		Concurrency: 2,
	}, catalog, s3)

	batch := []RowState{
		{Op: "I", Row: map[string]any{"id": int32(1), "name": "alice", "price": int32(100)}},
		{Op: "I", Row: map[string]any{"id": int32(2), "name": "bob", "price": int32(200)}},
	}
	writeTestData(t, ctx, tw, catalog.testCompactCatalog, s3, ns, [][]RowState{batch}, ts.PK)

	// Load table metadata and set MetadataLocation (simulating what the catalog returns).
	tm, err := catalog.LoadTable(ctx, ns, "orders")
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}

	metadataKey := TableBasePath(tm.Metadata.Location, ns, "orders") + "/metadata/00001-abc.metadata.json"
	metadataURI := s3.URIForKey(metadataKey)
	s3.Upload(ctx, metadataKey, []byte(`{"format-version":2}`))
	tm.MetadataLocation = metadataURI

	// Verify metadata file exists before maintenance.
	if _, err := s3.Download(ctx, metadataKey); err != nil {
		t.Fatalf("metadata file should exist before maintenance: %v", err)
	}

	// Run maintenance with zero grace period — all unreferenced old files are eligible.
	_, err = MaintainTable(ctx, catalog, s3, ns, "orders", MaintenanceConfig{
		SnapshotRetention: 24 * time.Hour,
		OrphanGracePeriod: 0,
	})
	if err != nil {
		t.Fatalf("MaintainTable: %v", err)
	}

	// The metadata.json file MUST still exist after maintenance.
	if _, err := s3.Download(ctx, metadataKey); err != nil {
		t.Errorf("BUG: metadata.json was deleted by orphan cleanup! MetadataLocation=%s", metadataURI)
	}
}
