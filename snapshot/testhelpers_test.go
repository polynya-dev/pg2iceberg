//go:build integration

package snapshot_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

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

// --- In-memory test doubles ---

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
		if len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix) {
			result = append(result, iceberg.ObjectInfo{Key: key, LastModified: time.Now().Add(-1 * time.Hour)})
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

type memCatalog struct {
	mu        sync.Mutex
	tables    map[string]*iceberg.TableMetadata
	manifests map[string][]iceberg.ManifestFileInfo
}

func newMemCatalog() *memCatalog {
	return &memCatalog{
		tables:    make(map[string]*iceberg.TableMetadata),
		manifests: make(map[string][]iceberg.ManifestFileInfo),
	}
}

func (c *memCatalog) EnsureNamespace(_ context.Context, ns string) error { return nil }

func (c *memCatalog) LoadTable(_ context.Context, ns, table string) (*iceberg.TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tables[ns+"."+table], nil
}

func (c *memCatalog) CreateTable(_ context.Context, ns, table string, ts *postgres.TableSchema, location string, partSpec *iceberg.PartitionSpec) (*iceberg.TableMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tm := &iceberg.TableMetadata{}
	tm.Metadata.FormatVersion = 2
	tm.Metadata.Location = location
	c.tables[ns+"."+table] = tm
	return tm, nil
}

func (c *memCatalog) CommitSnapshot(_ context.Context, ns, table string, currentSnapshotID int64, sc iceberg.SnapshotCommit) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := ns + "." + table
	tm := c.tables[key]
	if tm == nil {
		return fmt.Errorf("table not found: %s", key)
	}
	tm.Metadata.CurrentSnapshotID = sc.SnapshotID
	tm.Metadata.LastSequenceNumber = sc.SequenceNumber
	snap := struct {
		SnapshotID     int64             `json:"snapshot-id"`
		TimestampMs    int64             `json:"timestamp-ms"`
		ManifestList   string            `json:"manifest-list"`
		Summary        map[string]string `json:"summary"`
		SchemaID       int               `json:"schema-id"`
		SequenceNumber int64             `json:"sequence-number"`
	}{
		SnapshotID: sc.SnapshotID, TimestampMs: sc.TimestampMs,
		ManifestList: sc.ManifestListPath, Summary: sc.Summary,
		SchemaID: sc.SchemaID, SequenceNumber: sc.SequenceNumber,
	}
	tm.Metadata.Snapshots = append(tm.Metadata.Snapshots, snap)
	return nil
}

func (c *memCatalog) CommitTransaction(ctx context.Context, ns string, commits []iceberg.TableCommit) error {
	for _, tc := range commits {
		tcNs := tc.Namespace
		if tcNs == "" {
			tcNs = ns
		}
		if err := c.CommitSnapshot(ctx, tcNs, tc.Table, tc.CurrentSnapshotID, tc.Snapshot); err != nil {
			return err
		}
		if tc.NewManifests != nil {
			c.SetManifests(tcNs, tc.Table, tc.NewManifests)
		}
	}
	return nil
}

func (c *memCatalog) EvolveSchema(_ context.Context, ns, table string, currentSchemaID int, newSchema *postgres.TableSchema) (int, error) {
	return currentSchemaID + 1, nil
}

func (c *memCatalog) Manifests(ns, table string) []iceberg.ManifestFileInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.manifests[ns+"."+table]
}

func (c *memCatalog) SetManifests(ns, table string, manifests []iceberg.ManifestFileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manifests[ns+"."+table] = manifests
}

func (c *memCatalog) DataFiles(ns, table string) []iceberg.DataFileInfo   { return nil }
func (c *memCatalog) SetDataFiles(ns, table string, _ []iceberg.DataFileInfo) {}
func (c *memCatalog) FileIndex(ns, table string) *iceberg.FileIndex       { return nil }
func (c *memCatalog) SetFileIndex(ns, table string, _ *iceberg.FileIndex) {}

// readAllDataFileRows reads all rows from all data files in a table's current snapshot.
func readAllDataFileRows(t *testing.T, ctx context.Context, s3 *memStorage, tm *iceberg.TableMetadata) []map[string]any {
	t.Helper()
	if tm == nil || tm.Metadata.CurrentSnapshotID == 0 {
		return nil
	}
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil
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
	var allRows []map[string]any
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
			if e.DataFile.Content != 0 {
				continue
			}
			dfKey, err := iceberg.KeyFromURI(e.DataFile.Path)
			if err != nil {
				t.Fatalf("parse data file URI: %v", err)
			}
			dfData, err := s3.Download(ctx, dfKey)
			if err != nil {
				t.Fatalf("download data file: %v", err)
			}
			rows, err := iceberg.ReadParquetRows(dfData, nil)
			if err != nil {
				t.Fatalf("read parquet rows: %v", err)
			}
			allRows = append(allRows, rows...)
		}
	}
	return allRows
}
