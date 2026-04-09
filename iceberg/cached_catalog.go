package iceberg

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

// MetadataCache is the single interface all Iceberg consumers use.
// It subsumes the Catalog interface and adds per-table caching for
// manifests, data files, and the PK→file index. Commit methods
// accept post-commit metadata in TableCommit and apply cache updates
// atomically — callers never need manual Set calls after a commit.
type MetadataCache interface {
	Catalog

	// Manifests returns the cached manifest list, or nil on cold start.
	Manifests(ns, table string) []ManifestFileInfo
	// SetManifests updates the manifest cache (used during cold-start seeding).
	SetManifests(ns, table string, manifests []ManifestFileInfo)

	// DataFiles returns cached parsed data file entries, or nil on cold start.
	DataFiles(ns, table string) []DataFileInfo
	// SetDataFiles updates the data file cache (used during cold-start seeding).
	SetDataFiles(ns, table string, files []DataFileInfo)

	// FileIndex returns the cached PK→file index, or nil if not yet built.
	FileIndex(ns, table string) *FileIndex
	// SetFileIndex stores a freshly-built file index (used on cold start).
	SetFileIndex(ns, table string, idx *FileIndex)
}

// tableStore caches all Iceberg metadata for a single table.
type tableStore struct {
	tm         *TableMetadata     // from catalog LoadTable/CreateTable
	manifests  []ManifestFileInfo // manifest list entries
	dataFiles  []DataFileInfo     // parsed manifest entries (data content only)
	fileIndex  *FileIndex         // PK→file mapping
	lastAccess time.Time
}

type tableKey struct {
	ns, table string
}

// MetadataStore implements MetadataCache with a write-through LRU cache
// over a CatalogClient.
//
// pg2iceberg is a single-writer system: after cold start, every LoadTable
// is a cache hit because we update the cache after every successful write.
// CommitSnapshot and CommitTransaction apply all cache updates atomically
// from the TableCommit metadata — no manual post-commit step needed.
type MetadataStore struct {
	inner *CatalogClient

	mu         sync.RWMutex
	tables     map[tableKey]*tableStore
	namespaces map[string]bool
	maxTables  int
}

// NewMetadataStore creates a write-through metadata cache.
// maxTables bounds memory via LRU eviction; 0 means default (100).
func NewMetadataStore(inner *CatalogClient, maxTables int) *MetadataStore {
	if maxTables <= 0 {
		maxTables = 100
	}
	return &MetadataStore{
		inner:      inner,
		tables:     make(map[tableKey]*tableStore),
		namespaces: make(map[string]bool),
		maxTables:  maxTables,
	}
}

// Inner returns the underlying CatalogClient for operations that need
// to bypass the cache (e.g. vended credential refresh).
func (ms *MetadataStore) Inner() *CatalogClient {
	return ms.inner
}

// --- Catalog interface ---

func (ms *MetadataStore) EnsureNamespace(ctx context.Context, ns string) error {
	ms.mu.RLock()
	known := ms.namespaces[ns]
	ms.mu.RUnlock()
	if known {
		_, span := tracer.Start(ctx, "catalog.EnsureNamespace (cached)", trace.WithAttributes(
			attribute.String("iceberg.namespace", ns),
			attribute.Bool("cache.hit", true),
		))
		span.End()
		return nil
	}

	if err := ms.inner.EnsureNamespace(ctx, ns); err != nil {
		return err
	}

	ms.mu.Lock()
	ms.namespaces[ns] = true
	ms.mu.Unlock()
	return nil
}

func (ms *MetadataStore) LoadTable(ctx context.Context, ns, table string) (*TableMetadata, error) {
	key := tableKey{ns, table}

	ms.mu.RLock()
	ts, ok := ms.tables[key]
	ms.mu.RUnlock()

	if ok {
		_, span := tracer.Start(ctx, "catalog.LoadTable "+table+" (cached)", trace.WithAttributes(
			attribute.String("iceberg.namespace", ns),
			attribute.String("iceberg.table", table),
			attribute.Bool("cache.hit", true),
		))
		span.End()

		ms.mu.Lock()
		ts.lastAccess = time.Now()
		ms.mu.Unlock()
		return ts.tm, nil
	}

	// Cache miss — cold start.
	tm, err := ms.inner.LoadTable(ctx, ns, table)
	if err != nil {
		return nil, err
	}

	if tm != nil {
		ms.mu.Lock()
		ms.putLocked(key, &tableStore{tm: tm, lastAccess: time.Now()})
		ms.mu.Unlock()
	}

	return tm, nil
}

func (ms *MetadataStore) CreateTable(ctx context.Context, ns, table string, schema *postgres.TableSchema, location string, partSpec *PartitionSpec) (*TableMetadata, error) {
	tm, err := ms.inner.CreateTable(ctx, ns, table, schema, location, partSpec)
	if err != nil {
		return nil, err
	}

	key := tableKey{ns, table}
	ms.mu.Lock()
	ms.putLocked(key, &tableStore{
		tm:        tm,
		fileIndex: NewFileIndex(), // seed empty so incremental updates work from first commit
		lastAccess: time.Now(),
	})
	ms.mu.Unlock()

	return tm, nil
}

// CommitSnapshot commits a snapshot and seamlessly updates all cached state
// from the TableCommit metadata (manifests, data files, file index).
func (ms *MetadataStore) CommitSnapshot(ctx context.Context, ns, table string, currentSnapshotID int64, snapshot SnapshotCommit) error {
	if err := ms.inner.CommitSnapshot(ctx, ns, table, currentSnapshotID, snapshot); err != nil {
		return err
	}

	key := tableKey{ns, table}
	ms.mu.Lock()
	ms.applySnapshotLocked(key, snapshot)
	ms.mu.Unlock()

	return nil
}

// CommitTransaction commits atomically and seamlessly updates all cached
// state for every table in the transaction. Post-commit metadata from each
// TableCommit (manifests, new data files, deleted PKs) is applied to the
// cache — callers never need a separate "apply" step.
func (ms *MetadataStore) CommitTransaction(ctx context.Context, ns string, commits []TableCommit) error {
	if err := ms.inner.CommitTransaction(ctx, ns, commits); err != nil {
		return err
	}

	ms.mu.Lock()
	for _, tc := range commits {
		key := tableKey{ns, tc.Table}
		ms.applySnapshotLocked(key, tc.Snapshot)
		ms.applyPostCommitLocked(key, tc)
	}
	ms.mu.Unlock()

	return nil
}

func (ms *MetadataStore) EvolveSchema(ctx context.Context, ns, table string, currentSchemaID int, newSchema *postgres.TableSchema) (int, error) {
	newID, err := ms.inner.EvolveSchema(ctx, ns, table, currentSchemaID, newSchema)
	if err != nil {
		return 0, err
	}

	key := tableKey{ns, table}
	ms.mu.Lock()
	if ts, ok := ms.tables[key]; ok {
		ts.tm.Metadata.CurrentSchemaID = newID
		ts.lastAccess = time.Now()
	}
	ms.mu.Unlock()

	return newID, nil
}

// --- Metadata cache ---

func (ms *MetadataStore) Manifests(ns, table string) []ManifestFileInfo {
	key := tableKey{ns, table}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ts, ok := ms.tables[key]; ok {
		return ts.manifests
	}
	return nil
}

func (ms *MetadataStore) SetManifests(ns, table string, manifests []ManifestFileInfo) {
	key := tableKey{ns, table}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ts, ok := ms.tables[key]; ok {
		ts.manifests = manifests
		ts.lastAccess = time.Now()
	}
}

func (ms *MetadataStore) DataFiles(ns, table string) []DataFileInfo {
	key := tableKey{ns, table}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ts, ok := ms.tables[key]; ok {
		return ts.dataFiles
	}
	return nil
}

func (ms *MetadataStore) SetDataFiles(ns, table string, files []DataFileInfo) {
	key := tableKey{ns, table}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ts, ok := ms.tables[key]; ok {
		ts.dataFiles = files
		ts.lastAccess = time.Now()
	}
}

func (ms *MetadataStore) FileIndex(ns, table string) *FileIndex {
	key := tableKey{ns, table}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ts, ok := ms.tables[key]; ok {
		return ts.fileIndex
	}
	return nil
}

func (ms *MetadataStore) SetFileIndex(ns, table string, idx *FileIndex) {
	key := tableKey{ns, table}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ts, ok := ms.tables[key]; ok {
		ts.fileIndex = idx
		ts.lastAccess = time.Now()
	}
}

// --- Maintenance ---

// RemoveSnapshots removes the specified snapshots from catalog metadata and
// trims the cached snapshot list.
func (ms *MetadataStore) RemoveSnapshots(ctx context.Context, ns, table string, snapshotIDs []int64) error {
	if err := ms.inner.RemoveSnapshots(ctx, ns, table, snapshotIDs); err != nil {
		return err
	}

	removedSet := make(map[int64]bool, len(snapshotIDs))
	for _, id := range snapshotIDs {
		removedSet[id] = true
	}

	key := tableKey{ns, table}
	ms.mu.Lock()
	ms.trimSnapshotsLocked(key, removedSet)
	ms.mu.Unlock()
	return nil
}

// trimSnapshotsLocked removes expired snapshot entries from the cached metadata.
func (ms *MetadataStore) trimSnapshotsLocked(key tableKey, removedIDs map[int64]bool) {
	ts, ok := ms.tables[key]
	if !ok {
		return
	}
	kept := ts.tm.Metadata.Snapshots[:0]
	for _, snap := range ts.tm.Metadata.Snapshots {
		if !removedIDs[snap.SnapshotID] {
			kept = append(kept, snap)
		}
	}
	ts.tm.Metadata.Snapshots = kept
	ts.lastAccess = time.Now()
}

// --- GetConfig / SetPrefix pass-through ---

func (ms *MetadataStore) GetConfig(ctx context.Context, warehouse string) (*CatalogConfig, error) {
	return ms.inner.GetConfig(ctx, warehouse)
}

func (ms *MetadataStore) SetPrefix(prefix string) {
	ms.inner.SetPrefix(prefix)
}

// --- Internal helpers ---

// applySnapshotLocked updates cached TableMetadata after a successful commit.
func (ms *MetadataStore) applySnapshotLocked(key tableKey, snapshot SnapshotCommit) {
	ts, ok := ms.tables[key]
	if !ok {
		return
	}
	ts.tm.Metadata.CurrentSnapshotID = snapshot.SnapshotID
	ts.tm.Metadata.LastSequenceNumber = snapshot.SequenceNumber
	ts.tm.Metadata.LastUpdatedMs = snapshot.TimestampMs

	ts.tm.Metadata.Snapshots = append(ts.tm.Metadata.Snapshots, struct {
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
	})

	ts.lastAccess = time.Now()
}

// applyPostCommitLocked updates manifests, data files, and file index
// from a TableCommit's post-commit metadata. This is the seamless part:
// callers put everything in TableCommit, the store applies it all.
func (ms *MetadataStore) applyPostCommitLocked(key tableKey, tc TableCommit) {
	ts, ok := ms.tables[key]
	if !ok {
		return
	}

	// Update manifest list.
	if tc.NewManifests != nil {
		ts.manifests = tc.NewManifests
	}

	// Update file index incrementally.
	if ts.fileIndex != nil {
		for _, pk := range tc.DeletedPKs {
			if filePath, ok := ts.fileIndex.PkToFile[pk]; ok {
				delete(ts.fileIndex.PkToFile, pk)
				if pks, ok := ts.fileIndex.FilePKs[filePath]; ok {
					delete(pks, pk)
				}
			}
		}
		for _, fe := range tc.NewDataFiles {
			ts.fileIndex.AddFile(fe.DataFile, fe.PKKeys)
		}
		ts.fileIndex.SnapshotID = tc.Snapshot.SnapshotID
	}

	ts.lastAccess = time.Now()
}

// putLocked inserts a cache entry, evicting the LRU entry if at capacity.
func (ms *MetadataStore) putLocked(key tableKey, ts *tableStore) {
	ms.tables[key] = ts

	if len(ms.tables) > ms.maxTables {
		ms.evictLRULocked()
	}
}

// evictLRULocked removes the least recently accessed table entry.
func (ms *MetadataStore) evictLRULocked() {
	var oldestKey tableKey
	var oldestTime time.Time
	first := true

	for k, ts := range ms.tables {
		if first || ts.lastAccess.Before(oldestTime) {
			oldestKey = k
			oldestTime = ts.lastAccess
			first = false
		}
	}

	if !first {
		delete(ms.tables, oldestKey)
	}
}
