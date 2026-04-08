package iceberg

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

// CatalogWithCache extends Catalog with manifest cache operations.
// Implemented by CachedCatalog; allows callers to read/write the shared
// manifest cache without maintaining their own copies.
type CatalogWithCache interface {
	Catalog
	Manifests(ns, table string) []ManifestFileInfo
	SetManifests(ns, table string, manifests []ManifestFileInfo)
}

// CachedCatalog wraps a CatalogClient with an in-memory write-through cache.
//
// pg2iceberg is a single-writer system: it is the only process mutating its
// Iceberg tables. After cold start (the first LoadTable per table), every
// subsequent LoadTable can be served from cache because we update the cache
// after every successful write (CommitSnapshot, CommitTransaction, EvolveSchema,
// CreateTable).
//
// Writes always go to the real catalog first; the cache is updated only after
// the remote call succeeds. Reads return cached data on hit, falling through
// to the real catalog on miss.
type CachedCatalog struct {
	inner *CatalogClient

	mu         sync.RWMutex
	tables     map[tableKey]*cachedTable
	namespaces map[string]bool
	maxTables  int // LRU eviction threshold
}

type tableKey struct {
	ns, table string
}

// cachedTable holds the subset of TableMetadata that callers need between
// commits: snapshot ID, sequence number, schema ID, location, and the
// manifest list. This is enough to avoid any LoadTable round-trip in
// steady state.
type cachedTable struct {
	tm         *TableMetadata    // full metadata from last load/create/commit
	manifests  []ManifestFileInfo // current manifest list (nil = not yet loaded)
	lastAccess time.Time
}

// NewCachedCatalog creates a write-through cache over the given catalog client.
// maxTables bounds memory; 0 means use default (100).
func NewCachedCatalog(inner *CatalogClient, maxTables int) *CachedCatalog {
	if maxTables <= 0 {
		maxTables = 100
	}
	return &CachedCatalog{
		inner:      inner,
		tables:     make(map[tableKey]*cachedTable),
		namespaces: make(map[string]bool),
		maxTables:  maxTables,
	}
}

// Inner returns the underlying CatalogClient for operations that need
// to bypass the cache (e.g. vended credential refresh).
func (cc *CachedCatalog) Inner() *CatalogClient {
	return cc.inner
}

// --- Catalog interface implementation ---

func (cc *CachedCatalog) EnsureNamespace(ctx context.Context, ns string) error {
	cc.mu.RLock()
	known := cc.namespaces[ns]
	cc.mu.RUnlock()
	if known {
		_, span := tracer.Start(ctx, "catalog.EnsureNamespace (cached)", trace.WithAttributes(
			attribute.String("iceberg.namespace", ns),
			attribute.Bool("cache.hit", true),
		))
		span.End()
		return nil
	}

	if err := cc.inner.EnsureNamespace(ctx, ns); err != nil {
		return err
	}

	cc.mu.Lock()
	cc.namespaces[ns] = true
	cc.mu.Unlock()
	return nil
}

func (cc *CachedCatalog) LoadTable(ctx context.Context, ns, table string) (*TableMetadata, error) {
	key := tableKey{ns, table}

	cc.mu.RLock()
	ct, ok := cc.tables[key]
	cc.mu.RUnlock()

	if ok {
		_, span := tracer.Start(ctx, "catalog.LoadTable "+table+" (cached)", trace.WithAttributes(
			attribute.String("iceberg.namespace", ns),
			attribute.String("iceberg.table", table),
			attribute.Bool("cache.hit", true),
		))
		span.End()

		cc.mu.Lock()
		ct.lastAccess = time.Now()
		cc.mu.Unlock()
		return ct.tm, nil
	}

	// Cache miss — cold start path. The inner LoadTable creates its own span.
	tm, err := cc.inner.LoadTable(ctx, ns, table)
	if err != nil {
		return nil, err
	}

	if tm != nil {
		cc.mu.Lock()
		cc.putLocked(key, &cachedTable{tm: tm, lastAccess: time.Now()})
		cc.mu.Unlock()
	}

	return tm, nil
}

func (cc *CachedCatalog) CreateTable(ctx context.Context, ns, table string, ts *postgres.TableSchema, location string, partSpec *PartitionSpec) (*TableMetadata, error) {
	tm, err := cc.inner.CreateTable(ctx, ns, table, ts, location, partSpec)
	if err != nil {
		return nil, err
	}

	key := tableKey{ns, table}
	cc.mu.Lock()
	cc.putLocked(key, &cachedTable{tm: tm, lastAccess: time.Now()})
	cc.mu.Unlock()

	return tm, nil
}

func (cc *CachedCatalog) CommitSnapshot(ctx context.Context, ns, table string, currentSnapshotID int64, snapshot SnapshotCommit) error {
	if err := cc.inner.CommitSnapshot(ctx, ns, table, currentSnapshotID, snapshot); err != nil {
		return err
	}

	// Update cache with the new snapshot state.
	key := tableKey{ns, table}
	cc.mu.Lock()
	cc.applySnapshotLocked(key, snapshot)
	cc.mu.Unlock()

	return nil
}

func (cc *CachedCatalog) CommitTransaction(ctx context.Context, ns string, commits []TableCommit) error {
	if err := cc.inner.CommitTransaction(ctx, ns, commits); err != nil {
		return err
	}

	cc.mu.Lock()
	for _, tc := range commits {
		key := tableKey{ns, tc.Table}
		cc.applySnapshotLocked(key, tc.Snapshot)
	}
	cc.mu.Unlock()

	return nil
}

func (cc *CachedCatalog) EvolveSchema(ctx context.Context, ns, table string, currentSchemaID int, newSchema *postgres.TableSchema) (int, error) {
	newID, err := cc.inner.EvolveSchema(ctx, ns, table, currentSchemaID, newSchema)
	if err != nil {
		return 0, err
	}

	key := tableKey{ns, table}
	cc.mu.Lock()
	if ct, ok := cc.tables[key]; ok {
		ct.tm.Metadata.CurrentSchemaID = newID
		ct.lastAccess = time.Now()
	}
	cc.mu.Unlock()

	return newID, nil
}

// --- Manifest cache ---

// Manifests returns the cached manifest list for a table, or nil if not cached.
func (cc *CachedCatalog) Manifests(ns, table string) []ManifestFileInfo {
	key := tableKey{ns, table}
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if ct, ok := cc.tables[key]; ok {
		return ct.manifests
	}
	return nil
}

// SetManifests updates the cached manifest list for a table.
func (cc *CachedCatalog) SetManifests(ns, table string, manifests []ManifestFileInfo) {
	key := tableKey{ns, table}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if ct, ok := cc.tables[key]; ok {
		ct.manifests = manifests
		ct.lastAccess = time.Now()
	}
}

// --- GetConfig pass-through ---

func (cc *CachedCatalog) GetConfig(ctx context.Context, warehouse string) (*CatalogConfig, error) {
	return cc.inner.GetConfig(ctx, warehouse)
}

// SetPrefix delegates to the inner CatalogClient.
func (cc *CachedCatalog) SetPrefix(prefix string) {
	cc.inner.SetPrefix(prefix)
}

// --- Internal helpers ---

// applySnapshotLocked updates cached table metadata after a successful commit.
// Must be called with cc.mu held for writing.
func (cc *CachedCatalog) applySnapshotLocked(key tableKey, snapshot SnapshotCommit) {
	ct, ok := cc.tables[key]
	if !ok {
		return
	}
	ct.tm.Metadata.CurrentSnapshotID = snapshot.SnapshotID
	ct.tm.Metadata.LastSequenceNumber = snapshot.SequenceNumber
	ct.tm.Metadata.LastUpdatedMs = snapshot.TimestampMs

	// Append snapshot to the metadata snapshots list.
	ct.tm.Metadata.Snapshots = append(ct.tm.Metadata.Snapshots, struct {
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

	ct.lastAccess = time.Now()
}

// putLocked inserts a cache entry, evicting the LRU entry if at capacity.
// Must be called with cc.mu held for writing.
func (cc *CachedCatalog) putLocked(key tableKey, ct *cachedTable) {
	cc.tables[key] = ct

	if len(cc.tables) > cc.maxTables {
		cc.evictLRULocked()
	}
}

// evictLRULocked removes the least recently accessed table entry.
// Must be called with cc.mu held for writing.
func (cc *CachedCatalog) evictLRULocked() {
	var oldestKey tableKey
	var oldestTime time.Time
	first := true

	for k, ct := range cc.tables {
		if first || ct.lastAccess.Before(oldestTime) {
			oldestKey = k
			oldestTime = ct.lastAccess
			first = false
		}
	}

	if !first {
		delete(cc.tables, oldestKey)
	}
}
