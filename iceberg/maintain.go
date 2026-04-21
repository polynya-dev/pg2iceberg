package iceberg

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

var maintainTracer = otel.Tracer("pg2iceberg/maintain")

// MaintenanceCatalog is the subset of catalog operations needed by maintenance.
type MaintenanceCatalog interface {
	LoadTable(ctx context.Context, ns, table string) (*TableMetadata, error)
	RemoveSnapshots(ctx context.Context, ns, table string, snapshotIDs []int64) error
}

// MaintenanceConfig holds parameters for table maintenance.
type MaintenanceConfig struct {
	SnapshotRetention time.Duration // remove snapshots older than this
	OrphanGracePeriod time.Duration // don't delete files newer than this
}

// MaintenanceResult captures per-phase counts and timing for one MaintainTable run.
type MaintenanceResult struct {
	SnapshotsExpired int
	ExpireDuration   time.Duration
	OrphansDeleted   int
	OrphanBytesFreed int64
	CleanupDuration  time.Duration
}

// MaintainTable runs snapshot expiry and orphan file deletion for a single table.
// Returns per-phase stats so callers can record them to the meta tables.
func MaintainTable(ctx context.Context, catalog MaintenanceCatalog, s3 ObjectStorage, ns, table string, cfg MaintenanceConfig) (MaintenanceResult, error) {
	ctx, span := maintainTracer.Start(ctx, "maintain.Table "+table, trace.WithAttributes(
		attribute.String("iceberg.namespace", ns),
		attribute.String("iceberg.table", table),
	))
	defer span.End()

	var result MaintenanceResult
	start := time.Now()
	defer func() {
		pipeline.MaintenanceDurationSeconds.WithLabelValues(table).Observe(time.Since(start).Seconds())
	}()
	pipeline.MaintenanceRunsTotal.WithLabelValues(table).Inc()

	// 1. Load table metadata.
	tm, err := catalog.LoadTable(ctx, ns, table)
	if err != nil {
		pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
		return result, fmt.Errorf("load table %s: %w", table, err)
	}
	if tm == nil {
		return result, nil // table doesn't exist
	}

	// 2. Expire old snapshots.
	expireStart := time.Now()
	expired, err := expireSnapshots(ctx, catalog, ns, table, tm, cfg.SnapshotRetention)
	result.ExpireDuration = time.Since(expireStart)
	result.SnapshotsExpired = expired
	if err != nil {
		pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
		return result, fmt.Errorf("expire snapshots for %s: %w", table, err)
	}
	if expired > 0 {
		pipeline.MaintenanceSnapshotsExpiredTotal.WithLabelValues(table).Add(float64(expired))
		log.Printf("[maintain] %s: expired %d snapshots", table, expired)

		// Reload metadata to reflect post-expiry state.
		// MetadataStore.RemoveSnapshots already trims the cache, so this
		// returns the accurate post-expiry snapshot list.
		tm, err = catalog.LoadTable(ctx, ns, table)
		if err != nil {
			pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
			return result, fmt.Errorf("reload table %s after expiry: %w", table, err)
		}
	}

	// 3. Clean orphan files.
	cleanStart := time.Now()
	deleted, bytesFreed, err := cleanOrphanFiles(ctx, s3, ns, table, tm, cfg.OrphanGracePeriod)
	result.CleanupDuration = time.Since(cleanStart)
	result.OrphansDeleted = deleted
	result.OrphanBytesFreed = bytesFreed
	if err != nil {
		pipeline.MaintenanceErrorsTotal.WithLabelValues(table).Inc()
		return result, fmt.Errorf("clean orphans for %s: %w", table, err)
	}
	if deleted > 0 {
		pipeline.MaintenanceOrphansDeletedTotal.WithLabelValues(table).Add(float64(deleted))
		log.Printf("[maintain] %s: deleted %d orphan files (%d bytes)", table, deleted, bytesFreed)
	}

	return result, nil
}

// expireSnapshots removes snapshots older than retention, never removing the current snapshot.
func expireSnapshots(ctx context.Context, catalog MaintenanceCatalog, ns, table string, tm *TableMetadata, retention time.Duration) (int, error) {
	if tm.Metadata.CurrentSnapshotID <= 0 || len(tm.Metadata.Snapshots) <= 1 {
		return 0, nil
	}

	cutoff := time.Now().Add(-retention).UnixMilli()
	var toRemove []int64

	for _, snap := range tm.Metadata.Snapshots {
		if snap.SnapshotID == tm.Metadata.CurrentSnapshotID {
			continue // never remove current
		}
		if snap.TimestampMs < cutoff {
			toRemove = append(toRemove, snap.SnapshotID)
		}
	}

	if len(toRemove) == 0 {
		return 0, nil
	}

	if err := catalog.RemoveSnapshots(ctx, ns, table, toRemove); err != nil {
		return 0, err
	}
	return len(toRemove), nil
}

// cleanOrphanFiles lists all S3 objects under the table path, compares against
// files referenced by surviving snapshots, and deletes unreferenced files that
// are older than the grace period.
//
// Parallelism:
//   - ListObjects runs concurrently with manifest walks
//   - Each snapshot's manifest tree is walked concurrently
//   - Within each snapshot, manifest downloads are concurrent
//   - Delete batches are concurrent (handled by S3Client.DeleteObjects)
func cleanOrphanFiles(ctx context.Context, s3 ObjectStorage, ns, table string, tm *TableMetadata, gracePeriod time.Duration) (int, int64, error) {
	if tm == nil || tm.Metadata.CurrentSnapshotID <= 0 {
		return 0, 0, nil
	}

	// Run ListObjects and manifest walks concurrently.
	g, gctx := errgroup.WithContext(ctx)

	// 1. ListObjects in background.
	var objects []ObjectInfo
	prefix := TableBasePath(tm.Metadata.Location, ns, table) + "/"
	g.Go(func() error {
		var err error
		objects, err = s3.ListObjects(gctx, prefix)
		if err != nil {
			return fmt.Errorf("list objects under %s: %w", prefix, err)
		}
		return nil
	})

	// 2. Walk all snapshots' manifests concurrently to collect referenced keys.
	var mu sync.Mutex
	referenced := make(map[string]bool)

	addRef := func(key string) {
		mu.Lock()
		referenced[key] = true
		mu.Unlock()
	}

	// The active metadata.json must never be deleted.
	if tm.MetadataLocation != "" {
		if metaKey, err := KeyFromURI(tm.MetadataLocation); err == nil {
			addRef(metaKey)
		}
	}

	for _, snap := range tm.Metadata.Snapshots {
		if snap.ManifestList == "" {
			continue
		}
		g.Go(func() error {
			return collectSnapshotRefs(gctx, s3, snap.ManifestList, addRef)
		})
	}

	if err := g.Wait(); err != nil {
		return 0, 0, err
	}

	// 3. Diff: orphans = listed - referenced - grace-period-protected.
	graceCutoff := time.Now().Add(-gracePeriod)
	var orphanKeys []string
	var orphanBytes int64
	for _, obj := range objects {
		if referenced[obj.Key] {
			continue
		}
		if obj.LastModified.After(graceCutoff) {
			continue
		}
		orphanKeys = append(orphanKeys, obj.Key)
		orphanBytes += obj.Size
	}

	if len(orphanKeys) == 0 {
		return 0, 0, nil
	}

	// 4. Delete orphans (batches parallelized inside S3Client.DeleteObjects).
	if err := s3.DeleteObjects(ctx, orphanKeys); err != nil {
		return 0, 0, fmt.Errorf("delete orphan files: %w", err)
	}
	return len(orphanKeys), orphanBytes, nil
}

// collectSnapshotRefs downloads a snapshot's manifest list and all its manifests,
// calling addRef for every referenced S3 key. Manifest downloads within the
// snapshot are parallelized.
func collectSnapshotRefs(ctx context.Context, s3 ObjectStorage, manifestListURI string, addRef func(string)) error {
	mlKey, err := KeyFromURI(manifestListURI)
	if err != nil {
		return fmt.Errorf("parse manifest list URI %s: %w", manifestListURI, err)
	}
	addRef(mlKey)

	mlData, err := DownloadWithRetry(ctx, s3, mlKey)
	if err != nil {
		return fmt.Errorf("download manifest list %s: %w", manifestListURI, err)
	}
	manifests, err := ReadManifestList(mlData)
	if err != nil {
		return fmt.Errorf("read manifest list %s: %w", manifestListURI, err)
	}

	// Download all manifests within this snapshot concurrently.
	g, gctx := errgroup.WithContext(ctx)
	for _, mfi := range manifests {
		g.Go(func() error {
			mKey, err := KeyFromURI(mfi.Path)
			if err != nil {
				return fmt.Errorf("parse manifest URI %s: %w", mfi.Path, err)
			}
			addRef(mKey)

			mData, err := DownloadWithRetry(gctx, s3, mKey)
			if err != nil {
				return fmt.Errorf("download manifest %s: %w", mfi.Path, err)
			}
			entries, err := ReadManifest(mData)
			if err != nil {
				return fmt.Errorf("read manifest %s: %w", mfi.Path, err)
			}

			for _, e := range entries {
				if e.Status == 2 {
					continue
				}
				dfKey, err := KeyFromURI(e.DataFile.Path)
				if err != nil {
					continue
				}
				addRef(dfKey)
			}
			return nil
		})
	}

	return g.Wait()
}
