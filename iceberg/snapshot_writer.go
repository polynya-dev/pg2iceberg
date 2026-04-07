package iceberg

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"golang.org/x/sync/errgroup"
)

// SnapshotWriterConfig holds configuration for direct snapshot writes
// to a materialized Iceberg table.
type SnapshotWriterConfig struct {
	Namespace   string
	IcebergName string
	SrcSchema   *postgres.TableSchema
	PartSpec    *PartitionSpec
	SchemaID    int
	TargetSize  int64
}

// SnapshotWriter accumulates rows into per-partition RollingWriters and
// commits directly to the materialized Iceberg table. This is an append-only
// writer (no equality deletes, no file index, no TOAST resolution).
//
// Concurrency is managed externally by the Snapshotter's worker pool —
// one table per worker, chunks sequential within a table. The SnapshotWriter
// does not create its own goroutines or pools.
type SnapshotWriter struct {
	cfg     SnapshotWriterConfig
	catalog Catalog
	s3      ObjectStorage

	// Per-partition writers, keyed by partition key string ("" for unpartitioned).
	partitions map[string]*snapshotPartition

	// Carried-forward manifests from prior commits.
	manifests []ManifestFileInfo
}

type snapshotPartition struct {
	writer     *RollingWriter
	partValues map[string]any
	partPath   string
}

// NewSnapshotWriter creates a new snapshot writer.
func NewSnapshotWriter(cfg SnapshotWriterConfig, catalog Catalog, s3 ObjectStorage) *SnapshotWriter {
	return &SnapshotWriter{
		cfg:        cfg,
		catalog:    catalog,
		s3:         s3,
		partitions: make(map[string]*snapshotPartition),
	}
}

// AddRow routes a row to the correct partition writer.
func (sw *SnapshotWriter) AddRow(row map[string]any) error {
	key := ""
	partitioned := sw.cfg.PartSpec != nil && !sw.cfg.PartSpec.IsUnpartitioned()
	if partitioned {
		key = sw.cfg.PartSpec.PartitionKey(row, sw.cfg.SrcSchema)
	}

	sp, ok := sw.partitions[key]
	if !ok {
		sp = &snapshotPartition{
			writer: NewRollingDataWriter(sw.cfg.SrcSchema, sw.cfg.TargetSize),
		}
		if partitioned {
			sp.partValues = sw.cfg.PartSpec.PartitionValues(row, sw.cfg.SrcSchema)
			sp.partPath = sw.cfg.PartSpec.PartitionPath(sp.partValues)
		}
		sw.partitions[key] = sp
	}

	return sp.writer.Add(row)
}

// RowCount returns the total number of rows accumulated across all partitions.
func (sw *SnapshotWriter) RowCount() int {
	total := 0
	for _, sp := range sw.partitions {
		total += sp.writer.Len()
	}
	return total
}

// Commit flushes all accumulated rows, uploads Parquet files to S3,
// assembles manifests, and commits an append snapshot to the catalog.
// Returns the number of rows committed. Returns 0 with no error if empty.
func (sw *SnapshotWriter) Commit(ctx context.Context) (int64, error) {
	cfg := sw.cfg
	partitioned := cfg.PartSpec != nil && !cfg.PartSpec.IsUnpartitioned()
	basePath := fmt.Sprintf("%s.db/%s", cfg.Namespace, cfg.IcebergName)

	// Flush all partition writers to get file chunks.
	type pendingFile struct {
		key             string
		chunk           FileChunk
		partitionValues map[string]any
	}

	var pending []pendingFile
	for key, sp := range sw.partitions {
		chunks, err := sp.writer.FlushAll()
		if err != nil {
			return 0, fmt.Errorf("flush partition %q: %w", key, err)
		}
		if len(chunks) == 0 {
			continue
		}

		var avroPartValues map[string]any
		if partitioned && sp.partValues != nil {
			avroPartValues = cfg.PartSpec.PartitionAvroValue(sp.partValues, cfg.SrcSchema)
		}
		for i, chunk := range chunks {
			var fileKey string
			if partitioned && sp.partPath != "" {
				fileKey = fmt.Sprintf("%s/data/%s/%s-snap-%d.parquet", basePath, sp.partPath, uuid.New().String(), i)
			} else {
				fileKey = fmt.Sprintf("%s/data/%s-snap-%d.parquet", basePath, uuid.New().String(), i)
			}
			pending = append(pending, pendingFile{
				key:             fileKey,
				chunk:           chunk,
				partitionValues: avroPartValues,
			})
		}
	}

	if len(pending) == 0 {
		return 0, nil
	}

	// Upload files concurrently — I/O-bound, no pooling needed.
	type uploadedFile struct {
		uri string
		pf  pendingFile
	}
	uploaded := make([]uploadedFile, len(pending))
	g, gctx := errgroup.WithContext(ctx)
	for i, pf := range pending {
		i, pf := i, pf
		g.Go(func() error {
			uri, err := sw.s3.Upload(gctx, pf.key, pf.chunk.Data)
			if err != nil {
				return fmt.Errorf("upload %s: %w", pf.key, err)
			}
			// Each goroutine writes to its own index — no mutex needed.
			uploaded[i] = uploadedFile{uri: uri, pf: pf}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, fmt.Errorf("upload snapshot files: %w", err)
	}

	// Load current table metadata for commit.
	tm, err := sw.catalog.LoadTable(ctx, cfg.Namespace, cfg.IcebergName)
	if err != nil {
		return 0, fmt.Errorf("load table: %w", err)
	}

	var prevSnapID int64
	seqNum := int64(1)
	if tm != nil {
		prevSnapID = tm.Metadata.CurrentSnapshotID
		seqNum = tm.Metadata.LastSequenceNumber + 1
	}

	// Load existing manifests (on first commit only).
	if sw.manifests == nil && tm != nil && tm.Metadata.CurrentSnapshotID > 0 {
		mlURI := tm.CurrentManifestList()
		if mlURI != "" {
			mlKey, err := KeyFromURI(mlURI)
			if err != nil {
				return 0, fmt.Errorf("parse manifest list URI: %w", err)
			}
			mlData, err := DownloadWithRetry(ctx, sw.s3, mlKey)
			if err != nil {
				return 0, fmt.Errorf("download manifest list: %w", err)
			}
			sw.manifests, err = ReadManifestList(mlData)
			if err != nil {
				return 0, fmt.Errorf("read manifest list: %w", err)
			}
		}
	}

	now := time.Now()
	snapshotID := now.UnixMilli()

	// Build manifest entries from uploaded files.
	entries := make([]ManifestEntry, len(uploaded))
	var totalRows int64
	for i, u := range uploaded {
		entries[i] = ManifestEntry{
			Status:     1,
			SnapshotID: snapshotID,
			DataFile: DataFileInfo{
				Path:            u.uri,
				FileSizeBytes:   int64(len(u.pf.chunk.Data)),
				RecordCount:     u.pf.chunk.RowCount,
				Content:         0,
				PartitionValues: u.pf.partitionValues,
			},
		}
		totalRows += u.pf.chunk.RowCount
	}

	// Write data manifest.
	manifestBytes, err := WriteManifest(cfg.SrcSchema, entries, seqNum, 0, cfg.PartSpec)
	if err != nil {
		return 0, fmt.Errorf("write manifest: %w", err)
	}
	manifestKey := fmt.Sprintf("%s/metadata/%s-snap-data.avro", basePath, uuid.New().String())
	manifestURI, err := sw.s3.Upload(ctx, manifestKey, manifestBytes)
	if err != nil {
		return 0, fmt.Errorf("upload manifest: %w", err)
	}

	newManifest := ManifestFileInfo{
		Path:           manifestURI,
		Length:         int64(len(manifestBytes)),
		Content:        0,
		SnapshotID:     snapshotID,
		AddedFiles:     len(entries),
		AddedRows:      totalRows,
		SequenceNumber: seqNum,
	}

	allManifests := append(sw.manifests, newManifest)

	// Write manifest list.
	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return 0, fmt.Errorf("write manifest list: %w", err)
	}
	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := sw.s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return 0, fmt.Errorf("upload manifest list: %w", err)
	}

	// Commit to catalog.
	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		SchemaID:         cfg.SchemaID,
		Summary: map[string]string{
			"operation": "append",
		},
	}

	if err := sw.catalog.CommitSnapshot(ctx, cfg.Namespace, cfg.IcebergName, prevSnapID, commit); err != nil {
		return 0, fmt.Errorf("commit snapshot: %w", err)
	}

	// Update manifest cache for next chunk.
	sw.manifests = allManifests

	log.Printf("[snapshot-writer] committed %d rows (%d files) to %s.%s (snapshot=%d, seq=%d)",
		totalRows, len(entries), cfg.Namespace, cfg.IcebergName, snapshotID, seqNum)

	// Clear partition writers for next chunk.
	sw.Reset()

	return totalRows, nil
}

// Reset clears in-memory row buffers for the next chunk without discarding
// the manifest cache (which carries forward between chunks).
func (sw *SnapshotWriter) Reset() {
	sw.partitions = make(map[string]*snapshotPartition)
}
