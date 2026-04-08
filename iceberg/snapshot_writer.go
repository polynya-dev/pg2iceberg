package iceberg

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/postgres"
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
	catalog MetadataCache
	s3      ObjectStorage

	// Per-partition writers, keyed by partition key string ("" for unpartitioned).
	partitions map[string]*snapshotPartition
}

type snapshotPartition struct {
	writer     *RollingWriter
	partValues map[string]any
	partPath   string
	pkKeys     []string // PK keys for rows in this partition (for FileIndex seeding)
}

// NewSnapshotWriter creates a new snapshot writer.
func NewSnapshotWriter(cfg SnapshotWriterConfig, catalog MetadataCache, s3 ObjectStorage) *SnapshotWriter {
	return &SnapshotWriter{
		cfg:        cfg,
		catalog:    catalog,
		s3:         s3,
		partitions: make(map[string]*snapshotPartition),
	}
}

// AddRow routes a row to the correct partition writer and tracks the PK
// for FileIndex seeding.
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

	// Track PK for FileIndex seeding (append-only, no deletes in snapshots).
	if pk := sw.cfg.SrcSchema.PK; len(pk) > 0 {
		sp.pkKeys = append(sp.pkKeys, BuildPKKey(row, pk))
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
		pkKeys          []string // for FileIndex seeding
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
		// Distribute PKs across chunks proportionally by row count.
		pkOffset := 0
		for i, chunk := range chunks {
			end := pkOffset + int(chunk.RowCount)
			if end > len(sp.pkKeys) {
				end = len(sp.pkKeys)
			}
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
				pkKeys:          sp.pkKeys[pkOffset:end],
			})
			pkOffset = end
		}
	}

	if len(pending) == 0 {
		return 0, nil
	}

	// Load current table metadata (cache hit in steady state).
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

	// Load existing manifests from catalog cache (seed on cold start).
	existingManifests := sw.catalog.Manifests(cfg.Namespace, cfg.IcebergName)
	if existingManifests == nil && tm != nil && tm.Metadata.CurrentSnapshotID > 0 {
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
			existingManifests, err = ReadManifestList(mlData)
			if err != nil {
				return 0, fmt.Errorf("read manifest list: %w", err)
			}
			sw.catalog.SetManifests(cfg.Namespace, cfg.IcebergName, existingManifests)
		}
	}

	now := time.Now()
	snapshotID := now.UnixMilli()

	// Build manifest entries and FileIndex entries using pre-computed URIs.
	// URIs are deterministic (s3://{bucket}/{key}) — no need to wait for upload.
	entries := make([]ManifestEntry, len(pending))
	var newDataFiles []FileIndexEntry
	var totalRows int64
	for i, pf := range pending {
		df := DataFileInfo{
			Path:            sw.s3.URIForKey(pf.key),
			FileSizeBytes:   int64(len(pf.chunk.Data)),
			RecordCount:     pf.chunk.RowCount,
			Content:         0,
			PartitionValues: pf.partitionValues,
		}
		entries[i] = ManifestEntry{
			Status:     1,
			SnapshotID: snapshotID,
			DataFile:   df,
		}
		if len(pf.pkKeys) > 0 {
			newDataFiles = append(newDataFiles, FileIndexEntry{DataFile: df, PKKeys: pf.pkKeys})
		}
		totalRows += pf.chunk.RowCount
	}

	// Build + upload manifests + manifest list + data files in one batch.
	bundle, err := BuildCommit(BuildCommitConfig{
		S3: sw.s3, Schema: cfg.SrcSchema, PartSpec: cfg.PartSpec, BasePath: basePath,
		SnapshotID: snapshotID, SeqNum: seqNum, ExistingManifests: existingManifests,
	}, []ManifestGroup{{Entries: entries, Content: 0}})
	if err != nil {
		return 0, err
	}

	dataUploads := make([]PendingData, len(pending))
	for i, pf := range pending {
		dataUploads[i] = PendingData{Key: pf.key, Data: pf.chunk.Data}
	}
	if err := UploadAll(ctx, sw.s3, dataUploads, bundle, 0); err != nil {
		return 0, err
	}

	mlURI := bundle.ManifestListURI
	allManifests := bundle.AllManifests

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

	tc := TableCommit{
		Table:             cfg.IcebergName,
		CurrentSnapshotID: prevSnapID,
		Snapshot:          commit,
		NewManifests:      allManifests,
		NewDataFiles:      newDataFiles,
	}
	if err := sw.catalog.CommitTransaction(ctx, cfg.Namespace, []TableCommit{tc}); err != nil {
		return 0, fmt.Errorf("commit snapshot: %w", err)
	}

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
