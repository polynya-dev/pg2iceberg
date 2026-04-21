package iceberg

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/postgres"
)

// MetaWriter buffers append-only metadata rows for a single control-plane
// Iceberg table and produces a TableCommit that can be piggybacked on the
// customer-data CommitTransaction call.
//
// Unlike TableWriter, it has no deletes, no file index, and no TOAST logic.
type MetaWriter struct {
	namespace   string
	icebergName string
	srcSchema   *postgres.TableSchema
	partSpec    *PartitionSpec

	catalog MetadataCache
	s3      ObjectStorage

	// Per-partition buffered rows, keyed by partition key ("" if unpartitioned).
	partitions map[string]*metaPartition
}

type metaPartition struct {
	writer     *RollingWriter
	partValues map[string]any
	partPath   string
}

// NewMetaWriter creates a MetaWriter for a pre-created Iceberg table in the
// metadata namespace. The caller is responsible for ensuring the table exists
// (see EnsureMetaTables).
func NewMetaWriter(namespace, icebergName string, schema *postgres.TableSchema, partSpec *PartitionSpec, catalog MetadataCache, s3 ObjectStorage) *MetaWriter {
	return &MetaWriter{
		namespace:   namespace,
		icebergName: icebergName,
		srcSchema:   schema,
		partSpec:    partSpec,
		catalog:     catalog,
		s3:          s3,
		partitions:  make(map[string]*metaPartition),
	}
}

// Append buffers a row for the next BuildTableCommit call.
func (mw *MetaWriter) Append(row map[string]any) error {
	key := ""
	partitioned := mw.partSpec != nil && !mw.partSpec.IsUnpartitioned()
	if partitioned {
		key = mw.partSpec.PartitionKey(row, mw.srcSchema)
	}

	mp, ok := mw.partitions[key]
	if !ok {
		mp = &metaPartition{
			writer: NewRollingDataWriter(mw.srcSchema, 8*1024*1024),
		}
		if partitioned {
			mp.partValues = mw.partSpec.PartitionValues(row, mw.srcSchema)
			mp.partPath = mw.partSpec.PartitionPath(mp.partValues)
		}
		mw.partitions[key] = mp
	}
	return mp.writer.Add(row)
}

// HasRows reports whether any rows are currently buffered.
func (mw *MetaWriter) HasRows() bool {
	for _, mp := range mw.partitions {
		if mp.writer.Len() > 0 {
			return true
		}
	}
	return false
}

// BuildTableCommit serializes buffered rows, uploads them + manifests to S3,
// and returns a TableCommit tagged with the metadata namespace. The caller
// passes the returned TableCommit to MetadataCache.CommitTransaction alongside
// customer-table commits.
//
// Returns nil if there are no buffered rows. On success, internal buffers
// are cleared.
func (mw *MetaWriter) BuildTableCommit(ctx context.Context) (*TableCommit, error) {
	if !mw.HasRows() {
		return nil, nil
	}

	partitioned := mw.partSpec != nil && !mw.partSpec.IsUnpartitioned()

	tm, err := mw.catalog.LoadTable(ctx, mw.namespace, mw.icebergName)
	if err != nil {
		return nil, fmt.Errorf("load meta table %s.%s: %w", mw.namespace, mw.icebergName, err)
	}
	if tm == nil {
		return nil, fmt.Errorf("meta table %s.%s does not exist", mw.namespace, mw.icebergName)
	}

	basePath := TableBasePath(tm.Metadata.Location, mw.namespace, mw.icebergName)

	type pendingFile struct {
		key             string
		chunk           FileChunk
		partitionValues map[string]any
	}
	var pending []pendingFile
	for _, mp := range mw.partitions {
		chunks, err := mp.writer.FlushAll()
		if err != nil {
			return nil, fmt.Errorf("flush meta partition: %w", err)
		}
		if len(chunks) == 0 {
			continue
		}
		var avroPartValues map[string]any
		if partitioned && mp.partValues != nil {
			avroPartValues = mw.partSpec.PartitionAvroValue(mp.partValues, mw.srcSchema)
		}
		for i, chunk := range chunks {
			var fileKey string
			if partitioned && mp.partPath != "" {
				fileKey = fmt.Sprintf("%s/data/%s/%s-meta-%d.parquet", basePath, mp.partPath, uuid.New().String(), i)
			} else {
				fileKey = fmt.Sprintf("%s/data/%s-meta-%d.parquet", basePath, uuid.New().String(), i)
			}
			pending = append(pending, pendingFile{key: fileKey, chunk: chunk, partitionValues: avroPartValues})
		}
	}

	if len(pending) == 0 {
		return nil, nil
	}

	prevSnapID := tm.Metadata.CurrentSnapshotID
	seqNum := tm.Metadata.LastSequenceNumber + 1

	existingManifests := mw.catalog.Manifests(mw.namespace, mw.icebergName)
	if existingManifests == nil && prevSnapID > 0 {
		mlURI := ""
		for _, snap := range tm.Metadata.Snapshots {
			if snap.SnapshotID == prevSnapID {
				mlURI = snap.ManifestList
				break
			}
		}
		if mlURI != "" {
			mlKey, err := KeyFromURI(mlURI)
			if err != nil {
				return nil, fmt.Errorf("parse manifest list URI: %w", err)
			}
			mlData, err := DownloadWithRetry(ctx, mw.s3, mlKey)
			if err != nil {
				return nil, fmt.Errorf("download manifest list: %w", err)
			}
			existingManifests, err = ReadManifestList(mlData)
			if err != nil {
				return nil, fmt.Errorf("read manifest list: %w", err)
			}
			mw.catalog.SetManifests(mw.namespace, mw.icebergName, existingManifests)
		}
	}

	now := time.Now()
	snapshotID := now.UnixMilli()

	entries := make([]ManifestEntry, len(pending))
	for i, pf := range pending {
		df := DataFileInfo{
			Path:            mw.s3.URIForKey(pf.key),
			FileSizeBytes:   int64(len(pf.chunk.Data)),
			RecordCount:     pf.chunk.RowCount,
			Content:         0,
			PartitionValues: pf.partitionValues,
		}
		entries[i] = ManifestEntry{Status: 1, SnapshotID: snapshotID, DataFile: df}
	}

	bundle, err := BuildCommit(BuildCommitConfig{
		S3: mw.s3, Schema: mw.srcSchema, PartSpec: mw.partSpec, BasePath: basePath,
		SnapshotID: snapshotID, SeqNum: seqNum, ExistingManifests: existingManifests,
	}, []ManifestGroup{{Entries: entries, Content: 0}})
	if err != nil {
		return nil, err
	}

	dataUploads := make([]PendingData, len(pending))
	for i, pf := range pending {
		dataUploads[i] = PendingData{Key: pf.key, Data: pf.chunk.Data}
	}
	if err := UploadAll(ctx, mw.s3, dataUploads, bundle, 0); err != nil {
		return nil, err
	}

	schemaID := tm.Metadata.CurrentSchemaID
	var addedRows, addedBytes int64
	for _, e := range entries {
		addedRows += e.DataFile.RecordCount
		addedBytes += e.DataFile.FileSizeBytes
	}
	delta := SummaryDelta{
		AddedDataFiles: int64(len(entries)),
		AddedRecords:   addedRows,
		AddedFilesSize: addedBytes,
	}
	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: bundle.ManifestListURI,
		SchemaID:         schemaID,
		Summary:          BuildSummary("append", PrevSnapshotSummary(tm, prevSnapID), delta),
	}

	// Clear buffers for next batch.
	mw.partitions = make(map[string]*metaPartition)

	return &TableCommit{
		Table:             mw.icebergName,
		Namespace:         mw.namespace,
		CurrentSnapshotID: prevSnapID,
		Snapshot:          commit,
		NewManifests:      bundle.AllManifests,
	}, nil
}

// EnsureMetaTables creates the control-plane `commits` and `checkpoints`
// tables in the given namespace if they don't already exist, and evolves
// them additively if the expected schema has more columns than the live
// table. Idempotent — safe to call on every pipeline start.
//
// Assumes schema evolution is strictly additive: existing FieldIDs are
// immutable and new columns are appended with nullable=true. Violations
// will surface as catalog errors.
func EnsureMetaTables(ctx context.Context, catalog MetadataCache, warehouse, namespace string) error {
	if err := catalog.EnsureNamespace(ctx, namespace); err != nil {
		return fmt.Errorf("ensure meta namespace %s: %w", namespace, err)
	}

	for _, entry := range []struct {
		name   string
		schema *postgres.TableSchema
	}{
		{MetaCommitsTable, MetaCommitsSchema()},
		{MetaCheckpointsTable, MetaCheckpointsSchema()},
		{MetaCompactionsTable, MetaCompactionsSchema()},
		{MetaMaintenanceTable, MetaMaintenanceSchema()},
	} {
		tm, err := catalog.LoadTable(ctx, namespace, entry.name)
		if err != nil {
			return fmt.Errorf("load meta table %s.%s: %w", namespace, entry.name, err)
		}
		if tm == nil {
			partSpec, err := MetaPartitionSpec(entry.schema)
			if err != nil {
				return fmt.Errorf("build meta partition spec for %s: %w", entry.name, err)
			}
			var location string
			if IsStorageURI(warehouse) {
				location = fmt.Sprintf("%s%s.db/%s", warehouse, namespace, entry.name)
			}
			if _, err := catalog.CreateTable(ctx, namespace, entry.name, entry.schema, location, partSpec); err != nil {
				return fmt.Errorf("create meta table %s.%s: %w", namespace, entry.name, err)
			}
			continue
		}
		// Table exists — evolve if expected schema has more columns.
		// Detection uses LastColumnID rather than column-by-column comparison
		// because the parsed TableMetadata doesn't expose column details.
		// This is safe so long as the schema is strictly additive: MaxFieldID
		// grows monotonically and expected FieldIDs match the history.
		if tm.Metadata.LastColumnID < entry.schema.MaxFieldID() {
			log.Printf("[meta] evolving %s.%s: %d → %d columns",
				namespace, entry.name, tm.Metadata.LastColumnID, entry.schema.MaxFieldID())
			if _, err := catalog.EvolveSchema(ctx, namespace, entry.name, tm.Metadata.CurrentSchemaID, entry.schema); err != nil {
				return fmt.Errorf("evolve meta table %s.%s: %w", namespace, entry.name, err)
			}
		}
	}
	return nil
}
