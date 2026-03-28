package sink

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	pq "github.com/parquet-go/parquet-go"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/schema"
)

// Compactor periodically merges small data files and applies equality deletes.
type Compactor struct {
	cfg        config.SinkConfig
	sink       *Sink
	schemas    map[string]*schema.TableSchema
	partSpecs  map[string]*PartitionSpec // keyed by PG table name
}

func NewCompactor(cfg config.SinkConfig, s *Sink, schemas map[string]*schema.TableSchema) *Compactor {
	// Collect partition specs from the sink's registered tables.
	partSpecs := make(map[string]*PartitionSpec)
	for pgTable, ts := range s.tables {
		partSpecs[pgTable] = ts.partSpec
	}
	return &Compactor{
		cfg:       cfg,
		sink:      s,
		schemas:   schemas,
		partSpecs: partSpecs,
	}
}

// Run starts the compaction loop. It blocks until ctx is cancelled.
func (c *Compactor) Run(ctx context.Context) {
	interval := c.cfg.CompactionDuration()
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for pgTable, ts := range c.schemas {
				icebergTable := pgTableToIceberg(pgTable)
				if err := c.CompactTable(ctx, icebergTable, ts); err != nil {
					metrics.CompactionRunsTotal.WithLabelValues(icebergTable, "error").Inc()
					log.Printf("[compactor] error compacting %s: %v", pgTable, err)
				}
			}
		}
	}
}

// CompactTable merges small data files and applies equality deletes for one table.
func (c *Compactor) CompactTable(ctx context.Context, icebergTable string, ts *schema.TableSchema) error {
	start := time.Now()
	catalog := c.sink.Catalog()
	s3 := c.sink.S3()
	ns := c.cfg.Namespace

	// Load current table metadata.
	tm, err := catalog.LoadTable(ns, icebergTable)
	if err != nil {
		return fmt.Errorf("load table: %w", err)
	}
	if tm == nil || tm.CurrentManifestList() == "" {
		return nil // no snapshots yet
	}

	// Read current manifest list.
	mlKey, err := KeyFromURI(tm.CurrentManifestList())
	if err != nil {
		return fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return fmt.Errorf("download manifest list: %w", err)
	}
	manifestInfos, err := ReadManifestList(mlData)
	if err != nil {
		return fmt.Errorf("read manifest list: %w", err)
	}

	// Read all manifest entries: separate data files and delete files.
	var allDataFiles []DataFileInfo
	var allDeleteFiles []DataFileInfo
	targetSize := c.cfg.CompactionTargetSizeOrDefault()

	for _, mfi := range manifestInfos {
		mKey, err := KeyFromURI(mfi.Path)
		if err != nil {
			continue
		}
		mData, err := s3.Download(ctx, mKey)
		if err != nil {
			continue
		}
		entries, err := ReadManifest(mData)
		if err != nil {
			continue
		}

		for _, e := range entries {
			if e.Status == 2 { // deleted entry, skip
				continue
			}
			switch e.DataFile.Content {
			case 0: // data
				allDataFiles = append(allDataFiles, e.DataFile)
			case 2: // equality deletes
				allDeleteFiles = append(allDeleteFiles, e.DataFile)
			}
		}
	}

	// Decide if compaction is needed:
	// 1. We have equality delete files to apply, OR
	// 2. We have enough small data files to merge.
	hasDeletes := len(allDeleteFiles) > 0
	smallFiles := 0
	for _, df := range allDataFiles {
		if df.FileSizeBytes < targetSize {
			smallFiles++
		}
	}

	minFiles := c.cfg.CompactionMinFilesOrDefault()
	if !hasDeletes && smallFiles < minFiles {
		return nil // nothing to compact
	}

	log.Printf("[compactor] compacting %s: %d data files (%d small), %d delete files",
		icebergTable, len(allDataFiles), smallFiles, len(allDeleteFiles))

	// Build the set of PKs to delete.
	deleteSet, err := c.buildDeleteSet(ctx, s3, allDeleteFiles, ts)
	if err != nil {
		return fmt.Errorf("build delete set: %w", err)
	}

	// Read all data files, filter out deleted rows, write new merged files.
	writer := NewRollingDataWriter(ts, targetSize)
	var totalInput, totalOutput int64

	for _, df := range allDataFiles {
		dfKey, err := KeyFromURI(df.Path)
		if err != nil {
			return fmt.Errorf("parse data file URI: %w", err)
		}
		data, err := s3.Download(ctx, dfKey)
		if err != nil {
			return fmt.Errorf("download data file: %w", err)
		}

		rows, err := readParquetRows(data, ts)
		if err != nil {
			return fmt.Errorf("read parquet file: %w", err)
		}

		for _, row := range rows {
			totalInput++
			pk := extractPKString(row, ts.PK)
			if _, deleted := deleteSet[pk]; deleted {
				continue
			}
			if err := writer.Add(row); err != nil {
				return fmt.Errorf("add row: %w", err)
			}
			totalOutput++
		}
	}

	chunks, err := writer.FlushAll()
	if err != nil {
		return fmt.Errorf("flush compacted: %w", err)
	}

	if len(chunks) == 0 {
		// All rows were deleted — nothing to write.
		// Still need to commit to remove old files.
	}

	// Upload new data files.
	now := time.Now()
	snapshotID := now.UnixMilli()
	seqNum := tm.Metadata.LastSequenceNumber + 1
	basePath := fmt.Sprintf("%s.db/%s", ns, icebergTable)

	var newDataFiles []DataFileInfo
	for i, chunk := range chunks {
		fileUUID := uuid.New().String()
		key := fmt.Sprintf("%s/data/%s-compacted-%d.parquet", basePath, fileUUID, i)
		uri, err := s3.Upload(ctx, key, chunk.Data)
		if err != nil {
			return fmt.Errorf("upload compacted file: %w", err)
		}
		newDataFiles = append(newDataFiles, DataFileInfo{
			Path:          uri,
			FileSizeBytes: int64(len(chunk.Data)),
			RecordCount:   chunk.RowCount,
			Content:       0,
		})
	}

	// Resolve partition spec for this table.
	var partSpec *PartitionSpec
	for pgTable, ps := range c.partSpecs {
		if pgTableToIceberg(pgTable) == icebergTable {
			partSpec = ps
			break
		}
	}

	// Build new manifest with only the new data files.
	var newManifestInfos []ManifestFileInfo

	if len(newDataFiles) > 0 {
		entries := make([]ManifestEntry, len(newDataFiles))
		for i, df := range newDataFiles {
			entries[i] = ManifestEntry{
				Status:     1, // added
				SnapshotID: snapshotID,
				DataFile:   df,
			}
		}
		manifestBytes, err := WriteManifest(ts, entries, seqNum, 0, partSpec)
		if err != nil {
			return fmt.Errorf("write compacted manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-compact-m0.avro", basePath, uuid.New().String())
		manifestURI, err := s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload compacted manifest: %w", err)
		}

		var totalRows int64
		for _, df := range newDataFiles {
			totalRows += df.RecordCount
		}
		newManifestInfos = append(newManifestInfos, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0,
			SnapshotID:     snapshotID,
			AddedFiles:     len(newDataFiles),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	// Write new manifest list (replaces all old manifests).
	newMLBytes, err := WriteManifestList(newManifestInfos)
	if err != nil {
		return fmt.Errorf("write compacted manifest list: %w", err)
	}

	mlNewKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s3.Upload(ctx, mlNewKey, newMLBytes)
	if err != nil {
		return fmt.Errorf("upload compacted manifest list: %w", err)
	}

	// Commit the compacted snapshot.
	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		Summary: map[string]string{
			"operation": "replace",
		},
	}

	if err := catalog.CommitSnapshot(ns, icebergTable, tm.Metadata.CurrentSnapshotID, commit); err != nil {
		return fmt.Errorf("commit compacted snapshot: %w", err)
	}

	metrics.CompactionDurationSeconds.WithLabelValues(icebergTable).Observe(time.Since(start).Seconds())
	metrics.CompactionRunsTotal.WithLabelValues(icebergTable, "success").Inc()
	metrics.CompactionInputRowsTotal.WithLabelValues(icebergTable).Add(float64(totalInput))
	metrics.CompactionDeletedRowsTotal.WithLabelValues(icebergTable).Add(float64(totalInput - totalOutput))

	log.Printf("[compactor] compacted %s: %d input rows -> %d output rows (%d deleted), %d files -> %d files",
		icebergTable, totalInput, totalOutput, totalInput-totalOutput, len(allDataFiles), len(newDataFiles))

	c.sink.NotifyCompactionDone()
	return nil
}

// buildDeleteSet reads all equality delete files and returns a set of PK strings to delete.
func (c *Compactor) buildDeleteSet(ctx context.Context, s3 ObjectStorage, deleteFiles []DataFileInfo, ts *schema.TableSchema) (map[string]struct{}, error) {
	deleteSet := make(map[string]struct{})
	for _, df := range deleteFiles {
		key, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		data, err := s3.Download(ctx, key)
		if err != nil {
			continue
		}
		rows, err := readParquetRows(data, ts)
		if err != nil {
			continue
		}
		for _, row := range rows {
			pk := extractPKString(row, ts.PK)
			deleteSet[pk] = struct{}{}
		}
	}
	return deleteSet, nil
}

// extractPKString creates a string key from PK columns for set membership testing.
func extractPKString(row map[string]any, pk []string) string {
	if len(pk) == 1 {
		return fmt.Sprintf("%v", row[pk[0]])
	}
	key := ""
	for i, col := range pk {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%v", row[col])
	}
	return key
}

// readParquetRows reads a parquet file and returns rows as maps.
func readParquetRows(data []byte, ts *schema.TableSchema) ([]map[string]any, error) {
	reader := pq.NewReader(bytes.NewReader(data))

	// Build column name mapping from schema.
	colNames := make([]string, len(reader.Schema().Fields()))
	for i, f := range reader.Schema().Fields() {
		colNames[i] = f.Name()
	}

	var rows []map[string]any
	rowBuf := make([]pq.Row, 1)
	for {
		n, err := reader.ReadRows(rowBuf)
		if n == 0 {
			break
		}
		if err != nil && n == 0 {
			return nil, fmt.Errorf("read row: %w", err)
		}

		row := make(map[string]any, len(colNames))
		for _, v := range rowBuf[0] {
			colIdx := v.Column()
			if colIdx >= 0 && colIdx < len(colNames) {
				if v.IsNull() {
					row[colNames[colIdx]] = nil
				} else {
					row[colNames[colIdx]] = parquetValueToGo(v)
				}
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// parquetValueToGo converts a parquet.Value to a Go native type.
func parquetValueToGo(v pq.Value) any {
	switch v.Kind() {
	case pq.Boolean:
		return v.Boolean()
	case pq.Int32:
		return fmt.Sprintf("%d", v.Int32())
	case pq.Int64:
		return fmt.Sprintf("%d", v.Int64())
	case pq.Float:
		return fmt.Sprintf("%g", v.Float())
	case pq.Double:
		return fmt.Sprintf("%g", v.Double())
	case pq.ByteArray, pq.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

