package sink

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hasyimibhar/pg2iceberg/config"
	"github.com/hasyimibhar/pg2iceberg/schema"
	"github.com/hasyimibhar/pg2iceberg/source"
)

// Sink buffers ChangeEvents and periodically flushes them to Iceberg.
type Sink struct {
	cfg     config.SinkConfig
	catalog *CatalogClient
	s3      *S3Client

	// Per-table state
	tables map[string]*tableSink
}

type tableSink struct {
	schema      *schema.TableSchema
	icebergName string // table name in Iceberg catalog
	dataWriter  *ParquetWriter
	delWriter   *ParquetWriter
	totalRows   int
}

func NewSink(cfg config.SinkConfig) (*Sink, error) {
	catalog := NewCatalogClient(cfg.CatalogURI)

	s3Client, err := NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.Warehouse)
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &Sink{
		cfg:     cfg,
		catalog: catalog,
		s3:      s3Client,
		tables:  make(map[string]*tableSink),
	}, nil
}

// RegisterTable sets up writers for a table and ensures it exists in the catalog.
func (s *Sink) RegisterTable(ctx context.Context, ts *schema.TableSchema) error {
	icebergTable := pgTableToIceberg(ts.Table)

	// Ensure namespace exists
	if err := s.catalog.EnsureNamespace(s.cfg.Namespace); err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}

	// Load or create table in catalog
	tm, err := s.catalog.LoadTable(s.cfg.Namespace, icebergTable)
	if err != nil {
		return fmt.Errorf("load table: %w", err)
	}

	if tm == nil {
		location := fmt.Sprintf("%s%s.db/%s", s.cfg.Warehouse, s.cfg.Namespace, icebergTable)
		tm, err = s.catalog.CreateTable(s.cfg.Namespace, icebergTable, ts, location)
		if err != nil {
			return fmt.Errorf("create table: %w", err)
		}
		log.Printf("[sink] created Iceberg table %s.%s", s.cfg.Namespace, icebergTable)
	} else {
		log.Printf("[sink] using existing Iceberg table %s.%s", s.cfg.Namespace, icebergTable)
	}

	s.tables[ts.Table] = &tableSink{
		schema:      ts,
		icebergName: icebergTable,
		dataWriter:  NewDataWriter(ts),
		delWriter:   NewDeleteWriter(ts),
	}
	return nil
}

// Write buffers a ChangeEvent for the next flush.
func (s *Sink) Write(event source.ChangeEvent) error {
	ts, ok := s.tables[event.Table]
	if !ok {
		return fmt.Errorf("unregistered table: %s", event.Table)
	}

	switch event.Operation {
	case source.OpInsert:
		if event.After != nil {
			ts.dataWriter.Add(event.After)
			ts.totalRows++
		}
	case source.OpUpdate:
		// Equality delete for old row + insert new row
		if event.Before != nil {
			pkRow := extractPK(event.Before, event.PK)
			ts.delWriter.Add(pkRow)
		}
		if event.After != nil {
			ts.dataWriter.Add(event.After)
			ts.totalRows++
		}
	case source.OpDelete:
		if event.Before != nil {
			pkRow := extractPK(event.Before, event.PK)
			ts.delWriter.Add(pkRow)
		}
	}
	return nil
}

// ShouldFlush checks if any table has hit the flush threshold.
func (s *Sink) ShouldFlush() bool {
	for _, ts := range s.tables {
		if ts.dataWriter.Len()+ts.delWriter.Len() > 0 {
			return true
		}
	}
	return false
}

// TotalBuffered returns the total number of buffered rows across all tables.
func (s *Sink) TotalBuffered() int {
	total := 0
	for _, ts := range s.tables {
		total += ts.dataWriter.Len() + ts.delWriter.Len()
	}
	return total
}

// Flush writes all buffered data to Iceberg for each table.
func (s *Sink) Flush(ctx context.Context) error {
	for pgTable, ts := range s.tables {
		if ts.dataWriter.Len() == 0 && ts.delWriter.Len() == 0 {
			continue
		}

		if err := s.flushTable(ctx, pgTable, ts); err != nil {
			return fmt.Errorf("flush %s: %w", pgTable, err)
		}
	}
	return nil
}

func (s *Sink) flushTable(ctx context.Context, pgTable string, ts *tableSink) error {
	now := time.Now()
	snapshotID := now.UnixMilli()
	fileUUID := uuid.New().String()
	basePath := fmt.Sprintf("%s.db/%s", s.cfg.Namespace, ts.icebergName)

	var dataFiles []DataFileInfo
	var deleteFiles []DataFileInfo
	var manifestInfos []ManifestFileInfo

	// 1. Write data file
	if ts.dataWriter.Len() > 0 {
		dataBytes, rowCount, err := ts.dataWriter.Flush()
		if err != nil {
			return fmt.Errorf("flush data: %w", err)
		}

		dataKey := fmt.Sprintf("%s/data/%s-data.parquet", basePath, fileUUID)
		dataURI, err := s.s3.Upload(ctx, dataKey, dataBytes)
		if err != nil {
			return fmt.Errorf("upload data: %w", err)
		}

		dataFiles = append(dataFiles, DataFileInfo{
			Path:          dataURI,
			FileSizeBytes: int64(len(dataBytes)),
			RecordCount:   rowCount,
			Content:       0, // data
		})
		ts.dataWriter.Reset()
	}

	// 2. Write equality delete file
	if ts.delWriter.Len() > 0 {
		delBytes, rowCount, err := ts.delWriter.Flush()
		if err != nil {
			return fmt.Errorf("flush deletes: %w", err)
		}

		delKey := fmt.Sprintf("%s/data/%s-deletes.parquet", basePath, fileUUID)
		delURI, err := s.s3.Upload(ctx, delKey, delBytes)
		if err != nil {
			return fmt.Errorf("upload deletes: %w", err)
		}

		deleteFiles = append(deleteFiles, DataFileInfo{
			Path:             delURI,
			FileSizeBytes:    int64(len(delBytes)),
			RecordCount:      rowCount,
			Content:          2, // equality deletes
			EqualityFieldIDs: ts.schema.PKFieldIDs(),
		})
		ts.delWriter.Reset()
	}

	// 3. Load current table metadata to get existing manifests
	tm, err := s.catalog.LoadTable(s.cfg.Namespace, ts.icebergName)
	if err != nil {
		return fmt.Errorf("load table: %w", err)
	}

	seqNum := tm.Metadata.LastSequenceNumber + 1
	var existingManifests []ManifestFileInfo

	if ml := tm.CurrentManifestList(); ml != "" {
		mlKey, err := KeyFromURI(ml)
		if err != nil {
			return fmt.Errorf("parse manifest list URI: %w", err)
		}
		mlData, err := s.s3.Download(ctx, mlKey)
		if err != nil {
			return fmt.Errorf("download manifest list: %w", err)
		}
		existingManifests, err = ReadManifestList(mlData)
		if err != nil {
			return fmt.Errorf("read manifest list: %w", err)
		}
	}

	// 4. Write data manifest (if we have data files)
	if len(dataFiles) > 0 {
		entries := make([]ManifestEntry, len(dataFiles))
		for i, df := range dataFiles {
			entries[i] = ManifestEntry{
				Status:     1, // added
				SnapshotID: snapshotID,
				DataFile:   df,
			}
		}
		manifestBytes, err := WriteManifest(ts.schema, entries, seqNum, 0)
		if err != nil {
			return fmt.Errorf("write data manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-m0.avro", basePath, fileUUID)
		manifestURI, err := s.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload data manifest: %w", err)
		}

		var totalRows int64
		for _, df := range dataFiles {
			totalRows += df.RecordCount
		}
		manifestInfos = append(manifestInfos, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0, // data
			SnapshotID:     snapshotID,
			AddedFiles:     len(dataFiles),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	// 5. Write delete manifest (if we have delete files)
	if len(deleteFiles) > 0 {
		entries := make([]ManifestEntry, len(deleteFiles))
		for i, df := range deleteFiles {
			entries[i] = ManifestEntry{
				Status:     1,
				SnapshotID: snapshotID,
				DataFile:   df,
			}
		}
		manifestBytes, err := WriteManifest(ts.schema, entries, seqNum, 1)
		if err != nil {
			return fmt.Errorf("write delete manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-m1.avro", basePath, fileUUID)
		manifestURI, err := s.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload delete manifest: %w", err)
		}

		var totalRows int64
		for _, df := range deleteFiles {
			totalRows += df.RecordCount
		}
		manifestInfos = append(manifestInfos, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        1, // deletes
			SnapshotID:     snapshotID,
			AddedFiles:     len(deleteFiles),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	// 6. Write manifest list (existing + new manifests)
	allManifests := append(existingManifests, manifestInfos...)
	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return fmt.Errorf("write manifest list: %w", err)
	}

	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s.s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return fmt.Errorf("upload manifest list: %w", err)
	}

	// 7. Commit snapshot
	operation := "append"
	if len(deleteFiles) > 0 {
		operation = "overwrite"
	}

	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		Summary: map[string]string{
			"operation": operation,
		},
	}

	if err := s.catalog.CommitSnapshot(s.cfg.Namespace, ts.icebergName, tm.Metadata.CurrentSnapshotID, commit); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}

	dataCount := 0
	for _, df := range dataFiles {
		dataCount += int(df.RecordCount)
	}
	delCount := 0
	for _, df := range deleteFiles {
		delCount += int(df.RecordCount)
	}

	log.Printf("[sink] committed snapshot %d for %s (seq=%d, data_rows=%d, delete_rows=%d)",
		snapshotID, pgTable, seqNum, dataCount, delCount)

	ts.totalRows = 0
	return nil
}

func extractPK(row map[string]any, pk []string) map[string]any {
	result := make(map[string]any, len(pk))
	for _, col := range pk {
		result[col] = row[col]
	}
	return result
}

// pgTableToIceberg converts "public.orders" to "orders" for the Iceberg table name.
func pgTableToIceberg(pgTable string) string {
	parts := strings.Split(pgTable, ".")
	return parts[len(parts)-1]
}
