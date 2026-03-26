package sink

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/source"
)

// Sink buffers ChangeEvents and periodically flushes them to Iceberg.
type Sink struct {
	cfg   config.SinkConfig
	pgCfg config.PostgresConfig // source PG config for TOAST lookups

	catalog *CatalogClient
	s3      *S3Client

	// Per-table state
	tables map[string]*tableSink

	// Backpressure: compactor signals when snapshots are cleaned up.
	compactionDone chan struct{}
	mu             sync.Mutex
}

// toastPendingRow holds a reference to a buffered row that has unchanged TOAST columns.
type toastPendingRow struct {
	row           map[string]any // reference to the buffered row (shared with writer)
	unchangedCols []string       // columns that need to be fetched
}

type tableSink struct {
	schema      *schema.TableSchema
	icebergName string // table name in Iceberg catalog
	dataWriter  *RollingWriter
	delWriter   *RollingWriter
	totalRows   int

	// toastPending tracks rows that have unchanged TOAST columns.
	// The row references are shared with the dataWriter, so mutating
	// row[col] here also updates the writer's buffer.
	toastPending []toastPendingRow
}

func NewSink(cfg config.SinkConfig, pgCfg config.PostgresConfig) (*Sink, error) {
	catalog := NewCatalogClient(cfg.CatalogURI)

	s3Client, err := NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.Warehouse)
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &Sink{
		cfg:            cfg,
		pgCfg:          pgCfg,
		catalog:        catalog,
		s3:             s3Client,
		tables:         make(map[string]*tableSink),
		compactionDone: make(chan struct{}, 1),
	}, nil
}

// Catalog returns the catalog client (for use by compactor).
func (s *Sink) Catalog() *CatalogClient { return s.catalog }

// S3 returns the S3 client (for use by compactor).
func (s *Sink) S3() *S3Client { return s.s3 }

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

	targetSize := s.cfg.TargetFileSizeOrDefault()
	s.tables[ts.Table] = &tableSink{
		schema:      ts,
		icebergName: icebergTable,
		dataWriter:  NewRollingDataWriter(ts, targetSize),
		delWriter:   NewRollingDeleteWriter(ts, targetSize),
	}
	return nil
}

// UnregisterTable removes a table from the sink. Any buffered data is discarded.
func (s *Sink) UnregisterTable(pgTable string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, pgTable)
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
			if err := ts.dataWriter.Add(event.After); err != nil {
				return err
			}
			ts.totalRows++
		}
	case source.OpUpdate:
		// Equality delete for old row + insert new row
		if event.Before != nil {
			pkRow := extractPK(event.Before, event.PK)
			if err := ts.delWriter.Add(pkRow); err != nil {
				return err
			}
		}
		if event.After != nil {
			if err := ts.dataWriter.Add(event.After); err != nil {
				return err
			}
			ts.totalRows++
			if len(event.UnchangedCols) > 0 {
				ts.toastPending = append(ts.toastPending, toastPendingRow{
					row:           event.After,
					unchangedCols: event.UnchangedCols,
				})
			}
		}
	case source.OpDelete:
		if event.Before != nil {
			pkRow := extractPK(event.Before, event.PK)
			if err := ts.delWriter.Add(pkRow); err != nil {
				return err
			}
		}
	}
	return nil
}

// ShouldFlush checks if any table has buffered data.
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

// TotalBufferedBytes returns the estimated buffered bytes across all tables.
func (s *Sink) TotalBufferedBytes() int64 {
	var total int64
	for _, ts := range s.tables {
		total += ts.dataWriter.EstimatedBytes() + ts.delWriter.EstimatedBytes()
	}
	return total
}

// CheckBackpressure blocks if the snapshot count exceeds MaxSnapshots,
// waiting for compaction to reduce it. Returns nil when it's safe to proceed.
func (s *Sink) CheckBackpressure(ctx context.Context) error {
	if s.cfg.MaxSnapshots <= 0 {
		return nil
	}

	for {
		overLimit := false
		for _, ts := range s.tables {
			tm, err := s.catalog.LoadTable(s.cfg.Namespace, ts.icebergName)
			if err != nil {
				return fmt.Errorf("check backpressure: %w", err)
			}
			if tm != nil && len(tm.Metadata.Snapshots) >= s.cfg.MaxSnapshots {
				overLimit = true
				break
			}
		}
		if !overLimit {
			return nil
		}

		log.Printf("[sink] backpressure: snapshot limit (%d) reached, waiting for compaction...", s.cfg.MaxSnapshots)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.compactionDone:
			// Compaction completed, re-check.
		case <-time.After(5 * time.Second):
			// Poll periodically in case we missed a signal.
		}
	}
}

// NotifyCompactionDone signals that compaction has completed (called by compactor).
func (s *Sink) NotifyCompactionDone() {
	select {
	case s.compactionDone <- struct{}{}:
	default:
	}
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
	// Resolve any unchanged TOAST columns before serializing to Parquet.
	if len(ts.toastPending) > 0 {
		if err := s.resolveToast(ctx, pgTable, ts); err != nil {
			return fmt.Errorf("resolve TOAST: %w", err)
		}
	}

	now := time.Now()
	snapshotID := now.UnixMilli()
	basePath := fmt.Sprintf("%s.db/%s", s.cfg.Namespace, ts.icebergName)

	var dataFiles []DataFileInfo
	var deleteFiles []DataFileInfo
	var manifestInfos []ManifestFileInfo

	// 1. Flush and upload data files (rolling writer may produce multiple)
	dataChunks, err := ts.dataWriter.FlushAll()
	if err != nil {
		return fmt.Errorf("flush data: %w", err)
	}
	for i, chunk := range dataChunks {
		fileUUID := uuid.New().String()
		dataKey := fmt.Sprintf("%s/data/%s-data-%d.parquet", basePath, fileUUID, i)
		dataURI, err := s.s3.Upload(ctx, dataKey, chunk.Data)
		if err != nil {
			return fmt.Errorf("upload data: %w", err)
		}
		dataFiles = append(dataFiles, DataFileInfo{
			Path:          dataURI,
			FileSizeBytes: int64(len(chunk.Data)),
			RecordCount:   chunk.RowCount,
			Content:       0, // data
		})
	}

	// 2. Flush and upload equality delete files
	delChunks, err := ts.delWriter.FlushAll()
	if err != nil {
		return fmt.Errorf("flush deletes: %w", err)
	}
	for i, chunk := range delChunks {
		fileUUID := uuid.New().String()
		delKey := fmt.Sprintf("%s/data/%s-deletes-%d.parquet", basePath, fileUUID, i)
		delURI, err := s.s3.Upload(ctx, delKey, chunk.Data)
		if err != nil {
			return fmt.Errorf("upload deletes: %w", err)
		}
		deleteFiles = append(deleteFiles, DataFileInfo{
			Path:             delURI,
			FileSizeBytes:    int64(len(chunk.Data)),
			RecordCount:      chunk.RowCount,
			Content:          2, // equality deletes
			EqualityFieldIDs: ts.schema.PKFieldIDs(),
		})
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

		manifestKey := fmt.Sprintf("%s/metadata/%s-m0.avro", basePath, uuid.New().String())
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

		manifestKey := fmt.Sprintf("%s/metadata/%s-m1.avro", basePath, uuid.New().String())
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

	log.Printf("[sink] committed snapshot %d for %s (seq=%d, data_rows=%d, delete_rows=%d, data_files=%d, delete_files=%d)",
		snapshotID, pgTable, seqNum, dataCount, delCount, len(dataFiles), len(deleteFiles))

	ts.totalRows = 0
	ts.toastPending = nil
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
