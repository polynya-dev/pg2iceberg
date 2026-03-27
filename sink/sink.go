package sink

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/source"
	"github.com/pg2iceberg/pg2iceberg/worker"
	"golang.org/x/sync/errgroup"
)

// txBuffer holds events for a single in-flight PG transaction.
type txBuffer struct {
	xid       uint32
	events    []source.ChangeEvent
	tables    map[string]bool // which tables this tx touches
	committed bool
	commitTS  time.Time // PG commit timestamp
}

// Sink buffers ChangeEvents and periodically flushes them to Iceberg.
type Sink struct {
	cfg       config.SinkConfig
	pgCfg     config.PostgresConfig // source PG config for TOAST lookups
	tableCfgs []config.TableConfig

	catalog *CatalogClient
	s3      *S3Client

	// Per-table state
	tables map[string]*tableSink

	// Transaction tracking (only used when consistency is enabled).
	consistencyEnabled bool
	openTxns           map[uint32]*txBuffer // XID -> in-flight tx
	committedTxns      []*txBuffer          // txns that received Commit, in order

	// Backpressure: compactor signals when snapshots are cleaned up.
	compactionDone chan struct{}
	mu             sync.Mutex
}

// toastPendingRow holds a reference to a buffered row that has unchanged TOAST columns.
type toastPendingRow struct {
	row           map[string]any // reference to the buffered row (shared with writer)
	unchangedCols []string       // columns that need to be fetched
}

// partitionedWriter holds data and delete writers for a single partition.
type partitionedWriter struct {
	dataWriter *RollingWriter
	delWriter  *RollingWriter
	partValues map[string]any // partition values for this partition (nil for unpartitioned)
}

type tableSink struct {
	schema      *schema.TableSchema
	icebergName string // table name in Iceberg catalog
	partSpec    *PartitionSpec
	targetSize  int64

	// Per-partition writers, keyed by partition key string ("" for unpartitioned).
	partitions map[string]*partitionedWriter
	totalRows  int

	// toastPending tracks rows that have unchanged TOAST columns.
	// The row references are shared with the dataWriter, so mutating
	// row[col] here also updates the writer's buffer.
	toastPending []toastPendingRow
}

func NewSink(cfg config.SinkConfig, pgCfg config.PostgresConfig, tableCfgs []config.TableConfig) (*Sink, error) {
	var httpClient *http.Client
	if cfg.CatalogAuth == "sigv4" {
		transport, err := NewSigV4Transport(cfg.S3Region)
		if err != nil {
			return nil, fmt.Errorf("create sigv4 transport: %w", err)
		}
		httpClient = &http.Client{Transport: transport}
	}
	catalog := NewCatalogClient(cfg.CatalogURI, httpClient)

	s3Client, err := NewS3Client(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Region, cfg.Warehouse)
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &Sink{
		cfg:                cfg,
		pgCfg:              pgCfg,
		tableCfgs:          tableCfgs,
		catalog:            catalog,
		s3:                 s3Client,
		tables:             make(map[string]*tableSink),
		consistencyEnabled: cfg.ConsistencyTable,
		openTxns:           make(map[uint32]*txBuffer),
		compactionDone:     make(chan struct{}, 1),
	}, nil
}

// Catalog returns the catalog client (for use by compactor).
func (s *Sink) Catalog() *CatalogClient { return s.catalog }

// S3 returns the S3 client (for use by compactor).
func (s *Sink) S3() *S3Client { return s.s3 }

// RegisterTable sets up writers for a table and ensures it exists in the catalog.
func (s *Sink) RegisterTable(ctx context.Context, ts *schema.TableSchema) error {
	icebergTable := pgTableToIceberg(ts.Table)

	// Build partition spec from config.
	var partExprs []string
	for _, tc := range s.tableCfgs {
		if tc.Name == ts.Table {
			partExprs = tc.Iceberg.Partition
			break
		}
	}
	partSpec, err := BuildPartitionSpec(partExprs, ts)
	if err != nil {
		return fmt.Errorf("build partition spec: %w", err)
	}

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
		tm, err = s.catalog.CreateTable(s.cfg.Namespace, icebergTable, ts, location, partSpec)
		if err != nil {
			return fmt.Errorf("create table: %w", err)
		}
		log.Printf("[sink] created Iceberg table %s.%s", s.cfg.Namespace, icebergTable)
	} else {
		log.Printf("[sink] using existing Iceberg table %s.%s", s.cfg.Namespace, icebergTable)
	}

	targetSize := s.cfg.TargetFileSizeOrDefault()
	tSink := &tableSink{
		schema:      ts,
		icebergName: icebergTable,
		partSpec:    partSpec,
		targetSize:  targetSize,
		partitions:  make(map[string]*partitionedWriter),
	}
	// Pre-create the default (unpartitioned) writer if no partition spec.
	if partSpec.IsUnpartitioned() {
		tSink.partitions[""] = &partitionedWriter{
			dataWriter: NewRollingDataWriter(ts, targetSize),
			delWriter:  NewRollingDeleteWriter(ts, targetSize),
		}
	}
	s.tables[ts.Table] = tSink
	return nil
}

// UnregisterTable removes a table from the sink. Any buffered data is discarded.
func (s *Sink) UnregisterTable(pgTable string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tables, pgTable)
}

// getPartitionWriter returns the partitioned writer for a given row, creating it if needed.
func (ts *tableSink) getPartitionWriter(row map[string]any) *partitionedWriter {
	if ts.partSpec.IsUnpartitioned() {
		return ts.partitions[""]
	}

	key, values := ts.partSpec.PartitionKey(row, ts.schema)
	pw, ok := ts.partitions[key]
	if !ok {
		pw = &partitionedWriter{
			dataWriter: NewRollingDataWriter(ts.schema, ts.targetSize),
			delWriter:  NewRollingDeleteWriter(ts.schema, ts.targetSize),
			partValues: values,
		}
		ts.partitions[key] = pw
	}
	return pw
}

// Write buffers a ChangeEvent for the next flush. When consistency mode is
// enabled, DML events are held in per-transaction buffers until the
// corresponding OpCommit arrives.
func (s *Sink) Write(event source.ChangeEvent) error {
	// Skip transaction boundary events when consistency mode is disabled.
	if !s.consistencyEnabled {
		if event.Operation == source.OpBegin || event.Operation == source.OpCommit {
			return nil
		}
	}

	// Transaction boundary events (consistency mode only).
	if s.consistencyEnabled {
		switch event.Operation {
		case source.OpBegin:
			s.openTxns[event.TransactionID] = &txBuffer{
				xid:    event.TransactionID,
				tables: make(map[string]bool),
			}
			return nil
		case source.OpCommit:
			tx, ok := s.openTxns[event.TransactionID]
			if !ok {
				return nil // no tracked DML in this tx
			}
			tx.committed = true
			tx.commitTS = event.SourceTimestamp
			s.committedTxns = append(s.committedTxns, tx)
			delete(s.openTxns, event.TransactionID)
			return nil
		}

		// DML event — buffer in the transaction.
		if event.TransactionID != 0 {
			tx, ok := s.openTxns[event.TransactionID]
			if ok {
				tx.events = append(tx.events, event)
				tx.tables[event.Table] = true
				return nil
			}
		}
		// Fall through: events without a transaction ID (e.g., snapshot)
		// are written directly.
	}

	return s.writeDirect(event)
}

// writeDirect writes an event directly to the per-table writers.
func (s *Sink) writeDirect(event source.ChangeEvent) error {
	ts, ok := s.tables[event.Table]
	if !ok {
		return fmt.Errorf("unregistered table: %s", event.Table)
	}

	switch event.Operation {
	case source.OpInsert:
		if event.After != nil {
			pw := ts.getPartitionWriter(event.After)
			if err := pw.dataWriter.Add(event.After); err != nil {
				return err
			}
			ts.totalRows++
		}
	case source.OpUpdate:
		// Equality delete for old row (routed by old row's partition).
		if event.Before != nil {
			pkRow := extractPK(event.Before, event.PK)
			pw := ts.getPartitionWriter(event.Before)
			if err := pw.delWriter.Add(pkRow); err != nil {
				return err
			}
		}
		// Insert new row (routed by new row's partition).
		if event.After != nil {
			pw := ts.getPartitionWriter(event.After)
			if err := pw.dataWriter.Add(event.After); err != nil {
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
			pw := ts.getPartitionWriter(event.Before)
			if err := pw.delWriter.Add(pkRow); err != nil {
				return err
			}
		}
	}
	return nil
}

// ShouldFlush checks if there is flushable data. When consistency is enabled,
// only committed transactions count.
func (s *Sink) ShouldFlush() bool {
	if s.consistencyEnabled {
		return len(s.committedTxns) > 0
	}
	for _, ts := range s.tables {
		for _, pw := range ts.partitions {
			if pw.dataWriter.Len()+pw.delWriter.Len() > 0 {
				return true
			}
		}
	}
	return false
}

// TotalBuffered returns the total number of buffered rows eligible for flush.
// When consistency is enabled, only committed transaction events are counted.
func (s *Sink) TotalBuffered() int {
	if s.consistencyEnabled {
		total := 0
		for _, tx := range s.committedTxns {
			total += len(tx.events)
		}
		return total
	}
	total := 0
	for _, ts := range s.tables {
		for _, pw := range ts.partitions {
			total += pw.dataWriter.Len() + pw.delWriter.Len()
		}
	}
	return total
}

// TotalBufferedBytes returns the estimated buffered bytes across all tables.
// In consistency mode this includes both committed and in-flight data (for
// backpressure monitoring).
func (s *Sink) TotalBufferedBytes() int64 {
	var total int64
	for _, ts := range s.tables {
		for _, pw := range ts.partitions {
			total += pw.dataWriter.EstimatedBytes() + pw.delWriter.EstimatedBytes()
		}
	}
	// Include in-flight transaction events not yet in writers.
	if s.consistencyEnabled {
		for _, tx := range s.committedTxns {
			total += int64(len(tx.events)) * 128 // rough estimate per event
		}
		for _, tx := range s.openTxns {
			total += int64(len(tx.events)) * 128
		}
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

// Flush writes all buffered data to Iceberg for each table, flushing tables in parallel.
// When consistency is enabled, committed transactions are replayed into writers
// first, and a consistency record is written after all tables commit.
func (s *Sink) Flush(ctx context.Context) error {
	if s.consistencyEnabled {
		return s.flushWithConsistency(ctx)
	}
	return s.flushDirect(ctx)
}

// flushDirect is the original flush path (no transaction tracking).
func (s *Sink) flushDirect(ctx context.Context) error {
	snapshotIDs, err := s.flushAllTables(ctx)
	if err != nil {
		return err
	}
	_ = snapshotIDs
	return nil
}

// flushWithConsistency replays committed transactions into writers, flushes
// all tables in parallel, then writes a consistency record.
func (s *Sink) flushWithConsistency(ctx context.Context) error {
	if len(s.committedTxns) == 0 {
		return nil
	}

	// Drain committed transactions into per-table writers.
	txns := s.committedTxns
	for _, tx := range txns {
		for _, event := range tx.events {
			if err := s.writeDirect(event); err != nil {
				return fmt.Errorf("replay tx %d: %w", tx.xid, err)
			}
		}
	}

	// Flush all tables in parallel, collecting snapshot IDs.
	snapshotIDs, err := s.flushAllTables(ctx)
	if err != nil {
		return err
	}

	// Write consistency records.
	if err := s.writeConsistencyRecords(ctx, txns, snapshotIDs); err != nil {
		return fmt.Errorf("write consistency records: %w", err)
	}

	// Clear committed transactions only after everything succeeded.
	s.committedTxns = nil
	return nil
}

// flushAllTables flushes all tables that have buffered data in parallel.
// Returns a map of pgTable -> snapshotID for each flushed table.
func (s *Sink) flushAllTables(ctx context.Context) (map[string]int64, error) {
	type tableFlushResult struct {
		pgTable    string
		snapshotID int64
	}

	var tasks []worker.Task
	resultCh := make(chan tableFlushResult, len(s.tables))

	for pgTable, ts := range s.tables {
		hasData := false
		for _, pw := range ts.partitions {
			if pw.dataWriter.Len()+pw.delWriter.Len() > 0 {
				hasData = true
				break
			}
		}
		if !hasData {
			continue
		}

		pgTable, ts := pgTable, ts
		tasks = append(tasks, worker.Task{
			Name: pgTable,
			Fn: func(ctx context.Context, progress *worker.Progress) error {
				snapID, err := s.flushTable(ctx, pgTable, ts)
				if err != nil {
					return err
				}
				resultCh <- tableFlushResult{pgTable: pgTable, snapshotID: snapID}
				return nil
			},
		})
	}

	if len(tasks) == 0 {
		return nil, nil
	}

	pool := worker.NewPool(len(tasks))
	_, err := pool.Run(ctx, tasks)
	close(resultCh)
	if err != nil {
		return nil, err
	}

	snapshotIDs := make(map[string]int64, len(tasks))
	for r := range resultCh {
		snapshotIDs[r.pgTable] = r.snapshotID
	}
	return snapshotIDs, nil
}

// pendingUpload holds the data needed to upload a single Parquet file to S3.
type pendingUpload struct {
	key             string
	data            []byte
	content         int // 0=data, 2=equality delete
	recordCount     int64
	equalityFieldIDs []int
	partitionValues map[string]any
}

// uploadResult holds the outcome of a completed upload.
type uploadResult struct {
	uri string
	pendingUpload
}

func (s *Sink) flushTable(ctx context.Context, pgTable string, ts *tableSink) (int64, error) {
	// Resolve any unchanged TOAST columns before serializing to Parquet.
	if len(ts.toastPending) > 0 {
		if err := s.resolveToast(ctx, pgTable, ts); err != nil {
			return 0, fmt.Errorf("resolve TOAST: %w", err)
		}
	}

	now := time.Now()
	snapshotID := now.UnixMilli()
	basePath := fmt.Sprintf("%s.db/%s", s.cfg.Namespace, ts.icebergName)

	// 1. Flush all partitions to Parquet bytes and collect upload tasks.
	var uploads []pendingUpload
	for _, pw := range ts.partitions {
		partPath := ""
		if ts.partSpec != nil {
			partPath = ts.partSpec.PartitionPath(pw.partValues)
		}

		avroPartValues := map[string]any{}
		if ts.partSpec != nil && pw.partValues != nil {
			avroPartValues = ts.partSpec.PartitionAvroValue(pw.partValues, ts.schema)
		}

		dataChunks, err := pw.dataWriter.FlushAll()
		if err != nil {
			return 0, fmt.Errorf("flush data: %w", err)
		}
		for i, chunk := range dataChunks {
			fileUUID := uuid.New().String()
			var key string
			if partPath != "" {
				key = fmt.Sprintf("%s/data/%s/%s-data-%d.parquet", basePath, partPath, fileUUID, i)
			} else {
				key = fmt.Sprintf("%s/data/%s-data-%d.parquet", basePath, fileUUID, i)
			}
			uploads = append(uploads, pendingUpload{
				key:             key,
				data:            chunk.Data,
				content:         0,
				recordCount:     chunk.RowCount,
				partitionValues: avroPartValues,
			})
		}

		delChunks, err := pw.delWriter.FlushAll()
		if err != nil {
			return 0, fmt.Errorf("flush deletes: %w", err)
		}
		for i, chunk := range delChunks {
			fileUUID := uuid.New().String()
			var key string
			if partPath != "" {
				key = fmt.Sprintf("%s/data/%s/%s-deletes-%d.parquet", basePath, partPath, fileUUID, i)
			} else {
				key = fmt.Sprintf("%s/data/%s-deletes-%d.parquet", basePath, fileUUID, i)
			}
			uploads = append(uploads, pendingUpload{
				key:              key,
				data:             chunk.Data,
				content:          2,
				recordCount:      chunk.RowCount,
				equalityFieldIDs: ts.schema.PKFieldIDs(),
				partitionValues:  avroPartValues,
			})
		}
	}

	// 2. Upload all Parquet files to S3 in parallel.
	results := make([]uploadResult, len(uploads))
	g, gctx := errgroup.WithContext(ctx)
	for i, u := range uploads {
		i, u := i, u
		g.Go(func() error {
			uri, err := s.s3.Upload(gctx, u.key, u.data)
			if err != nil {
				return fmt.Errorf("upload %s: %w", u.key, err)
			}
			results[i] = uploadResult{uri: uri, pendingUpload: u}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}

	// Separate uploaded files into data and delete lists.
	var dataFiles, deleteFiles []DataFileInfo
	for _, r := range results {
		fi := DataFileInfo{
			Path:            r.uri,
			FileSizeBytes:   int64(len(r.data)),
			RecordCount:     r.recordCount,
			Content:         r.content,
			PartitionValues: r.partitionValues,
		}
		if r.content == 2 {
			fi.EqualityFieldIDs = r.equalityFieldIDs
			deleteFiles = append(deleteFiles, fi)
		} else {
			dataFiles = append(dataFiles, fi)
		}
	}

	// 3. Load current table metadata to get existing manifests.
	tm, err := s.catalog.LoadTable(s.cfg.Namespace, ts.icebergName)
	if err != nil {
		return 0, fmt.Errorf("load table: %w", err)
	}

	seqNum := tm.Metadata.LastSequenceNumber + 1
	var existingManifests []ManifestFileInfo

	if ml := tm.CurrentManifestList(); ml != "" {
		mlKey, err := KeyFromURI(ml)
		if err != nil {
			return 0, fmt.Errorf("parse manifest list URI: %w", err)
		}
		mlData, err := s.s3.Download(ctx, mlKey)
		if err != nil {
			return 0, fmt.Errorf("download manifest list: %w", err)
		}
		existingManifests, err = ReadManifestList(mlData)
		if err != nil {
			return 0, fmt.Errorf("read manifest list: %w", err)
		}
	}

	// 4. Write data manifest (if we have data files).
	var manifestInfos []ManifestFileInfo
	if len(dataFiles) > 0 {
		entries := make([]ManifestEntry, len(dataFiles))
		for i, df := range dataFiles {
			entries[i] = ManifestEntry{
				Status:     1, // added
				SnapshotID: snapshotID,
				DataFile:   df,
			}
		}
		manifestBytes, err := WriteManifest(ts.schema, entries, seqNum, 0, ts.partSpec)
		if err != nil {
			return 0, fmt.Errorf("write data manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-m0.avro", basePath, uuid.New().String())
		manifestURI, err := s.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return 0, fmt.Errorf("upload data manifest: %w", err)
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

	// 5. Write delete manifest (if we have delete files).
	if len(deleteFiles) > 0 {
		entries := make([]ManifestEntry, len(deleteFiles))
		for i, df := range deleteFiles {
			entries[i] = ManifestEntry{
				Status:     1,
				SnapshotID: snapshotID,
				DataFile:   df,
			}
		}
		manifestBytes, err := WriteManifest(ts.schema, entries, seqNum, 1, ts.partSpec)
		if err != nil {
			return 0, fmt.Errorf("write delete manifest: %w", err)
		}

		manifestKey := fmt.Sprintf("%s/metadata/%s-m1.avro", basePath, uuid.New().String())
		manifestURI, err := s.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return 0, fmt.Errorf("upload delete manifest: %w", err)
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

	// 6. Write manifest list (existing + new manifests).
	allManifests := append(existingManifests, manifestInfos...)
	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return 0, fmt.Errorf("write manifest list: %w", err)
	}

	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s.s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return 0, fmt.Errorf("upload manifest list: %w", err)
	}

	// 7. Commit snapshot.
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
		return 0, fmt.Errorf("commit snapshot: %w", err)
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

	// Commit writers only after successful catalog commit so that a
	// retry after a transient failure re-uploads the same data.
	for _, pw := range ts.partitions {
		pw.dataWriter.Commit()
		pw.delWriter.Commit()
	}

	ts.totalRows = 0
	ts.toastPending = nil

	// Clean up empty partition writers (keep the default "" for unpartitioned).
	if !ts.partSpec.IsUnpartitioned() {
		for key := range ts.partitions {
			delete(ts.partitions, key)
		}
	}

	return snapshotID, nil
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
