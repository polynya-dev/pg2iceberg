package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pq "github.com/parquet-go/parquet-go"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/worker"
)

// ChangeEventBuffer is a thread-safe buffer for change events, shared between
// the Sink (producer) and the Materializer (consumer). Created before both and
// injected into each, avoiding a circular dependency.
type ChangeEventBuffer struct {
	events     map[string][]changeEvent
	snapshotID map[string]int64 // latest events table snapshot ID per table
	mu         sync.Mutex
}

// NewChangeEventBuffer creates a new empty event buffer.
func NewChangeEventBuffer() *ChangeEventBuffer {
	return &ChangeEventBuffer{
		events:     make(map[string][]changeEvent),
		snapshotID: make(map[string]int64),
	}
}

// PushEvents adds change events for a table. Called by the Sink after flush.
// snapshotID is the events table snapshot that produced these events.
func (b *ChangeEventBuffer) PushEvents(pgTable string, events []changeEvent, snapshotID int64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events[pgTable] = append(b.events[pgTable], events...)
	b.snapshotID[pgTable] = snapshotID
	metrics.MaterializerBufferSize.WithLabelValues(pgTable).Set(float64(len(b.events[pgTable])))
}

// Drain removes and returns all buffered events for a table,
// along with the latest events snapshot ID.
func (b *ChangeEventBuffer) Drain(pgTable string) ([]changeEvent, int64) {
	if b == nil {
		return nil, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	events := b.events[pgTable]
	snapID := b.snapshotID[pgTable]
	delete(b.events, pgTable)
	delete(b.snapshotID, pgTable)
	metrics.MaterializerBufferSize.WithLabelValues(pgTable).Set(0)
	return events, snapID
}

// fileIndex tracks which PKs live in which data files for a single table.
// Used to resolve TOAST unchanged columns by downloading only the affected files.
type fileIndex struct {
	// pkToFile maps PK key → file path (the DataFileInfo.Path).
	pkToFile map[string]string
	// files maps file path → DataFileInfo.
	files map[string]DataFileInfo
	// filePKs maps file path → set of PK keys in that file.
	filePKs map[string]map[string]bool
	// snapshotID is the materialized table snapshot this index was built from.
	snapshotID int64
}

func newFileIndex() *fileIndex {
	return &fileIndex{
		pkToFile: make(map[string]string),
		files:    make(map[string]DataFileInfo),
		filePKs:  make(map[string]map[string]bool),
	}
}

// addFile registers all PKs in a file.
func (fi *fileIndex) addFile(df DataFileInfo, pkKeys []string) {
	fi.files[df.Path] = df
	pks := make(map[string]bool, len(pkKeys))
	for _, pk := range pkKeys {
		fi.pkToFile[pk] = df.Path
		pks[pk] = true
	}
	fi.filePKs[df.Path] = pks
}

// affectedFiles returns the set of file paths that contain any of the given PKs.
func (fi *fileIndex) affectedFiles(pks []string) map[string]bool {
	paths := make(map[string]bool)
	for _, pk := range pks {
		if path, ok := fi.pkToFile[pk]; ok {
			paths[path] = true
		}
	}
	return paths
}

// Materializer reads change events from events tables and applies them to
// the materialized (flattened) tables using Merge-on-Read: equality delete
// files mark old rows, new data files contain updated rows. A file index
// tracks PK→file mappings to resolve TOAST unchanged columns when needed.
type Materializer struct {
	cfg     config.SinkConfig
	catalog Catalog
	s3      ObjectStorage
	tables  map[string]*tableSink
	buf     *ChangeEventBuffer

	// Tracks the last processed events snapshot per table.
	// Keyed by PG table name.
	lastEventsSnapshot map[string]int64

	// Per-table file index for TOAST resolution: tracks which PKs are in
	// which data files so we only download files containing affected PKs.
	fileIndexes map[string]*fileIndex
}

// NewMaterializer creates a new Materializer.
func NewMaterializer(cfg config.SinkConfig, catalog Catalog, s3 ObjectStorage, tables map[string]*tableSink, buf *ChangeEventBuffer) *Materializer {
	return &Materializer{
		cfg:                cfg,
		catalog:            catalog,
		s3:                 s3,
		tables:             tables,
		buf:                buf,
		lastEventsSnapshot: make(map[string]int64),
		fileIndexes:        make(map[string]*fileIndex),
	}
}

// SetLastEventsSnapshot restores the checkpoint for a table (called on startup).
func (m *Materializer) SetLastEventsSnapshot(pgTable string, snapshotID int64) {
	m.lastEventsSnapshot[pgTable] = snapshotID
}

// LastEventsSnapshots returns the current checkpoint state for persistence.
func (m *Materializer) LastEventsSnapshots() map[string]int64 {
	return m.lastEventsSnapshot
}

// MaterializeAll runs a single materialization pass for all tables.
// Called during graceful shutdown to ensure all flushed events are materialized.
//
// The in-memory ChangeEventBuffer is drained and discarded so that
// MaterializeTable falls through to the S3 path, which reads event files
// directly from the Iceberg events table. This is necessary because the
// Run() goroutine may have drained the buffer but failed to materialize
// (e.g. context cancelled), leaving a gap. The events table on S3 is the
// authoritative record of all flushed events, so reading from it guarantees
// nothing is skipped.
func (m *Materializer) MaterializeAll(ctx context.Context) {
	for pgTable := range m.tables {
		m.buf.Drain(pgTable)
	}
	for pgTable, ts := range m.tables {
		if err := m.MaterializeTable(ctx, pgTable, ts); err != nil {
			metrics.MaterializerErrorsTotal.WithLabelValues(pgTable).Inc()
			log.Printf("[materializer] final pass error for %s: %v", pgTable, err)
		}
	}
}

// Run starts the materialization loop. Blocks until ctx is cancelled.
func (m *Materializer) Run(ctx context.Context) {
	interval := m.cfg.MaterializerDuration()
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for pgTable, ts := range m.tables {
				if err := m.MaterializeTable(ctx, pgTable, ts); err != nil {
					metrics.MaterializerErrorsTotal.WithLabelValues(pgTable).Inc()
					log.Printf("[materializer] error materializing %s: %v", pgTable, err)
				}
			}
		}
	}
}

// changeEvent represents a parsed change event from the events table.
type changeEvent struct {
	op            string // "I", "U", "D"
	lsn           int64
	seq           int64
	unchangedCols []string
	row           map[string]any // user columns only
}

// MaterializeTable reads new events for one table and applies them to the
// materialized table using Merge-on-Read. Instead of downloading and rewriting
// existing data files (CoW), it writes:
//   - Equality delete files: for UPDATEs and DELETEs (marks old rows for removal)
//   - New data files: for INSERTs and UPDATEs (contains the new/updated rows)
//
// Existing manifests from the previous snapshot are carried forward unchanged.
func (m *Materializer) MaterializeTable(ctx context.Context, pgTable string, ts *tableSink) error {
	start := time.Now()
	catalog := m.catalog
	s3 := m.s3
	ns := m.cfg.Namespace

	// Prefer in-memory events (fast path: no S3 reads).
	events, bufSnapID := m.buf.Drain(pgTable)
	fromBuffer := len(events) > 0

	if !fromBuffer {
		// No in-memory events — fall back to S3 (crash recovery path).
		eventsTm, err := catalog.LoadTable(ns, ts.eventsIcebergName)
		if err != nil {
			return fmt.Errorf("load events table: %w", err)
		}
		if eventsTm == nil || eventsTm.Metadata.CurrentSnapshotID == 0 {
			return nil // no events yet
		}

		currentSnapshotID := eventsTm.Metadata.CurrentSnapshotID
		lastProcessed := m.lastEventsSnapshot[pgTable]

		if currentSnapshotID == lastProcessed {
			return nil // already up to date
		}

		newFiles, err := m.findNewEventFiles(ctx, s3, eventsTm, lastProcessed)
		if err != nil {
			return fmt.Errorf("find new event files: %w", err)
		}
		if len(newFiles) == 0 {
			m.lastEventsSnapshot[pgTable] = currentSnapshotID
			return nil
		}

		events, err = m.readEvents(ctx, s3, newFiles, ts.srcSchema)
		if err != nil {
			return fmt.Errorf("read events: %w", err)
		}
		if len(events) == 0 {
			m.lastEventsSnapshot[pgTable] = currentSnapshotID
			return nil
		}

		// Update checkpoint now — S3 events are loaded.
		m.lastEventsSnapshot[pgTable] = currentSnapshotID
	}

	// Phase timing for diagnostics.
	tDrain := time.Since(start)

	pk := ts.srcSchema.PK
	if len(pk) == 0 {
		log.Printf("[materializer] skipping %s: no primary key", pgTable)
		return nil
	}

	// Load the materialized table metadata (needed for TOAST + commit).
	matTm, err := catalog.LoadTable(ns, ts.icebergName)
	if err != nil {
		return fmt.Errorf("load materialized table: %w", err)
	}

	var prevMatSnapID int64
	if matTm != nil {
		prevMatSnapID = matTm.Metadata.CurrentSnapshotID
	}
	seqNum := int64(1)
	if matTm != nil {
		seqNum = matTm.Metadata.LastSequenceNumber + 1
	}

	now := time.Now()
	snapshotID := now.UnixMilli()
	basePath := fmt.Sprintf("%s.db/%s", ns, ts.icebergName)
	targetSize := m.cfg.MaterializerTargetFileSizeOrDefault()
	pkFieldIDs := ts.srcSchema.PKFieldIDs()
	partitioned := ts.partSpec != nil && !ts.partSpec.IsUnpartitioned()

	// --- Bucket events by partition key (or PK hash for unpartitioned) ---
	// Each bucket is independent so fold + serialize can run in parallel.
	type eventBucket struct {
		key        string
		events     []changeEvent
		partValues map[string]any
		partPath   string
	}

	// File index for TOAST resolution and DELETE partition routing.
	// Built once per cycle, shared read-only across buckets.
	var fi *fileIndex
	if matTm != nil && matTm.Metadata.CurrentSnapshotID > 0 {
		fi = m.fileIndexes[pgTable]
		if fi == nil || fi.snapshotID != matTm.Metadata.CurrentSnapshotID {
			fi, err = m.buildFileIndex(ctx, pgTable, ts, matTm)
			if err != nil {
				return fmt.Errorf("build file index: %w", err)
			}
			m.fileIndexes[pgTable] = fi
		}
	}

	bucketMap := make(map[string]*eventBucket)
	numShards := runtime.NumCPU()
	if numShards < 2 {
		numShards = 2
	}

	// For partitioned tables, DELETE events may lack partition columns (PG only
	// sends PK in the Before tuple). Track PK→partition key so DELETEs can be
	// routed to the correct bucket.
	pkPartKey := make(map[string]string) // PK key → partition bucket key

	for i := range events {
		ev := &events[i]
		var bKey string
		if partitioned {
			pkKey := buildPKKey(ev.row, pk)
			if ev.op == "D" {
				// DELETE events lack non-PK columns; use the partition
				// recorded from the preceding INSERT/UPDATE.
				if cached, ok := pkPartKey[pkKey]; ok {
					bKey = cached
				} else if fi != nil {
					// Row was inserted in a previous cycle — look up its
					// partition from the file index.
					if filePath, ok := fi.pkToFile[pkKey]; ok {
						bKey = extractPartBucketKey(filePath)
					} else {
						continue
					}
				} else {
					continue
				}
			} else {
				bKey = ts.partSpec.PartitionKey(ev.row, ts.srcSchema)
				pkPartKey[pkKey] = bKey
			}
		} else {
			pkKey := buildPKKey(ev.row, pk)
			var h uint32
			for j := 0; j < len(pkKey); j++ {
				h = h*31 + uint32(pkKey[j])
			}
			bKey = strconv.Itoa(int(h % uint32(numShards)))
		}
		b, ok := bucketMap[bKey]
		if !ok {
			b = &eventBucket{key: bKey}
			if partitioned && ev.op != "D" {
				b.partValues = ts.partSpec.PartitionValues(ev.row, ts.srcSchema)
				b.partPath = ts.partSpec.PartitionPath(b.partValues)
			}
			bucketMap[bKey] = b
		}
		// If a non-DELETE event arrives for a bucket that was created by a
		// DELETE (no partValues yet), fill in the partition values now.
		if partitioned && b.partValues == nil && ev.op != "D" {
			b.partValues = ts.partSpec.PartitionValues(ev.row, ts.srcSchema)
			b.partPath = ts.partSpec.PartitionPath(b.partValues)
		}
		b.events = append(b.events, *ev)
	}

	// Resolve partition values for DELETE-only buckets using the file index path.
	if partitioned && fi != nil {
		for bKey, b := range bucketMap {
			if b.partValues != nil {
				continue
			}
			// bKey is the partition key (e.g. "seq_truncate=0") extracted from file path.
			b.partPath = bKey
			b.partValues = ts.partSpec.ParsePartitionPath(bKey, ts.srcSchema)
		}
	}

	buckets := make([]*eventBucket, 0, len(bucketMap))
	for _, b := range bucketMap {
		buckets = append(buckets, b)
	}

	// --- Parallel fold + serialize per bucket ---
	type pkState struct {
		op            string
		row           map[string]any
		unchangedCols []string
	}
	type pendingFile struct {
		key              string
		chunk            FileChunk
		content          int // 0=data, 2=equality delete
		equalityFieldIDs []int
		partitionValues  map[string]any
	}
	type bucketResult struct {
		pending        []pendingFile
		deleteRowCount int64
		finalState     map[string]*pkState // needed for TOAST resolution
	}

	// --- Phase 1a: Parallel fold (events → final state per PK) ---
	bucketResults := make([]bucketResult, len(buckets))

	foldTasks := make([]worker.Task, len(buckets))
	for i, b := range buckets {
		i, b := i, b
		foldTasks[i] = worker.Task{
			Name: b.key,
			Fn: func(ctx context.Context, _ *worker.Progress) error {
				state := make(map[string]*pkState, len(b.events)/2)
				for _, ev := range b.events {
					pkKey := buildPKKey(ev.row, pk)
					existing := state[pkKey]

					switch ev.op {
					case "I":
						state[pkKey] = &pkState{op: "I", row: ev.row}
					case "U":
						if existing != nil && len(ev.unchangedCols) > 0 {
							merged := make(map[string]any, len(ev.row))
							for k, v := range ev.row {
								merged[k] = v
							}
							for _, col := range ev.unchangedCols {
								if val, ok := existing.row[col]; ok {
									merged[col] = val
								}
							}
							state[pkKey] = &pkState{op: "U", row: merged}
						} else {
							state[pkKey] = &pkState{
								op:            "U",
								row:           ev.row,
								unchangedCols: ev.unchangedCols,
							}
						}
					case "D":
						state[pkKey] = &pkState{op: "D", row: ev.row}
					}
				}
				bucketResults[i].finalState = state
				return nil
			},
		}
	}

	foldConcurrency := runtime.NumCPU()
	if len(foldTasks) < foldConcurrency {
		foldConcurrency = len(foldTasks)
	}
	if foldConcurrency < 1 {
		foldConcurrency = 1
	}
	foldPool := worker.NewPool(foldConcurrency)
	if _, err := foldPool.Run(ctx, foldTasks); err != nil {
		return fmt.Errorf("fold: %w", err)
	}

	tFold := time.Since(start)

	// --- Phase 1b: TOAST resolution (must run before serialization) ---
	// Collect unresolved PKs across all buckets.
	allFinalState := make(map[string]*pkState)
	var allUnresolvedPKs []string
	for _, r := range bucketResults {
		for k, v := range r.finalState {
			if v.op == "U" && len(v.unchangedCols) > 0 {
				allUnresolvedPKs = append(allUnresolvedPKs, k)
			}
			allFinalState[k] = v
		}
	}

	if len(allUnresolvedPKs) > 0 && fi != nil {
		affectedFilePaths := fi.affectedFiles(allUnresolvedPKs)
		resolved := 0
		for path := range affectedFilePaths {
			dfKey, err := KeyFromURI(path)
			if err != nil {
				continue
			}
			data, err := s3.Download(ctx, dfKey)
			if err != nil {
				log.Printf("[materializer] TOAST: failed to download %s: %v", path, err)
				continue
			}
			rows, err := readParquetRows(data, ts.srcSchema)
			if err != nil {
				log.Printf("[materializer] TOAST: failed to read %s: %v", path, err)
				continue
			}
			for _, row := range rows {
				pkKey := buildPKKey(row, pk)
				state, ok := allFinalState[pkKey]
				if !ok || state.op != "U" || len(state.unchangedCols) == 0 {
					continue
				}
				for _, col := range state.unchangedCols {
					if val, exists := row[col]; exists {
						state.row[col] = val
					}
				}
				state.unchangedCols = nil
				resolved++
			}
		}
		if resolved > 0 {
			log.Printf("[materializer] TOAST: resolved %d/%d rows for %s (%d files scanned)",
				resolved, len(allUnresolvedPKs), pgTable, len(affectedFilePaths))
		}
	}

	tToast := time.Since(start)

	// --- Phase 1c: Parallel serialize (state → Parquet files) ---
	serializeTasks := make([]worker.Task, len(buckets))
	for i, b := range buckets {
		i, b := i, b
		serializeTasks[i] = worker.Task{
			Name: b.key,
			Fn: func(ctx context.Context, _ *worker.Progress) error {
				state := bucketResults[i].finalState

				// Serialize equality delete files.
				var deleteRowCount int64
				deleteWriter := NewRollingDeleteWriter(ts.srcSchema, targetSize)
				for _, s := range state {
					if s.op == "U" || s.op == "D" {
						if err := deleteWriter.Add(s.row); err != nil {
							return fmt.Errorf("add delete row: %w", err)
						}
						deleteRowCount++
					}
				}
				var pf []pendingFile
				if deleteRowCount > 0 {
					chunks, err := deleteWriter.FlushAll()
					if err != nil {
						return fmt.Errorf("flush deletes: %w", err)
					}
					for j, chunk := range chunks {
						var delKey string
						dpf := pendingFile{
							chunk:            chunk,
							content:          2,
							equalityFieldIDs: pkFieldIDs,
						}
						if partitioned {
							delKey = fmt.Sprintf("%s/data/%s/%s-eq-delete-%d.parquet", basePath, b.partPath, uuid.New().String(), j)
							dpf.partitionValues = ts.partSpec.PartitionAvroValue(b.partValues, ts.srcSchema)
						} else {
							delKey = fmt.Sprintf("%s/data/%s-eq-delete-%d.parquet", basePath, uuid.New().String(), j)
						}
						dpf.key = delKey
						pf = append(pf, dpf)
					}
				}

				// Serialize data files.
				dataWriter := NewRollingDataWriter(ts.srcSchema, targetSize)
				for _, s := range state {
					if s.op == "I" || s.op == "U" {
						if err := dataWriter.Add(s.row); err != nil {
							return fmt.Errorf("add data row: %w", err)
						}
					}
				}
				chunks, err := dataWriter.FlushAll()
				if err != nil {
					return fmt.Errorf("flush data: %w", err)
				}
				if partitioned {
					avroPartValues := ts.partSpec.PartitionAvroValue(b.partValues, ts.srcSchema)
					for j, chunk := range chunks {
						key := fmt.Sprintf("%s/data/%s/%s-mat-%d.parquet", basePath, b.partPath, uuid.New().String(), j)
						pf = append(pf, pendingFile{
							key:             key,
							chunk:           chunk,
							content:         0,
							partitionValues: avroPartValues,
						})
					}
				} else {
					for j, chunk := range chunks {
						key := fmt.Sprintf("%s/data/%s-mat-%d.parquet", basePath, uuid.New().String(), j)
						pf = append(pf, pendingFile{key: key, chunk: chunk, content: 0})
					}
				}

				bucketResults[i].pending = pf
				bucketResults[i].deleteRowCount = deleteRowCount
				return nil
			},
		}
	}

	serializePool := worker.NewPool(foldConcurrency)
	if _, err := serializePool.Run(ctx, serializeTasks); err != nil {
		return fmt.Errorf("serialize: %w", err)
	}

	tSerialize := time.Since(start)

	// Collect results from all buckets.
	var pending []pendingFile
	var deleteRowCount int64
	for _, r := range bucketResults {
		pending = append(pending, r.pending...)
		deleteRowCount += r.deleteRowCount
	}

	// --- Phase 2: Upload all files in parallel (I/O-bound) ---
	var deleteEntries []ManifestEntry
	var dataEntries []ManifestEntry

	if len(pending) > 0 {
		type uploadedFile struct {
			uri string
			pf  pendingFile
		}

		var mu sync.Mutex
		var uploaded []uploadedFile

		tasks := make([]worker.Task, len(pending))
		for i, pf := range pending {
			pf := pf
			tasks[i] = worker.Task{
				Name: pf.key,
				Fn: func(ctx context.Context, _ *worker.Progress) error {
					uri, err := s3.Upload(ctx, pf.key, pf.chunk.Data)
					if err != nil {
						return fmt.Errorf("upload %s: %w", pf.key, err)
					}
					mu.Lock()
					uploaded = append(uploaded, uploadedFile{uri: uri, pf: pf})
					mu.Unlock()
					return nil
				},
			}
		}

		concurrency := m.cfg.MaterializerConcurrencyOrDefault()
		if len(tasks) < concurrency {
			concurrency = len(tasks)
		}
		pool := worker.NewPool(concurrency)
		if _, err := pool.Run(ctx, tasks); err != nil {
			return fmt.Errorf("upload materialized files: %w", err)
		}

		for _, u := range uploaded {
			entry := ManifestEntry{
				Status:     1,
				SnapshotID: snapshotID,
				DataFile: DataFileInfo{
					Path:             u.uri,
					FileSizeBytes:    int64(len(u.pf.chunk.Data)),
					RecordCount:      u.pf.chunk.RowCount,
					Content:          u.pf.content,
					EqualityFieldIDs: u.pf.equalityFieldIDs,
					PartitionValues:  u.pf.partitionValues,
				},
			}
			if u.pf.content == 0 {
				dataEntries = append(dataEntries, entry)
			} else {
				deleteEntries = append(deleteEntries, entry)
			}
		}
	}

	tUpload := time.Since(start)

	// Nothing to commit if no deletes and no data.
	if len(deleteEntries) == 0 && len(dataEntries) == 0 {
		if fromBuffer && bufSnapID > 0 {
			m.lastEventsSnapshot[pgTable] = bufSnapID
		}
		return nil
	}

	// --- Phase 3: Carry forward existing manifests and commit ---
	// Use cached manifests from previous cycle. On first run (cache empty),
	// load from S3 if a manifest list exists (crash recovery).
	if ts.matManifests == nil && matTm != nil && matTm.Metadata.CurrentSnapshotID > 0 {
		var err error
		ts.matManifests, err = m.loadExistingManifests(ctx, s3, matTm)
		if err != nil {
			return fmt.Errorf("load existing manifests: %w", err)
		}
	}
	existingManifests := ts.matManifests

	var newManifests []ManifestFileInfo

	// Write new data manifest (if we have data entries).
	if len(dataEntries) > 0 {
		manifestBytes, err := WriteManifest(ts.srcSchema, dataEntries, seqNum, 0, ts.partSpec)
		if err != nil {
			return fmt.Errorf("write data manifest: %w", err)
		}
		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-data.avro", basePath, uuid.New().String())
		manifestURI, err := s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload data manifest: %w", err)
		}
		var totalRows int64
		for _, e := range dataEntries {
			totalRows += e.DataFile.RecordCount
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0, // data
			SnapshotID:     snapshotID,
			AddedFiles:     len(dataEntries),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	// Write new delete manifest (if we have delete entries).
	if len(deleteEntries) > 0 {
		manifestBytes, err := WriteManifest(ts.srcSchema, deleteEntries, seqNum, 1, ts.partSpec)
		if err != nil {
			return fmt.Errorf("write delete manifest: %w", err)
		}
		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-deletes.avro", basePath, uuid.New().String())
		manifestURI, err := s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return fmt.Errorf("upload delete manifest: %w", err)
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        1, // deletes
			SnapshotID:     snapshotID,
			AddedFiles:     len(deleteEntries),
			AddedRows:      deleteRowCount,
			SequenceNumber: seqNum,
		})
	}

	// Manifest list = existing manifests + new manifests. Cache for next cycle.
	allManifests := append(existingManifests, newManifests...)
	ts.matManifests = allManifests

	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return fmt.Errorf("write manifest list: %w", err)
	}

	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return fmt.Errorf("upload manifest list: %w", err)
	}

	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		SchemaID:         ts.matSchemaID,
		Summary: map[string]string{
			"operation": "overwrite",
		},
	}

	if err := catalog.CommitSnapshot(ns, ts.icebergName, prevMatSnapID, commit); err != nil {
		return fmt.Errorf("commit materialized snapshot: %w", err)
	}
	tCommit := time.Since(start)

	// Invalidate the file index so it's rebuilt on next TOAST resolution.
	// MoR doesn't rewrite files, but equality deletes logically remove old rows.
	// Rebuilding ensures the index reflects the current state.
	if fi := m.fileIndexes[pgTable]; fi != nil {
		fi.snapshotID = 0 // force rebuild on next use
	}

	// Update checkpoint for in-memory path (S3 path already updated above).
	if fromBuffer && bufSnapID > 0 {
		m.lastEventsSnapshot[pgTable] = bufSnapID
	}

	source := "buffer"
	if !fromBuffer {
		source = "s3"
	}
	duration := time.Since(start)

	// Track highest materialized LSN.
	var maxLSN int64
	for _, ev := range events {
		if ev.lsn > maxLSN {
			maxLSN = ev.lsn
		}
	}
	if maxLSN > 0 {
		metrics.MaterializerMaterializedLSN.WithLabelValues(pgTable).Set(float64(maxLSN))
	}

	metrics.MaterializerDurationSeconds.WithLabelValues(pgTable).Observe(duration.Seconds())
	metrics.MaterializerRunsTotal.WithLabelValues(pgTable, source).Inc()
	metrics.MaterializerEventsTotal.WithLabelValues(pgTable).Add(float64(len(events)))
	metrics.MaterializerDataFilesWrittenTotal.WithLabelValues(pgTable).Add(float64(len(dataEntries)))
	metrics.MaterializerDeleteFilesWrittenTotal.WithLabelValues(pgTable).Add(float64(len(deleteEntries)))
	metrics.MaterializerDeleteRowsTotal.WithLabelValues(pgTable).Add(float64(deleteRowCount))

	log.Printf("[materializer] materialized %s: %d events (%s), %d buckets, %d data files, %d delete files (%.1fs) [drain=%.0fms fold=%.0fms toast=%.0fms upload=%.0fms commit=%.0fms]",
		pgTable, len(events), source, len(buckets), len(dataEntries), len(deleteEntries), duration.Seconds(),
		float64(tDrain.Milliseconds()), float64((tFold-tDrain).Milliseconds()),
		float64((tToast-tFold).Milliseconds()),
		float64((tUpload-tSerialize).Milliseconds()), float64((tCommit-tUpload).Milliseconds()))

	return nil
}

// loadExistingManifests reads all manifest file entries from the current
// snapshot's manifest list. These are carried forward unchanged into the
// new manifest list.
func (m *Materializer) loadExistingManifests(ctx context.Context, s3 ObjectStorage, tm *TableMetadata) ([]ManifestFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	return ReadManifestList(mlData)
}

// buildFileIndex reads all data files for a materialized table and builds the
// PK→file index. Called on startup or when the index is missing (crash recovery).
func (m *Materializer) buildFileIndex(ctx context.Context, pgTable string, ts *tableSink, matTm *TableMetadata) (*fileIndex, error) {
	fi := newFileIndex()
	if matTm == nil || matTm.Metadata.CurrentSnapshotID == 0 {
		return fi, nil
	}
	fi.snapshotID = matTm.Metadata.CurrentSnapshotID

	allFiles, err := m.loadAllDataFiles(ctx, m.s3, matTm)
	if err != nil {
		return nil, fmt.Errorf("load data files for index: %w", err)
	}

	pk := ts.srcSchema.PK

	// Process files in parallel with bounded concurrency using the worker pool.
	maxConcurrency := m.cfg.MaterializerConcurrencyOrDefault()

	type fileResult struct {
		df     DataFileInfo
		pkKeys []string
	}

	var (
		mu          sync.Mutex
		fileResults []fileResult
	)

	tasks := make([]worker.Task, 0, len(allFiles))
	for _, df := range allFiles {
		dfKey, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		tasks = append(tasks, worker.Task{
			Name: dfKey,
			Fn: func(ctx context.Context, _ *worker.Progress) error {
				size, err := m.s3.StatObject(ctx, dfKey)
				if err != nil {
					return fmt.Errorf("stat %s: %w", df.Path, err)
				}
				ra := &s3ReaderAt{ctx: ctx, s3: m.s3, key: dfKey}
				pkKeys, err := readParquetPKKeysFromReaderAt(ra, size, pk)
				if err != nil {
					return fmt.Errorf("read PKs from %s: %w", df.Path, err)
				}
				mu.Lock()
				fileResults = append(fileResults, fileResult{df: df, pkKeys: pkKeys})
				mu.Unlock()
				return nil
			},
		})
	}

	pool := worker.NewPool(maxConcurrency)
	if _, err := pool.Run(ctx, tasks); err != nil {
		return nil, fmt.Errorf("build file index: %w", err)
	}

	for _, r := range fileResults {
		fi.addFile(r.df, r.pkKeys)
	}

	log.Printf("[materializer] built file index for %s: %d files, %d PKs", pgTable, len(fi.files), len(fi.pkToFile))
	return fi, nil
}

// loadAllDataFiles returns all live data files from a table's current snapshot.
func (m *Materializer) loadAllDataFiles(ctx context.Context, s3 ObjectStorage, tm *TableMetadata) ([]DataFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	manifestInfos, err := ReadManifestList(mlData)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}

	var dataFiles []DataFileInfo
	for _, mfi := range manifestInfos {
		if mfi.Content != 0 {
			continue
		}
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
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			dataFiles = append(dataFiles, e.DataFile)
		}
	}
	return dataFiles, nil
}

// findNewEventFiles returns data files added to the events table since the
// given snapshot ID. If lastSnapshotID is 0, returns all data files.
func (m *Materializer) findNewEventFiles(ctx context.Context, s3 ObjectStorage, tm *TableMetadata, lastSnapshotID int64) ([]DataFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := s3.Download(ctx, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	manifestInfos, err := ReadManifestList(mlData)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}

	var dataFiles []DataFileInfo
	for _, mfi := range manifestInfos {
		if mfi.Content != 0 {
			continue
		}
		// If we have a checkpoint, only read manifests added after it.
		if lastSnapshotID > 0 && mfi.SnapshotID <= lastSnapshotID {
			continue
		}

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
			if e.Status == 2 || e.DataFile.Content != 0 {
				continue
			}
			dataFiles = append(dataFiles, e.DataFile)
		}
	}
	return dataFiles, nil
}

// readEvents reads event parquet files and parses them into changeEvent structs.
func (m *Materializer) readEvents(ctx context.Context, s3 ObjectStorage, files []DataFileInfo, srcSchema *schema.TableSchema) ([]changeEvent, error) {
	var events []changeEvent

	// Build the events schema to read the parquet files.
	eventsSchema := EventsTableSchema(srcSchema)

	for _, df := range files {
		key, err := KeyFromURI(df.Path)
		if err != nil {
			continue
		}
		data, err := s3.Download(ctx, key)
		if err != nil {
			log.Printf("[materializer] failed to download event file %s: %v", df.Path, err)
			continue
		}

		rows, err := readParquetRows(data, eventsSchema)
		if err != nil {
			log.Printf("[materializer] failed to read event file %s: %v", df.Path, err)
			continue
		}

		for _, row := range rows {
			ev := changeEvent{
				op:  fmt.Sprintf("%v", row["_op"]),
				lsn: toInt64(row["_lsn"]),
				seq: toInt64(row["_seq"]),
			}

			// Parse unchanged cols.
			if uc, ok := row["_unchanged_cols"]; ok && uc != nil {
				ucStr := fmt.Sprintf("%v", uc)
				if ucStr != "" {
					ev.unchangedCols = strings.Split(ucStr, ",")
				}
			}

			// Extract user columns only.
			ev.row = make(map[string]any, len(srcSchema.Columns))
			for _, col := range srcSchema.Columns {
				ev.row[col.Name] = row[col.Name]
			}

			events = append(events, ev)
		}
	}

	return events, nil
}

// readParquetRows reads a parquet file and returns rows as maps.
func readParquetRows(data []byte, ts *schema.TableSchema) ([]map[string]any, error) {
	reader := pq.NewReader(bytes.NewReader(data))

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
		return v.Int32()
	case pq.Int64:
		return v.Int64()
	case pq.Float:
		return v.Float()
	case pq.Double:
		return v.Double()
	case pq.ByteArray, pq.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

// readParquetPKKeysFromReaderAt reads only PK columns from a parquet file via
// an io.ReaderAt (backed by S3 range reads). Only the footer and PK column
// chunks are fetched — non-PK column data is never read from storage.
func readParquetPKKeysFromReaderAt(r io.ReaderAt, size int64, pk []string) ([]string, error) {
	file, err := pq.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	schema := file.Schema()

	// Find PK column indices in the parquet schema.
	pkColIdx := make(map[string]int, len(pk))
	for i, f := range schema.Fields() {
		for _, p := range pk {
			if f.Name() == p {
				pkColIdx[p] = i
			}
		}
	}
	if len(pkColIdx) != len(pk) {
		return nil, fmt.Errorf("parquet schema missing PK columns: have %v, want %v", pkColIdx, pk)
	}

	var keys []string

	for _, rg := range file.RowGroups() {
		numRows := rg.NumRows()

		// Read values from each PK column chunk.
		pkValues := make(map[string][]string, len(pk))
		for _, p := range pk {
			idx := pkColIdx[p]
			chunk := rg.ColumnChunks()[idx]
			pages := chunk.Pages()

			vals := make([]string, 0, numRows)
			for {
				page, err := pages.ReadPage()
				if page == nil || err != nil {
					break
				}
				pageValues := page.Values()
				vBuf := make([]pq.Value, page.NumValues())
				n, _ := pageValues.ReadValues(vBuf)
				for i := 0; i < n; i++ {
					if vBuf[i].IsNull() {
						vals = append(vals, "<nil>")
					} else {
						vals = append(vals, fmt.Sprintf("%v", parquetValueToGo(vBuf[i])))
					}
				}
			}
			pages.Close()
			pkValues[p] = vals
		}

		// Build PK key strings.
		nRows := int(numRows)
		if len(pk) == 1 {
			keys = append(keys, pkValues[pk[0]]...)
		} else {
			for i := 0; i < nRows; i++ {
				var b strings.Builder
				for j, col := range pk {
					if j > 0 {
						b.WriteByte(0)
					}
					if i < len(pkValues[col]) {
						b.WriteString(pkValues[col][i])
					}
				}
				keys = append(keys, b.String())
			}
		}
	}

	return keys, nil
}

// buildPKKey creates a unique key for a row based on its PK columns.
// Uses null byte as separator to avoid collisions.
// extractPartBucketKey extracts the partition bucket key from a file path.
// E.g. ".../data/seq_truncate=0/uuid-mat-0.parquet" → "seq_truncate=0"
func extractPartBucketKey(filePath string) string {
	idx := strings.Index(filePath, "/data/")
	if idx < 0 {
		return ""
	}
	afterData := filePath[idx+len("/data/"):]
	slashIdx := strings.Index(afterData, "/")
	if slashIdx < 0 {
		return ""
	}
	return afterData[:slashIdx]
}

func buildPKKey(row map[string]any, pk []string) string {
	if len(pk) == 1 {
		return anyToString(row[pk[0]])
	}
	var b strings.Builder
	for i, col := range pk {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(anyToString(row[col]))
	}
	return b.String()
}

// anyToString converts a value to string without reflection for common PK types.
func anyToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.Itoa(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case []byte:
		return string(x)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
