package iceberg

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/utils"
)

// RowState is the final state of a single PK after folding/dedup.
type RowState struct {
	Op            string         // "I", "U", "D"
	Row           map[string]any // full row for I/U, PK-only for D
	UnchangedCols []string       // non-empty only if TOAST columns still unresolved
}

// TableWriteConfig holds everything needed to write to a materialized Iceberg table.
type TableWriteConfig struct {
	Namespace   string
	IcebergName string
	SrcSchema   *postgres.TableSchema
	PartSpec    *PartitionSpec
	SchemaID    int
	TargetSize  int64
	Concurrency int
}

// PreparedCommit holds everything assembled for catalog commit.
// The caller decides when/how to commit (single or multi-table).
type PreparedCommit struct {
	IcebergName    string
	PrevSnapshotID int64
	Commit         SnapshotCommit
	NewManifests   []ManifestFileInfo // updated manifest cache for post-commit

	// Diagnostics.
	DataCount      int
	DeleteCount    int
	DeleteRowCount int64
	BucketCount    int
}

// FileIndex tracks which PKs live in which data files for a single table.
// Used to resolve TOAST unchanged columns and route DELETEs to correct partitions.
type FileIndex struct {
	// PkToFile maps PK key → file path (the DataFileInfo.Path).
	PkToFile map[string]string
	// Files maps file path → DataFileInfo.
	Files map[string]DataFileInfo
	// FilePKs maps file path → set of PK keys in that file.
	FilePKs map[string]map[string]bool
	// SnapshotID is the materialized table snapshot this index was built from.
	SnapshotID int64
}

// NewFileIndex creates a new empty file index.
func NewFileIndex() *FileIndex {
	return &FileIndex{
		PkToFile: make(map[string]string),
		Files:    make(map[string]DataFileInfo),
		FilePKs:  make(map[string]map[string]bool),
	}
}

// AddFile registers all PKs in a file.
func (fi *FileIndex) AddFile(df DataFileInfo, pkKeys []string) {
	fi.Files[df.Path] = df
	pks := make(map[string]bool, len(pkKeys))
	for _, pk := range pkKeys {
		fi.PkToFile[pk] = df.Path
		pks[pk] = true
	}
	fi.FilePKs[df.Path] = pks
}

// AffectedFiles returns the set of file paths that contain any of the given PKs.
func (fi *FileIndex) AffectedFiles(pks []string) map[string]bool {
	paths := make(map[string]bool)
	for _, pk := range pks {
		if path, ok := fi.PkToFile[pk]; ok {
			paths[path] = true
		}
	}
	return paths
}

// TableWriter writes PK-keyed row states to an Iceberg table using
// merge-on-read: equality deletes for old PKs, data files for new rows.
type TableWriter struct {
	cfg     TableWriteConfig
	catalog Catalog
	s3      ObjectStorage

	// Cached manifest list from previous cycle (carried forward).
	manifests []ManifestFileInfo

	// File index for DELETE partition routing and TOAST resolution.
	FileIdx *FileIndex
}

// UpdateSchema updates the writer's schema after a schema evolution.
func (tw *TableWriter) UpdateSchema(srcSchema *postgres.TableSchema, schemaID int) {
	tw.cfg.SrcSchema = srcSchema
	tw.cfg.SchemaID = schemaID
}

// NewTableWriter creates a new TableWriter.
func NewTableWriter(cfg TableWriteConfig, catalog Catalog, s3 ObjectStorage) *TableWriter {
	return &TableWriter{
		cfg:     cfg,
		catalog: catalog,
		s3:      s3,
	}
}

// pendingFile holds a serialized Parquet file ready for upload.
type pendingFile struct {
	key              string
	chunk            FileChunk
	content          int // 0=data, 2=equality delete
	equalityFieldIDs []int
	partitionValues  map[string]any
}

// bucketResult holds the output of serialization for one partition bucket.
type bucketResult struct {
	pending        []pendingFile
	deleteRowCount int64
}

// Prepare takes the final state per PK (already folded/deduped), partitions it,
// serializes to Parquet, uploads to S3, and assembles manifests. Returns a
// prepared commit — the caller decides when to call catalog.CommitTransaction.
//
// Returns nil if there are no rows to write.
func (tw *TableWriter) Prepare(ctx context.Context, rows []RowState, pk []string) (*PreparedCommit, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	cfg := tw.cfg
	ts := cfg.SrcSchema
	partSpec := cfg.PartSpec
	targetSize := cfg.TargetSize
	pkFieldIDs := ts.PKFieldIDs()
	partitioned := partSpec != nil && !partSpec.IsUnpartitioned()
	basePath := fmt.Sprintf("%s.db/%s", cfg.Namespace, cfg.IcebergName)

	// Load materialized table metadata for commit.
	matTm, err := tw.catalog.LoadTable(ctx, cfg.Namespace, cfg.IcebergName)
	if err != nil {
		return nil, fmt.Errorf("load materialized table: %w", err)
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

	// --- Partition bucketing ---
	type eventBucket struct {
		key        string
		rows       []RowState
		partValues map[string]any
		partPath   string
	}

	// For partitioned tables, DELETE rows may lack partition columns.
	// Track PK→partition key so DELETEs can be routed to the correct bucket.
	pkPartKey := make(map[string]string)
	bucketMap := make(map[string]*eventBucket)
	numShards := runtime.NumCPU()
	if numShards < 2 {
		numShards = 2
	}

	for i := range rows {
		rs := &rows[i]
		var bKey string
		if partitioned {
			pkKey := BuildPKKey(rs.Row, pk)
			if rs.Op == "D" {
				if cached, ok := pkPartKey[pkKey]; ok {
					bKey = cached
				} else if tw.FileIdx != nil {
					if filePath, ok := tw.FileIdx.PkToFile[pkKey]; ok {
						bKey = ExtractPartBucketKey(filePath)
					} else {
						continue
					}
				} else {
					continue
				}
			} else {
				bKey = partSpec.PartitionKey(rs.Row, ts)
				pkPartKey[pkKey] = bKey
			}
		} else {
			pkKey := BuildPKKey(rs.Row, pk)
			var h uint32
			for j := 0; j < len(pkKey); j++ {
				h = h*31 + uint32(pkKey[j])
			}
			bKey = strconv.Itoa(int(h % uint32(numShards)))
		}

		b, ok := bucketMap[bKey]
		if !ok {
			b = &eventBucket{key: bKey}
			if partitioned && rs.Op != "D" {
				b.partValues = partSpec.PartitionValues(rs.Row, ts)
				b.partPath = partSpec.PartitionPath(b.partValues)
			}
			bucketMap[bKey] = b
		}
		if partitioned && b.partValues == nil && rs.Op != "D" {
			b.partValues = partSpec.PartitionValues(rs.Row, ts)
			b.partPath = partSpec.PartitionPath(b.partValues)
		}
		b.rows = append(b.rows, *rs)
	}

	// Resolve partition values for DELETE-only buckets using the file index path.
	if partitioned && tw.FileIdx != nil {
		for bKey, b := range bucketMap {
			if b.partValues != nil {
				continue
			}
			b.partPath = bKey
			b.partValues = partSpec.ParsePartitionPath(bKey, ts)
		}
	}

	buckets := make([]*eventBucket, 0, len(bucketMap))
	for _, b := range bucketMap {
		buckets = append(buckets, b)
	}

	// --- Serialize per bucket (equality deletes + data files) ---
	results := make([]bucketResult, len(buckets))

	serializeTasks := make([]utils.Task, len(buckets))
	for i, b := range buckets {
		i, b := i, b
		serializeTasks[i] = utils.Task{
			Name: b.key,
			Fn: func(ctx context.Context, _ *utils.Progress) error {
				// Equality delete files for U/D rows.
				var deleteRowCount int64
				deleteWriter := NewRollingDeleteWriter(ts, targetSize)
				for _, rs := range b.rows {
					if rs.Op == "U" || rs.Op == "D" {
						if err := deleteWriter.Add(rs.Row); err != nil {
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
						dpf := pendingFile{
							chunk:            chunk,
							content:          2,
							equalityFieldIDs: pkFieldIDs,
						}
						if partitioned {
							dpf.key = fmt.Sprintf("%s/data/%s/%s-eq-delete-%d.parquet", basePath, b.partPath, uuid.New().String(), j)
							dpf.partitionValues = partSpec.PartitionAvroValue(b.partValues, ts)
						} else {
							dpf.key = fmt.Sprintf("%s/data/%s-eq-delete-%d.parquet", basePath, uuid.New().String(), j)
						}
						pf = append(pf, dpf)
					}
				}

				// Data files for I/U rows.
				dataWriter := NewRollingDataWriter(ts, targetSize)
				for _, rs := range b.rows {
					if rs.Op == "I" || rs.Op == "U" {
						if err := dataWriter.Add(rs.Row); err != nil {
							return fmt.Errorf("add data row: %w", err)
						}
					}
				}
				chunks, err := dataWriter.FlushAll()
				if err != nil {
					return fmt.Errorf("flush data: %w", err)
				}
				if partitioned {
					avroPartValues := partSpec.PartitionAvroValue(b.partValues, ts)
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

				results[i] = bucketResult{pending: pf, deleteRowCount: deleteRowCount}
				return nil
			},
		}
	}

	concurrency := tw.concurrency()
	if len(serializeTasks) < concurrency {
		concurrency = len(serializeTasks)
	}
	if concurrency < 1 {
		concurrency = 1
	}
	serializePool := utils.NewPool(concurrency)
	if _, err := serializePool.Run(ctx, serializeTasks); err != nil {
		return nil, fmt.Errorf("serialize: %w", err)
	}

	// Collect results.
	var allPending []pendingFile
	var totalDeleteRows int64
	for _, r := range results {
		allPending = append(allPending, r.pending...)
		totalDeleteRows += r.deleteRowCount
	}

	// --- Upload all files in parallel ---
	var deleteEntries []ManifestEntry
	var dataEntries []ManifestEntry

	if len(allPending) > 0 {
		type uploadedFile struct {
			uri string
			pf  pendingFile
		}

		var mu sync.Mutex
		var uploaded []uploadedFile

		uploadTasks := make([]utils.Task, len(allPending))
		for i, pf := range allPending {
			pf := pf
			uploadTasks[i] = utils.Task{
				Name: pf.key,
				Fn: func(ctx context.Context, _ *utils.Progress) error {
					uri, err := tw.s3.Upload(ctx, pf.key, pf.chunk.Data)
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

		uploadConcurrency := tw.concurrency()
		if len(uploadTasks) < uploadConcurrency {
			uploadConcurrency = len(uploadTasks)
		}
		uploadPool := utils.NewPool(uploadConcurrency)
		if _, err := uploadPool.Run(ctx, uploadTasks); err != nil {
			return nil, fmt.Errorf("upload materialized files: %w", err)
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

	// Nothing to commit.
	if len(deleteEntries) == 0 && len(dataEntries) == 0 {
		return nil, nil
	}

	// --- Manifest assembly ---
	// Load cached manifests or fetch from S3 on first run.
	if tw.manifests == nil && matTm != nil && matTm.Metadata.CurrentSnapshotID > 0 {
		tw.manifests, err = tw.loadExistingManifests(ctx, matTm)
		if err != nil {
			return nil, fmt.Errorf("load existing manifests: %w", err)
		}
	}
	existingManifests := tw.manifests

	var newManifests []ManifestFileInfo

	if len(dataEntries) > 0 {
		manifestBytes, err := WriteManifest(ts, dataEntries, seqNum, 0, partSpec)
		if err != nil {
			return nil, fmt.Errorf("write data manifest: %w", err)
		}
		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-data.avro", basePath, uuid.New().String())
		manifestURI, err := tw.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return nil, fmt.Errorf("upload data manifest: %w", err)
		}
		var totalRows int64
		for _, e := range dataEntries {
			totalRows += e.DataFile.RecordCount
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        0,
			SnapshotID:     snapshotID,
			AddedFiles:     len(dataEntries),
			AddedRows:      totalRows,
			SequenceNumber: seqNum,
		})
	}

	if len(deleteEntries) > 0 {
		manifestBytes, err := WriteManifest(ts, deleteEntries, seqNum, 1, partSpec)
		if err != nil {
			return nil, fmt.Errorf("write delete manifest: %w", err)
		}
		manifestKey := fmt.Sprintf("%s/metadata/%s-mat-deletes.avro", basePath, uuid.New().String())
		manifestURI, err := tw.s3.Upload(ctx, manifestKey, manifestBytes)
		if err != nil {
			return nil, fmt.Errorf("upload delete manifest: %w", err)
		}
		newManifests = append(newManifests, ManifestFileInfo{
			Path:           manifestURI,
			Length:         int64(len(manifestBytes)),
			Content:        1,
			SnapshotID:     snapshotID,
			AddedFiles:     len(deleteEntries),
			AddedRows:      totalDeleteRows,
			SequenceNumber: seqNum,
		})
	}

	allManifests := append(existingManifests, newManifests...)

	mlBytes, err := WriteManifestList(allManifests)
	if err != nil {
		return nil, fmt.Errorf("write manifest list: %w", err)
	}
	mlKey := fmt.Sprintf("%s/metadata/snap-%d-0-manifest-list.avro", basePath, snapshotID)
	mlURI, err := tw.s3.Upload(ctx, mlKey, mlBytes)
	if err != nil {
		return nil, fmt.Errorf("upload manifest list: %w", err)
	}

	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		SchemaID:         cfg.SchemaID,
		Summary: map[string]string{
			"operation": "overwrite",
		},
	}

	return &PreparedCommit{
		IcebergName:    cfg.IcebergName,
		PrevSnapshotID: prevMatSnapID,
		Commit:         commit,
		NewManifests:   allManifests,
		DataCount:      len(dataEntries),
		DeleteCount:    len(deleteEntries),
		DeleteRowCount: totalDeleteRows,
		BucketCount:    len(buckets),
	}, nil
}

// ApplyPostCommit updates the manifest cache and invalidates the file index
// after a successful catalog commit. Must be called only after CommitTransaction succeeds.
func (tw *TableWriter) ApplyPostCommit(pc *PreparedCommit) {
	tw.manifests = pc.NewManifests
	// Invalidate file index so it's rebuilt on next cycle.
	if tw.FileIdx != nil {
		tw.FileIdx.SnapshotID = 0
	}
}

// ToTableCommit converts a PreparedCommit into a TableCommit for catalog.CommitTransaction.
func (pc *PreparedCommit) ToTableCommit() TableCommit {
	return TableCommit{
		Table:             pc.IcebergName,
		CurrentSnapshotID: pc.PrevSnapshotID,
		Snapshot:          pc.Commit,
	}
}

// BuildFileIndex reads all data files for the materialized table and builds the
// PK→file index. Returns the index or reuses the cached one if it's still current.
func (tw *TableWriter) BuildFileIndex(ctx context.Context, pk []string) (*FileIndex, error) {
	matTm, err := tw.catalog.LoadTable(ctx, tw.cfg.Namespace, tw.cfg.IcebergName)
	if err != nil {
		return nil, fmt.Errorf("load table for file index: %w", err)
	}
	if matTm == nil || matTm.Metadata.CurrentSnapshotID == 0 {
		fi := NewFileIndex()
		tw.FileIdx = fi
		return fi, nil
	}

	// Return cached index if still current.
	if tw.FileIdx != nil && tw.FileIdx.SnapshotID == matTm.Metadata.CurrentSnapshotID {
		return tw.FileIdx, nil
	}

	fi := NewFileIndex()
	fi.SnapshotID = matTm.Metadata.CurrentSnapshotID

	allFiles, err := tw.LoadAllDataFiles(ctx, matTm)
	if err != nil {
		return nil, fmt.Errorf("load data files for index: %w", err)
	}

	concurrency := tw.concurrency()

	type fileResult struct {
		df     DataFileInfo
		pkKeys []string
	}

	var (
		mu          sync.Mutex
		fileResults []fileResult
	)

	tasks := make([]utils.Task, 0, len(allFiles))
	for _, df := range allFiles {
		dfKey, err := KeyFromURI(df.Path)
		if err != nil {
			return nil, fmt.Errorf("parse data file URI %s: %w", df.Path, err)
		}
		tasks = append(tasks, utils.Task{
			Name: dfKey,
			Fn: func(ctx context.Context, _ *utils.Progress) error {
				size, err := tw.s3.StatObject(ctx, dfKey)
				if err != nil {
					return fmt.Errorf("stat %s: %w", df.Path, err)
				}
				ra := &S3ReaderAt{Ctx: ctx, S3: tw.s3, Key: dfKey}
				pkKeys, err := ReadParquetPKKeysFromReaderAt(ra, size, pk)
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

	pool := utils.NewPool(concurrency)
	if _, err := pool.Run(ctx, tasks); err != nil {
		return nil, fmt.Errorf("build file index: %w", err)
	}

	for _, r := range fileResults {
		fi.AddFile(r.df, r.pkKeys)
	}

	log.Printf("[tablewriter] built file index for %s: %d files, %d PKs", tw.cfg.IcebergName, len(fi.Files), len(fi.PkToFile))
	tw.FileIdx = fi
	return fi, nil
}

// LoadAllDataFiles returns all live data files from a table's current snapshot.
func (tw *TableWriter) LoadAllDataFiles(ctx context.Context, tm *TableMetadata) ([]DataFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := DownloadWithRetry(ctx, tw.s3, mlKey)
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
			return nil, fmt.Errorf("parse manifest URI %s: %w", mfi.Path, err)
		}
		mData, err := DownloadWithRetry(ctx, tw.s3, mKey)
		if err != nil {
			return nil, fmt.Errorf("download manifest %s: %w", mfi.Path, err)
		}
		entries, err := ReadManifest(mData)
		if err != nil {
			return nil, fmt.Errorf("read manifest %s: %w", mfi.Path, err)
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

// loadExistingManifests reads all manifest file entries from the current
// snapshot's manifest list.
func (tw *TableWriter) loadExistingManifests(ctx context.Context, tm *TableMetadata) ([]ManifestFileInfo, error) {
	mlURI := tm.CurrentManifestList()
	if mlURI == "" {
		return nil, nil
	}

	mlKey, err := KeyFromURI(mlURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest list URI: %w", err)
	}
	mlData, err := DownloadWithRetry(ctx, tw.s3, mlKey)
	if err != nil {
		return nil, fmt.Errorf("download manifest list: %w", err)
	}
	return ReadManifestList(mlData)
}

func (tw *TableWriter) concurrency() int {
	c := tw.cfg.Concurrency
	if c <= 0 {
		c = runtime.NumCPU()
	}
	return c
}
