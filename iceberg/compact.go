package iceberg

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/google/uuid"
	pq "github.com/parquet-go/parquet-go"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var compactTracer = otel.Tracer("pg2iceberg/compact")

// CompactionConfig holds thresholds that trigger compaction.
type CompactionConfig struct {
	DataFileThreshold   int // compact when data files exceed this
	DeleteFileThreshold int // compact when delete files exceed this
}

// ReadParquetRowsFromReaderAt reads all rows from a parquet file via io.ReaderAt
// (S3 range reads). Returns rows as maps keyed by column name.
func ReadParquetRowsFromReaderAt(r io.ReaderAt, size int64, schema *postgres.TableSchema) ([]map[string]any, error) {
	file, err := pq.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	pqSchema := file.Schema()
	fields := pqSchema.Fields()
	colNames := make([]string, len(fields))
	for i, f := range fields {
		colNames[i] = f.Name()
	}

	var rows []map[string]any
	rowBuf := make([]pq.Row, 1)

	for _, rg := range file.RowGroups() {
		reader := pq.NewRowGroupReader(rg)
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
					row[colNames[colIdx]] = parquetValueWithField(v, fields[colIdx])
				}
			}
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// Compact rewrites all data and delete files into clean data-only files.
// Returns nil if file counts are below thresholds (no compaction needed).
// The caller must commit via CommitSnapshot (which updates the MetadataCache).
func (tw *TableWriter) Compact(ctx context.Context, pk []string, cc CompactionConfig) (*PreparedCommit, error) {
	cfg := tw.cfg
	ns := cfg.Namespace

	// Load table state from catalog cache (cache hit in steady state).
	matTm, err := tw.catalog.LoadTable(ctx, ns, cfg.IcebergName)
	if err != nil {
		return nil, fmt.Errorf("load table: %w", err)
	}
	if matTm == nil || matTm.Metadata.CurrentSnapshotID <= 0 {
		return nil, nil
	}
	basePath := TableBasePath(matTm.Metadata.Location, ns, cfg.IcebergName)
	currentSnapID := matTm.Metadata.CurrentSnapshotID
	lastSeqNum := matTm.Metadata.LastSequenceNumber

	// Check if last snapshot was compaction — skip if so.
	for _, snap := range matTm.Metadata.Snapshots {
		if snap.SnapshotID == currentSnapID {
			if snap.Summary["operation"] == "replace" {
				return nil, nil
			}
			break
		}
	}

	// Seed manifest cache on cold start.
	manifests := tw.catalog.Manifests(ns, cfg.IcebergName)
	if manifests == nil {
		manifests, _ = tw.loadExistingManifests(ctx, matTm)
		if manifests != nil {
			tw.catalog.SetManifests(ns, cfg.IcebergName, manifests)
		}
	}
	if manifests == nil {
		return nil, nil
	}

	// Count data and delete files.
	var dataFileCount, deleteFileCount int
	for _, mfi := range manifests {
		if mfi.Content == 0 {
			dataFileCount += mfi.AddedFiles
		} else {
			deleteFileCount += mfi.AddedFiles
		}
	}

	if dataFileCount < cc.DataFileThreshold && deleteFileCount < cc.DeleteFileThreshold {
		return nil, nil // below thresholds
	}

	// Only create a span when compaction actually runs.
	ctx, span := compactTracer.Start(ctx, "pg2iceberg.compact", trace.WithAttributes(
		attribute.String("iceberg.table", cfg.IcebergName),
		attribute.Int("data_files", dataFileCount),
		attribute.Int("delete_files", deleteFileCount),
	))
	defer span.End()

	log.Printf("[compact] %s: %d data files, %d delete files — compacting", cfg.IcebergName, dataFileCount, deleteFileCount)

	// Read all manifest entries with sequence numbers.
	type seqFile struct {
		DataFileInfo
		SeqNum int64
	}
	var dataFiles []seqFile
	var deleteFiles []seqFile

	for _, mfi := range manifests {
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
			if e.Status == 2 { // deleted entry
				continue
			}
			sf := seqFile{DataFileInfo: e.DataFile, SeqNum: e.SequenceNumber}
			if e.DataFile.Content == 0 {
				dataFiles = append(dataFiles, sf)
			} else if e.DataFile.Content == 2 {
				deleteFiles = append(deleteFiles, sf)
			}
		}
	}

	if len(dataFiles) == 0 {
		return nil, nil
	}

	// Collect PKs to delete with their sequence numbers.
	// Per Iceberg spec: equality deletes apply to data files with
	// sequence number STRICTLY LESS THAN the delete's sequence number.
	// deletePKSeq maps PK → delete sequence number.
	deletePKSeq := make(map[string]int64)
	concurrency := tw.concurrency()

	if len(deleteFiles) > 0 {
		type deleteResult struct {
			keys   []string
			seqNum int64
		}
		results := make([]deleteResult, len(deleteFiles))

		tasks := make([]utils.Task, len(deleteFiles))
		for i, df := range deleteFiles {
			i, df := i, df
			tasks[i] = utils.Task{
				Name: df.Path,
				Fn: func(ctx context.Context, _ *utils.Progress) error {
					dfKey, err := KeyFromURI(df.Path)
					if err != nil {
						return fmt.Errorf("parse delete file URI: %w", err)
					}
					size, err := tw.s3.StatObject(ctx, dfKey)
					if err != nil {
						return fmt.Errorf("stat %s: %w", df.Path, err)
					}
					ra := &S3ReaderAt{Ctx: ctx, S3: tw.s3, Key: dfKey}
					keys, err := ReadParquetPKKeysFromReaderAt(ra, size, pk)
					if err != nil {
						return fmt.Errorf("read delete PKs from %s: %w", df.Path, err)
					}
					results[i] = deleteResult{keys: keys, seqNum: df.SeqNum}
					return nil
				},
			}
		}

		pool := utils.NewPool(concurrency)
		if _, err := pool.Run(ctx, tasks); err != nil {
			return nil, fmt.Errorf("read delete files: %w", err)
		}

		for _, r := range results {
			for _, k := range r.keys {
				// Keep the highest delete sequence for each PK.
				if existing, ok := deletePKSeq[k]; !ok || r.seqNum > existing {
					deletePKSeq[k] = r.seqNum
				}
			}
		}
		log.Printf("[compact] %s: %d PKs to delete", cfg.IcebergName, len(deletePKSeq))
	}

	// Identify which data files are affected by deletes using the file index.
	// Unaffected files are carried forward as-is (no read/rewrite needed).
	ts := cfg.SrcSchema
	targetSize := cfg.TargetSize
	if targetSize <= 0 {
		targetSize = 128 * 1024 * 1024
	}

	// Build a quick PK→file lookup if we have deletes.
	affectedFiles := make(map[string]bool) // file paths that need rewriting
	fileIdx := tw.catalog.FileIndex(ns, cfg.IcebergName)
	if len(deletePKSeq) > 0 && fileIdx != nil && fileIdx.SnapshotID == currentSnapID {
		// Use cached file index to find affected files.
		pks := make([]string, 0, len(deletePKSeq))
		for pk := range deletePKSeq {
			pks = append(pks, pk)
		}
		affectedFiles = fileIdx.AffectedFiles(pks)
		log.Printf("[compact] %s: %d/%d data files affected by deletes",
			cfg.IcebergName, len(affectedFiles), len(dataFiles))
	} else if len(deletePKSeq) > 0 {
		// No cached file index — must check all files (fallback).
		for _, df := range dataFiles {
			affectedFiles[df.Path] = true
		}
	}

	// Also merge small files: any file under 50% of target size is rewritten.
	smallFileThreshold := targetSize / 2
	for _, df := range dataFiles {
		if df.FileSizeBytes < smallFileThreshold {
			affectedFiles[df.Path] = true
		}
	}

	// Separate files into "carry forward" and "rewrite", preserving sequence numbers.
	type rewriteFile struct {
		DataFileInfo
		SeqNum int64
	}
	var filesToRewrite []rewriteFile
	var carriedEntries []ManifestEntry

	now := time.Now()
	snapshotID := now.UnixMilli()
	seqNum := lastSeqNum + 1

	for _, df := range dataFiles {
		if affectedFiles[df.Path] {
			filesToRewrite = append(filesToRewrite, rewriteFile{DataFileInfo: df.DataFileInfo, SeqNum: df.SeqNum})
		} else {
			carriedEntries = append(carriedEntries, ManifestEntry{
				Status:     1,
				SnapshotID: snapshotID,
				DataFile:   df.DataFileInfo,
			})
		}
	}

	if len(filesToRewrite) == 0 {
		log.Printf("[compact] %s: no files need rewriting, skipping", cfg.IcebergName)
		return nil, nil
	}

	log.Printf("[compact] %s: rewriting %d files, carrying forward %d files",
		cfg.IcebergName, len(filesToRewrite), len(carriedEntries))

	// Read affected data files, preserving which file each row came from.
	type readResult struct {
		rows   []map[string]any
		seqNum int64
	}
	readResults := make([]readResult, len(filesToRewrite))

	readTasks := make([]utils.Task, len(filesToRewrite))
	for i, df := range filesToRewrite {
		i, df := i, df
		readTasks[i] = utils.Task{
			Name: df.Path,
			Fn: func(ctx context.Context, _ *utils.Progress) error {
				dfKey, err := KeyFromURI(df.Path)
				if err != nil {
					return fmt.Errorf("parse data file URI: %w", err)
				}
				size, err := tw.s3.StatObject(ctx, dfKey)
				if err != nil {
					return fmt.Errorf("stat %s: %w", df.Path, err)
				}
				ra := &S3ReaderAt{Ctx: ctx, S3: tw.s3, Key: dfKey}
				rows, err := ReadParquetRowsFromReaderAt(ra, size, ts)
				if err != nil {
					return fmt.Errorf("read data from %s: %w", df.Path, err)
				}
				readResults[i] = readResult{rows: rows, seqNum: df.SeqNum}
				return nil
			},
		}
	}

	readPool := utils.NewPool(concurrency)
	if _, err := readPool.Run(ctx, readTasks); err != nil {
		return nil, fmt.Errorf("read data files: %w", err)
	}

	// Sequence-aware delete application per Iceberg spec:
	// An equality delete with sequence S applies only to data files with
	// sequence number STRICTLY LESS THAN S.
	// Data files with sequence >= S are NOT affected by that delete.
	//
	// Additionally, deduplicate by PK — keep the row from the highest
	// sequence number (latest write wins).
	type rowWithSeq struct {
		row    map[string]any
		seqNum int64
	}
	dedupRows := make(map[string]rowWithSeq)
	var totalRewriteRows int64
	for _, rr := range readResults {
		for _, row := range rr.rows {
			totalRewriteRows++
			pkKey := BuildPKKey(row, pk)
			if existing, ok := dedupRows[pkKey]; !ok || rr.seqNum > existing.seqNum {
				dedupRows[pkKey] = rowWithSeq{row: row, seqNum: rr.seqNum}
			}
		}
	}

	// Apply deletes: only remove a row if the delete sequence > the row's sequence.
	for pkKey, rws := range dedupRows {
		deleteSeq, hasDelete := deletePKSeq[pkKey]
		if hasDelete && deleteSeq > rws.seqNum {
			delete(dedupRows, pkKey)
		}
	}

	writer := NewRollingDataWriter(ts, targetSize)
	var filteredRows int64
	for _, rws := range dedupRows {
		if err := writer.Add(rws.row); err != nil {
			return nil, fmt.Errorf("add compacted row: %w", err)
		}
		filteredRows++
	}

	chunks, err := writer.FlushAll()
	if err != nil {
		return nil, fmt.Errorf("flush compacted data: %w", err)
	}

	// Build entries using pre-computed URIs.
	var dataUploads []PendingData
	var newDataEntries []ManifestEntry
	for i, chunk := range chunks {
		key := fmt.Sprintf("%s/data/%s-compact-%d.parquet", basePath, uuid.New().String(), i)
		newDataEntries = append(newDataEntries, ManifestEntry{
			Status:     1,
			SnapshotID: snapshotID,
			DataFile: DataFileInfo{
				Path:          tw.s3.URIForKey(key),
				FileSizeBytes: int64(len(chunk.Data)),
				RecordCount:   chunk.RowCount,
				Content:       0,
			},
		})
		dataUploads = append(dataUploads, PendingData{Key: key, Data: chunk.Data})
	}

	allEntries := append(carriedEntries, newDataEntries...)

	// Build + upload everything in one parallel batch.
	partSpec := cfg.PartSpec
	bundle, err := BuildCommit(BuildCommitConfig{
		S3: tw.s3, Schema: ts, PartSpec: partSpec, BasePath: basePath,
		SnapshotID: snapshotID, SeqNum: seqNum, ExistingManifests: nil, // compaction replaces all manifests
	}, []ManifestGroup{{Entries: allEntries, Content: 0}})
	if err != nil {
		return nil, err
	}
	if err := UploadAll(ctx, tw.s3, dataUploads, bundle, concurrency); err != nil {
		return nil, err
	}

	mlURI := bundle.ManifestListURI
	newManifests := bundle.NewManifests

	deletedRows := totalRewriteRows - filteredRows
	beforeFiles := dataFileCount + deleteFileCount
	afterFiles := len(allEntries)
	log.Printf("[compact] %s: %d files -> %d files (%d rewritten, %d carried, %d deletes applied, %d rows removed)",
		cfg.IcebergName, beforeFiles, afterFiles, len(filesToRewrite), len(carriedEntries), len(deleteFiles), deletedRows)

	commit := SnapshotCommit{
		SnapshotID:       snapshotID,
		SequenceNumber:   seqNum,
		TimestampMs:      now.UnixMilli(),
		ManifestListPath: mlURI,
		SchemaID:         cfg.SchemaID,
		Summary: map[string]string{
			"operation": "replace",
		},
	}

	return &PreparedCommit{
		IcebergName:    cfg.IcebergName,
		PrevSnapshotID: currentSnapID,
		Commit:         commit,
		NewManifests:   newManifests,
		DataCount:      len(allEntries),
		DeleteCount:    len(deleteFiles),
		DeleteRowCount: deletedRows,
		BucketCount:    len(chunks),
	}, nil
}
