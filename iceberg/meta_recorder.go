package iceberg

import (
	"context"
	"log"
	"sync"
	"time"
)

// CommitMode identifies the kind of commit that produced a meta row.
const (
	CommitModeQuery        = "query"
	CommitModeMaterialize  = "materialize"
)

// FlushStats captures the outcome of a single per-table commit for the meta
// `commits` table. A row is emitted per table per commit, uniquely identified
// by (TableName, SnapshotID).
type FlushStats struct {
	Ts             time.Time
	WorkerID       string
	TableName      string // PG-qualified name (e.g. "public.orders")
	Mode           string // CommitModeQuery or CommitModeMaterialize
	SnapshotID     int64  // Iceberg snapshot ID of this commit
	SequenceNumber int64  // Iceberg sequence number; monotonic per table
	LSN            int64
	Rows           int64
	Bytes          int64
	DurationMs     int64
	DataFiles      int
	DeleteFiles    int
}

// CheckpointStats captures a checkpoint save for the meta `checkpoints` table.
type CheckpointStats struct {
	Ts          time.Time
	WorkerID    string
	LSN         int64
	LastFlushAt time.Time
}

// CompactionStats captures one compaction commit for the meta `compactions`
// table. Uniquely identified by (TableName, SnapshotID).
type CompactionStats struct {
	Ts             time.Time
	WorkerID       string
	TableName      string
	SnapshotID     int64
	SequenceNumber int64
	DurationMs     int64
	CompactionMetrics
}

// MetaRecorder coordinates writes to the control-plane meta tables. It buffers
// rows and produces TableCommit values that callers append to their existing
// CommitTransaction invocation, so meta rows land atomically with the commit
// they describe.
//
// Checkpoint rows are buffered across calls — since `store.Save` runs after
// the commit that triggered it, checkpoint rows ride the NEXT commit instead
// of the commit they describe.
//
// A nil MetaRecorder is valid: all methods are no-ops. This lets callers
// opt-out by passing nil.
type MetaRecorder struct {
	workerID string

	mu           sync.Mutex
	commitsW     *MetaWriter
	checkpointsW *MetaWriter
	compactionsW *MetaWriter
}

// NewMetaRecorder constructs a recorder bound to the meta namespace. The
// meta tables must already exist (see EnsureMetaTables). Pass an empty
// workerID if not running in distributed/horizontal mode.
func NewMetaRecorder(ctx context.Context, namespace, workerID string, catalog MetadataCache, s3 ObjectStorage) (*MetaRecorder, error) {
	commitsSchema := MetaCommitsSchema()
	commitsPart, err := MetaPartitionSpec(commitsSchema)
	if err != nil {
		return nil, err
	}
	checkpointsSchema := MetaCheckpointsSchema()
	checkpointsPart, err := MetaPartitionSpec(checkpointsSchema)
	if err != nil {
		return nil, err
	}
	compactionsSchema := MetaCompactionsSchema()
	compactionsPart, err := MetaPartitionSpec(compactionsSchema)
	if err != nil {
		return nil, err
	}
	return &MetaRecorder{
		workerID:     workerID,
		commitsW:     NewMetaWriter(namespace, MetaCommitsTable, commitsSchema, commitsPart, catalog, s3),
		checkpointsW: NewMetaWriter(namespace, MetaCheckpointsTable, checkpointsSchema, checkpointsPart, catalog, s3),
		compactionsW: NewMetaWriter(namespace, MetaCompactionsTable, compactionsSchema, compactionsPart, catalog, s3),
	}, nil
}

// RecordFlush buffers a commits-table row. Safe to call with a nil receiver.
func (r *MetaRecorder) RecordFlush(s FlushStats) {
	if r == nil {
		return
	}
	if s.Ts.IsZero() {
		s.Ts = time.Now()
	}
	if s.WorkerID == "" {
		s.WorkerID = r.workerID
	}
	row := map[string]any{
		"ts":              s.Ts,
		"worker_id":       nullableString(s.WorkerID),
		"table_name":      s.TableName,
		"mode":            s.Mode,
		"snapshot_id":     s.SnapshotID,
		"sequence_number": s.SequenceNumber,
		"lsn":             nullableInt64(s.LSN),
		"rows":            nullableInt64(s.Rows),
		"bytes":           nullableInt64(s.Bytes),
		"duration_ms":     nullableInt64(s.DurationMs),
		"data_files":      nullableInt32(int32(s.DataFiles)),
		"delete_files":    nullableInt32(int32(s.DeleteFiles)),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.commitsW.Append(row); err != nil {
		log.Printf("[meta] append commits row: %v", err)
	}
}

// RecordCheckpoint buffers a checkpoints-table row to be flushed on the next
// BuildCommits call. Safe to call with a nil receiver.
func (r *MetaRecorder) RecordCheckpoint(s CheckpointStats) {
	if r == nil {
		return
	}
	if s.Ts.IsZero() {
		s.Ts = time.Now()
	}
	if s.WorkerID == "" {
		s.WorkerID = r.workerID
	}
	row := map[string]any{
		"ts":        s.Ts,
		"worker_id": nullableString(s.WorkerID),
		"lsn":       nullableInt64(s.LSN),
	}
	if s.LastFlushAt.IsZero() {
		row["last_flush_at"] = nil
	} else {
		row["last_flush_at"] = s.LastFlushAt
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkpointsW.Append(row); err != nil {
		log.Printf("[meta] append checkpoints row: %v", err)
	}
}

// RecordCompaction buffers a compactions-table row. Safe to call with a nil
// receiver.
func (r *MetaRecorder) RecordCompaction(s CompactionStats) {
	if r == nil {
		return
	}
	if s.Ts.IsZero() {
		s.Ts = time.Now()
	}
	if s.WorkerID == "" {
		s.WorkerID = r.workerID
	}
	row := map[string]any{
		"ts":                 s.Ts,
		"worker_id":          nullableString(s.WorkerID),
		"table_name":         s.TableName,
		"snapshot_id":        s.SnapshotID,
		"sequence_number":    s.SequenceNumber,
		"input_data_files":   nullableInt32(int32(s.InputDataFiles)),
		"input_delete_files": nullableInt32(int32(s.InputDeleteFiles)),
		"output_data_files":  nullableInt32(int32(s.OutputDataFiles)),
		"rows_rewritten":     nullableInt64(s.RowsRewritten),
		"rows_removed":       nullableInt64(s.RowsRemoved),
		"bytes_before":       nullableInt64(s.BytesBefore),
		"bytes_after":        nullableInt64(s.BytesAfter),
		"duration_ms":        nullableInt64(s.DurationMs),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.compactionsW.Append(row); err != nil {
		log.Printf("[meta] append compactions row: %v", err)
	}
}

// BuildCommits drains all buffered rows into TableCommit values ready to be
// passed to MetadataCache.CommitTransaction. Returns nil slice if nothing
// is buffered. Safe to call with a nil receiver.
func (r *MetaRecorder) BuildCommits(ctx context.Context) ([]TableCommit, error) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []TableCommit
	if r.commitsW.HasRows() {
		tc, err := r.commitsW.BuildTableCommit(ctx)
		if err != nil {
			return nil, err
		}
		if tc != nil {
			out = append(out, *tc)
		}
	}
	if r.checkpointsW.HasRows() {
		tc, err := r.checkpointsW.BuildTableCommit(ctx)
		if err != nil {
			return nil, err
		}
		if tc != nil {
			out = append(out, *tc)
		}
	}
	if r.compactionsW.HasRows() {
		tc, err := r.compactionsW.BuildTableCommit(ctx)
		if err != nil {
			return nil, err
		}
		if tc != nil {
			out = append(out, *tc)
		}
	}
	return out, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableInt32(v int32) any {
	if v == 0 {
		return nil
	}
	return v
}
