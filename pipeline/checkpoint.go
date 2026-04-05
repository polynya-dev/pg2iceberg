package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint holds replication state that survives restarts.
type Checkpoint struct {
	// Mode records which source mode produced this checkpoint ("query" or "logical").
	Mode string `json:"mode"`

	// Watermark is the last seen watermark value (query mode).
	// Stored as RFC3339 string for timestamp watermarks.
	Watermark string `json:"watermark,omitempty"`

	// LSN is the confirmed flush LSN (logical replication mode).
	LSN uint64 `json:"lsn,omitempty"`

	// SnapshotComplete indicates whether the initial table snapshot has been
	// fully copied into Iceberg. When false and LSN == 0, a fresh snapshot
	// is needed before streaming WAL.
	SnapshotComplete bool `json:"snapshot_complete,omitempty"`

	// SnapshotedTables tracks which tables have completed their initial
	// snapshot. On crash recovery, only tables not in this map are re-snapshotted.
	SnapshotedTables map[string]bool `json:"snapshoted_tables,omitempty"`

	// LastSnapshotID is the Iceberg snapshot ID of the last successful commit.
	LastSnapshotID int64 `json:"last_snapshot_id,omitempty"`

	// LastSequenceNumber is the Iceberg sequence number of the last commit.
	LastSequenceNumber int64 `json:"last_sequence_number,omitempty"`

	// MaterializerSnapshots tracks the last processed events table snapshot
	// per table. Keyed by PG table name (e.g. "public.orders").
	MaterializerSnapshots map[string]int64 `json:"materializer_snapshots,omitempty"`

	// QueryWatermarks stores per-table watermarks for query mode.
	// Keyed by PG table name (e.g. "public.orders"), values are RFC3339Nano timestamps.
	// Replaces the single Watermark field which applied one value to all tables.
	QueryWatermarks map[string]string `json:"query_watermarks,omitempty"`

	// UpdatedAt is when this checkpoint was last saved.
	UpdatedAt time.Time `json:"updated_at"`
}

// CheckpointStore abstracts checkpoint persistence.
type CheckpointStore interface {
	Load(pipelineID string) (*Checkpoint, error)
	Save(pipelineID string, cp *Checkpoint) error
	Close()
}

// FileCheckpointStore persists checkpoints to a local JSON file.
type FileCheckpointStore struct {
	path string
}

func NewFileCheckpointStore(path string) *FileCheckpointStore {
	return &FileCheckpointStore{path: path}
}

// Load reads the checkpoint from disk. Returns a zero Checkpoint if the file doesn't exist.
func (s *FileCheckpointStore) Load(pipelineID string) (*Checkpoint, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return &Checkpoint{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	return &cp, nil
}

// Save writes the checkpoint to disk atomically (write tmp + rename).
func (s *FileCheckpointStore) Save(pipelineID string, cp *Checkpoint) error {
	cp.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, "checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("close temp: %w", err)
	}

	if err := os.Rename(tmp.Name(), s.path); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}

// Close is a no-op for FileCheckpointStore.
func (s *FileCheckpointStore) Close() {}

// MemCheckpointStore is an in-memory CheckpointStore for testing.
type MemCheckpointStore struct {
	checkpoints map[string]*Checkpoint
}

func NewMemCheckpointStore() *MemCheckpointStore {
	return &MemCheckpointStore{checkpoints: make(map[string]*Checkpoint)}
}

func (s *MemCheckpointStore) Load(pipelineID string) (*Checkpoint, error) {
	cp, ok := s.checkpoints[pipelineID]
	if !ok {
		return &Checkpoint{}, nil
	}
	return cp, nil
}

func (s *MemCheckpointStore) Save(pipelineID string, cp *Checkpoint) error {
	cp.UpdatedAt = time.Now()
	s.checkpoints[pipelineID] = cp
	return nil
}

func (s *MemCheckpointStore) Close() {}
