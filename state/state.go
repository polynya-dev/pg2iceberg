package state

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

	// UpdatedAt is when this checkpoint was last saved.
	UpdatedAt time.Time `json:"updated_at"`
}

// CheckpointStore abstracts checkpoint persistence.
type CheckpointStore interface {
	Load(pipelineID string) (*Checkpoint, error)
	Save(pipelineID string, cp *Checkpoint) error
	Close()
}

// FileStore persists checkpoints to a local JSON file.
type FileStore struct {
	path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Load reads the checkpoint from disk. Returns a zero Checkpoint if the file doesn't exist.
func (s *FileStore) Load(pipelineID string) (*Checkpoint, error) {
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
func (s *FileStore) Save(pipelineID string, cp *Checkpoint) error {
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

// Close is a no-op for FileStore.
func (s *FileStore) Close() {}
