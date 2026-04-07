package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CheckpointVersion is the current schema version.
// Increment when the checkpoint format changes in a backward-incompatible way.
const CheckpointVersion = 1

// CommitSHA is the git commit hash of the pg2iceberg binary.
// Set at startup from the build-time linker flag.
var CommitSHA string

// Checkpoint holds replication state that survives restarts.
type Checkpoint struct {
	// Version is the schema version of this checkpoint.
	Version int `json:"version"`

	// Checksum is a SHA-256 hash of the serialized checkpoint fields (excluding
	// Checksum itself). Used to detect accidental corruption or tampering.
	Checksum string `json:"checksum,omitempty"`

	// WrittenBy is the git commit SHA of the pg2iceberg binary that last wrote
	// this checkpoint. Set automatically by Seal().
	WrittenBy string `json:"written_by,omitempty"`

	// Revision is a monotonic counter incremented on every save. Used for
	// optimistic concurrency control — if two instances race to update the
	// same checkpoint, the loser detects the mismatch and fails.
	Revision int64 `json:"revision"`

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

	// SnapshotChunks tracks the last completed CTID chunk index per table
	// during the initial snapshot. On crash recovery, chunks with index <= this
	// value are skipped. Keyed by PG table name (e.g. "public.orders").
	SnapshotChunks map[string]int `json:"snapshot_chunks,omitempty"`

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

// computeChecksum returns a SHA-256 hex digest over the checkpoint's content
// fields (everything except Checksum itself). The serialization is deterministic:
// fields are marshalled in a fixed order.
func (cp *Checkpoint) computeChecksum() string {
	// Build a deterministic representation of the checkpoint fields.
	// We use a sorted key=value approach to avoid JSON map ordering issues.
	var parts []string
	parts = append(parts, fmt.Sprintf("version=%d", cp.Version))
	parts = append(parts, fmt.Sprintf("written_by=%s", cp.WrittenBy))
	parts = append(parts, fmt.Sprintf("revision=%d", cp.Revision))
	parts = append(parts, fmt.Sprintf("mode=%s", cp.Mode))
	parts = append(parts, fmt.Sprintf("watermark=%s", cp.Watermark))
	parts = append(parts, fmt.Sprintf("lsn=%d", cp.LSN))
	parts = append(parts, fmt.Sprintf("snapshot_complete=%t", cp.SnapshotComplete))
	parts = append(parts, fmt.Sprintf("last_snapshot_id=%d", cp.LastSnapshotID))
	parts = append(parts, fmt.Sprintf("last_sequence_number=%d", cp.LastSequenceNumber))
	parts = append(parts, fmt.Sprintf("updated_at=%s", cp.UpdatedAt.UTC().Format(time.RFC3339Nano)))

	// Deterministic serialization of maps.
	if len(cp.SnapshotedTables) > 0 {
		parts = append(parts, fmt.Sprintf("snapshoted_tables=%s", sortedMapStr(cp.SnapshotedTables)))
	}
	if len(cp.SnapshotChunks) > 0 {
		parts = append(parts, fmt.Sprintf("snapshot_chunks=%s", sortedMapStr(cp.SnapshotChunks)))
	}
	if len(cp.MaterializerSnapshots) > 0 {
		parts = append(parts, fmt.Sprintf("materializer_snapshots=%s", sortedMapStr(cp.MaterializerSnapshots)))
	}
	if len(cp.QueryWatermarks) > 0 {
		parts = append(parts, fmt.Sprintf("query_watermarks=%s", sortedMapStr(cp.QueryWatermarks)))
	}

	h := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(h[:])
}

func sortedMapStr[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var pairs []string
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, m[k]))
	}
	return strings.Join(pairs, ",")
}

// ErrConcurrentUpdate is returned when another instance updated the checkpoint
// between Load and Save, indicating two pg2iceberg instances are running
// against the same pipeline.
var ErrConcurrentUpdate = fmt.Errorf("concurrent checkpoint update detected; " +
	"another pg2iceberg instance may be running with the same pipeline ID")

// Seal increments the revision, sets the version and commit SHA, and computes
// the checksum. Call before saving.
func (cp *Checkpoint) Seal() {
	cp.Revision++
	cp.Version = CheckpointVersion
	cp.WrittenBy = CommitSHA
	cp.Checksum = cp.computeChecksum()
}

// Verify checks the version and checksum. Call after loading.
// Returns nil for zero-value checkpoints (fresh start).
func (cp *Checkpoint) Verify() error {
	// Zero-value checkpoint (no prior state) — nothing to verify.
	if cp.Mode == "" && cp.Version == 0 {
		return nil
	}

	if cp.Version > CheckpointVersion {
		return fmt.Errorf("checkpoint version %d is newer than supported version %d; "+
			"upgrade pg2iceberg or delete the checkpoint to start fresh", cp.Version, CheckpointVersion)
	}

	// Checkpoints from before versioning have no checksum — skip verification.
	if cp.Checksum == "" {
		return nil
	}

	expected := cp.computeChecksum()
	if cp.Checksum != expected {
		return fmt.Errorf("checkpoint checksum mismatch (expected %s, got %s); "+
			"the checkpoint may have been modified externally; "+
			"delete the checkpoint to start fresh", expected, cp.Checksum)
	}

	return nil
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
	log.Printf("[checkpoint] WARNING: using file-based checkpoint store (%s); "+
		"this is not recommended for production. Use state.postgres_url for durable, "+
		"concurrency-safe checkpoint storage.", path)
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
	if err := cp.Verify(); err != nil {
		return nil, err
	}
	return &cp, nil
}

// Save writes the checkpoint to disk atomically (write tmp + rename).
// It checks the on-disk revision to detect concurrent writes.
func (s *FileCheckpointStore) Save(pipelineID string, cp *Checkpoint) error {
	cp.UpdatedAt = time.Now()
	expectedRevision := cp.Revision // before Seal increments
	cp.Seal()

	// Optimistic concurrency check: re-read and verify revision hasn't changed.
	if expectedRevision > 0 {
		current, err := os.ReadFile(s.path)
		if err == nil {
			var diskCP Checkpoint
			if json.Unmarshal(current, &diskCP) == nil && diskCP.Revision != expectedRevision {
				return ErrConcurrentUpdate
			}
		}
	}

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
	if err := cp.Verify(); err != nil {
		return nil, err
	}
	return cp, nil
}

func (s *MemCheckpointStore) Save(pipelineID string, cp *Checkpoint) error {
	cp.UpdatedAt = time.Now()
	cp.Seal()
	s.checkpoints[pipelineID] = cp
	return nil
}

func (s *MemCheckpointStore) Close() {}
