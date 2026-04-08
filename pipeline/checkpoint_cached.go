package pipeline

import (
	"context"
	"sync"
)

// CachedCheckpointStore wraps a CheckpointStore with an in-memory cache.
// Load returns the cached checkpoint after the first read. Save writes
// through to the underlying store and updates the cache on success.
// On ErrConcurrentUpdate (another instance took over), the cache is
// invalidated so the next Load re-reads from the store.
type CachedCheckpointStore struct {
	inner CheckpointStore
	mu    sync.Mutex
	cache map[string]*Checkpoint // pipelineID → cached checkpoint
}

// NewCachedCheckpointStore wraps a store with an in-memory cache.
func NewCachedCheckpointStore(inner CheckpointStore) *CachedCheckpointStore {
	return &CachedCheckpointStore{
		inner: inner,
		cache: make(map[string]*Checkpoint),
	}
}

func (s *CachedCheckpointStore) Load(ctx context.Context, pipelineID string) (*Checkpoint, error) {
	s.mu.Lock()
	if cp, ok := s.cache[pipelineID]; ok {
		clone := cloneCheckpoint(cp)
		s.mu.Unlock()
		return clone, nil
	}
	s.mu.Unlock()

	// Cache miss — cold start.
	cp, err := s.inner.Load(ctx, pipelineID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.cache[pipelineID] = cloneCheckpoint(cp)
	s.mu.Unlock()
	return cp, nil
}

func (s *CachedCheckpointStore) Save(ctx context.Context, pipelineID string, cp *Checkpoint) error {
	err := s.inner.Save(ctx, pipelineID, cp)
	if err == ErrConcurrentUpdate {
		// Another instance won — invalidate so next Load re-reads.
		s.mu.Lock()
		delete(s.cache, pipelineID)
		s.mu.Unlock()
		return err
	}
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.cache[pipelineID] = cloneCheckpoint(cp)
	s.mu.Unlock()
	return nil
}

func (s *CachedCheckpointStore) Close() {
	s.inner.Close()
}

// cloneCheckpoint returns a deep copy of a Checkpoint, including all map fields.
func cloneCheckpoint(cp *Checkpoint) *Checkpoint {
	clone := *cp
	if cp.SnapshotedTables != nil {
		clone.SnapshotedTables = make(map[string]bool, len(cp.SnapshotedTables))
		for k, v := range cp.SnapshotedTables {
			clone.SnapshotedTables[k] = v
		}
	}
	if cp.SnapshotChunks != nil {
		clone.SnapshotChunks = make(map[string]int, len(cp.SnapshotChunks))
		for k, v := range cp.SnapshotChunks {
			clone.SnapshotChunks[k] = v
		}
	}
	if cp.MaterializerSnapshots != nil {
		clone.MaterializerSnapshots = make(map[string]int64, len(cp.MaterializerSnapshots))
		for k, v := range cp.MaterializerSnapshots {
			clone.MaterializerSnapshots[k] = v
		}
	}
	if cp.QueryWatermarks != nil {
		clone.QueryWatermarks = make(map[string]string, len(cp.QueryWatermarks))
		for k, v := range cp.QueryWatermarks {
			clone.QueryWatermarks[k] = v
		}
	}
	return &clone
}
