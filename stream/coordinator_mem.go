package stream

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemCoordinator is an in-memory Coordinator for unit tests.
type MemCoordinator struct {
	mu      sync.Mutex
	seqs    map[string]int64      // table -> next_offset
	index   map[string][]LogEntry // table -> entries (sorted by end_offset)
	cursors map[string]int64      // table -> last_offset
	locks   map[string]lockEntry
	workers map[string]time.Time // worker_id -> expires_at
}

type lockEntry struct {
	workerID  string
	expiresAt time.Time
}

func NewMemCoordinator() *MemCoordinator {
	return &MemCoordinator{
		seqs:    make(map[string]int64),
		index:   make(map[string][]LogEntry),
		cursors: make(map[string]int64),
		locks:   make(map[string]lockEntry),
		workers: make(map[string]time.Time),
	}
}

func (c *MemCoordinator) Migrate(ctx context.Context) error { return nil }
func (c *MemCoordinator) Close()                            {}

func (c *MemCoordinator) ClaimOffsets(ctx context.Context, appends []LogAppend) ([]LogEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries := make([]LogEntry, len(appends))
	for i, a := range appends {
		start := c.seqs[a.Table]
		end := start + int64(a.RecordCount)
		c.seqs[a.Table] = end

		e := LogEntry{
			Table:       a.Table,
			StartOffset: start,
			EndOffset:   end,
			S3Path:      a.S3Path,
			RecordCount: a.RecordCount,
			ByteSize:    a.ByteSize,
			CreatedAt:   time.Now(),
		}
		c.index[a.Table] = append(c.index[a.Table], e)
		entries[i] = e
	}
	return entries, nil
}

func (c *MemCoordinator) ReadLog(ctx context.Context, table string, afterOffset int64) ([]LogEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []LogEntry
	for _, e := range c.index[table] {
		if e.EndOffset > afterOffset {
			result = append(result, e)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].EndOffset < result[j].EndOffset
	})
	return result, nil
}

func (c *MemCoordinator) TruncateLog(ctx context.Context, table string, offset int64) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var paths []string
	var remaining []LogEntry
	for _, e := range c.index[table] {
		if e.EndOffset <= offset {
			paths = append(paths, e.S3Path)
		} else {
			remaining = append(remaining, e)
		}
	}
	c.index[table] = remaining
	return paths, nil
}

func (c *MemCoordinator) EnsureCursor(ctx context.Context, group, table string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := group + "/" + table
	if _, ok := c.cursors[key]; !ok {
		c.cursors[key] = -1
	}
	return nil
}

func (c *MemCoordinator) GetCursor(ctx context.Context, group, table string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	offset, ok := c.cursors[group+"/"+table]
	if !ok {
		return -1, nil
	}
	return offset, nil
}

func (c *MemCoordinator) SetCursor(ctx context.Context, group, table string, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursors[group+"/"+table] = offset
	return nil
}

func (c *MemCoordinator) RegisterConsumer(ctx context.Context, group, workerID string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[group+"/"+workerID] = time.Now().Add(ttl)
	return nil
}

func (c *MemCoordinator) UnregisterConsumer(ctx context.Context, group, workerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.workers, group+"/"+workerID)
	return nil
}

func (c *MemCoordinator) ActiveConsumers(ctx context.Context, group string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := group + "/"
	now := time.Now()
	var active []string
	for key, exp := range c.workers {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			if now.Before(exp) {
				active = append(active, key[len(prefix):])
			} else {
				delete(c.workers, key)
			}
		}
	}
	sort.Strings(active)
	return active, nil
}

func (c *MemCoordinator) TryLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.locks[table]; ok {
		if time.Now().Before(existing.expiresAt) {
			return false, nil // held by another worker
		}
		// Expired — reclaim.
	}
	c.locks[table] = lockEntry{workerID: workerID, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (c *MemCoordinator) RenewLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	existing, ok := c.locks[table]
	if !ok || existing.workerID != workerID {
		return false, nil
	}
	c.locks[table] = lockEntry{workerID: workerID, expiresAt: time.Now().Add(ttl)}
	return true, nil
}

func (c *MemCoordinator) ReleaseLock(ctx context.Context, table, workerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.locks[table]; ok && existing.workerID == workerID {
		delete(c.locks, table)
	}
	return nil
}
