// Package stream implements the leaderless log protocol for staging WAL change
// events. It replaces the Iceberg events table with S3-staged Parquet files
// coordinated through a linearizable store (PostgreSQL).
//
// The package provides two core abstractions:
//   - Coordinator: interface for distributed coordination primitives (Layer 0+1+2)
//   - Stream: append-only log composing Coordinator + ObjectStorage (Layer 1)
package stream

import (
	"context"
	"time"
)

// Coordinator provides distributed coordination backed by a linearizable store.
// The implementation manages log indexing, materializer cursors, and heartbeat
// locks. For pg2iceberg, the backing store is the source PostgreSQL database
// using the _pg2iceberg schema.
type Coordinator interface {
	// Migrate ensures all coordination tables exist. Idempotent.
	Migrate(ctx context.Context) error

	// Close releases the underlying connection pool.
	Close()

	// --- Log Index (Layer 1: Leaderless Log) ---

	// ClaimOffsets atomically increments offset counters for each table and
	// inserts log index entries. The entire operation runs in a single
	// transaction — either all entries are registered or none.
	// Returns the created LogEntry for each input (with assigned offsets).
	ClaimOffsets(ctx context.Context, appends []LogAppend) ([]LogEntry, error)

	// ReadLog returns entries with end_offset > afterOffset, ordered by
	// end_offset ascending.
	ReadLog(ctx context.Context, table string, afterOffset int64) ([]LogEntry, error)

	// TruncateLog deletes entries with end_offset <= offset.
	// Returns s3_path values of deleted entries for S3 cleanup.
	TruncateLog(ctx context.Context, table string, offset int64) ([]string, error)

	// --- Cursors (scoped by consumer group) ---

	// EnsureCursor creates a cursor row for a group+table if it doesn't exist.
	EnsureCursor(ctx context.Context, group, table string) error

	// GetCursor returns the last committed offset for a group+table.
	// Returns -1 if no cursor exists.
	GetCursor(ctx context.Context, group, table string) (int64, error)

	// SetCursor updates the cursor for a group+table.
	SetCursor(ctx context.Context, group, table string, offset int64) error

	// --- Consumer Registry + Table Assignment (Layer 2) ---

	// RegisterConsumer upserts a consumer's heartbeat within a consumer group.
	// The group scopes consumers so multiple pg2iceberg deployments sharing
	// the same coordinator schema don't interfere with each other.
	RegisterConsumer(ctx context.Context, group, consumerID string, ttl time.Duration) error

	// UnregisterConsumer removes a consumer from its group.
	UnregisterConsumer(ctx context.Context, group, consumerID string) error

	// ActiveConsumers returns all consumers in a group with valid heartbeats, sorted by ID.
	ActiveConsumers(ctx context.Context, group string) ([]string, error)

	// --- Heartbeat Locks (Layer 2: Task Claiming) ---

	// TryLock attempts to acquire a heartbeat lock for a table. Expired locks
	// are reclaimed automatically. Returns true if the lock was acquired.
	TryLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error)

	// RenewLock extends the lock TTL. Returns false if the lock was lost
	// (expired and reclaimed by another worker). The caller MUST abort any
	// in-progress work when RenewLock returns false.
	RenewLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error)

	// ReleaseLock releases a held lock. No-op if not held by this worker.
	ReleaseLock(ctx context.Context, table, workerID string) error
}

// LogAppend is the input to ClaimOffsets — a staged file to register in the log.
type LogAppend struct {
	Table       string // PG table name (e.g. "public.orders")
	S3Path      string // full S3 key or URI of the staged Parquet file
	RecordCount int    // number of change events in the file
	ByteSize    int64  // file size in bytes
}

// LogEntry is a registered entry in the log index, representing a staged
// Parquet file covering the offset range [StartOffset, EndOffset).
type LogEntry struct {
	Table       string
	StartOffset int64
	EndOffset   int64
	S3Path      string
	RecordCount int
	ByteSize    int64
	CreatedAt   time.Time
}
