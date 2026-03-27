package source

import (
	"context"
	"time"
)

// Op represents a change operation type.
type Op int

const (
	OpInsert Op = iota
	OpUpdate
	OpDelete
	OpSnapshotTableComplete
	OpSnapshotComplete
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	case OpSnapshotTableComplete:
		return "SNAPSHOT_TABLE_COMPLETE"
	case OpSnapshotComplete:
		return "SNAPSHOT_COMPLETE"
	default:
		return "UNKNOWN"
	}
}

// ChangeEvent is the unified event emitted by all source modes.
type ChangeEvent struct {
	Table     string
	Operation Op
	// Before contains old row values (PK at minimum). Nil for INSERTs.
	Before map[string]any
	// After contains new row values. Nil for DELETEs.
	After map[string]any
	// PK lists primary key column names for this table.
	PK []string
	// SourceTimestamp is when the change was committed in PostgreSQL
	// (equivalent to Debezium's source.ts_ms). For snapshot and query-mode
	// events this equals ProcessingTimestamp.
	SourceTimestamp time.Time
	// ProcessingTimestamp is when pg2iceberg processed the event
	// (equivalent to Debezium's ts_ms). Useful for measuring replication lag.
	ProcessingTimestamp time.Time
	// UnchangedCols lists column names that were sent as 'unchanged' (TOAST).
	// These columns are set to nil in After and need to be fetched from the source.
	UnchangedCols []string
}

// Source captures change events from PostgreSQL.
type Source interface {
	// Capture starts emitting change events into the channel.
	// Blocks until ctx is cancelled or a fatal error occurs.
	Capture(ctx context.Context, events chan<- ChangeEvent) error

	// Close performs graceful cleanup (e.g. drop replication slot).
	Close() error
}
