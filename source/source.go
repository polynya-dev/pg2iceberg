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
)

func (o Op) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
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
	PK        []string
	Timestamp time.Time
}

// Source captures change events from PostgreSQL.
type Source interface {
	// Capture starts emitting change events into the channel.
	// Blocks until ctx is cancelled or a fatal error occurs.
	Capture(ctx context.Context, events chan<- ChangeEvent) error

	// Close performs graceful cleanup (e.g. drop replication slot).
	Close() error
}
