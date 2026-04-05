// Package source defines the shared types for change data capture.
// These types are used by both the logical and query pipelines,
// as well as the sink and pipeline packages.
package source

import (
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
	OpBegin
	OpCommit
	OpSchemaChange
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
	case OpBegin:
		return "BEGIN"
	case OpCommit:
		return "COMMIT"
	case OpSchemaChange:
		return "SCHEMA_CHANGE"
	default:
		return "UNKNOWN"
	}
}

// SchemaChange describes a DDL-driven schema change detected from a
// RelationMessage diff in the WAL stream.
type SchemaChange struct {
	Table          string
	AddedColumns   []SchemaColumn
	DroppedColumns []string     // column names removed
	TypeChanges    []TypeChange // columns whose PG type changed
}

// SchemaColumn describes a column discovered from a RelationMessage.
type SchemaColumn struct {
	Name   string
	PGType string // resolved from OID, e.g. "int4", "text"
}

// TypeChange records an OID-level type change for a single column.
type TypeChange struct {
	Name    string
	OldType string
	NewType string
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
	// LSN is the WAL position of this event (logical replication mode only).
	// Zero for snapshot and query-mode events.
	LSN uint64
	// TransactionID is the PostgreSQL transaction ID (XID) from BeginMessage.
	// Set on OpBegin, OpCommit, and all DML events within the transaction.
	// Zero for snapshot and query-mode events.
	TransactionID uint32
	// SchemaChange is populated only for OpSchemaChange events.
	SchemaChange *SchemaChange
}

