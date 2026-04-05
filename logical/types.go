package logical

import "github.com/pg2iceberg/pg2iceberg/source"

// Re-export shared types from the source package so that code within the
// logical package can reference them without a package qualifier. This avoids
// modifying every reference in the files moved from package source.
type ChangeEvent = source.ChangeEvent
type SchemaChange = source.SchemaChange
type SchemaColumn = source.SchemaColumn
type TypeChange = source.TypeChange
type Op = source.Op

const (
	OpInsert              = source.OpInsert
	OpUpdate              = source.OpUpdate
	OpDelete              = source.OpDelete
	OpSnapshotTableComplete = source.OpSnapshotTableComplete
	OpSnapshotComplete    = source.OpSnapshotComplete
	OpBegin               = source.OpBegin
	OpCommit              = source.OpCommit
	OpSchemaChange        = source.OpSchemaChange
)
