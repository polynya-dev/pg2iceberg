package iceberg

import "github.com/pg2iceberg/pg2iceberg/postgres"

// Control-plane metadata table names.
const (
	MetaCommitsTable     = "commits"
	MetaCheckpointsTable = "checkpoints"
	MetaCompactionsTable = "compactions"
)

// MetaCommitsSchema returns the schema for the control-plane `commits` table.
// One row per table per commit — uniquely identified by (table_name, snapshot_id).
//
// Schema evolution rules (enforced by EnsureMetaTables):
//   - FieldIDs are immutable. Existing columns must keep their FieldIDs.
//   - New columns are appended at the end with the next sequential FieldID.
//   - All new columns must be nullable (Iceberg rejects required-column adds).
//   - Columns are never removed or renamed; that would require a rewrite.
func MetaCommitsSchema() *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: MetaCommitsTable,
		Columns: []postgres.Column{
			{Name: "ts", PGType: postgres.TimestampTZ, IsNullable: false, FieldID: 1},
			{Name: "worker_id", PGType: postgres.Text, IsNullable: true, FieldID: 2},
			{Name: "table_name", PGType: postgres.Text, IsNullable: false, FieldID: 3},
			{Name: "mode", PGType: postgres.Text, IsNullable: false, FieldID: 4},
			{Name: "snapshot_id", PGType: postgres.Int8, IsNullable: false, FieldID: 5},
			{Name: "sequence_number", PGType: postgres.Int8, IsNullable: false, FieldID: 6},
			{Name: "lsn", PGType: postgres.Int8, IsNullable: true, FieldID: 7},
			{Name: "rows", PGType: postgres.Int8, IsNullable: true, FieldID: 8},
			{Name: "bytes", PGType: postgres.Int8, IsNullable: true, FieldID: 9},
			{Name: "duration_ms", PGType: postgres.Int8, IsNullable: true, FieldID: 10},
			{Name: "data_files", PGType: postgres.Int4, IsNullable: true, FieldID: 11},
			{Name: "delete_files", PGType: postgres.Int4, IsNullable: true, FieldID: 12},
		},
	}
}

// MetaCompactionsSchema returns the schema for the control-plane `compactions`
// table. One row per compaction commit, identified by (table_name, snapshot_id).
// Follows the same additive evolution rules as MetaCommitsSchema.
//
// The `partition` column is null today. Once compaction is made partition-aware
// it will hold the partition path (e.g. "day=2026-04-21") and grain shifts to
// one row per (table, partition, snapshot_id).
func MetaCompactionsSchema() *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: MetaCompactionsTable,
		Columns: []postgres.Column{
			{Name: "ts", PGType: postgres.TimestampTZ, IsNullable: false, FieldID: 1},
			{Name: "worker_id", PGType: postgres.Text, IsNullable: true, FieldID: 2},
			{Name: "table_name", PGType: postgres.Text, IsNullable: false, FieldID: 3},
			{Name: "partition", PGType: postgres.Text, IsNullable: true, FieldID: 4},
			{Name: "snapshot_id", PGType: postgres.Int8, IsNullable: false, FieldID: 5},
			{Name: "sequence_number", PGType: postgres.Int8, IsNullable: false, FieldID: 6},
			{Name: "input_data_files", PGType: postgres.Int4, IsNullable: true, FieldID: 7},
			{Name: "input_delete_files", PGType: postgres.Int4, IsNullable: true, FieldID: 8},
			{Name: "output_data_files", PGType: postgres.Int4, IsNullable: true, FieldID: 9},
			{Name: "rows_rewritten", PGType: postgres.Int8, IsNullable: true, FieldID: 10},
			{Name: "rows_removed", PGType: postgres.Int8, IsNullable: true, FieldID: 11},
			{Name: "bytes_before", PGType: postgres.Int8, IsNullable: true, FieldID: 12},
			{Name: "bytes_after", PGType: postgres.Int8, IsNullable: true, FieldID: 13},
			{Name: "duration_ms", PGType: postgres.Int8, IsNullable: true, FieldID: 14},
		},
	}
}

// MetaCheckpointsSchema returns the schema for the control-plane `checkpoints` table.
// One row per checkpoint save.
func MetaCheckpointsSchema() *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: MetaCheckpointsTable,
		Columns: []postgres.Column{
			{Name: "ts", PGType: postgres.TimestampTZ, IsNullable: false, FieldID: 1},
			{Name: "worker_id", PGType: postgres.Text, IsNullable: true, FieldID: 2},
			{Name: "lsn", PGType: postgres.Int8, IsNullable: true, FieldID: 3},
			{Name: "last_flush_at", PGType: postgres.TimestampTZ, IsNullable: true, FieldID: 4},
		},
	}
}

// MetaPartitionSpec returns the day-partition spec on `ts` for meta tables.
func MetaPartitionSpec(ts *postgres.TableSchema) (*PartitionSpec, error) {
	return BuildPartitionSpec([]string{"day(ts)"}, ts)
}
