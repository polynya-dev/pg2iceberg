package iceberg

import "github.com/pg2iceberg/pg2iceberg/postgres"

// Control-plane metadata table names.
const (
	MetaCommitsTable     = "commits"
	MetaCheckpointsTable = "checkpoints"
	MetaCompactionsTable = "compactions"
	MetaMaintenanceTable = "maintenance"
)

// Maintenance operation kinds.
const (
	MaintenanceOpExpireSnapshots = "expire_snapshots"
	MaintenanceOpCleanOrphans    = "clean_orphans"
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
			// Latest source-side timestamp of any event in this commit. In
			// logical mode: max PG commit timestamp across bundled events.
			// In query mode: the watermark value at flush time. Used to
			// compute data freshness (now - max_source_ts) independently of
			// pipeline health (ts - max_source_ts).
			{Name: "max_source_ts", PGType: postgres.TimestampTZ, IsNullable: true, FieldID: 13},
			// Iceberg table schema ID at commit time. Lets dashboards correlate
			// throughput/latency changes with schema evolutions.
			{Name: "schema_id", PGType: postgres.Int4, IsNullable: true, FieldID: 14},
			// Logical mode: distinct PG transaction IDs coalesced into this
			// commit. Null in query mode and for snapshot-time rows.
			{Name: "tx_count", PGType: postgres.Int4, IsNullable: true, FieldID: 15},
			// Git commit SHA of the pg2iceberg binary that wrote this row.
			// Set from the LDFLAGS -X pipeline.CommitSHA build flag.
			{Name: "pg2iceberg_commit_sha", PGType: postgres.Text, IsNullable: true, FieldID: 16},
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
			{Name: "pg2iceberg_commit_sha", PGType: postgres.Text, IsNullable: true, FieldID: 15},
		},
	}
}

// MetaMaintenanceSchema returns the schema for the control-plane `maintenance`
// table. One row per maintenance operation per table (snapshot expiry and
// orphan cleanup are separate rows). Captures what snapshot summaries can't:
// files deleted from S3 and snapshots dropped from catalog history.
func MetaMaintenanceSchema() *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: MetaMaintenanceTable,
		Columns: []postgres.Column{
			{Name: "ts", PGType: postgres.TimestampTZ, IsNullable: false, FieldID: 1},
			{Name: "worker_id", PGType: postgres.Text, IsNullable: true, FieldID: 2},
			{Name: "table_name", PGType: postgres.Text, IsNullable: false, FieldID: 3},
			{Name: "operation", PGType: postgres.Text, IsNullable: false, FieldID: 4},
			{Name: "items_affected", PGType: postgres.Int4, IsNullable: true, FieldID: 5},
			{Name: "bytes_freed", PGType: postgres.Int8, IsNullable: true, FieldID: 6},
			{Name: "duration_ms", PGType: postgres.Int8, IsNullable: true, FieldID: 7},
			{Name: "pg2iceberg_commit_sha", PGType: postgres.Text, IsNullable: true, FieldID: 8},
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
			{Name: "pg2iceberg_commit_sha", PGType: postgres.Text, IsNullable: true, FieldID: 5},
		},
	}
}

// MetaPartitionSpec returns the day-partition spec on `ts` for meta tables.
func MetaPartitionSpec(ts *postgres.TableSchema) (*PartitionSpec, error) {
	return BuildPartitionSpec([]string{"day(ts)"}, ts)
}
