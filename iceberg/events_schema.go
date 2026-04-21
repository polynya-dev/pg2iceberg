package iceberg

import (
	"strings"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

// EventsTableName returns the Iceberg table name for the events table.
// e.g., "orders" → "orders_events"
func EventsTableName(icebergTable string) string {
	return icebergTable + "_events"
}

// StagedEventSchema returns the Parquet schema for staged WAL files.
// User columns are JSON-encoded into the _data column.
//
// Evolution rule: columns are append-only and nullable so the materializer
// can read files written by older versions (parquet returns null for
// absent columns).
//
// Columns:
//
//	_op              TEXT        "I", "U", "D"
//	_lsn             INT8        WAL position
//	_ts              TIMESTAMPTZ PG commit time
//	_unchanged_cols  TEXT        comma-separated (nullable)
//	_data            TEXT        JSON-encoded user columns
//	_xid             INT8        PG transaction ID (nullable; null in older files)
func StagedEventSchema() *postgres.TableSchema {
	return &postgres.TableSchema{
		Table: "__staged_events__",
		Columns: []postgres.Column{
			{Name: "_op", PGType: postgres.Text, IsNullable: false, FieldID: 1},
			{Name: "_lsn", PGType: postgres.Int8, IsNullable: false, FieldID: 2},
			{Name: "_ts", PGType: postgres.TimestampTZ, IsNullable: false, FieldID: 3},
			{Name: "_unchanged_cols", PGType: postgres.Text, IsNullable: true, FieldID: 4},
			{Name: "_data", PGType: postgres.Text, IsNullable: false, FieldID: 5},
			{Name: "_xid", PGType: postgres.Int8, IsNullable: true, FieldID: 6},
		},
	}
}

// OpString returns the single-character operation code for an event.
func OpString(op string) string {
	switch op {
	case "INSERT":
		return "I"
	case "UPDATE":
		return "U"
	case "DELETE":
		return "D"
	default:
		return op
	}
}

// UnchangedColsString joins unchanged column names into a comma-separated string.
// Returns "" if there are no unchanged columns.
func UnchangedColsString(cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	return strings.Join(cols, ",")
}
