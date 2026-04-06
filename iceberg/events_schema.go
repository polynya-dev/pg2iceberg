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

// EventsTableSchema builds an events table schema from a source TableSchema.
// The events table prepends metadata columns (_op, _lsn, _ts, _seq, _unchanged_cols)
// and forces all user columns to be nullable (since DELETEs only carry PK values,
// and TOAST updates have null for unchanged columns).
func EventsTableSchema(src *postgres.TableSchema) *postgres.TableSchema {
	// Metadata columns use field IDs starting at 1.
	metaCols := []postgres.Column{
		{Name: "_op", PGType: postgres.Text, IsNullable: false, FieldID: 1},
		{Name: "_lsn", PGType: postgres.Int8, IsNullable: false, FieldID: 2},
		{Name: "_ts", PGType: postgres.TimestampTZ, IsNullable: false, FieldID: 3},
		{Name: "_seq", PGType: postgres.Int8, IsNullable: false, FieldID: 4},
		{Name: "_unchanged_cols", PGType: postgres.Text, IsNullable: true, FieldID: 5},
	}

	// User columns are offset by 5 (the number of metadata columns) from the
	// source schema's field IDs. This preserves monotonicity when columns are
	// dropped: the remaining columns keep their original field IDs, so the max
	// field ID never decreases.
	cols := make([]postgres.Column, 0, len(metaCols)+len(src.Columns))
	cols = append(cols, metaCols...)
	for _, col := range src.Columns {
		cols = append(cols, postgres.Column{
			Name:       col.Name,
			PGType:     col.PGType,
			IsNullable: true, // always nullable in events table
			FieldID:    5 + col.FieldID,
			Precision:  col.Precision,
			Scale:      col.Scale,
		})
	}

	return &postgres.TableSchema{
		Table:   src.Table,
		Columns: cols,
		PK:      nil, // events table has no PK — it's append-only
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
