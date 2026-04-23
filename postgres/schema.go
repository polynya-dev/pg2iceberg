package postgres

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Type represents a canonical PostgreSQL column type.
type Type string

const (
	Int2        Type = "int2"
	Int4        Type = "int4"
	Int8        Type = "int8"
	Float4      Type = "float4"
	Float8      Type = "float8"
	Numeric     Type = "numeric"
	Bool        Type = "bool"
	Text        Type = "text"
	Varchar     Type = "varchar"
	Bpchar      Type = "bpchar"
	Name        Type = "name"
	Bytea       Type = "bytea"
	Date        Type = "date"
	Time        Type = "time"
	TimeTZ      Type = "timetz"
	Timestamp   Type = "timestamp"
	TimestampTZ Type = "timestamptz"
	UUID        Type = "uuid"
	JSON        Type = "json"
	JSONB       Type = "jsonb"
	OID         Type = "oid"
)

// ParseType normalizes a PostgreSQL type name (as returned by udt_name or
// format_type) into its canonical Type constant.
func ParseType(name string) Type {
	switch strings.ToLower(name) {
	case "int2", "smallint":
		return Int2
	case "int4", "integer", "serial":
		return Int4
	case "int8", "bigint", "bigserial":
		return Int8
	case "float4", "real":
		return Float4
	case "float8", "double precision":
		return Float8
	case "numeric", "decimal":
		return Numeric
	case "bool", "boolean":
		return Bool
	case "text":
		return Text
	case "varchar", "character varying":
		return Varchar
	case "bpchar", "char", "character":
		return Bpchar
	case "name":
		return Name
	case "bytea":
		return Bytea
	case "date":
		return Date
	case "time", "time without time zone":
		return Time
	case "timetz", "time with time zone":
		return TimeTZ
	case "timestamp", "timestamp without time zone":
		return Timestamp
	case "timestamptz", "timestamp with time zone":
		return TimestampTZ
	case "uuid":
		return UUID
	case "json":
		return JSON
	case "jsonb":
		return JSONB
	case "oid":
		return OID
	default:
		return Text
	}
}

// Column represents a table column with type information.
type Column struct {
	Name       string
	PGType     Type
	IsNullable bool
	FieldID    int // Iceberg field ID (1-based, assigned in order)
	Precision  int // numeric precision; 0 = unspecified
	Scale      int // numeric scale; 0 = unspecified
}

// IcebergType maps the column's PostgreSQL type to an Iceberg type string.
// The returned bool is true when the mapping required truncation (e.g. decimal
// precision clamped to 38), so callers can log a warning.
// Reference: https://iceberg.apache.org/spec/#primitive-types
func (c Column) IcebergType() (string, bool) {
	switch c.PGType {
	case Int2:
		return "int", false
	case Int4, OID:
		return "int", false
	case Int8:
		return "long", false
	case Float4:
		return "float", false
	case Float8:
		return "double", false
	case Numeric:
		return icebergDecimal(c.Precision, c.Scale)
	case Bool:
		return "boolean", false
	case Text, Varchar, Bpchar, Name:
		return "string", false
	case Bytea:
		return "binary", false
	case Date:
		return "date", false
	case Time, TimeTZ:
		return "time", false
	case Timestamp:
		return "timestamp", false
	case TimestampTZ:
		return "timestamptz", false
	case UUID:
		return "uuid", false
	case JSON, JSONB:
		return "string", false
	default:
		return "string", false
	}
}

const MaxDecimalPrecision = 38

// icebergDecimal returns the Iceberg decimal type string for the given
// precision and scale.
func icebergDecimal(precision, scale int) (string, bool) {
	if precision <= 0 {
		// Unconstrained PG numeric — use practical default.
		return fmt.Sprintf("decimal(%d,%d)", MaxDecimalPrecision, 18), true
	}
	truncated := false
	if precision > MaxDecimalPrecision {
		precision = MaxDecimalPrecision
		truncated = true
	}
	if scale > precision {
		scale = precision
		truncated = true
	}
	return fmt.Sprintf("decimal(%d,%d)", precision, scale), truncated
}

// Validate checks the schema for columns that cannot be safely represented
// in Iceberg, and that the table has a primary key. It should be called at
// pipeline startup to fail fast.
func (ts *TableSchema) Validate() error {
	if len(ts.PK) == 0 {
		return fmt.Errorf(
			"table %s has no primary key; pg2iceberg requires a primary key on every "+
				"replicated table so downstream tooling (CDC, compaction, diffing) can "+
				"uniquely identify rows — add a PRIMARY KEY in PostgreSQL or exclude this table",
			ts.Table)
	}
	for _, col := range ts.Columns {
		if col.PGType == Numeric && col.Precision > MaxDecimalPrecision {
			return fmt.Errorf(
				"column %s.%s has type numeric(%d,%d) which exceeds Iceberg's max decimal precision of %d; "+
					"reduce the column precision in PostgreSQL or exclude this table",
				ts.Table, col.Name, col.Precision, col.Scale, MaxDecimalPrecision)
		}
	}
	return nil
}

// TableSchema holds discovered schema for a single table.
type TableSchema struct {
	Table               string   // fully qualified: schema.table
	Columns             []Column
	PK                  []string // primary key column names
	ReplicaIdentityFull bool     // true if table uses REPLICA IDENTITY FULL
	Partitioned         bool     // true if table uses declarative partitioning (relkind = 'p')
}

// PKFieldIDs returns the Iceberg field IDs corresponding to primary key columns.
func (ts *TableSchema) PKFieldIDs() []int {
	ids := make([]int, 0, len(ts.PK))
	for _, pk := range ts.PK {
		for _, col := range ts.Columns {
			if col.Name == pk {
				ids = append(ids, col.FieldID)
				break
			}
		}
	}
	return ids
}

// MaxFieldID returns the highest Iceberg field ID assigned in this schema.
func (ts *TableSchema) MaxFieldID() int {
	max := 0
	for _, col := range ts.Columns {
		if col.FieldID > max {
			max = col.FieldID
		}
	}
	return max
}

// DiscoverSchema queries PostgreSQL to build the table schema.
func DiscoverSchema(ctx context.Context, conn *pgx.Conn, table string) (*TableSchema, error) {
	schema, tableName := splitTableName(table)

	// Discover columns
	rows, err := conn.Query(ctx, `
		SELECT column_name, udt_name, is_nullable,
		       COALESCE(numeric_precision, 0), COALESCE(numeric_scale, 0)
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	ts := &TableSchema{Table: table}
	fieldID := 1
	for rows.Next() {
		var name, udtName, nullable string
		var precision, scale int
		if err := rows.Scan(&name, &udtName, &nullable, &precision, &scale); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		col := Column{
			Name:       name,
			PGType:     ParseType(udtName),
			IsNullable: nullable == "YES",
			FieldID:    fieldID,
			Precision:  precision,
			Scale:      scale,
		}
		if iceType, truncated := col.IcebergType(); truncated {
			log.Printf("WARN: column %s.%s type %s(%d,%d) truncated to Iceberg %s", table, name, udtName, precision, scale, iceType)
		}
		ts.Columns = append(ts.Columns, col)
		fieldID++
	}

	if len(ts.Columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", table)
	}

	// Discover primary key
	pkRows, err := conn.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)
	`, table)
	if err != nil {
		return nil, fmt.Errorf("query primary key: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var pkCol string
		if err := pkRows.Scan(&pkCol); err != nil {
			return nil, fmt.Errorf("scan pk: %w", err)
		}
		ts.PK = append(ts.PK, pkCol)
	}

	// Discover replica identity and partitioning status.
	var relReplIdent, relKind byte
	err = conn.QueryRow(ctx,
		`SELECT relreplident, relkind FROM pg_class WHERE oid = $1::regclass`, table,
	).Scan(&relReplIdent, &relKind)
	if err == nil {
		ts.ReplicaIdentityFull = relReplIdent == 'f'
		ts.Partitioned = relKind == 'p'
	}

	return ts, nil
}

func splitTableName(table string) (string, string) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

// IcebergSchemaJSON builds the Iceberg schema as a map for the REST catalog API.
func IcebergSchemaJSON(ts *TableSchema) map[string]any {
	return IcebergSchemaJSONWithID(ts, 0)
}

// IcebergSchemaJSONWithID builds the Iceberg schema with a specific schema-id.
func IcebergSchemaJSONWithID(ts *TableSchema, schemaID int) map[string]any {
	fields := make([]map[string]any, len(ts.Columns))
	for i, col := range ts.Columns {
		iceType, _ := col.IcebergType()
		fields[i] = map[string]any{
			"id":       col.FieldID,
			"name":     col.Name,
			"required": !col.IsNullable,
			"type":     iceType,
		}
	}
	schema := map[string]any{
		"type":      "struct",
		"schema-id": schemaID,
		"fields":    fields,
	}
	// Iceberg requires identifier fields to be `required`. PG primary-key
	// columns are always NOT NULL, so this holds implicitly; we emit the ids
	// only when a PK was discovered.
	if pkIDs := ts.PKFieldIDs(); len(pkIDs) > 0 {
		schema["identifier-field-ids"] = pkIDs
	}
	return schema
}

// TableToIceberg converts a fully qualified PG table name like "public.orders"
// to the short Iceberg table name "orders".
func TableToIceberg(pgTable string) string {
	if idx := strings.LastIndex(pgTable, "."); idx >= 0 {
		return pgTable[idx+1:]
	}
	return pgTable
}
