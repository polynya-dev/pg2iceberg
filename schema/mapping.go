package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Column represents a table column with type information.
type Column struct {
	Name       string
	PGType     string // e.g. "integer", "text", "timestamp with time zone"
	IsNullable bool
	FieldID    int // Iceberg field ID (1-based, assigned in order)
}

// TableSchema holds discovered schema for a single table.
type TableSchema struct {
	Table   string   // fully qualified: schema.table
	Columns []Column
	PK      []string // primary key column names
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
		SELECT column_name, udt_name, is_nullable
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
		if err := rows.Scan(&name, &udtName, &nullable); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		ts.Columns = append(ts.Columns, Column{
			Name:       name,
			PGType:     udtName,
			IsNullable: nullable == "YES",
			FieldID:    fieldID,
		})
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

	return ts, nil
}

func splitTableName(table string) (string, string) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", parts[0]
}

// IcebergType maps a PostgreSQL type to an Iceberg type string.
func IcebergType(pgType string) string {
	switch strings.ToLower(pgType) {
	case "int2", "smallint":
		return "int"
	case "int4", "integer", "serial":
		return "int"
	case "int8", "bigint", "bigserial":
		return "long"
	case "float4", "real":
		return "float"
	case "float8", "double precision":
		return "double"
	case "numeric", "decimal":
		return "decimal(38,18)"
	case "bool", "boolean":
		return "boolean"
	case "text", "varchar", "character varying", "bpchar", "char", "name":
		return "string"
	case "bytea":
		return "binary"
	case "date":
		return "date"
	case "timestamp", "timestamp without time zone":
		return "timestamp"
	case "timestamptz", "timestamp with time zone":
		return "timestamptz"
	case "uuid":
		return "uuid"
	case "jsonb", "json":
		return "string"
	default:
		return "string"
	}
}

// IcebergSchemaJSON builds the Iceberg schema as a map for the REST catalog API.
func IcebergSchemaJSON(ts *TableSchema) map[string]any {
	return IcebergSchemaJSONWithID(ts, 0)
}

// IcebergSchemaJSONWithID builds the Iceberg schema with a specific schema-id.
func IcebergSchemaJSONWithID(ts *TableSchema, schemaID int) map[string]any {
	fields := make([]map[string]any, len(ts.Columns))
	for i, col := range ts.Columns {
		fields[i] = map[string]any{
			"id":       col.FieldID,
			"name":     col.Name,
			"required": !col.IsNullable,
			"type":     icebergTypeJSON(col.PGType),
		}
	}
	return map[string]any{
		"type":      "struct",
		"schema-id": schemaID,
		"fields":    fields,
	}
}

func icebergTypeJSON(pgType string) any {
	switch strings.ToLower(pgType) {
	case "int2", "smallint", "int4", "integer", "serial":
		return "int"
	case "int8", "bigint", "bigserial":
		return "long"
	case "float4", "real":
		return "float"
	case "float8", "double precision":
		return "double"
	case "numeric", "decimal":
		// For v1, store numeric as string to avoid precision issues
		return "string"
	case "bool", "boolean":
		return "boolean"
	case "text", "varchar", "character varying", "bpchar", "char", "name":
		return "string"
	case "bytea":
		return "binary"
	case "date":
		return "date"
	case "timestamp", "timestamp without time zone":
		return "timestamp"
	case "timestamptz", "timestamp with time zone":
		return "timestamptz"
	case "uuid":
		return "string" // store UUID as string for v1
	case "jsonb", "json":
		return "string"
	default:
		return "string"
	}
}
