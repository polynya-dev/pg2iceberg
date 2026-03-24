package sink

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/hasyimibhar/pg2iceberg/schema"
	"github.com/parquet-go/parquet-go"
)

// ParquetWriter accumulates rows and flushes them as a Parquet file.
type ParquetWriter struct {
	tableSchema *schema.TableSchema
	pqSchema    *parquet.Schema
	columns     []schema.Column // columns to write (subset for delete files)
	// colOrder maps column name to its index in the parquet schema's leaf columns.
	colOrder map[string]int
	rows     []map[string]any
}

// NewDataWriter creates a writer for data files (all columns).
func NewDataWriter(ts *schema.TableSchema) *ParquetWriter {
	pqSchema := buildParquetSchema("data", ts.Columns)
	return &ParquetWriter{
		tableSchema: ts,
		pqSchema:    pqSchema,
		columns:     ts.Columns,
		colOrder:    schemaColumnOrder(pqSchema),
	}
}

// NewDeleteWriter creates a writer for equality delete files (PK columns only).
func NewDeleteWriter(ts *schema.TableSchema) *ParquetWriter {
	pkCols := make([]schema.Column, 0)
	for _, pk := range ts.PK {
		for _, col := range ts.Columns {
			if col.Name == pk {
				pkCols = append(pkCols, col)
				break
			}
		}
	}
	pqSchema := buildParquetSchema("equality_delete", pkCols)
	return &ParquetWriter{
		tableSchema: ts,
		pqSchema:    pqSchema,
		columns:     pkCols,
		colOrder:    schemaColumnOrder(pqSchema),
	}
}

// schemaColumnOrder extracts the leaf column ordering from a parquet schema.
func schemaColumnOrder(s *parquet.Schema) map[string]int {
	order := make(map[string]int)
	for i, field := range s.Fields() {
		order[field.Name()] = i
	}
	return order
}

func (w *ParquetWriter) Add(row map[string]any) {
	w.rows = append(w.rows, row)
}

func (w *ParquetWriter) Len() int {
	return len(w.rows)
}

func (w *ParquetWriter) Reset() {
	w.rows = w.rows[:0]
}

// Flush writes all buffered rows to a Parquet file and returns the bytes.
func (w *ParquetWriter) Flush() ([]byte, int64, error) {
	if len(w.rows) == 0 {
		return nil, 0, nil
	}

	rowBuf := parquet.NewBuffer(w.pqSchema)

	for _, row := range w.rows {
		pqRow := w.encodeRow(row)
		if _, err := rowBuf.WriteRows([]parquet.Row{pqRow}); err != nil {
			return nil, 0, fmt.Errorf("write row to buffer: %w", err)
		}
	}

	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf)
	if _, err := writer.WriteRowGroup(rowBuf); err != nil {
		return nil, 0, fmt.Errorf("write row group: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("close writer: %w", err)
	}

	count := int64(len(w.rows))
	return buf.Bytes(), count, nil
}

func (w *ParquetWriter) encodeRow(data map[string]any) parquet.Row {
	// Build row with values at the correct column indices from the parquet schema.
	row := make(parquet.Row, len(w.colOrder))
	for _, col := range w.columns {
		idx, ok := w.colOrder[col.Name]
		if !ok {
			continue
		}
		v := data[col.Name]
		if v == nil {
			if col.IsNullable {
				row[idx] = parquet.Value{}.Level(0, 0, idx)
			} else {
				row[idx] = zeroValue(col).Level(0, 0, idx)
			}
			continue
		}
		pv := toParquetValue(col, v)
		if col.IsNullable {
			row[idx] = pv.Level(0, 1, idx)
		} else {
			row[idx] = pv.Level(0, 0, idx)
		}
	}
	return row
}

func zeroValue(col schema.Column) parquet.Value {
	switch strings.ToLower(col.PGType) {
	case "int2", "smallint", "int4", "integer", "serial":
		return parquet.Int32Value(0)
	case "int8", "bigint", "bigserial":
		return parquet.Int64Value(0)
	case "float4", "real":
		return parquet.FloatValue(0)
	case "float8", "double precision":
		return parquet.DoubleValue(0)
	case "bool", "boolean":
		return parquet.BooleanValue(false)
	case "timestamptz", "timestamp with time zone", "timestamp", "timestamp without time zone":
		return parquet.Int64Value(0)
	case "date":
		return parquet.Int32Value(0)
	default:
		return parquet.ByteArrayValue([]byte(""))
	}
}

func toParquetValue(col schema.Column, v any) parquet.Value {
	switch strings.ToLower(col.PGType) {
	case "int2", "smallint":
		return parquet.Int32Value(toInt32(v))
	case "int4", "integer", "serial":
		return parquet.Int32Value(toInt32(v))
	case "int8", "bigint", "bigserial":
		return parquet.Int64Value(toInt64(v))
	case "float4", "real":
		return parquet.FloatValue(toFloat32(v))
	case "float8", "double precision":
		return parquet.DoubleValue(toFloat64(v))
	case "bool", "boolean":
		return parquet.BooleanValue(toBool(v))
	case "timestamptz", "timestamp with time zone":
		return parquet.Int64Value(toTimestampMicros(v))
	case "timestamp", "timestamp without time zone":
		return parquet.Int64Value(toTimestampMicros(v))
	case "date":
		return parquet.Int32Value(toDateDays(v))
	default:
		// text, varchar, numeric, json, uuid, etc. → string
		return parquet.ByteArrayValue([]byte(toString(v)))
	}
}

func buildParquetSchema(name string, columns []schema.Column) *parquet.Schema {
	group := make(parquet.Group)
	for _, col := range columns {
		node := pgToParquetNode(col.PGType)
		if col.IsNullable {
			node = parquet.Optional(node)
		}
		group[col.Name] = node
	}
	return parquet.NewSchema(name, group)
}

func pgToParquetNode(pgType string) parquet.Node {
	switch strings.ToLower(pgType) {
	case "int2", "smallint", "int4", "integer", "serial":
		return parquet.Leaf(parquet.Int32Type)
	case "int8", "bigint", "bigserial":
		return parquet.Leaf(parquet.Int64Type)
	case "float4", "real":
		return parquet.Leaf(parquet.FloatType)
	case "float8", "double precision":
		return parquet.Leaf(parquet.DoubleType)
	case "bool", "boolean":
		return parquet.Leaf(parquet.BooleanType)
	case "timestamptz", "timestamp with time zone":
		return parquet.Timestamp(parquet.Microsecond)
	case "timestamp", "timestamp without time zone":
		return parquet.Timestamp(parquet.Microsecond)
	case "date":
		return parquet.Date()
	default:
		return parquet.String()
	}
}

// --- type conversion helpers ---

func toInt32(v any) int32 {
	switch x := v.(type) {
	case int32:
		return x
	case int64:
		return int32(x)
	case int:
		return int32(x)
	case float64:
		return int32(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 32)
		return int32(n)
	default:
		return 0
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

func toFloat32(v any) float32 {
	switch x := v.(type) {
	case float32:
		return x
	case float64:
		return float32(x)
	case string:
		f, _ := strconv.ParseFloat(x, 32)
		return float32(f)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

func toBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x == "t" || x == "true" || x == "1"
	default:
		return false
	}
}

func toTimestampMicros(v any) int64 {
	switch x := v.(type) {
	case time.Time:
		return x.UnixMicro()
	case string:
		// Try common PG timestamp formats.
		// PG logical replication sends timestamps like: "2026-03-24 10:48:20.123456+08"
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999-07:00",
			"2006-01-02 15:04:05.999999-07",
			"2006-01-02 15:04:05.999999+00",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05-07",
			"2006-01-02 15:04:05+00",
			"2006-01-02 15:04:05",
		} {
			if t, err := time.Parse(layout, x); err == nil {
				return t.UnixMicro()
			}
		}
		// Last resort: try replacing space with T for RFC3339 compatibility
		if len(x) > 10 {
			rfc := x[:10] + "T" + x[11:]
			if t, err := time.Parse(time.RFC3339Nano, rfc); err == nil {
				return t.UnixMicro()
			}
		}
		return 0
	default:
		return 0
	}
}

func toDateDays(v any) int32 {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	switch x := v.(type) {
	case time.Time:
		return int32(x.Sub(epoch).Hours() / 24)
	case string:
		if t, err := time.Parse("2006-01-02", x); err == nil {
			return int32(t.Sub(epoch).Hours() / 24)
		}
		return 0
	default:
		return 0
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case fmt.Stringer:
		return x.String()
	case *big.Rat:
		f, _ := x.Float64()
		return strconv.FormatFloat(f, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}
