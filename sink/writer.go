package sink

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/parquet-go/parquet-go"
)

// colSizer holds precomputed per-column sizing info to avoid repeated string
// comparisons in the hot path. fixedSize > 0 means the column has a known
// fixed byte width; fixedSize == 0 means it's variable-length (text, json, etc.).
type colSizer struct {
	name      string
	fixedSize int64 // 0 = variable-length
}

// buildColSizers precomputes sizing info from column types.
func buildColSizers(columns []schema.Column) []colSizer {
	sizers := make([]colSizer, len(columns))
	for i, col := range columns {
		sizers[i].name = col.Name
		switch strings.ToLower(col.PGType) {
		case "int2", "smallint", "int4", "integer", "serial", "float4", "real", "date":
			sizers[i].fixedSize = 4
		case "int8", "bigint", "bigserial", "float8", "double precision",
			"timestamptz", "timestamp with time zone", "timestamp", "timestamp without time zone":
			sizers[i].fixedSize = 8
		case "bool", "boolean":
			sizers[i].fixedSize = 1
		}
	}
	return sizers
}

// colConverter holds precomputed conversion functions per column to avoid
// strings.ToLower on every row in the parquet encoding hot path.
type colConverter struct {
	name     string
	idx      int // parquet column index
	nullable bool
	convert  func(any) parquet.Value // converts a non-nil Go value to parquet
	zero     parquet.Value           // zero value for non-nullable null
}

// buildColConverters precomputes parquet conversion functions for each column.
func buildColConverters(columns []schema.Column, colOrder map[string]int) []colConverter {
	converters := make([]colConverter, 0, len(columns))
	for _, col := range columns {
		idx, ok := colOrder[col.Name]
		if !ok {
			continue
		}
		cc := colConverter{
			name:     col.Name,
			idx:      idx,
			nullable: col.IsNullable,
		}
		switch strings.ToLower(col.PGType) {
		case "int2", "smallint":
			cc.convert = func(v any) parquet.Value { return parquet.Int32Value(toInt32(v)) }
			cc.zero = parquet.Int32Value(0)
		case "int4", "integer", "serial":
			cc.convert = func(v any) parquet.Value { return parquet.Int32Value(toInt32(v)) }
			cc.zero = parquet.Int32Value(0)
		case "int8", "bigint", "bigserial":
			cc.convert = func(v any) parquet.Value { return parquet.Int64Value(toInt64(v)) }
			cc.zero = parquet.Int64Value(0)
		case "float4", "real":
			cc.convert = func(v any) parquet.Value { return parquet.FloatValue(toFloat32(v)) }
			cc.zero = parquet.FloatValue(0)
		case "float8", "double precision":
			cc.convert = func(v any) parquet.Value { return parquet.DoubleValue(toFloat64(v)) }
			cc.zero = parquet.DoubleValue(0)
		case "bool", "boolean":
			cc.convert = func(v any) parquet.Value { return parquet.BooleanValue(toBool(v)) }
			cc.zero = parquet.BooleanValue(false)
		case "timestamptz", "timestamp with time zone", "timestamp", "timestamp without time zone":
			cc.convert = func(v any) parquet.Value { return parquet.Int64Value(toTimestampMicros(v)) }
			cc.zero = parquet.Int64Value(0)
		case "date":
			cc.convert = func(v any) parquet.Value { return parquet.Int32Value(toDateDays(v)) }
			cc.zero = parquet.Int32Value(0)
		default:
			cc.convert = func(v any) parquet.Value { return parquet.ByteArrayValue([]byte(toString(v))) }
			cc.zero = parquet.ByteArrayValue([]byte(""))
		}
		converters = append(converters, cc)
	}
	return converters
}

// ParquetWriter accumulates rows and flushes them as a Parquet file.
type ParquetWriter struct {
	tableSchema   *schema.TableSchema
	pqSchema      *parquet.Schema
	columns       []schema.Column   // columns to write (subset for delete files)
	colSizers     []colSizer        // precomputed per-column sizing
	colConverters []colConverter     // precomputed parquet converters
	// colOrder maps column name to its index in the parquet schema's leaf columns.
	colOrder       map[string]int
	rows           []map[string]any
	estimatedBytes int64

	// Reusable parquet encoding state — avoids re-allocating internal column
	// buffers on every flush.
	rowBuf   *parquet.Buffer
	pqWriter *parquet.Writer
	outBuf   bytes.Buffer
	pqRow    parquet.Row
}

// NewDataWriter creates a writer for data files (all columns).
func NewDataWriter(ts *schema.TableSchema) *ParquetWriter {
	pqSchema := buildParquetSchema("data", ts.Columns)
	colOrder := schemaColumnOrder(pqSchema)
	w := &ParquetWriter{
		tableSchema:   ts,
		pqSchema:      pqSchema,
		columns:       ts.Columns,
		colSizers:     buildColSizers(ts.Columns),
		colConverters: buildColConverters(ts.Columns, colOrder),
		colOrder:      colOrder,
		rowBuf:        parquet.NewBuffer(pqSchema),
		pqRow:         make(parquet.Row, len(colOrder)),
	}
	w.pqWriter = parquet.NewWriter(&w.outBuf)
	return w
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
	colOrder := schemaColumnOrder(pqSchema)
	w := &ParquetWriter{
		tableSchema:   ts,
		pqSchema:      pqSchema,
		columns:       pkCols,
		colSizers:     buildColSizers(pkCols),
		colConverters: buildColConverters(pkCols, colOrder),
		colOrder:      colOrder,
		rowBuf:        parquet.NewBuffer(pqSchema),
		pqRow:         make(parquet.Row, len(colOrder)),
	}
	w.pqWriter = parquet.NewWriter(&w.outBuf)
	return w
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
	w.estimatedBytes += estimateRowBytes(w.colSizers, row)
}

func (w *ParquetWriter) Len() int {
	return len(w.rows)
}

func (w *ParquetWriter) EstimatedBytes() int64 {
	return w.estimatedBytes
}

func (w *ParquetWriter) Reset() {
	// Nil out references so GC can collect row maps between flushes.
	// Row maps are NOT released to the pool here because some may still
	// be referenced by rowCache. They are released when rowCache is cleared.
	for i := range w.rows {
		w.rows[i] = nil
	}
	w.rows = w.rows[:0]
	w.estimatedBytes = 0
}

// estimateRowBytes returns a rough byte count for a row using precomputed column sizers.
func estimateRowBytes(sizers []colSizer, row map[string]any) int64 {
	var size int64
	for i := range sizers {
		v := row[sizers[i].name]
		if v == nil {
			size++
			continue
		}
		if sizers[i].fixedSize > 0 {
			size += sizers[i].fixedSize
		} else {
			size += int64(len(toString(v))) + 4
		}
	}
	return size
}

// FileChunk represents a completed parquet file from the rolling writer.
type FileChunk struct {
	Data     []byte
	RowCount int64
}

// RollingWriter wraps a ParquetWriter and automatically splits into
// multiple files when the estimated size exceeds the target.
type RollingWriter struct {
	schema     *schema.TableSchema
	newWriter  func(*schema.TableSchema) *ParquetWriter
	writer     *ParquetWriter
	targetSize int64
	completed  []FileChunk
}

// NewRollingDataWriter creates a rolling writer for data files.
func NewRollingDataWriter(ts *schema.TableSchema, targetSize int64) *RollingWriter {
	return &RollingWriter{
		schema:     ts,
		newWriter:  NewDataWriter,
		writer:     NewDataWriter(ts),
		targetSize: targetSize,
	}
}

// NewRollingDeleteWriter creates a rolling writer for equality delete files.
func NewRollingDeleteWriter(ts *schema.TableSchema, targetSize int64) *RollingWriter {
	return &RollingWriter{
		schema:     ts,
		newWriter:  NewDeleteWriter,
		writer:     NewDeleteWriter(ts),
		targetSize: targetSize,
	}
}

func (rw *RollingWriter) Add(row map[string]any) error {
	rw.writer.Add(row)

	if rw.targetSize > 0 && rw.writer.EstimatedBytes() >= rw.targetSize {
		if err := rw.rollover(); err != nil {
			return err
		}
	}
	return nil
}

func (rw *RollingWriter) rollover() error {
	data, rowCount, err := rw.writer.Flush()
	if err != nil {
		return fmt.Errorf("rolling flush: %w", err)
	}
	if data != nil {
		rw.completed = append(rw.completed, FileChunk{Data: data, RowCount: rowCount})
	}
	rw.writer.Reset()
	return nil
}

// FlushAll serializes any remaining rows into file chunks and returns all
// completed chunks. This is non-destructive — calling FlushAll again without
// an intervening Commit returns the same chunks. Call Commit after a
// successful upload/commit to clear the buffer.
func (rw *RollingWriter) FlushAll() ([]FileChunk, error) {
	if rw.writer.Len() > 0 {
		if err := rw.rollover(); err != nil {
			return nil, err
		}
	}
	return rw.completed, nil
}

// Commit clears all completed chunks. Call this after the data has been
// successfully persisted (uploaded and committed to the catalog).
func (rw *RollingWriter) Commit() {
	rw.completed = nil
}

func (rw *RollingWriter) Len() int {
	total := rw.writer.Len()
	for _, c := range rw.completed {
		total += int(c.RowCount)
	}
	return total
}

func (rw *RollingWriter) EstimatedBytes() int64 {
	total := rw.writer.EstimatedBytes()
	for _, c := range rw.completed {
		total += int64(len(c.Data))
	}
	return total
}

func (rw *RollingWriter) Reset() {
	rw.writer.Reset()
	rw.completed = nil
}

// DiscardCompleted clears completed chunks without touching active writer rows.
// Used before flush retries to discard stale chunks from a prior failed flush
// while preserving data written directly to the writer (e.g. snapshot rows).
func (rw *RollingWriter) DiscardCompleted() {
	rw.completed = nil
}

// Flush writes all buffered rows to a Parquet file and returns the bytes.
func (w *ParquetWriter) Flush() ([]byte, int64, error) {
	if len(w.rows) == 0 {
		return nil, 0, nil
	}

	w.rowBuf.Reset()
	batch := [1]parquet.Row{} // stack-allocated single-element batch for WriteRows

	for _, row := range w.rows {
		w.encodeRowInto(w.pqRow, row)
		batch[0] = w.pqRow
		if _, err := w.rowBuf.WriteRows(batch[:]); err != nil {
			return nil, 0, fmt.Errorf("write row to buffer: %w", err)
		}
	}

	w.outBuf.Reset()
	w.outBuf.Grow(int(w.estimatedBytes))
	w.pqWriter.Reset(&w.outBuf)
	if _, err := w.pqWriter.WriteRowGroup(w.rowBuf); err != nil {
		return nil, 0, fmt.Errorf("write row group: %w", err)
	}
	if err := w.pqWriter.Close(); err != nil {
		return nil, 0, fmt.Errorf("close writer: %w", err)
	}

	// Copy output — the caller takes ownership and outBuf will be reused.
	out := make([]byte, w.outBuf.Len())
	copy(out, w.outBuf.Bytes())

	count := int64(len(w.rows))
	return out, count, nil
}

// encodeRowInto populates the pre-allocated dst slice with parquet values.
func (w *ParquetWriter) encodeRowInto(dst parquet.Row, data map[string]any) {
	for i := range w.colConverters {
		cc := &w.colConverters[i]
		v := data[cc.name]
		if v == nil {
			if cc.nullable {
				dst[cc.idx] = parquet.Value{}.Level(0, 0, cc.idx)
			} else {
				dst[cc.idx] = cc.zero.Level(0, 0, cc.idx)
			}
			continue
		}
		pv := cc.convert(v)
		if cc.nullable {
			dst[cc.idx] = pv.Level(0, 1, cc.idx)
		} else {
			dst[cc.idx] = pv.Level(0, 0, cc.idx)
		}
	}
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
		// Iceberg requires PARQUET:field_id on each column so readers
		// (DuckDB, Spark, etc.) can map columns by field ID, not name.
		node = parquet.FieldID(node, col.FieldID)
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
	case int64:
		return x // already microseconds since epoch (e.g. from parquet roundtrip)
	case int32:
		return int64(x)
	case string:
		us, _ := fastParseTimestamp(x)
		return us
	default:
		return 0
	}
}

// fastParseTimestamp parses the PG logical replication timestamp format directly
// without going through time.Parse. Expected format:
//
//	"2006-01-02 15:04:05.999999+08"     (short tz)
//	"2006-01-02 15:04:05.999999+08:00"  (long tz)
//	"2006-01-02 15:04:05+08"            (no fractional seconds)
//
// Returns microseconds since Unix epoch and true if parsed successfully.
func fastParseTimestamp(s string) (int64, bool) {
	// Minimum: "2006-01-02 15:04:05+08" = 22 chars
	if len(s) < 22 || s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' || s[16] != ':' {
		return 0, false
	}

	year := atoi4(s[0:4])
	month := atoi2(s[5:7])
	day := atoi2(s[8:10])
	hour := atoi2(s[11:13])
	min := atoi2(s[14:16])
	sec := atoi2(s[17:19])

	if year < 0 || month < 1 || month > 12 || day < 1 || day > 31 ||
		hour < 0 || hour > 23 || min < 0 || min > 59 || sec < 0 || sec > 60 {
		return 0, false
	}

	// Parse fractional seconds and timezone offset.
	var micros int
	rest := s[19:]
	if len(rest) > 0 && rest[0] == '.' {
		rest = rest[1:]
		// Read up to 6 fractional digits.
		digits := 0
		for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
			digits++
		}
		if digits == 0 {
			return 0, false
		}
		micros = atoiN(rest[:digits])
		// Scale to microseconds (e.g., 3 digits = milliseconds * 1000).
		for i := digits; i < 6; i++ {
			micros *= 10
		}
		// Truncate if more than 6 digits.
		for i := 6; i < digits; i++ {
			micros /= 10
		}
		rest = rest[digits:]
	}

	// Parse timezone: +08, -05:30, +00:00, Z, or absent (timestamp without tz).
	var tzOffsetSec int
	if len(rest) == 0 {
		// No timezone — timestamp without time zone. Treat as UTC.
		days := daysSinceEpoch(year, int(month), day)
		unixSec := int64(days)*86400 + int64(hour)*3600 + int64(min)*60 + int64(sec)
		return unixSec*1_000_000 + int64(micros), true
	}
	switch rest[0] {
	case 'Z':
		// UTC, offset 0
	case '+', '-':
		if len(rest) < 3 {
			return 0, false
		}
		tzH := atoi2(rest[1:3])
		if tzH < 0 || tzH > 14 {
			return 0, false
		}
		tzM := 0
		if len(rest) >= 5 && rest[3] == ':' {
			tzM = atoi2(rest[4:6])
		} else if len(rest) >= 5 && rest[3] >= '0' && rest[3] <= '9' {
			tzM = atoi2(rest[3:5])
		}
		if tzM < 0 || tzM > 59 {
			return 0, false
		}
		tzOffsetSec = tzH*3600 + tzM*60
		if rest[0] == '-' {
			tzOffsetSec = -tzOffsetSec
		}
	default:
		return 0, false
	}

	// Convert to Unix timestamp using a fast days-since-epoch calculation.
	days := daysSinceEpoch(year, int(month), day)
	unixSec := int64(days)*86400 + int64(hour)*3600 + int64(min)*60 + int64(sec) - int64(tzOffsetSec)
	return unixSec*1_000_000 + int64(micros), true
}

// daysSinceEpoch returns the number of days from 1970-01-01 to the given date.
func daysSinceEpoch(year, month, day int) int64 {
	// Adjust for months before March (shifts leap day to end of year).
	if month <= 2 {
		year--
		month += 12
	}
	era := year / 400
	yoe := year - era*400                          // year of era [0, 399]
	doy := (153*(month-3)+2)/5 + day - 1           // day of year [0, 365]
	doe := yoe*365 + yoe/4 - yoe/100 + doy         // day of era [0, 146096]
	return int64(era)*146097 + int64(doe) - 719468  // days since 1970-01-01
}

func atoi2(s string) int {
	d0, d1 := int(s[0]-'0'), int(s[1]-'0')
	if d0 < 0 || d0 > 9 || d1 < 0 || d1 > 9 {
		return -1
	}
	return d0*10 + d1
}

func atoi4(s string) int {
	d0, d1, d2, d3 := int(s[0]-'0'), int(s[1]-'0'), int(s[2]-'0'), int(s[3]-'0')
	if d0 < 0 || d0 > 9 || d1 < 0 || d1 > 9 || d2 < 0 || d2 > 9 || d3 < 0 || d3 > 9 {
		return -1
	}
	return d0*1000 + d1*100 + d2*10 + d3
}

func atoiN(s string) int {
	n := 0
	for _, c := range []byte(s) {
		n = n*10 + int(c-'0')
	}
	return n
}

func toDateDays(v any) int32 {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	switch x := v.(type) {
	case time.Time:
		return int32(x.Sub(epoch).Hours() / 24)
	case int32:
		return x // already days since epoch (e.g. from parquet roundtrip)
	case int64:
		return int32(x)
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
