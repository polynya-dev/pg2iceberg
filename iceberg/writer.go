package iceberg

import (
	"bytes"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	apq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/pg2iceberg/pg2iceberg/postgres"
)

const fieldIDKey = "PARQUET:field_id"

// colSizer holds precomputed per-column sizing info to avoid repeated string
// comparisons in the hot path. fixedSize > 0 means the column has a known
// fixed byte width; fixedSize == 0 means it's variable-length (text, json, etc.).
type colSizer struct {
	name      string
	fixedSize int64 // 0 = variable-length
}

// buildColSizers precomputes sizing info from column types.
func buildColSizers(columns []postgres.Column) []colSizer {
	sizers := make([]colSizer, len(columns))
	for i, col := range columns {
		sizers[i].name = col.Name
		switch col.PGType {
		case postgres.Int2, postgres.Int4, postgres.OID, postgres.Float4, postgres.Date:
			sizers[i].fixedSize = 4
		case postgres.Int8, postgres.Float8,
			postgres.TimestampTZ, postgres.Timestamp,
			postgres.Time, postgres.TimeTZ:
			sizers[i].fixedSize = 8
		case postgres.Numeric:
			sizers[i].fixedSize = 16 // Decimal128
		case postgres.Bool:
			sizers[i].fixedSize = 1
		}
	}
	return sizers
}

// colAppender holds precomputed per-column append functions that push values
// directly into Arrow array builders, avoiding intermediate row storage.
type colAppender struct {
	name       string
	idx        int
	nullable   bool
	appendVal  func(array.Builder, any) error // appends a non-nil Go value; returns error on conversion failure
	appendZero func(array.Builder)      // appends a typed zero for non-nullable nulls
}

// buildColAppenders precomputes Arrow builder append functions for each column.
func buildColAppenders(columns []postgres.Column) []colAppender {
	appenders := make([]colAppender, len(columns))
	for i, col := range columns {
		ca := colAppender{
			name:     col.Name,
			idx:      i,
			nullable: col.IsNullable,
		}
		switch col.PGType {
		case postgres.Int2, postgres.Int4, postgres.OID:
			ca.appendVal = func(b array.Builder, v any) error {
				n, err := ToInt32(v)
				if err != nil {
					return err
				}
				b.(*array.Int32Builder).Append(n)
				return nil
			}
			ca.appendZero = func(b array.Builder) { b.(*array.Int32Builder).Append(0) }
		case postgres.Int8:
			ca.appendVal = func(b array.Builder, v any) error {
				n, err := ToInt64(v)
				if err != nil {
					return err
				}
				b.(*array.Int64Builder).Append(n)
				return nil
			}
			ca.appendZero = func(b array.Builder) { b.(*array.Int64Builder).Append(0) }
		case postgres.Float4:
			ca.appendVal = func(b array.Builder, v any) error {
				f, err := ToFloat32(v)
				if err != nil {
					return err
				}
				b.(*array.Float32Builder).Append(f)
				return nil
			}
			ca.appendZero = func(b array.Builder) { b.(*array.Float32Builder).Append(0) }
		case postgres.Float8:
			ca.appendVal = func(b array.Builder, v any) error {
				f, err := ToFloat64(v)
				if err != nil {
					return err
				}
				b.(*array.Float64Builder).Append(f)
				return nil
			}
			ca.appendZero = func(b array.Builder) { b.(*array.Float64Builder).Append(0) }
		case postgres.Bool:
			ca.appendVal = func(b array.Builder, v any) error {
				val, err := ToBool(v)
				if err != nil {
					return err
				}
				b.(*array.BooleanBuilder).Append(val)
				return nil
			}
			ca.appendZero = func(b array.Builder) { b.(*array.BooleanBuilder).Append(false) }
		case postgres.TimestampTZ, postgres.Timestamp:
			ca.appendVal = func(b array.Builder, v any) error {
				us, err := ToTimestampMicros(v)
				if err != nil {
					return err
				}
				b.(*array.TimestampBuilder).Append(arrow.Timestamp(us))
				return nil
			}
			ca.appendZero = func(b array.Builder) {
				b.(*array.TimestampBuilder).Append(0)
			}
		case postgres.Date:
			ca.appendVal = func(b array.Builder, v any) error {
				d, err := ToDateDays(v)
				if err != nil {
					return err
				}
				b.(*array.Date32Builder).Append(arrow.Date32(d))
				return nil
			}
			ca.appendZero = func(b array.Builder) {
				b.(*array.Date32Builder).Append(0)
			}
		case postgres.Time, postgres.TimeTZ:
			ca.appendVal = func(b array.Builder, v any) error {
				us, err := ToTimeMicros(v)
				if err != nil {
					return err
				}
				b.(*array.Time64Builder).Append(arrow.Time64(us))
				return nil
			}
			ca.appendZero = func(b array.Builder) {
				b.(*array.Time64Builder).Append(0)
			}
		case postgres.Numeric:
			prec, scale := decimalPrecScale(col)
			ca.appendVal = func(b array.Builder, v any) error {
				n, err := ToDecimal128(v, prec, scale)
				if err != nil {
					// Value overflows the declared decimal precision — write NULL.
					// This happens when PG precision > Iceberg's max of 38.
					b.(*array.Decimal128Builder).AppendNull()
					return nil
				}
				b.(*array.Decimal128Builder).Append(n)
				return nil
			}
			ca.appendZero = func(b array.Builder) {
				b.(*array.Decimal128Builder).Append(decimal128.Num{})
			}
		default:
			// text, varchar, numeric, json, uuid, etc. → string
			ca.appendVal = func(b array.Builder, v any) error {
				b.(*array.StringBuilder).Append(ToString(v))
				return nil
			}
			ca.appendZero = func(b array.Builder) { b.(*array.StringBuilder).Append("") }
		}
		appenders[i] = ca
	}
	return appenders
}

// pgToArrowType maps a PostgreSQL column to an Arrow data type.
func pgToArrowType(col postgres.Column) arrow.DataType {
	switch col.PGType {
	case postgres.Int2, postgres.Int4, postgres.OID:
		return arrow.PrimitiveTypes.Int32
	case postgres.Int8:
		return arrow.PrimitiveTypes.Int64
	case postgres.Float4:
		return arrow.PrimitiveTypes.Float32
	case postgres.Float8:
		return arrow.PrimitiveTypes.Float64
	case postgres.Bool:
		return arrow.FixedWidthTypes.Boolean
	case postgres.TimestampTZ:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case postgres.Timestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case postgres.Date:
		return arrow.FixedWidthTypes.Date32
	case postgres.Time, postgres.TimeTZ:
		return arrow.FixedWidthTypes.Time64us
	case postgres.Numeric:
		prec, scale := decimalPrecScale(col)
		return &arrow.Decimal128Type{Precision: prec, Scale: scale}
	default:
		return arrow.BinaryTypes.String
	}
}

// decimalPrecScale returns the Decimal128 precision and scale for a column,
// clamped to Iceberg's max of 38.
func decimalPrecScale(col postgres.Column) (int32, int32) {
	prec, scale := int32(col.Precision), int32(col.Scale)
	if prec <= 0 {
		return 38, 38
	}
	if prec > 38 {
		prec = 38
	}
	if scale > prec {
		scale = prec
	}
	return prec, scale
}

// buildArrowSchema creates an Arrow schema from the column definitions.
func buildArrowSchema(columns []postgres.Column) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     pgToArrowType(col),
			Nullable: col.IsNullable,
			Metadata: arrow.MetadataFrom(map[string]string{
				fieldIDKey: strconv.Itoa(col.FieldID),
			}),
		}
	}
	return arrow.NewSchema(fields, nil)
}

// buildBuilders creates one Arrow array builder per schema field.
func buildBuilders(sc *arrow.Schema) []array.Builder {
	builders := make([]array.Builder, sc.NumFields())
	for i, f := range sc.Fields() {
		builders[i] = array.NewBuilder(memory.DefaultAllocator, f.Type)
	}
	return builders
}

// parquetWriterProps are shared across all ParquetWriter instances.
var parquetWriterProps = apq.NewWriterProperties(
	apq.WithCompression(compress.Codecs.Snappy),
	apq.WithVersion(apq.V2_LATEST),
)

// ParquetWriter accumulates rows into columnar Arrow builders and flushes
// them as a Parquet file via pqarrow. Values are appended directly into
// typed builders in Add(), eliminating intermediate row storage.
type ParquetWriter struct {
	tableSchema    *postgres.TableSchema
	arrowSchema    *arrow.Schema
	columns        []postgres.Column
	colSizers      []colSizer
	colAppenders   []colAppender
	builders       []array.Builder
	rowCount       int64
	estimatedBytes int64
	outBuf         bytes.Buffer
}

func newParquetWriter(ts *postgres.TableSchema, columns []postgres.Column) *ParquetWriter {
	arrowSchema := buildArrowSchema(columns)
	return &ParquetWriter{
		tableSchema:  ts,
		arrowSchema:  arrowSchema,
		columns:      columns,
		colSizers:    buildColSizers(columns),
		colAppenders: buildColAppenders(columns),
		builders:     buildBuilders(arrowSchema),
	}
}

// NewDataWriter creates a writer for data files (all columns).
func NewDataWriter(ts *postgres.TableSchema) *ParquetWriter {
	return newParquetWriter(ts, ts.Columns)
}

// NewDeleteWriter creates a writer for equality delete files (PK columns only).
func NewDeleteWriter(ts *postgres.TableSchema) *ParquetWriter {
	pkCols := make([]postgres.Column, 0)
	for _, pk := range ts.PK {
		for _, col := range ts.Columns {
			if col.Name == pk {
				pkCols = append(pkCols, col)
				break
			}
		}
	}
	return newParquetWriter(ts, pkCols)
}

func (w *ParquetWriter) Add(row map[string]any) error {
	for i := range w.colAppenders {
		ca := &w.colAppenders[i]
		v := row[ca.name]
		if v == nil {
			if ca.nullable {
				w.builders[ca.idx].AppendNull()
			} else {
				ca.appendZero(w.builders[ca.idx])
			}
		} else {
			if err := ca.appendVal(w.builders[ca.idx], v); err != nil {
				return fmt.Errorf("column %s: %w", ca.name, err)
			}
		}
	}
	w.rowCount++
	w.estimatedBytes += estimateRowBytes(w.colSizers, row)
	return nil
}

func (w *ParquetWriter) Len() int {
	return int(w.rowCount)
}

func (w *ParquetWriter) EstimatedBytes() int64 {
	return w.estimatedBytes
}

func (w *ParquetWriter) Reset() {
	for _, b := range w.builders {
		b.Release()
	}
	w.builders = buildBuilders(w.arrowSchema)
	w.rowCount = 0
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
			size += int64(len(ToString(v))) + 4
		}
	}
	return size
}

// Flush builds an Arrow record from the accumulated builders and writes it
// to a Parquet file. The builders are reset after the record is constructed.
func (w *ParquetWriter) Flush() ([]byte, int64, error) {
	if w.rowCount == 0 {
		return nil, 0, nil
	}

	// Build arrays from builders (also resets builders for reuse).
	arrays := make([]arrow.Array, len(w.builders))
	for i, b := range w.builders {
		arrays[i] = b.NewArray()
	}
	rec := array.NewRecordBatch(w.arrowSchema, arrays, w.rowCount)
	defer rec.Release()
	for _, a := range arrays {
		a.Release()
	}

	// Write the record to Parquet.
	w.outBuf.Reset()
	w.outBuf.Grow(int(w.estimatedBytes))

	fw, err := pqarrow.NewFileWriter(w.arrowSchema, &w.outBuf, parquetWriterProps, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, 0, fmt.Errorf("create parquet writer: %w", err)
	}
	if err := fw.Write(rec); err != nil {
		return nil, 0, fmt.Errorf("write record batch: %w", err)
	}
	if err := fw.Close(); err != nil {
		return nil, 0, fmt.Errorf("close parquet writer: %w", err)
	}

	// Copy output — the caller takes ownership and outBuf will be reused.
	out := make([]byte, w.outBuf.Len())
	copy(out, w.outBuf.Bytes())

	count := w.rowCount
	return out, count, nil
}

// FileChunk represents a completed parquet file from the rolling writer.
type FileChunk struct {
	Data     []byte
	RowCount int64
}

// RollingWriter wraps a ParquetWriter and automatically splits into
// multiple files when the estimated size exceeds the target.
type RollingWriter struct {
	schema     *postgres.TableSchema
	newWriter  func(*postgres.TableSchema) *ParquetWriter
	writer     *ParquetWriter
	targetSize int64
	completed  []FileChunk
}

// NewRollingDataWriter creates a rolling writer for data files.
func NewRollingDataWriter(ts *postgres.TableSchema, targetSize int64) *RollingWriter {
	return &RollingWriter{
		schema:     ts,
		newWriter:  NewDataWriter,
		writer:     NewDataWriter(ts),
		targetSize: targetSize,
	}
}

// NewRollingDeleteWriter creates a rolling writer for equality delete files.
func NewRollingDeleteWriter(ts *postgres.TableSchema, targetSize int64) *RollingWriter {
	return &RollingWriter{
		schema:     ts,
		newWriter:  NewDeleteWriter,
		writer:     NewDeleteWriter(ts),
		targetSize: targetSize,
	}
}

func (rw *RollingWriter) Add(row map[string]any) error {
	if err := rw.writer.Add(row); err != nil {
		return err
	}

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

// --- type conversion helpers ---

func ToInt32(v any) (int32, error) {
	switch x := v.(type) {
	case int32:
		return x, nil
	case int64:
		return int32(x), nil
	case int:
		return int32(x), nil
	case float64:
		return int32(x), nil
	case string:
		n, err := strconv.ParseInt(x, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("parse int32 %q: %w", x, err)
		}
		return int32(n), nil
	default:
		return 0, fmt.Errorf("unsupported type %T for int32", v)
	}
}

func ToInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse int64 %q: %w", x, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported type %T for int64", v)
	}
}

func ToFloat32(v any) (float32, error) {
	switch x := v.(type) {
	case float32:
		return x, nil
	case float64:
		return float32(x), nil
	case string:
		f, err := strconv.ParseFloat(x, 32)
		if err != nil {
			return 0, fmt.Errorf("parse float32 %q: %w", x, err)
		}
		return float32(f), nil
	default:
		return 0, fmt.Errorf("unsupported type %T for float32", v)
	}
}

func ToFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, fmt.Errorf("parse float64 %q: %w", x, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported type %T for float64", v)
	}
}

func ToBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case string:
		return x == "t" || x == "true" || x == "1", nil
	default:
		return false, fmt.Errorf("unsupported type %T for bool", v)
	}
}

func ToTimestampMicros(v any) (int64, error) {
	switch x := v.(type) {
	case time.Time:
		return x.UnixMicro(), nil
	case int64:
		return x, nil // already microseconds since epoch (e.g. from parquet roundtrip)
	case int32:
		return int64(x), nil
	case string:
		us, ok := FastParseTimestamp(x)
		if !ok {
			return 0, fmt.Errorf("parse timestamp %q", x)
		}
		return us, nil
	default:
		return 0, fmt.Errorf("unsupported type %T for timestamp", v)
	}
}

// FastParseTimestamp parses the PG logical replication timestamp format directly
// without going through time.Parse. Expected format:
//
//	"2006-01-02 15:04:05"               (no tz, timestamp without time zone)
//	"2006-01-02 15:04:05.999999"        (no tz, with fractional seconds)
//	"2006-01-02 15:04:05.999999+08"     (short tz)
//	"2006-01-02 15:04:05.999999+08:00"  (long tz)
//	"2006-01-02 15:04:05+08"            (no fractional seconds)
//
// Returns microseconds since Unix epoch and true if parsed successfully.
func FastParseTimestamp(s string) (int64, bool) {
	// Minimum: "2006-01-02 15:04:05" = 19 chars
	if len(s) < 19 || s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' || s[16] != ':' {
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
		days := DaysSinceEpoch(year, int(month), day)
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
	days := DaysSinceEpoch(year, int(month), day)
	unixSec := int64(days)*86400 + int64(hour)*3600 + int64(min)*60 + int64(sec) - int64(tzOffsetSec)
	return unixSec*1_000_000 + int64(micros), true
}

// DaysSinceEpoch returns the number of days from 1970-01-01 to the given date.
func DaysSinceEpoch(year, month, day int) int64 {
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

func ToDateDays(v any) (int32, error) {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	switch x := v.(type) {
	case time.Time:
		return int32(x.Sub(epoch).Hours() / 24), nil
	case int32:
		return x, nil // already days since epoch (e.g. from parquet roundtrip)
	case int64:
		return int32(x), nil
	case string:
		t, err := time.Parse("2006-01-02", x)
		if err != nil {
			return 0, fmt.Errorf("parse date %q: %w", x, err)
		}
		return int32(t.Sub(epoch).Hours() / 24), nil
	default:
		return 0, fmt.Errorf("unsupported type %T for date", v)
	}
}

// ToTimeMicros converts a value to microseconds since midnight for Iceberg time type.
// Handles PG text format "HH:MM:SS.ffffff" and "HH:MM:SS.ffffff+ZZ" (timetz).
func ToTimeMicros(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil // already microseconds since midnight (e.g. from parquet roundtrip)
	case string:
		us, ok := fastParseTime(x)
		if !ok {
			return 0, fmt.Errorf("parse time %q", x)
		}
		return us, nil
	default:
		return 0, fmt.Errorf("unsupported type %T for time", v)
	}
}

// fastParseTime parses PG time format "HH:MM:SS[.ffffff][+/-ZZ[:ZZ]]"
// and returns microseconds since midnight. Timezone offset is ignored (time is
// stored as local time of day per the Iceberg spec).
func fastParseTime(s string) (int64, bool) {
	// Minimum: "HH:MM:SS" = 8 chars
	if len(s) < 8 || s[2] != ':' || s[5] != ':' {
		return 0, false
	}
	h := atoi2(s[0:2])
	m := atoi2(s[3:5])
	sec := atoi2(s[6:8])
	if h < 0 || h > 23 || m < 0 || m > 59 || sec < 0 || sec > 59 {
		return 0, false
	}

	var micros int
	rest := s[8:]
	if len(rest) > 0 && rest[0] == '.' {
		rest = rest[1:]
		digits := 0
		for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
			digits++
		}
		if digits == 0 {
			return 0, false
		}
		micros = atoiN(rest[:digits])
		for i := digits; i < 6; i++ {
			micros *= 10
		}
		for i := 6; i < digits; i++ {
			micros /= 10
		}
	}

	return int64(h)*3_600_000_000 + int64(m)*60_000_000 + int64(sec)*1_000_000 + int64(micros), true
}

// ToDecimal128 converts a value to an Arrow Decimal128 number with the given
// precision and scale. Handles PG text representation of numeric values.
func ToDecimal128(v any, prec, scale int32) (decimal128.Num, error) {
	s := ToString(v)
	n, err := decimal128.FromString(s, prec, scale)
	if err != nil {
		return decimal128.Num{}, fmt.Errorf("convert %q to decimal128(%d,%d): %w", s, prec, scale, err)
	}
	return n, nil
}

func ToString(v any) string {
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
