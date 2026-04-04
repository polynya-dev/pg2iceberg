package sink

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/spaolacci/murmur3"
)

// PartitionField describes a single partition field in an Iceberg partition spec.
type PartitionField struct {
	SourceID       int    // Iceberg field ID of the source column
	FieldID        int    // Partition field ID (1000+)
	Name           string // Partition field name (e.g. "created_at_day")
	Transform      string // identity, year, month, day, hour, bucket, truncate
	TransformParam int    // N for bucket[N], W for truncate[W]

	// Cached column metadata for hot-path lookups (set by BuildPartitionSpec).
	sourceCol *schema.Column
}

// PartitionSpec describes the partition spec for an Iceberg table.
type PartitionSpec struct {
	Fields []PartitionField
}

// parsePartitionExpr parses partition expressions:
//
//	"day(created_at)"      → ("day", "created_at", 0)
//	"bucket[16](id)"       → ("bucket", "id", 16)
//	"truncate[4](name)"    → ("truncate", "name", 4)
//	"region"               → ("identity", "region", 0)
func parsePartitionExpr(expr string) (transform, column string, param int, err error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", 0, fmt.Errorf("empty partition expression")
	}

	if idx := strings.Index(expr, "("); idx > 0 {
		if !strings.HasSuffix(expr, ")") {
			return "", "", 0, fmt.Errorf("invalid partition expression %q: missing closing parenthesis", expr)
		}
		column = strings.TrimSpace(expr[idx+1 : len(expr)-1])
		if column == "" {
			return "", "", 0, fmt.Errorf("invalid partition expression %q: empty column name", expr)
		}

		transformPart := strings.ToLower(expr[:idx])

		// Check for parameterized transforms: bucket[N] or truncate[W].
		if bracketOpen := strings.Index(transformPart, "["); bracketOpen > 0 {
			if !strings.HasSuffix(transformPart, "]") {
				return "", "", 0, fmt.Errorf("invalid partition expression %q: missing closing bracket", expr)
			}
			transform = transformPart[:bracketOpen]
			paramStr := transformPart[bracketOpen+1 : len(transformPart)-1]
			n, parseErr := fmt.Sscanf(paramStr, "%d", &param)
			if parseErr != nil || n != 1 || param <= 0 {
				return "", "", 0, fmt.Errorf("invalid partition expression %q: parameter must be a positive integer", expr)
			}
			return transform, column, param, nil
		}

		return transformPart, column, 0, nil
	}

	return "identity", expr, 0, nil
}

// BuildPartitionSpec builds a PartitionSpec from config expressions and table schema.
// Each expression is either "transform(column)" (e.g. "day(created_at)") or just
// "column" for identity transform (e.g. "region").
func BuildPartitionSpec(exprs []string, ts *schema.TableSchema) (*PartitionSpec, error) {
	if len(exprs) == 0 {
		return &PartitionSpec{}, nil
	}

	spec := &PartitionSpec{}
	for i, expr := range exprs {
		transform, colName, param, err := parsePartitionExpr(expr)
		if err != nil {
			return nil, err
		}

		col := findColumn(ts, colName)
		if col == nil {
			return nil, fmt.Errorf("partition column %q not found in table %s", colName, ts.Table)
		}

		switch transform {
		case "identity":
			// valid for any type
		case "year", "month", "day", "hour":
			if !isTemporalType(col.PGType) {
				return nil, fmt.Errorf("partition transform %q requires a date/timestamp column, got %q (%s)", transform, colName, col.PGType)
			}
		case "bucket":
			if param <= 0 {
				return nil, fmt.Errorf("bucket transform requires a positive integer parameter, e.g. bucket[16](%s)", colName)
			}
			if !isBucketableType(col.PGType) {
				return nil, fmt.Errorf("partition transform %q not supported for column %q (%s)", transform, colName, col.PGType)
			}
		case "truncate":
			if param <= 0 {
				return nil, fmt.Errorf("truncate transform requires a positive integer parameter, e.g. truncate[4](%s)", colName)
			}
			if !isTruncatableType(col.PGType) {
				return nil, fmt.Errorf("partition transform %q not supported for column %q (%s)", transform, colName, col.PGType)
			}
		default:
			return nil, fmt.Errorf("unsupported partition transform %q in %q", transform, expr)
		}

		name := colName
		if transform != "identity" {
			name = colName + "_" + transform
		}

		spec.Fields = append(spec.Fields, PartitionField{
			SourceID:       col.FieldID,
			FieldID:        1000 + i,
			Name:           name,
			Transform:      transform,
			TransformParam: param,
			sourceCol:      col,
		})
	}
	return spec, nil
}

// IsUnpartitioned returns true if the spec has no partition fields.
func (ps *PartitionSpec) IsUnpartitioned() bool {
	return len(ps.Fields) == 0
}

// PartitionKey computes the partition key string for a row.
// Returns only the string key — use PartitionValues separately when the
// partition value map is needed (e.g. on cache miss).
func (ps *PartitionSpec) PartitionKey(row map[string]any, ts *schema.TableSchema) string {
	if ps.IsUnpartitioned() {
		return ""
	}

	var buf strings.Builder

	for i, field := range ps.Fields {
		if i > 0 {
			buf.WriteByte('/')
		}

		col := field.sourceCol
		if col == nil {
			col = findColumnByID(ts, field.SourceID)
		}
		if col == nil {
			buf.WriteString("__null__")
			continue
		}

		raw := row[col.Name]
		if raw == nil {
			buf.WriteString("__null__")
			continue
		}

		val := applyTransform(field.Transform, field.TransformParam, raw, col.PGType)
		appendPartValue(&buf, val)
	}

	return buf.String()
}

// PartitionValues computes the partition value map for a row.
// Only needed when persisting partition metadata (e.g. new partition creation).
func (ps *PartitionSpec) PartitionValues(row map[string]any, ts *schema.TableSchema) map[string]any {
	values := make(map[string]any, len(ps.Fields))
	for _, field := range ps.Fields {
		col := field.sourceCol
		if col == nil {
			col = findColumnByID(ts, field.SourceID)
		}
		if col == nil {
			values[field.Name] = nil
			continue
		}
		raw := row[col.Name]
		if raw == nil {
			values[field.Name] = nil
			continue
		}
		values[field.Name] = applyTransform(field.Transform, field.TransformParam, raw, col.PGType)
	}
	return values
}

// appendPartValue appends a partition value to the builder without fmt.Sprintf.
func appendPartValue(buf *strings.Builder, val any) {
	switch v := val.(type) {
	case int32:
		buf.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		buf.WriteString(strconv.FormatInt(v, 10))
	case int:
		buf.WriteString(strconv.Itoa(v))
	case string:
		buf.WriteString(v)
	default:
		fmt.Fprintf(buf, "%v", val)
	}
}

// PartitionPath returns the S3 directory path component for a partition.
func (ps *PartitionSpec) PartitionPath(partValues map[string]any) string {
	if ps.IsUnpartitioned() || len(partValues) == 0 {
		return ""
	}

	parts := make([]string, 0, len(ps.Fields))
	for _, field := range ps.Fields {
		val := partValues[field.Name]
		if val == nil {
			parts = append(parts, fmt.Sprintf("%s=__null__", field.Name))
		} else {
			parts = append(parts, fmt.Sprintf("%s=%v", field.Name, val))
		}
	}
	return strings.Join(parts, "/")
}

// PartitionSpecJSON returns the partition spec fields as a JSON array string for manifest metadata.
func (ps *PartitionSpec) PartitionSpecJSON() string {
	if ps.IsUnpartitioned() {
		return "[]"
	}

	fields := make([]string, len(ps.Fields))
	for i, f := range ps.Fields {
		fields[i] = fmt.Sprintf(`{"source-id": %d, "field-id": %d, "name": "%s", "transform": "%s"}`,
			f.SourceID, f.FieldID, f.Name, f.transformString())
	}
	return "[" + strings.Join(fields, ", ") + "]"
}

// CatalogPartitionSpec returns the partition spec as a map for the Iceberg REST catalog API.
func (ps *PartitionSpec) CatalogPartitionSpec() map[string]any {
	fields := make([]any, len(ps.Fields))
	for i, f := range ps.Fields {
		fields[i] = map[string]any{
			"field-id":  f.FieldID,
			"source-id": f.SourceID,
			"transform": f.transformString(),
			"name":      f.Name,
		}
	}
	return map[string]any{
		"spec-id": 0,
		"fields":  fields,
	}
}

// transformString returns the Iceberg spec transform string (e.g. "bucket[16]", "truncate[4]", "day").
func (f *PartitionField) transformString() string {
	if f.TransformParam > 0 {
		return fmt.Sprintf("%s[%d]", f.Transform, f.TransformParam)
	}
	return f.Transform
}

// PartitionRecordSchemaAvro returns the Avro schema for the partition tuple in manifest entries.
func (ps *PartitionSpec) PartitionRecordSchemaAvro(ts *schema.TableSchema) string {
	if ps.IsUnpartitioned() {
		return `{"type": "record", "name": "r102", "fields": []}`
	}

	fields := make([]string, len(ps.Fields))
	for i, f := range ps.Fields {
		avroType := partitionFieldAvroType(f.Transform, ts, f.SourceID)
		fields[i] = fmt.Sprintf(`{"name": "%s", "type": ["null", "%s"], "default": null, "field-id": %d}`,
			f.Name, avroType, f.FieldID)
	}

	return fmt.Sprintf(`{"type": "record", "name": "r102", "fields": [%s]}`, strings.Join(fields, ", "))
}

// PartitionAvroValue returns the partition values formatted for Avro encoding in manifest entries.
func (ps *PartitionSpec) PartitionAvroValue(partValues map[string]any, ts *schema.TableSchema) map[string]any {
	if ps.IsUnpartitioned() || len(partValues) == 0 {
		return map[string]any{}
	}

	result := make(map[string]any, len(ps.Fields))
	for _, f := range ps.Fields {
		val := partValues[f.Name]
		if val == nil {
			result[f.Name] = nil
			continue
		}
		avroType := partitionFieldAvroType(f.Transform, ts, f.SourceID)
		result[f.Name] = goavroUnion(avroType, val)
	}
	return result
}

// ParsePartitionPath reverses a partition path like "seq_truncate=0" back into
// partition values. Used when the original row is not available (e.g. DELETE
// events resolved from the file index).
func (ps *PartitionSpec) ParsePartitionPath(path string, ts *schema.TableSchema) map[string]any {
	values := make(map[string]any, len(ps.Fields))
	parts := strings.Split(path, "/")
	for _, part := range parts {
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		name := part[:eqIdx]
		valStr := part[eqIdx+1:]
		for _, f := range ps.Fields {
			if f.Name != name {
				continue
			}
			if valStr == "__null__" {
				values[name] = nil
			} else {
				avroType := partitionFieldAvroType(f.Transform, ts, f.SourceID)
				switch avroType {
				case "int":
					n, _ := strconv.ParseInt(valStr, 10, 32)
					values[name] = int32(n)
				case "long":
					n, _ := strconv.ParseInt(valStr, 10, 64)
					values[name] = n
				default:
					values[name] = valStr
				}
			}
		}
	}
	return values
}

// --- helpers ---

func findColumn(ts *schema.TableSchema, name string) *schema.Column {
	for i := range ts.Columns {
		if ts.Columns[i].Name == name {
			return &ts.Columns[i]
		}
	}
	return nil
}

func findColumnByID(ts *schema.TableSchema, fieldID int) *schema.Column {
	for i := range ts.Columns {
		if ts.Columns[i].FieldID == fieldID {
			return &ts.Columns[i]
		}
	}
	return nil
}

func isTemporalType(pgType string) bool {
	switch strings.ToLower(pgType) {
	case "date", "timestamp", "timestamp without time zone",
		"timestamptz", "timestamp with time zone":
		return true
	}
	return false
}

func isBucketableType(pgType string) bool {
	switch strings.ToLower(pgType) {
	case "integer", "int", "int4", "serial", "smallint", "int2",
		"bigint", "int8", "bigserial",
		"numeric", "decimal",
		"date",
		"timestamp", "timestamp without time zone",
		"timestamptz", "timestamp with time zone",
		"text", "varchar", "character varying", "char", "character", "name",
		"uuid", "bytea":
		return true
	}
	return false
}

func isTruncatableType(pgType string) bool {
	switch strings.ToLower(pgType) {
	case "integer", "int", "int4", "serial", "smallint", "int2",
		"bigint", "int8", "bigserial",
		"numeric", "decimal",
		"text", "varchar", "character varying", "char", "character", "name",
		"bytea":
		return true
	}
	return false
}


// applyTransform applies a partition transform to a value.
func applyTransform(transform string, param int, value any, pgType string) any {
	switch transform {
	case "identity":
		return value
	case "year":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		return int32(t.Year() - 1970)
	case "month":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		return int32((t.Year()-1970)*12 + int(t.Month()) - 1)
	case "day":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		return int32(t.Sub(epoch).Hours() / 24)
	case "hour":
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		return int32(t.Sub(epoch).Hours())
	case "bucket":
		return applyBucketTransform(param, value, pgType)
	case "truncate":
		return applyTruncateTransform(param, value, pgType)
	default:
		return value
	}
}

// applyBucketTransform computes bucket[N] using Murmur3 x86 32-bit hash (seed 0).
// Per Iceberg spec: (murmur3_x86_32_hash(bytes) & Integer.MAX_VALUE) % N
func applyBucketTransform(n int, value any, pgType string) int32 {
	b := valueToHashBytes(value, pgType)
	if b == nil {
		return 0
	}
	h := murmur3.Sum32(b)
	return int32(h&math.MaxInt32) % int32(n)
}

// valueToHashBytes converts a value to its byte representation for Murmur3 hashing,
// following the Iceberg spec's hash requirements per type.
func valueToHashBytes(value any, pgType string) []byte {
	lowerType := strings.ToLower(pgType)

	switch lowerType {
	case "integer", "int", "int4", "serial":
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(toInt32(value)))
		return buf
	case "bigint", "int8", "bigserial":
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(toInt64(value)))
		return buf
	case "smallint", "int2":
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(toInt32(value)))
		return buf
	case "date":
		// Hash as days since epoch (int32 LE).
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		days := int32(t.Sub(epoch).Hours() / 24)
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(days))
		return buf
	case "timestamp", "timestamp without time zone",
		"timestamptz", "timestamp with time zone":
		// Hash as microseconds since epoch (int64 LE).
		t := toTime(value, pgType)
		if t.IsZero() {
			return nil
		}
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		micros := t.Sub(epoch).Microseconds()
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(micros))
		return buf
	case "text", "varchar", "character varying", "char", "character", "name":
		return []byte(toString(value))
	case "uuid":
		return []byte(toString(value))
	case "bytea":
		if b, ok := value.([]byte); ok {
			return b
		}
		return []byte(toString(value))
	case "numeric", "decimal":
		return decimalToHashBytes(value)
	default:
		// Fall back to string hash.
		return []byte(toString(value))
	}
}

// decimalToHashBytes converts a decimal value to its unscaled 2's complement big-endian bytes
// for Murmur3 hashing per the Iceberg spec.
func decimalToHashBytes(value any) []byte {
	s := toString(value)
	// Parse as big.Float to handle arbitrary precision.
	f, _, err := new(big.Float).Parse(s, 10)
	if err != nil {
		return []byte(s)
	}
	// Determine scale from decimal places.
	parts := strings.Split(s, ".")
	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
	}
	// Compute unscaled value: value * 10^scale.
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	unscaled, _ := new(big.Float).Mul(f, new(big.Float).SetInt(multiplier)).Int(nil)
	return unscaled.Bytes()
}

// applyTruncateTransform computes truncate[W].
func applyTruncateTransform(w int, value any, pgType string) any {
	lowerType := strings.ToLower(pgType)

	switch lowerType {
	case "integer", "int", "int4", "serial", "smallint", "int2":
		v := toInt32(value)
		return truncateInt32(v, int32(w))
	case "bigint", "int8", "bigserial":
		v := toInt64(value)
		return truncateInt64(v, int64(w))
	case "numeric", "decimal":
		s := toString(value)
		return truncateDecimal(s, w)
	case "bytea":
		return truncateBytes(value, w)
	case "text", "varchar", "character varying", "char", "character", "name":
		s := toString(value)
		return truncateString(s, w)
	default:
		// For other types, fall back to string truncation.
		s := toString(value)
		return truncateString(s, w)
	}
}

// truncateInt32 computes v - (v % W) with positive remainder per Iceberg spec.
func truncateInt32(v, w int32) int32 {
	rem := v % w
	if rem < 0 {
		rem += w
	}
	return v - rem
}

// truncateInt64 computes v - (v % W) with positive remainder per Iceberg spec.
func truncateInt64(v, w int64) int64 {
	rem := v % w
	if rem < 0 {
		rem += w
	}
	return v - rem
}

// truncateString returns the first W UTF-8 code points of s.
func truncateString(s string, w int) string {
	count := 0
	for i := range s {
		if count == w {
			return s[:i]
		}
		count++
	}
	_ = utf8.RuneCountInString // ensure import
	return s
}

// truncateBytes truncates binary data to the first W bytes.
// Values from WAL arrive as Postgres hex strings (e.g., "\\x0102ab") or raw []byte.
// Returns URL-encoded base64 for use in partition paths per Iceberg spec.
func truncateBytes(value any, w int) string {
	var raw []byte
	switch v := value.(type) {
	case []byte:
		raw = v
	case string:
		// Postgres hex format: \x0102ab
		s := v
		if strings.HasPrefix(s, "\\x") {
			s = s[2:]
		} else if strings.HasPrefix(s, "\\\\x") {
			s = s[3:]
		}
		var err error
		raw, err = hex.DecodeString(s)
		if err != nil {
			// Not valid hex; treat as raw bytes.
			raw = []byte(v)
		}
	default:
		raw = []byte(toString(value))
	}

	if w < len(raw) {
		raw = raw[:w]
	}
	b64 := base64.StdEncoding.EncodeToString(raw)
	// URL-encode characters that conflict with partition path syntax.
	b64 = strings.ReplaceAll(b64, "/", "%2F")
	b64 = strings.ReplaceAll(b64, "+", "%2B")
	b64 = strings.ReplaceAll(b64, "=", "%3D")
	return b64
}

// truncateDecimal truncates a decimal string per the Iceberg spec.
// scaled_W = decimal(W, scale(v)), result = v - (v % scaled_W) with positive remainder.
// Example: W=50, "10.65" (scale=2) → scaled_W=0.50, result="10.50"
func truncateDecimal(s string, w int) string {
	// Determine scale from decimal places.
	parts := strings.Split(s, ".")
	scale := 0
	if len(parts) == 2 {
		scale = len(parts[1])
	}

	// Work in unscaled integer space to avoid floating-point errors.
	// Parse the unscaled value (e.g., "10.65" → 1065).
	unscaledStr := strings.Replace(s, ".", "", 1)
	unscaledStr = strings.TrimLeft(unscaledStr, "0")
	if unscaledStr == "" || unscaledStr == "-" {
		// Value is zero.
		if scale > 0 {
			return "0." + strings.Repeat("0", scale)
		}
		return "0"
	}

	negative := strings.HasPrefix(s, "-")
	if negative {
		unscaledStr = strings.TrimPrefix(unscaledStr, "-")
	}

	unscaled, ok := new(big.Int).SetString(unscaledStr, 10)
	if !ok {
		return s // fallback
	}
	if negative {
		unscaled.Neg(unscaled)
	}

	// W is the width in unscaled units (e.g., W=50 with scale=2 means 0.50).
	bigW := big.NewInt(int64(w))

	// Compute v - (v % W) with positive remainder.
	rem := new(big.Int).Mod(unscaled, bigW)
	if rem.Sign() < 0 {
		rem.Add(rem, bigW)
	}
	result := new(big.Int).Sub(unscaled, rem)

	// Format back to decimal string with original scale.
	return formatUnscaled(result, scale)
}

// formatUnscaled converts an unscaled big.Int back to a decimal string with the given scale.
func formatUnscaled(unscaled *big.Int, scale int) string {
	if scale == 0 {
		return unscaled.String()
	}

	negative := unscaled.Sign() < 0
	abs := new(big.Int).Abs(unscaled)
	s := abs.String()

	// Pad with leading zeros if needed.
	for len(s) <= scale {
		s = "0" + s
	}

	intPart := s[:len(s)-scale]
	fracPart := s[len(s)-scale:]

	result := intPart + "." + fracPart
	if negative {
		result = "-" + result
	}
	return result
}

// toTime converts a value to time.Time for partition transforms.
// Uses fastParseTimestamp (microseconds) to avoid time.Parse overhead.
// Also handles int64 (microseconds since epoch) and int32 (days since epoch)
// for values roundtripped through parquet.
func toTime(v any, pgType string) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case int64:
		// Microseconds since epoch (from parquet timestamp roundtrip).
		return time.Unix(x/1_000_000, (x%1_000_000)*1000).UTC()
	case int32:
		// Days since epoch (from parquet date roundtrip).
		epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		return epoch.AddDate(0, 0, int(x))
	case string:
		if strings.ToLower(pgType) == "date" {
			if len(x) >= 10 && x[4] == '-' && x[7] == '-' {
				year := atoi4(x[0:4])
				month := atoi2(x[5:7])
				day := atoi2(x[8:10])
				if year >= 0 && month >= 1 && month <= 12 && day >= 1 && day <= 31 {
					return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
				}
			}
			return time.Time{}
		}
		us, ok := fastParseTimestamp(x)
		if ok {
			return time.Unix(us/1_000_000, (us%1_000_000)*1000)
		}
	}
	return time.Time{}
}

// partitionFieldAvroType returns the Avro type for a partition field's transformed value.
func partitionFieldAvroType(transform string, ts *schema.TableSchema, sourceID int) string {
	switch transform {
	case "year", "month", "day", "hour", "bucket":
		return "int"
	case "truncate":
		col := findColumnByID(ts, sourceID)
		if col == nil {
			return "string"
		}
		return icebergToAvroType(schema.IcebergType(col.PGType))
	case "identity":
		col := findColumnByID(ts, sourceID)
		if col == nil {
			return "string"
		}
		return icebergToAvroType(schema.IcebergType(col.PGType))
	default:
		return "string"
	}
}

// icebergToAvroType maps Iceberg primitive types to Avro types.
func icebergToAvroType(icebergType string) string {
	switch icebergType {
	case "int":
		return "int"
	case "long":
		return "long"
	case "float":
		return "float"
	case "double":
		return "double"
	case "boolean":
		return "boolean"
	case "date":
		return "int"
	case "timestamp", "timestamptz":
		return "long"
	default:
		return "string"
	}
}

// goavroUnion wraps a Go value in a goavro union map.
func goavroUnion(avroType string, val any) map[string]any {
	// Ensure the Go type matches what goavro expects.
	switch avroType {
	case "int":
		return map[string]any{"int": toAvroInt32Value(val)}
	case "long":
		return map[string]any{"long": toAvroInt64Value(val)}
	case "float":
		return map[string]any{"float": toAvroFloat32Value(val)}
	case "double":
		return map[string]any{"double": toAvroFloat64Value(val)}
	case "boolean":
		return map[string]any{"boolean": toBool(val)}
	default:
		return map[string]any{"string": toString(val)}
	}
}

func toAvroInt32Value(v any) int32 {
	switch x := v.(type) {
	case int32:
		return x
	case int:
		return int32(x)
	case int64:
		return int32(x)
	case float64:
		return int32(x)
	default:
		return 0
	}
}

func toAvroInt64Value(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

func toAvroFloat32Value(v any) float32 {
	switch x := v.(type) {
	case float32:
		return x
	case float64:
		return float32(x)
	default:
		return 0
	}
}

func toAvroFloat64Value(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	default:
		return 0
	}
}
