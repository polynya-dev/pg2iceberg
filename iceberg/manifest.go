package iceberg

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/linkedin/goavro/v2"
)

// DataFileInfo describes a Parquet file written to S3.
type DataFileInfo struct {
	Path       string
	FileSizeBytes int64
	RecordCount   int64
	// Content: 0 = data, 1 = position deletes, 2 = equality deletes
	Content int
	// EqualityFieldIDs is set for equality delete files.
	EqualityFieldIDs []int
	// PartitionValues holds the Avro-encoded partition values for this file.
	// Empty map for unpartitioned tables.
	PartitionValues map[string]any
}

// ManifestEntry represents a single entry in a manifest file.
type ManifestEntry struct {
	Status         int // 1 = added, 0 = existing, 2 = deleted
	SnapshotID     int64
	SequenceNumber int64 // file's sequence number (for equality delete ordering)
	DataFile       DataFileInfo
}

// ManifestFileInfo describes a manifest file.
type ManifestFileInfo struct {
	Path            string
	Length          int64
	Content         int // 0 = data, 1 = deletes
	SnapshotID      int64
	AddedFiles      int
	AddedRows       int64
	SequenceNumber  int64
}

// manifestEntrySchema returns the Avro schema for manifest entries.
func manifestEntrySchema(ts *postgres.TableSchema, partSpec *PartitionSpec) string {
	partSchema := `{"type": "record", "name": "r102", "fields": []}`
	if partSpec != nil {
		partSchema = partSpec.PartitionRecordSchemaAvro(ts)
	}

	return fmt.Sprintf(`{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int", "field-id": 0},
    {"name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1},
    {"name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3},
    {"name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content", "type": "int", "field-id": 134},
        {"name": "file_path", "type": "string", "field-id": 100},
        {"name": "file_format", "type": "string", "field-id": 101},
        {"name": "partition", "type": %s, "field-id": 102},
        {"name": "record_count", "type": "long", "field-id": 103},
        {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
        {"name": "column_sizes", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}, "logicalType": "map"}], "default": null, "field-id": 108},
        {"name": "value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k119_v120", "fields": [{"name": "key", "type": "int", "field-id": 119}, {"name": "value", "type": "long", "field-id": 120}]}, "logicalType": "map"}], "default": null, "field-id": 109},
        {"name": "null_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k121_v122", "fields": [{"name": "key", "type": "int", "field-id": 121}, {"name": "value", "type": "long", "field-id": 122}]}, "logicalType": "map"}], "default": null, "field-id": 110},
        {"name": "nan_value_counts", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k138_v139", "fields": [{"name": "key", "type": "int", "field-id": 138}, {"name": "value", "type": "long", "field-id": 139}]}, "logicalType": "map"}], "default": null, "field-id": 137},
        {"name": "lower_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k126_v127", "fields": [{"name": "key", "type": "int", "field-id": 126}, {"name": "value", "type": "bytes", "field-id": 127}]}, "logicalType": "map"}], "default": null, "field-id": 125},
        {"name": "upper_bounds", "type": ["null", {"type": "array", "items": {"type": "record", "name": "k129_v130", "fields": [{"name": "key", "type": "int", "field-id": 129}, {"name": "value", "type": "bytes", "field-id": 130}]}, "logicalType": "map"}], "default": null, "field-id": 128},
        {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 131},
        {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132},
        {"name": "equality_ids", "type": ["null", {"type": "array", "items": "int", "element-id": 136}], "default": null, "field-id": 135},
        {"name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140}
      ]
    }, "field-id": 2}
  ]
}`, partSchema)
}

// manifestListSchema is the Avro schema for Iceberg V2 manifest list files.
// Per the V2 spec, added_snapshot_id and all count fields are required (non-nullable).
const manifestListSchema = `{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string", "field-id": 500},
    {"name": "manifest_length", "type": "long", "field-id": 501},
    {"name": "partition_spec_id", "type": "int", "field-id": 502},
    {"name": "content", "type": "int", "field-id": 517},
    {"name": "sequence_number", "type": "long", "default": 0, "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "default": 0, "field-id": 516},
    {"name": "added_snapshot_id", "type": "long", "field-id": 503},
    {"name": "added_files_count", "type": "int", "field-id": 504},
    {"name": "existing_files_count", "type": "int", "field-id": 505},
    {"name": "deleted_files_count", "type": "int", "field-id": 506},
    {"name": "added_rows_count", "type": "long", "field-id": 512},
    {"name": "existing_rows_count", "type": "long", "field-id": 513},
    {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    {"name": "partitions", "type": ["null", {"type": "array", "items": {"type": "record", "name": "r508", "fields": [{"name": "contains_null", "type": "boolean", "field-id": 509}, {"name": "contains_nan", "type": ["null", "boolean"], "default": null, "field-id": 518}, {"name": "lower_bound", "type": ["null", "bytes"], "default": null, "field-id": 510}, {"name": "upper_bound", "type": ["null", "bytes"], "default": null, "field-id": 511}]}, "element-id": 508}], "default": null, "field-id": 507},
    {"name": "key_metadata", "type": ["null", "bytes"], "default": null, "field-id": 519}
  ]
}`

// WriteManifest writes a manifest Avro file containing the given entries.
func WriteManifest(ts *postgres.TableSchema, entries []ManifestEntry, seqNum int64, content int, partSpec *PartitionSpec) ([]byte, error) {
	schemaJSON := manifestEntrySchema(ts, partSpec)
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("manifest codec: %w", err)
	}

	icebergSchemaJSON := IcebergSchemaJSONString(ts)

	// Build partition-spec metadata JSON.
	partSpecJSON := "[]"
	if partSpec != nil && !partSpec.IsUnpartitioned() {
		partSpecJSON = partSpec.PartitionSpecJSON()
	}

	ocfMeta := map[string][]byte{
		"schema":            []byte(icebergSchemaJSON),
		"schema-id":         []byte("0"),
		"partition-spec":    []byte(partSpecJSON),
		"partition-spec-id": []byte("0"),
		"format-version":    []byte("2"),
		"content":           []byte(contentStr(content)),
	}

	records := make([]any, len(entries))
	for i, e := range entries {
		partValues := map[string]any{}
		if e.DataFile.PartitionValues != nil {
			partValues = e.DataFile.PartitionValues
		}

		df := map[string]any{
			"content":            int32(e.DataFile.Content),
			"file_path":          e.DataFile.Path,
			"file_format":        "PARQUET",
			"partition":          partValues,
			"record_count":       e.DataFile.RecordCount,
			"file_size_in_bytes": e.DataFile.FileSizeBytes,
			"column_sizes":       nil,
			"value_counts":       nil,
			"null_value_counts":  nil,
			"nan_value_counts":   nil,
			"lower_bounds":       nil,
			"upper_bounds":       nil,
			"key_metadata":       nil,
			"split_offsets":      nil,
			"sort_order_id":      nil,
		}

		// Set equality_ids for delete files
		if e.DataFile.Content == 2 && len(e.DataFile.EqualityFieldIDs) > 0 {
			eqIDs := make([]any, len(e.DataFile.EqualityFieldIDs))
			for j, id := range e.DataFile.EqualityFieldIDs {
				eqIDs[j] = int32(id)
			}
			df["equality_ids"] = goavro.Union("array", eqIDs)
		} else {
			df["equality_ids"] = nil
		}

		records[i] = map[string]any{
			"status":               int32(e.Status),
			"snapshot_id":          goavro.Union("long", e.SnapshotID),
			"sequence_number":      goavro.Union("long", seqNum),
			"file_sequence_number": goavro.Union("long", seqNum),
			"data_file":            df,
		}
	}

	buf, err := encodeOCF(codec, records, ocfMeta)
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	return buf, nil
}

// WriteManifestList writes a manifest list Avro file.
func WriteManifestList(manifests []ManifestFileInfo) ([]byte, error) {
	codec, err := goavro.NewCodec(manifestListSchema)
	if err != nil {
		return nil, fmt.Errorf("manifest list codec: %w", err)
	}

	records := make([]any, len(manifests))
	for i, m := range manifests {
		records[i] = map[string]any{
			"manifest_path":       m.Path,
			"manifest_length":     m.Length,
			"partition_spec_id":   int32(0),
			"content":             int32(m.Content),
			"sequence_number":     m.SequenceNumber,
			"min_sequence_number": m.SequenceNumber,
			"added_snapshot_id":   m.SnapshotID,
			"added_files_count":   int32(m.AddedFiles),
			"existing_files_count": int32(0),
			"deleted_files_count":  int32(0),
			"added_rows_count":    m.AddedRows,
			"existing_rows_count": int64(0),
			"deleted_rows_count":  int64(0),
			"partitions":          nil,
			"key_metadata":        nil,
		}
	}

	meta := map[string][]byte{
		"format-version": []byte("2"),
	}

	buf, err := encodeOCF(codec, records, meta)
	if err != nil {
		return nil, fmt.Errorf("encode manifest list: %w", err)
	}
	return buf, nil
}

// ReadManifestList reads a manifest list Avro file and returns ManifestFileInfo entries.
func ReadManifestList(data []byte) ([]ManifestFileInfo, error) {
	reader, err := goavro.NewOCFReader(ocfReader(data))
	if err != nil {
		return nil, fmt.Errorf("open manifest list: %w", err)
	}

	var manifests []ManifestFileInfo
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("read manifest list entry: %w", err)
		}
		m, ok := record.(map[string]any)
		if !ok {
			continue
		}
		mfi := ManifestFileInfo{
			Path:           getString(m, "manifest_path"),
			Length:         getInt64(m, "manifest_length"),
			Content:        int(getInt32(m, "content")),
			SequenceNumber: getInt64(m, "sequence_number"),
			SnapshotID:     getInt64(m, "added_snapshot_id"),
			AddedFiles:     int(getInt32(m, "added_files_count")),
			AddedRows:      getInt64(m, "added_rows_count"),
		}
		manifests = append(manifests, mfi)
	}

	return manifests, nil
}

// ReadManifest reads a manifest Avro file and returns ManifestEntry entries.
func ReadManifest(data []byte) ([]ManifestEntry, error) {
	reader, err := goavro.NewOCFReader(ocfReader(data))
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}

	var entries []ManifestEntry
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("read manifest entry: %w", err)
		}
		m, ok := record.(map[string]any)
		if !ok {
			continue
		}

		entry := ManifestEntry{
			Status: int(getInt32(m, "status")),
		}
		if sid := getUnionLong(m, "snapshot_id"); sid != nil {
			entry.SnapshotID = *sid
		}
		if seq := getUnionLong(m, "sequence_number"); seq != nil {
			entry.SequenceNumber = *seq
		}

		if df, ok := m["data_file"].(map[string]any); ok {
			entry.DataFile = DataFileInfo{
				Content:       int(getInt32(df, "content")),
				Path:          getString(df, "file_path"),
				RecordCount:   getInt64(df, "record_count"),
				FileSizeBytes: getInt64(df, "file_size_in_bytes"),
			}

			// Read partition values for file filtering.
			if partRaw, ok := df["partition"]; ok && partRaw != nil {
				if partMap, ok := partRaw.(map[string]any); ok && len(partMap) > 0 {
					entry.DataFile.PartitionValues = partMap
				}
			}

			// Read equality_ids
			if eqRaw, ok := df["equality_ids"]; ok && eqRaw != nil {
				if um, ok := eqRaw.(map[string]any); ok {
					if arr, ok := um["array"].([]any); ok {
						for _, v := range arr {
							entry.DataFile.EqualityFieldIDs = append(entry.DataFile.EqualityFieldIDs, int(toAvroInt32(v)))
						}
					}
				}
			}
		}

		entries = append(entries, entry)
	}
	return entries, nil
}

func encodeOCF(codec *goavro.Codec, records []any, metadata map[string][]byte) ([]byte, error) {
	var b bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               &b,
		Codec:           codec,
		MetaData:  metadata,
	})
	if err != nil {
		return nil, err
	}
	if err := writer.Append(records); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// IcebergSchemaJSONString builds a JSON string of the Iceberg schema for Avro metadata.
func IcebergSchemaJSONString(ts *postgres.TableSchema) string {
	fields := ""
	for i, col := range ts.Columns {
		if i > 0 {
			fields += ","
		}
		iceType, _ := col.IcebergType()
		// Map high-level Iceberg types back to JSON type strings
		typeJSON := `"` + iceType + `"`
		fields += fmt.Sprintf(`{"id":%d,"name":"%s","required":%t,"type":%s}`,
			col.FieldID, col.Name, !col.IsNullable, typeJSON)
	}
	identifier := ""
	if pkIDs := ts.PKFieldIDs(); len(pkIDs) > 0 {
		parts := make([]string, len(pkIDs))
		for i, id := range pkIDs {
			parts[i] = fmt.Sprintf("%d", id)
		}
		identifier = fmt.Sprintf(`,"identifier-field-ids":[%s]`, strings.Join(parts, ","))
	}
	return fmt.Sprintf(`{"type":"struct","schema-id":0,"fields":[%s]%s}`, fields, identifier)
}

func contentStr(content int) string {
	if content == 1 {
		return "deletes"
	}
	return "data"
}

// --- Avro read helpers ---

func ocfReader(data []byte) io.Reader {
	return bytes.NewReader(data)
}

func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getInt32(m map[string]any, key string) int32 {
	if v, ok := m[key]; ok {
		switch x := v.(type) {
		case int32:
			return x
		case int:
			return int32(x)
		case int64:
			return int32(x)
		}
	}
	return 0
}

func getInt64(m map[string]any, key string) int64 {
	if v, ok := m[key]; ok {
		switch x := v.(type) {
		case int64:
			return x
		case int32:
			return int64(x)
		case int:
			return int64(x)
		}
	}
	return 0
}

func getUnionLong(m map[string]any, key string) *int64 {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	// goavro returns union values as map[string]any{"long": value}
	if um, ok := v.(map[string]any); ok {
		if lv, ok := um["long"]; ok {
			val := toAvroInt64(lv)
			return &val
		}
	}
	return nil
}

func getUnionInt(m map[string]any, key string) *int32 {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	if um, ok := v.(map[string]any); ok {
		if iv, ok := um["int"]; ok {
			val := toAvroInt32(iv)
			return &val
		}
	}
	return nil
}

func toAvroInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	default:
		return 0
	}
}

func toAvroInt32(v any) int32 {
	switch x := v.(type) {
	case int32:
		return x
	case int:
		return int32(x)
	case int64:
		return int32(x)
	default:
		return 0
	}
}
