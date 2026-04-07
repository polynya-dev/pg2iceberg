package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	pq "github.com/parquet-go/parquet-go"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/utils"
)

// DownloadWithRetry wraps an S3 download with exponential backoff for transient errors.
func DownloadWithRetry(ctx context.Context, s3 ObjectStorage, key string) ([]byte, error) {
	var data []byte
	err := utils.Do(ctx, 3, 100*time.Millisecond, 5*time.Second, func() error {
		var err error
		data, err = s3.Download(ctx, key)
		return err
	})
	return data, err
}

// ReadParquetRows reads a parquet file and returns rows as maps.
func ReadParquetRows(data []byte, ts *postgres.TableSchema) ([]map[string]any, error) {
	reader := pq.NewReader(bytes.NewReader(data))

	schema := reader.Schema()
	fields := schema.Fields()
	colNames := make([]string, len(fields))
	for i, f := range fields {
		colNames[i] = f.Name()
	}

	var rows []map[string]any
	rowBuf := make([]pq.Row, 1)
	for {
		n, err := reader.ReadRows(rowBuf)
		if n == 0 {
			break
		}
		if err != nil && n == 0 {
			return nil, fmt.Errorf("read row: %w", err)
		}

		row := make(map[string]any, len(colNames))
		for _, v := range rowBuf[0] {
			colIdx := v.Column()
			if colIdx >= 0 && colIdx < len(colNames) {
				row[colNames[colIdx]] = parquetValueWithField(v, fields[colIdx])
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// parquetValueToGo converts a parquet.Value to a Go native type.
// For FixedLenByteArray this returns raw bytes as string — use
// parquetValueWithSchema for decimal-aware conversion.
func parquetValueToGo(v pq.Value) any {
	switch v.Kind() {
	case pq.Boolean:
		return v.Boolean()
	case pq.Int32:
		return v.Int32()
	case pq.Int64:
		return v.Int64()
	case pq.Float:
		return v.Float()
	case pq.Double:
		return v.Double()
	case pq.ByteArray, pq.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

// parquetValueWithField converts a parquet Value using its schema field
// to properly decode types like decimals stored as FixedLenByteArray.
func parquetValueWithField(v pq.Value, field pq.Field) any {
	if v.IsNull() {
		return nil
	}
	// Check for decimal logical type on FixedLenByteArray.
	if v.Kind() == pq.FixedLenByteArray || v.Kind() == pq.ByteArray {
		if lt := field.Type().LogicalType(); lt != nil && lt.Decimal != nil {
			return decimalFromBytes(v.ByteArray(), int(lt.Decimal.Scale))
		}
		return string(v.ByteArray())
	}
	return parquetValueToGo(v)
}

// decimalFromBytes decodes a big-endian two's complement byte array into
// a decimal string with the given scale.
func decimalFromBytes(b []byte, scale int) string {
	// Big-endian two's complement → big.Int.
	n := new(big.Int).SetBytes(b)
	// Check sign bit.
	if len(b) > 0 && b[0]&0x80 != 0 {
		// Negative: subtract 2^(8*len) to get the signed value.
		twoN := new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8))
		n.Sub(n, twoN)
	}
	if scale <= 0 {
		return n.String()
	}
	neg := n.Sign() < 0
	abs := new(big.Int).Abs(n)
	s := abs.String()
	// Pad with leading zeros if needed.
	for len(s) <= scale {
		s = "0" + s
	}
	intPart := s[:len(s)-scale]
	fracPart := s[len(s)-scale:]
	result := intPart + "." + fracPart
	if neg {
		result = "-" + result
	}
	return result
}

// ReadParquetPKKeysFromReaderAt reads only PK columns from a parquet file via
// an io.ReaderAt (backed by S3 range reads). Only the footer and PK column
// chunks are fetched — non-PK column data is never read from storage.
func ReadParquetPKKeysFromReaderAt(r io.ReaderAt, size int64, pk []string) ([]string, error) {
	file, err := pq.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	pqSchema := file.Schema()

	// Find PK column indices in the parquet postgres.
	pkColIdx := make(map[string]int, len(pk))
	for i, f := range pqSchema.Fields() {
		for _, p := range pk {
			if f.Name() == p {
				pkColIdx[p] = i
			}
		}
	}
	if len(pkColIdx) != len(pk) {
		return nil, fmt.Errorf("parquet schema missing PK columns: have %v, want %v", pkColIdx, pk)
	}

	var keys []string

	for _, rg := range file.RowGroups() {
		numRows := rg.NumRows()

		// Read values from each PK column chunk.
		pkValues := make(map[string][]string, len(pk))
		for _, p := range pk {
			idx := pkColIdx[p]
			chunk := rg.ColumnChunks()[idx]
			pages := chunk.Pages()

			vals := make([]string, 0, numRows)
			for {
				page, err := pages.ReadPage()
				if page == nil || err != nil {
					break
				}
				pageValues := page.Values()
				vBuf := make([]pq.Value, page.NumValues())
				n, err := pageValues.ReadValues(vBuf)
				if err != nil && err != io.EOF {
					pages.Close()
					return nil, fmt.Errorf("read PK values from %s column: %w", p, err)
				}
				for i := 0; i < n; i++ {
					if vBuf[i].IsNull() {
						vals = append(vals, "<nil>")
					} else {
						vals = append(vals, fmt.Sprintf("%v", parquetValueToGo(vBuf[i])))
					}
				}
			}
			pages.Close()
			pkValues[p] = vals
		}

		// Build PK key strings.
		nRows := int(numRows)
		if len(pk) == 1 {
			keys = append(keys, pkValues[pk[0]]...)
		} else {
			for i := 0; i < nRows; i++ {
				var b strings.Builder
				for j, col := range pk {
					if j > 0 {
						b.WriteByte(0)
					}
					if i < len(pkValues[col]) {
						b.WriteString(pkValues[col][i])
					}
				}
				keys = append(keys, b.String())
			}
		}
	}

	return keys, nil
}

// BuildPKKey creates a unique key for a row based on its PK columns.
// Uses null byte as separator to avoid collisions.
func BuildPKKey(row map[string]any, pk []string) string {
	if len(pk) == 1 {
		return AnyToString(row[pk[0]])
	}
	var b strings.Builder
	for i, col := range pk {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(AnyToString(row[col]))
	}
	return b.String()
}

// AnyToString converts a value to string without reflection for common PK types.
func AnyToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.Itoa(x)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case []byte:
		return string(x)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
