package query

import (
	"github.com/pg2iceberg/pg2iceberg/iceberg"
)

// Buffer is an in-memory PK dedup buffer for query mode. When the same PK
// arrives multiple times between flushes, only the latest row is kept.
// All drained rows have Op="I" because query mode is an upsert-only stream.
type Buffer struct {
	pk       []string
	rows     map[string]map[string]any // PK key → latest row
	rowCount int
	byteEst  int64
}

// NewBuffer creates a new PK dedup buffer.
func NewBuffer(pk []string) *Buffer {
	return &Buffer{
		pk:   pk,
		rows: make(map[string]map[string]any),
	}
}

// Upsert adds or replaces a row in the buffer, keyed by PK.
func (b *Buffer) Upsert(row map[string]any) {
	key := iceberg.BuildPKKey(row, b.pk)
	if _, exists := b.rows[key]; !exists {
		b.rowCount++
	}
	b.rows[key] = row
	// Rough byte estimate: 128 bytes per row.
	b.byteEst = int64(b.rowCount) * 128
}

// Drain returns all buffered rows as RowStates (all Op="I") and clears the buffer.
func (b *Buffer) Drain() []iceberg.RowState {
	if b.rowCount == 0 {
		return nil
	}
	rows := make([]iceberg.RowState, 0, b.rowCount)
	for _, row := range b.rows {
		rows = append(rows, iceberg.RowState{
			Op:  "I",
			Row: row,
		})
	}
	b.rows = make(map[string]map[string]any)
	b.rowCount = 0
	b.byteEst = 0
	return rows
}

// Len returns the number of unique PKs buffered.
func (b *Buffer) Len() int {
	return b.rowCount
}

// EstimatedBytes returns a rough estimate of buffered data size.
func (b *Buffer) EstimatedBytes() int64 {
	return b.byteEst
}
