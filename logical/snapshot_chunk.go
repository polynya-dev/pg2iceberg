package logical

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ChunkRange describes a CTID page range for a single snapshot chunk.
type ChunkRange struct {
	Index     int   // chunk ordinal (0-based)
	StartPage int64 // inclusive
	EndPage   int64 // exclusive; -1 means unbounded (full table scan)
}

// ComputeChunks queries pg_class.relpages and divides the table into CTID
// page ranges. Returns a single unbounded chunk for empty/tiny tables.
func ComputeChunks(ctx context.Context, tx pgx.Tx, tableName string, chunkPages int) ([]ChunkRange, error) {
	if chunkPages <= 0 {
		chunkPages = 2048
	}

	// Parse schema.table into schema and table parts for pg_class lookup.
	parts := strings.SplitN(tableName, ".", 2)
	schema, table := "public", tableName
	if len(parts) == 2 {
		schema, table = parts[0], parts[1]
	}

	var relpages int64
	err := tx.QueryRow(ctx,
		"SELECT COALESCE(c.relpages, 0) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = $1 AND c.relname = $2",
		schema, table,
	).Scan(&relpages)
	if err != nil {
		return nil, fmt.Errorf("query relpages for %s: %w", tableName, err)
	}

	// For empty or tiny tables, return a single unbounded chunk.
	if relpages <= 0 {
		return []ChunkRange{{Index: 0, StartPage: 0, EndPage: -1}}, nil
	}

	var chunks []ChunkRange
	idx := 0
	for start := int64(0); start < relpages; start += int64(chunkPages) {
		end := start + int64(chunkPages)
		if end >= relpages {
			// Last chunk: use unbounded end to catch any pages beyond relpages
			// (relpages can be stale if ANALYZE hasn't run recently).
			end = -1
		}
		chunks = append(chunks, ChunkRange{Index: idx, StartPage: start, EndPage: end})
		idx++
	}

	return chunks, nil
}

// ChunkQuery returns the SQL query for a given chunk range.
func ChunkQuery(tableName string, chunk ChunkRange) string {
	sanitized := pgx.Identifier(strings.Split(tableName, ".")).Sanitize()

	if chunk.EndPage == -1 && chunk.StartPage == 0 {
		// Unbounded: full table scan.
		return fmt.Sprintf("SELECT * FROM %s", sanitized)
	}

	if chunk.EndPage == -1 {
		// Last chunk: no upper bound.
		return fmt.Sprintf("SELECT * FROM %s WHERE ctid >= '(%d,0)'::tid", sanitized, chunk.StartPage)
	}

	return fmt.Sprintf("SELECT * FROM %s WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid",
		sanitized, chunk.StartPage, chunk.EndPage)
}
