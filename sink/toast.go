package sink

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
)

// resolveToast fetches missing TOAST column values from PostgreSQL
// for all pending rows in a single batched query per table.
func (s *Sink) resolveToast(ctx context.Context, pgTable string, ts *tableSink) error {
	if len(ts.toastPending) == 0 {
		return nil
	}

	pk := ts.schema.PK
	if len(pk) == 0 {
		log.Printf("[sink] TOAST: skipping resolution for %s (no primary key)", pgTable)
		return nil
	}

	// Collect the set of columns we need to fetch.
	colSet := make(map[string]struct{})
	for _, p := range ts.toastPending {
		for _, col := range p.unchangedCols {
			colSet[col] = struct{}{}
		}
	}

	// Build SELECT column list: PK columns (as-is for WHERE matching) + unchanged columns cast to text.
	// Casting to text ensures we get the same string representation as the WAL text protocol.
	var selectCols []string
	for _, col := range pk {
		selectCols = append(selectCols, quoteIdent(col)+"::text")
	}
	for col := range colSet {
		isPK := false
		for _, p := range pk {
			if p == col {
				isPK = true
				break
			}
		}
		if !isPK {
			selectCols = append(selectCols, quoteIdent(col)+"::text")
		}
	}

	// Collect unique PK values to avoid duplicate lookups.
	// Use null byte as separator to avoid collisions with real data.
	seen := make(map[string]struct{})
	var pkValues [][]any

	for _, p := range ts.toastPending {
		key := buildPKKey(p.row, pk)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		vals := make([]any, len(pk))
		for i, col := range pk {
			vals[i] = p.row[col]
		}
		pkValues = append(pkValues, vals)
	}

	if len(pkValues) == 0 {
		return nil
	}

	// Build query and args.
	query, args := buildToastQuery(pgTable, selectCols, pk, pkValues)

	// Connect to source PostgreSQL.
	conn, err := pgx.Connect(ctx, s.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("connect to source for TOAST lookup: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("TOAST lookup query: %w", err)
	}
	defer rows.Close()

	// Build a map of PK -> fetched column values.
	fetched := make(map[string]map[string]*string)

	fieldDescs := rows.FieldDescriptions()
	for rows.Next() {
		// Scan all columns as *string to handle NULLs properly.
		ptrs := make([]*string, len(fieldDescs))
		scanArgs := make([]any, len(fieldDescs))
		for i := range ptrs {
			scanArgs[i] = &ptrs[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scan TOAST row: %w", err)
		}

		row := make(map[string]*string, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = ptrs[i]
		}

		// Build PK key from the fetched text values.
		pkVals := make(map[string]any, len(pk))
		for _, col := range pk {
			if row[col] != nil {
				pkVals[col] = *row[col]
			}
		}
		key := buildPKKey(pkVals, pk)
		fetched[key] = row
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("TOAST lookup rows: %w", err)
	}

	// Patch the pending rows with fetched values.
	resolved := 0
	for _, p := range ts.toastPending {
		key := buildPKKey(p.row, pk)
		fr, ok := fetched[key]
		if !ok {
			// Row was deleted between WAL event and lookup — leave as nil.
			// The subsequent DELETE event will cancel this row anyway.
			continue
		}
		for _, col := range p.unchangedCols {
			val, exists := fr[col]
			if !exists {
				continue
			}
			if val == nil {
				p.row[col] = nil // preserve real NULLs
			} else {
				p.row[col] = *val
			}
		}
		resolved++
	}

	log.Printf("[sink] TOAST: resolved %d/%d rows for %s (%d unique PKs queried)",
		resolved, len(ts.toastPending), pgTable, len(seen))

	return nil
}

// buildPKKey creates a unique key for a row based on its PK columns.
// Uses null byte as separator to avoid collisions.
func buildPKKey(row map[string]any, pk []string) string {
	parts := make([]string, len(pk))
	for i, col := range pk {
		parts[i] = fmt.Sprintf("%v", row[col])
	}
	return strings.Join(parts, "\x00")
}

// buildToastQuery constructs the SELECT query and args for TOAST lookups.
func buildToastQuery(pgTable string, selectCols []string, pk []string, pkValues [][]any) (string, []any) {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(strings.Join(selectCols, ", "))
	query.WriteString(" FROM ")
	query.WriteString(pgTable)

	if len(pk) == 1 {
		query.WriteString(" WHERE ")
		query.WriteString(quoteIdent(pk[0]))
		query.WriteString(" IN (")
		args := make([]any, len(pkValues))
		for i, vals := range pkValues {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(fmt.Sprintf("$%d", i+1))
			args[i] = vals[0]
		}
		query.WriteString(")")
		return query.String(), args
	}

	// Composite PK: WHERE (pk1, pk2) IN (($1,$2), ($3,$4), ...)
	query.WriteString(" WHERE (")
	for i, col := range pk {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(quoteIdent(col))
	}
	query.WriteString(") IN (")
	var args []any
	argIdx := 1
	for i, vals := range pkValues {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString("(")
		for j, v := range vals {
			if j > 0 {
				query.WriteString(", ")
			}
			query.WriteString(fmt.Sprintf("$%d", argIdx))
			args = append(args, v)
			argIdx++
		}
		query.WriteString(")")
	}
	query.WriteString(")")
	return query.String(), args
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
