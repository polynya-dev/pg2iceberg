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

	// Collect the set of columns we need to fetch and the unique PKs.
	colSet := make(map[string]struct{})
	for _, p := range ts.toastPending {
		for _, col := range p.unchangedCols {
			colSet[col] = struct{}{}
		}
	}

	// Build SELECT column list: PK columns + unchanged columns.
	selectCols := make([]string, 0, len(pk)+len(colSet))
	for _, col := range pk {
		selectCols = append(selectCols, quoteIdent(col))
	}
	for col := range colSet {
		// Avoid duplicates if a PK column is somehow unchanged (unlikely but safe).
		isPK := false
		for _, p := range pk {
			if p == col {
				isPK = true
				break
			}
		}
		if !isPK {
			selectCols = append(selectCols, quoteIdent(col))
		}
	}

	// Build WHERE pk IN (...) clause.
	// Collect unique PK values to avoid duplicate lookups.
	type pkKey string
	seen := make(map[pkKey]struct{})
	var pkValues [][]any

	for _, p := range ts.toastPending {
		vals := make([]any, len(pk))
		var parts []string
		for i, col := range pk {
			vals[i] = p.row[col]
			parts = append(parts, fmt.Sprintf("%v", p.row[col]))
		}
		key := pkKey(strings.Join(parts, "|"))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		pkValues = append(pkValues, vals)
	}

	if len(pkValues) == 0 {
		return nil
	}

	// Build query.
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(strings.Join(selectCols, ", "))
	query.WriteString(" FROM ")
	query.WriteString(pgTable) // already fully qualified: schema.table

	if len(pk) == 1 {
		// Simple: WHERE pk IN ($1, $2, ...)
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
		pkValues = [][]any{args} // flatten for single-PK case
	} else {
		// Composite PK: WHERE (pk1, pk2) IN ((v1, v2), ...)
		query.WriteString(" WHERE (")
		for i, col := range pk {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString(quoteIdent(col))
		}
		query.WriteString(") IN (")
		var allArgs []any
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
				allArgs = append(allArgs, v)
				argIdx++
			}
			query.WriteString(")")
		}
		query.WriteString(")")
		pkValues = [][]any{allArgs}
	}

	// Connect to source PostgreSQL.
	conn, err := pgx.Connect(ctx, s.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("connect to source for TOAST lookup: %w", err)
	}
	defer conn.Close(ctx)

	// Execute query.
	var args []any
	if len(pk) == 1 {
		args = pkValues[0]
	} else {
		args = pkValues[0]
	}

	rows, err := conn.Query(ctx, query.String(), args...)
	if err != nil {
		return fmt.Errorf("TOAST lookup query: %w", err)
	}
	defer rows.Close()

	// Build a map of PK -> fetched column values.
	type fetchedRow map[string]any
	fetched := make(map[string]fetchedRow)

	fieldDescs := rows.FieldDescriptions()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return fmt.Errorf("scan TOAST row: %w", err)
		}
		row := make(fetchedRow, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = vals[i]
		}
		// Build PK key for this fetched row.
		var parts []string
		for _, col := range pk {
			parts = append(parts, fmt.Sprintf("%v", row[col]))
		}
		key := strings.Join(parts, "|")
		fetched[key] = row
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("TOAST lookup rows: %w", err)
	}

	// Patch the pending rows with fetched values.
	resolved := 0
	for _, p := range ts.toastPending {
		var parts []string
		for _, col := range pk {
			parts = append(parts, fmt.Sprintf("%v", p.row[col]))
		}
		key := strings.Join(parts, "|")
		fr, ok := fetched[key]
		if !ok {
			// Row was deleted between WAL event and lookup — leave as nil.
			// The subsequent DELETE event will cancel this row anyway.
			continue
		}
		for _, col := range p.unchangedCols {
			if val, exists := fr[col]; exists {
				p.row[col] = fmt.Sprintf("%v", val)
			}
		}
		resolved++
	}

	log.Printf("[sink] TOAST: resolved %d/%d rows for %s (%d unique PKs queried)",
		resolved, len(ts.toastPending), pgTable, len(seen))

	return nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
