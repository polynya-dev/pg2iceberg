package query

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/utils"
)

// Poller executes periodic SELECT queries against PostgreSQL using a watermark
// column to detect new/changed rows. Returns rows directly instead of pushing
// to a channel (unlike the old source.QuerySource).
type Poller struct {
	pgCfg     config.PostgresConfig
	tableCfgs []config.TableConfig
	conn      *pgx.Conn
	schemas   map[string]*schema.TableSchema
	watermarks map[string]time.Time
}

// NewPoller creates a new watermark poller.
func NewPoller(pgCfg config.PostgresConfig, tableCfgs []config.TableConfig) *Poller {
	return &Poller{
		pgCfg:      pgCfg,
		tableCfgs:  tableCfgs,
		schemas:    make(map[string]*schema.TableSchema),
		watermarks: make(map[string]time.Time),
	}
}

// Connect establishes the PG connection and discovers schemas.
func (p *Poller) Connect(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, p.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	p.conn = conn

	for _, tbl := range p.tableCfgs {
		ts, err := schema.DiscoverSchema(ctx, conn, tbl.Name)
		if err != nil {
			return fmt.Errorf("discover schema for %s: %w", tbl.Name, err)
		}
		if len(tbl.PrimaryKey) > 0 {
			ts.PK = tbl.PrimaryKey
		}
		p.schemas[tbl.Name] = ts
	}
	return nil
}

// Schemas returns the discovered table schemas.
func (p *Poller) Schemas() map[string]*schema.TableSchema { return p.schemas }

// SetWatermark restores a watermark from a checkpoint.
func (p *Poller) SetWatermark(table string, t time.Time) {
	p.watermarks[table] = t
}

// Watermarks returns the current watermark per table.
func (p *Poller) Watermarks() map[string]time.Time {
	result := make(map[string]time.Time, len(p.watermarks))
	for k, v := range p.watermarks {
		result[k] = v
	}
	return result
}

// PollResult holds the rows returned for a single table in one poll cycle.
type PollResult struct {
	Table string
	Rows  []map[string]any
}

// Poll executes one polling cycle across all tables, returning new rows.
func (p *Poller) Poll(ctx context.Context) ([]PollResult, error) {
	var results []PollResult

	for _, tbl := range p.tableCfgs {
		watermark := p.watermarks[tbl.Name]

		quotedTable := pgx.Identifier(strings.Split(tbl.Name, ".")).Sanitize()
		quotedCol := pgx.Identifier{tbl.WatermarkColumn}.Sanitize()
		query := fmt.Sprintf(
			"SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC",
			quotedTable, quotedCol, quotedCol,
		)

		rows, err := p.conn.Query(ctx, query, watermark)
		if err != nil {
			return nil, fmt.Errorf("query %s: %w", tbl.Name, err)
		}

		descs := rows.FieldDescriptions()
		var tableRows []map[string]any

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan %s: %w", tbl.Name, err)
			}

			row := make(map[string]any, len(descs))
			for i, desc := range descs {
				row[string(desc.Name)] = values[i]
			}

			// Update watermark.
			if wm, ok := row[tbl.WatermarkColumn]; ok {
				if t, ok := wm.(time.Time); ok && t.After(watermark) {
					watermark = t
				}
			}

			tableRows = append(tableRows, row)
		}
		rows.Close()

		p.watermarks[tbl.Name] = watermark
		if len(tableRows) > 0 {
			log.Printf("[query] %s: polled %d rows, watermark=%v", tbl.Name, len(tableRows), watermark)
			results = append(results, PollResult{Table: tbl.Name, Rows: tableRows})
		}
	}

	return results, nil
}

// PollWithRetry wraps Poll with exponential backoff retry, reconnecting on failure.
func (p *Poller) PollWithRetry(ctx context.Context) ([]PollResult, error) {
	var results []PollResult
	err := utils.Do(ctx, 3, 100*time.Millisecond, 5*time.Second, func() error {
		var err error
		results, err = p.Poll(ctx)
		if err != nil {
			p.reconnect(ctx)
		}
		return err
	})
	return results, err
}

func (p *Poller) reconnect(ctx context.Context) {
	if p.conn != nil {
		p.conn.Close(ctx)
	}
	conn, err := pgx.Connect(ctx, p.pgCfg.DSN())
	if err != nil {
		log.Printf("[query] reconnect failed: %v", err)
		return
	}
	p.conn = conn
}

// Close closes the PG connection.
func (p *Poller) Close(ctx context.Context) {
	if p.conn != nil {
		p.conn.Close(ctx)
	}
}
