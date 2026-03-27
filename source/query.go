package source

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/jackc/pgx/v5"
)

// QuerySource implements Source using periodic SELECT queries with a watermark column.
type QuerySource struct {
	cfg       config.QueryConfig
	pgCfg     config.PostgresConfig
	tableCfgs []config.TableConfig
	conn      *pgx.Conn
	tables    map[string]*schema.TableSchema
	// watermarks tracks the last-seen watermark per table.
	watermarks map[string]time.Time
}

func NewQuerySource(pgCfg config.PostgresConfig, queryCfg config.QueryConfig, tableCfgs []config.TableConfig) *QuerySource {
	return &QuerySource{
		cfg:        queryCfg,
		pgCfg:      pgCfg,
		tableCfgs:  tableCfgs,
		tables:     make(map[string]*schema.TableSchema),
		watermarks: make(map[string]time.Time),
	}
}

// SetWatermark allows restoring watermark from a checkpoint.
func (q *QuerySource) SetWatermark(table string, t time.Time) {
	q.watermarks[table] = t
}

func (q *QuerySource) Capture(ctx context.Context, events chan<- ChangeEvent) error {
	var err error
	q.conn, err = pgx.Connect(ctx, q.pgCfg.DSN())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer q.conn.Close(ctx)

	// Discover schemas for all configured tables
	for _, tbl := range q.tableCfgs {
		ts, err := schema.DiscoverSchema(ctx, q.conn, tbl.Name)
		if err != nil {
			return fmt.Errorf("discover schema for %s: %w", tbl.Name, err)
		}
		if len(tbl.PrimaryKey) > 0 {
			ts.PK = tbl.PrimaryKey
		}
		q.tables[tbl.Name] = ts
	}

	interval := q.cfg.PollDuration()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Do an immediate first poll
	if err := q.poll(ctx, events); err != nil {
		log.Printf("[query] poll error: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := q.poll(ctx, events); err != nil {
				log.Printf("[query] poll error: %v", err)
			}
		}
	}
}

func (q *QuerySource) poll(ctx context.Context, events chan<- ChangeEvent) error {
	for _, tbl := range q.tableCfgs {
		ts := q.tables[tbl.Name]
		watermark := q.watermarks[tbl.Name]

		query := fmt.Sprintf(
			"SELECT * FROM %s WHERE %s > $1 ORDER BY %s ASC",
			tbl.Name, tbl.WatermarkColumn, tbl.WatermarkColumn,
		)

		rows, err := q.conn.Query(ctx, query, watermark)
		if err != nil {
			return fmt.Errorf("query %s: %w", tbl.Name, err)
		}

		descs := rows.FieldDescriptions()
		count := 0

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return fmt.Errorf("scan %s: %w", tbl.Name, err)
			}

			row := make(map[string]any, len(descs))
			for i, desc := range descs {
				row[string(desc.Name)] = values[i]
			}

			// Update watermark
			if wm, ok := row[tbl.WatermarkColumn]; ok {
				if t, ok := wm.(time.Time); ok && t.After(watermark) {
					watermark = t
				}
			}

			now := time.Now()
			select {
			case events <- ChangeEvent{
				Table:              tbl.Name,
				Operation:          OpInsert, // query mode treats everything as upserts
				After:              row,
				PK:                 ts.PK,
				SourceTimestamp:     now,
				ProcessingTimestamp: now,
			}:
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			}
			count++
		}
		rows.Close()

		q.watermarks[tbl.Name] = watermark
		if count > 0 {
			log.Printf("[query] %s: emitted %d rows, watermark=%v", tbl.Name, count, watermark)
		}
	}
	return nil
}

// AddTable adds a table to the polling set.
func (q *QuerySource) AddTable(tbl config.TableConfig) {
	q.tableCfgs = append(q.tableCfgs, tbl)
}

// RemoveTable removes a table from the polling set.
func (q *QuerySource) RemoveTable(tableName string) {
	for i, t := range q.tableCfgs {
		if t.Name == tableName {
			q.tableCfgs = append(q.tableCfgs[:i], q.tableCfgs[i+1:]...)
			delete(q.tables, tableName)
			delete(q.watermarks, tableName)
			return
		}
	}
}

func (q *QuerySource) Close() error {
	// Query mode has no persistent resources to clean up.
	return nil
}
