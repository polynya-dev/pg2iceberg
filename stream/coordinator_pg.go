package stream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var coordTracer = otel.Tracer("pg2iceberg/coordinator")

const defaultSchema = "_pg2iceberg"

// PgCoordinator implements Coordinator using PostgreSQL.
// All state is stored under a configurable schema (default: _pg2iceberg).
type PgCoordinator struct {
	pool    *pgxpool.Pool
	schema  string
	lockTTL time.Duration
}

// NewPgCoordinator creates a Coordinator backed by the given PostgreSQL DSN.
// Uses the default schema "_pg2iceberg".
func NewPgCoordinator(ctx context.Context, dsn string) (*PgCoordinator, error) {
	return NewPgCoordinatorWithSchema(ctx, dsn, defaultSchema)
}

// NewPgCoordinatorWithSchema creates a Coordinator with a custom schema name.
// This is useful when multiple pg2iceberg instances share the same database
// (e.g. parallel e2e tests).
func NewPgCoordinatorWithSchema(ctx context.Context, dsn, schema string) (*PgCoordinator, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to coordinator: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping coordinator: %w", err)
	}
	if schema == "" {
		schema = defaultSchema
	}
	return &PgCoordinator{pool: pool, schema: schema, lockTTL: 30 * time.Second}, nil
}

// NewPgCoordinatorFromPool creates a Coordinator using an existing connection pool.
func NewPgCoordinatorFromPool(pool *pgxpool.Pool) *PgCoordinator {
	return &PgCoordinator{pool: pool, schema: defaultSchema, lockTTL: 30 * time.Second}
}

func (c *PgCoordinator) Close() { c.pool.Close() }

// q qualifies a table name with the schema prefix.
func (c *PgCoordinator) q(table string) string {
	return c.schema + "." + table
}

// Migrate creates the schema and coordination tables if they don't exist.
func (c *PgCoordinator) Migrate(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, c.schema))
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	for _, ddl := range []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			table_name  TEXT PRIMARY KEY,
			next_offset BIGINT NOT NULL DEFAULT 0
		)`, c.q("log_seq")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			table_name   TEXT   NOT NULL,
			end_offset   BIGINT NOT NULL,
			start_offset BIGINT NOT NULL,
			s3_path      TEXT   NOT NULL,
			record_count INT    NOT NULL,
			byte_size    BIGINT NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (table_name, end_offset)
		)`, c.q("log_index")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			group_name     TEXT NOT NULL DEFAULT 'default',
			table_name     TEXT NOT NULL,
			last_offset    BIGINT NOT NULL DEFAULT -1,
			last_committed TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (group_name, table_name)
		)`, c.q("mat_cursor")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			table_name TEXT PRIMARY KEY,
			worker_id  TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL
		)`, c.q("lock")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			group_name TEXT NOT NULL,
			worker_id  TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (group_name, worker_id)
		)`, c.q("consumer")),
	} {
		if _, err := c.pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("migrate coordination tables: %w", err)
		}
	}
	return nil
}

// ClaimOffsets atomically increments offset counters and inserts log index entries.
func (c *PgCoordinator) ClaimOffsets(ctx context.Context, appends []LogAppend) ([]LogEntry, error) {
	if len(appends) == 0 {
		return nil, nil
	}

	ctx, span := coordTracer.Start(ctx, "coordinator.ClaimOffsets",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.PeerService("postgres"),
			attribute.Int("stream.batch_size", len(appends)),
		))
	defer span.End()

	entries := make([]LogEntry, len(appends))

	err := pgx.BeginTxFunc(ctx, c.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		type tableOffset struct {
			totalRecords int
			startOffset  int64
		}
		tableOffsets := make(map[string]*tableOffset)
		for _, a := range appends {
			to, ok := tableOffsets[a.Table]
			if !ok {
				to = &tableOffset{}
				tableOffsets[a.Table] = to
			}
			to.totalRecords += a.RecordCount
		}

		for table, to := range tableOffsets {
			_, err := tx.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s (table_name, next_offset)
				 VALUES ($1, 0) ON CONFLICT DO NOTHING`, c.q("log_seq")), table)
			if err != nil {
				return fmt.Errorf("ensure log_seq for %s: %w", table, err)
			}

			var newOffset int64
			err = tx.QueryRow(ctx,
				fmt.Sprintf(`UPDATE %s
				 SET next_offset = next_offset + $2
				 WHERE table_name = $1
				 RETURNING next_offset`, c.q("log_seq")), table, to.totalRecords).Scan(&newOffset)
			if err != nil {
				return fmt.Errorf("claim offsets for %s: %w", table, err)
			}
			to.startOffset = newOffset - int64(to.totalRecords)
		}

		running := make(map[string]int64)
		for table, to := range tableOffsets {
			running[table] = to.startOffset
		}

		for i, a := range appends {
			start := running[a.Table]
			end := start + int64(a.RecordCount)
			running[a.Table] = end

			_, err := tx.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s
				 (table_name, end_offset, start_offset, s3_path, record_count, byte_size)
				 VALUES ($1, $2, $3, $4, $5, $6)`, c.q("log_index")),
				a.Table, end, start, a.S3Path, a.RecordCount, a.ByteSize)
			if err != nil {
				return fmt.Errorf("insert log entry for %s: %w", a.Table, err)
			}

			entries[i] = LogEntry{
				Table:       a.Table,
				StartOffset: start,
				EndOffset:   end,
				S3Path:      a.S3Path,
				RecordCount: a.RecordCount,
				ByteSize:    a.ByteSize,
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return entries, nil
}

// ReadLog returns log entries with end_offset > afterOffset, ordered ascending.
func (c *PgCoordinator) ReadLog(ctx context.Context, table string, afterOffset int64) ([]LogEntry, error) {
	ctx, span := coordTracer.Start(ctx, "coordinator.ReadLog",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.PeerService("postgres"),
			attribute.String("stream.table", table),
			attribute.Int64("stream.after_offset", afterOffset),
		))
	defer span.End()
	rows, err := c.pool.Query(ctx,
		fmt.Sprintf(`SELECT table_name, start_offset, end_offset, s3_path, record_count, byte_size, created_at
		 FROM %s
		 WHERE table_name = $1 AND end_offset > $2
		 ORDER BY end_offset ASC`, c.q("log_index")), table, afterOffset)
	if err != nil {
		return nil, fmt.Errorf("read log for %s: %w", table, err)
	}
	defer rows.Close()

	var entries []LogEntry
	for rows.Next() {
		var e LogEntry
		if err := rows.Scan(&e.Table, &e.StartOffset, &e.EndOffset, &e.S3Path,
			&e.RecordCount, &e.ByteSize, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan log entry: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// TruncateLog deletes entries with end_offset <= offset and returns their S3 paths.
func (c *PgCoordinator) TruncateLog(ctx context.Context, table string, offset int64) ([]string, error) {
	rows, err := c.pool.Query(ctx,
		fmt.Sprintf(`DELETE FROM %s
		 WHERE table_name = $1 AND end_offset <= $2
		 RETURNING s3_path`, c.q("log_index")), table, offset)
	if err != nil {
		return nil, fmt.Errorf("truncate log for %s: %w", table, err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("scan truncated path: %w", err)
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// EnsureCursor creates a cursor row for a group+table if it doesn't exist.
func (c *PgCoordinator) EnsureCursor(ctx context.Context, group, table string) error {
	_, err := c.pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (group_name, table_name, last_offset)
		 VALUES ($1, $2, -1) ON CONFLICT DO NOTHING`, c.q("mat_cursor")),
		group, table)
	if err != nil {
		return fmt.Errorf("ensure cursor for %s/%s: %w", group, table, err)
	}
	return nil
}

// GetCursor returns the last committed offset for a group+table. Returns -1 if not found.
func (c *PgCoordinator) GetCursor(ctx context.Context, group, table string) (int64, error) {
	ctx, span := coordTracer.Start(ctx, "coordinator.GetCursor",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.PeerService("postgres"),
			attribute.String("stream.group", group),
			attribute.String("stream.table", table),
		))
	defer span.End()
	var offset int64
	err := c.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT last_offset FROM %s WHERE group_name = $1 AND table_name = $2`,
			c.q("mat_cursor")),
		group, table).Scan(&offset)
	if err == pgx.ErrNoRows {
		return -1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get cursor for %s/%s: %w", group, table, err)
	}
	return offset, nil
}

// SetCursor updates the cursor for a group+table.
func (c *PgCoordinator) SetCursor(ctx context.Context, group, table string, offset int64) error {
	ctx, span := coordTracer.Start(ctx, "coordinator.SetCursor",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.PeerService("postgres"),
			attribute.String("stream.group", group),
			attribute.String("stream.table", table),
			attribute.Int64("stream.offset", offset),
		))
	defer span.End()
	_, err := c.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
		 SET last_offset = $3, last_committed = now()
		 WHERE group_name = $1 AND table_name = $2`,
			c.q("mat_cursor")),
		group, table, offset)
	if err != nil {
		return fmt.Errorf("set cursor for %s/%s: %w", group, table, err)
	}
	return nil
}

// RegisterWorker upserts a worker heartbeat within a consumer group.
func (c *PgCoordinator) RegisterConsumer(ctx context.Context, group, workerID string, ttl time.Duration) error {
	_, err := c.pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (group_name, worker_id, expires_at)
		 VALUES ($1, $2, now() + $3::interval)
		 ON CONFLICT (group_name, worker_id) DO UPDATE SET expires_at = now() + $3::interval`,
			c.q("consumer")),
		group, workerID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return fmt.Errorf("register worker %s in group %s: %w", workerID, group, err)
	}
	return nil
}

// UnregisterWorker removes a worker from its consumer group.
func (c *PgCoordinator) UnregisterConsumer(ctx context.Context, group, workerID string) error {
	_, err := c.pool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE group_name = $1 AND worker_id = $2`, c.q("consumer")),
		group, workerID)
	if err != nil {
		return fmt.Errorf("unregister worker %s from group %s: %w", workerID, group, err)
	}
	return nil
}

// ActiveWorkers returns all workers in a group with valid heartbeats, sorted by ID.
func (c *PgCoordinator) ActiveConsumers(ctx context.Context, group string) ([]string, error) {
	// Expire stale workers first.
	c.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE expires_at < now()`, c.q("consumer")))

	rows, err := c.pool.Query(ctx,
		fmt.Sprintf(`SELECT worker_id FROM %s WHERE group_name = $1 ORDER BY worker_id`,
			c.q("consumer")), group)
	if err != nil {
		return nil, fmt.Errorf("list active workers in group %s: %w", group, err)
	}
	defer rows.Close()

	var workers []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		workers = append(workers, id)
	}
	return workers, rows.Err()
}

// TryLock attempts to acquire a heartbeat lock. Expired locks are reclaimed.
func (c *PgCoordinator) TryLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error) {
	ctx, span := coordTracer.Start(ctx, "coordinator.TryLock",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.PeerService("postgres"),
			attribute.String("stream.table", table),
			attribute.String("stream.worker_id", workerID),
		))
	defer span.End()
	_, err := c.pool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE table_name = $1 AND expires_at < now()`, c.q("lock")),
		table)
	if err != nil {
		return false, fmt.Errorf("expire lock for %s: %w", table, err)
	}

	ct, err := c.pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (table_name, worker_id, expires_at)
		 VALUES ($1, $2, now() + $3::interval)
		 ON CONFLICT DO NOTHING`, c.q("lock")),
		table, workerID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return false, fmt.Errorf("acquire lock for %s: %w", table, err)
	}
	return ct.RowsAffected() == 1, nil
}

// RenewLock extends the lock TTL. Returns false if the lock was lost.
func (c *PgCoordinator) RenewLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error) {
	ct, err := c.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
		 SET expires_at = now() + $3::interval
		 WHERE table_name = $1 AND worker_id = $2`, c.q("lock")),
		table, workerID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return false, fmt.Errorf("renew lock for %s: %w", table, err)
	}
	return ct.RowsAffected() == 1, nil
}

// ReleaseLock releases a held lock. No-op if not held by this worker.
func (c *PgCoordinator) ReleaseLock(ctx context.Context, table, workerID string) error {
	_, err := c.pool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE table_name = $1 AND worker_id = $2`, c.q("lock")),
		table, workerID)
	if err != nil {
		return fmt.Errorf("release lock for %s: %w", table, err)
	}
	return nil
}

// sanitizeSchema removes characters that aren't valid in a PG identifier.
func sanitizeSchema(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, strings.ToLower(s))
}
