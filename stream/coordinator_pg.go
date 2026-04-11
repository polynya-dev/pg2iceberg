package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgCoordinator implements Coordinator using PostgreSQL.
// All state is stored under the _pg2iceberg schema in the source database.
type PgCoordinator struct {
	pool    *pgxpool.Pool
	lockTTL time.Duration
}

// NewPgCoordinator creates a Coordinator backed by the given PostgreSQL DSN.
func NewPgCoordinator(ctx context.Context, dsn string) (*PgCoordinator, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to coordinator: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping coordinator: %w", err)
	}
	return &PgCoordinator{pool: pool, lockTTL: 30 * time.Second}, nil
}

// NewPgCoordinatorFromPool creates a Coordinator using an existing connection pool.
func NewPgCoordinatorFromPool(pool *pgxpool.Pool) *PgCoordinator {
	return &PgCoordinator{pool: pool, lockTTL: 30 * time.Second}
}

func (c *PgCoordinator) Close() { c.pool.Close() }

// Migrate creates the coordination tables if they don't exist.
func (c *PgCoordinator) Migrate(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS _pg2iceberg`)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS _pg2iceberg.log_seq (
			table_name  TEXT PRIMARY KEY,
			next_offset BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS _pg2iceberg.log_index (
			table_name   TEXT   NOT NULL,
			end_offset   BIGINT NOT NULL,
			start_offset BIGINT NOT NULL,
			s3_path      TEXT   NOT NULL,
			record_count INT    NOT NULL,
			byte_size    BIGINT NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (table_name, end_offset)
		)`,
		`CREATE TABLE IF NOT EXISTS _pg2iceberg.mat_cursor (
			table_name     TEXT PRIMARY KEY,
			last_offset    BIGINT NOT NULL DEFAULT -1,
			last_committed TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS _pg2iceberg.lock (
			table_name TEXT PRIMARY KEY,
			worker_id  TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL
		)`,
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

	entries := make([]LogEntry, len(appends))

	err := pgx.BeginTxFunc(ctx, c.pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		// Collect per-table record counts for batch offset claiming.
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

		// Claim offset ranges per table.
		for table, to := range tableOffsets {
			// Ensure log_seq row exists.
			_, err := tx.Exec(ctx,
				`INSERT INTO _pg2iceberg.log_seq (table_name, next_offset)
				 VALUES ($1, 0) ON CONFLICT DO NOTHING`, table)
			if err != nil {
				return fmt.Errorf("ensure log_seq for %s: %w", table, err)
			}

			// Atomic increment: returns the NEW value after adding totalRecords.
			var newOffset int64
			err = tx.QueryRow(ctx,
				`UPDATE _pg2iceberg.log_seq
				 SET next_offset = next_offset + $2
				 WHERE table_name = $1
				 RETURNING next_offset`, table, to.totalRecords).Scan(&newOffset)
			if err != nil {
				return fmt.Errorf("claim offsets for %s: %w", table, err)
			}
			// start_offset = newOffset - totalRecords
			to.startOffset = newOffset - int64(to.totalRecords)
		}

		// Assign offset ranges to each append and insert log index entries.
		// Track running offset per table.
		running := make(map[string]int64)
		for table, to := range tableOffsets {
			running[table] = to.startOffset
		}

		for i, a := range appends {
			start := running[a.Table]
			end := start + int64(a.RecordCount)
			running[a.Table] = end

			_, err := tx.Exec(ctx,
				`INSERT INTO _pg2iceberg.log_index
				 (table_name, end_offset, start_offset, s3_path, record_count, byte_size)
				 VALUES ($1, $2, $3, $4, $5, $6)`,
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
	rows, err := c.pool.Query(ctx,
		`SELECT table_name, start_offset, end_offset, s3_path, record_count, byte_size, created_at
		 FROM _pg2iceberg.log_index
		 WHERE table_name = $1 AND end_offset > $2
		 ORDER BY end_offset ASC`, table, afterOffset)
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
		`DELETE FROM _pg2iceberg.log_index
		 WHERE table_name = $1 AND end_offset <= $2
		 RETURNING s3_path`, table, offset)
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

// EnsureCursor creates a cursor row if it doesn't exist.
func (c *PgCoordinator) EnsureCursor(ctx context.Context, table string) error {
	_, err := c.pool.Exec(ctx,
		`INSERT INTO _pg2iceberg.mat_cursor (table_name, last_offset)
		 VALUES ($1, -1) ON CONFLICT DO NOTHING`, table)
	if err != nil {
		return fmt.Errorf("ensure cursor for %s: %w", table, err)
	}
	return nil
}

// GetCursor returns the last materialized offset. Returns -1 if not found.
func (c *PgCoordinator) GetCursor(ctx context.Context, table string) (int64, error) {
	var offset int64
	err := c.pool.QueryRow(ctx,
		`SELECT last_offset FROM _pg2iceberg.mat_cursor WHERE table_name = $1`,
		table).Scan(&offset)
	if err == pgx.ErrNoRows {
		return -1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get cursor for %s: %w", table, err)
	}
	return offset, nil
}

// SetCursor updates the materializer cursor.
func (c *PgCoordinator) SetCursor(ctx context.Context, table string, offset int64) error {
	_, err := c.pool.Exec(ctx,
		`UPDATE _pg2iceberg.mat_cursor
		 SET last_offset = $2, last_committed = now()
		 WHERE table_name = $1`, table, offset)
	if err != nil {
		return fmt.Errorf("set cursor for %s: %w", table, err)
	}
	return nil
}

// TryLock attempts to acquire a heartbeat lock. Expired locks are reclaimed.
func (c *PgCoordinator) TryLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error) {
	// Expire stale locks.
	_, err := c.pool.Exec(ctx,
		`DELETE FROM _pg2iceberg.lock WHERE table_name = $1 AND expires_at < now()`,
		table)
	if err != nil {
		return false, fmt.Errorf("expire lock for %s: %w", table, err)
	}

	// Attempt to acquire.
	ct, err := c.pool.Exec(ctx,
		`INSERT INTO _pg2iceberg.lock (table_name, worker_id, expires_at)
		 VALUES ($1, $2, now() + $3::interval)
		 ON CONFLICT DO NOTHING`,
		table, workerID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return false, fmt.Errorf("acquire lock for %s: %w", table, err)
	}
	return ct.RowsAffected() == 1, nil
}

// RenewLock extends the lock TTL. Returns false if the lock was lost.
func (c *PgCoordinator) RenewLock(ctx context.Context, table, workerID string, ttl time.Duration) (bool, error) {
	ct, err := c.pool.Exec(ctx,
		`UPDATE _pg2iceberg.lock
		 SET expires_at = now() + $3::interval
		 WHERE table_name = $1 AND worker_id = $2`,
		table, workerID, fmt.Sprintf("%d seconds", int(ttl.Seconds())))
	if err != nil {
		return false, fmt.Errorf("renew lock for %s: %w", table, err)
	}
	return ct.RowsAffected() == 1, nil
}

// ReleaseLock releases a held lock. No-op if not held by this worker.
func (c *PgCoordinator) ReleaseLock(ctx context.Context, table, workerID string) error {
	_, err := c.pool.Exec(ctx,
		`DELETE FROM _pg2iceberg.lock WHERE table_name = $1 AND worker_id = $2`,
		table, workerID)
	if err != nil {
		return fmt.Errorf("release lock for %s: %w", table, err)
	}
	return nil
}
