package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// PgCheckpointStore persists checkpoints to a PostgreSQL table under the _pg2iceberg schema.
type PgCheckpointStore struct {
	pool *pgxpool.Pool
}

func NewPgCheckpointStore(ctx context.Context, dsn string) (*PgCheckpointStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to checkpoint store: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping checkpoint store: %w", err)
	}

	s := &PgCheckpointStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return s, nil
}

func (s *PgCheckpointStore) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS _pg2iceberg`)
	if err != nil {
		return fmt.Errorf(
			"create checkpoint schema: %w\n\n"+
				"If the database user lacks CREATE privileges, run the migration manually.\n"+
				"See: https://pg2iceberg.dev/docs/state-store", err)
	}

	// Migrate from v0 (JSONB blob) to v1 (columnar).
	// Check if the old table exists with the JSONB column.
	var hasOldTable bool
	err = s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = '_pg2iceberg'
			  AND table_name = 'checkpoints'
			  AND column_name = 'checkpoint'
			  AND data_type = 'jsonb'
		)
	`).Scan(&hasOldTable)
	if err != nil {
		return fmt.Errorf("check checkpoint table schema: %w", err)
	}

	if hasOldTable {
		// Migrate existing data: read old rows, drop table, recreate with columns.
		rows, err := s.pool.Query(ctx,
			`SELECT pipeline_id, checkpoint FROM _pg2iceberg.checkpoints`)
		if err != nil {
			return fmt.Errorf("read old checkpoints: %w", err)
		}
		type oldRow struct {
			id   string
			data []byte
		}
		var oldRows []oldRow
		for rows.Next() {
			var r oldRow
			if err := rows.Scan(&r.id, &r.data); err != nil {
				rows.Close()
				return fmt.Errorf("scan old checkpoint: %w", err)
			}
			oldRows = append(oldRows, r)
		}
		rows.Close()

		_, err = s.pool.Exec(ctx, `DROP TABLE _pg2iceberg.checkpoints`)
		if err != nil {
			return fmt.Errorf("drop old checkpoint table: %w", err)
		}

		if err := s.createTable(ctx); err != nil {
			return err
		}

		// Re-insert old data into new schema.
		for _, r := range oldRows {
			var cp Checkpoint
			if err := json.Unmarshal(r.data, &cp); err != nil {
				return fmt.Errorf("parse old checkpoint for %s: %w", r.id, err)
			}
			if err := s.Save(ctx, r.id, &cp); err != nil {
				return fmt.Errorf("migrate checkpoint %s: %w", r.id, err)
			}
		}
		return nil
	}

	return s.createTable(ctx)
}

func (s *PgCheckpointStore) createTable(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _pg2iceberg.checkpoints (
			pipeline_id             TEXT PRIMARY KEY,
			version                 INTEGER NOT NULL DEFAULT 1,
			checksum                TEXT NOT NULL DEFAULT '',
			written_by              TEXT NOT NULL DEFAULT '',
			revision                BIGINT NOT NULL DEFAULT 0,
			mode                    TEXT NOT NULL DEFAULT '',
			lsn                     BIGINT NOT NULL DEFAULT 0,
			watermark               TEXT NOT NULL DEFAULT '',
			snapshot_complete       BOOLEAN NOT NULL DEFAULT FALSE,
			snapshoted_tables       JSONB,
			snapshot_chunks         JSONB,
			query_watermarks        JSONB,
			updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("create checkpoint table: %w", err)
	}
	return nil
}

// Load reads the checkpoint for a pipeline. Returns a zero Checkpoint if no row exists.
func (s *PgCheckpointStore) Load(ctx context.Context, pipelineID string) (*Checkpoint, error) {
	ctx, span := tracer.Start(ctx, "checkpoint.Load", trace.WithAttributes(attribute.String("pipeline.id", pipelineID)))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var cp Checkpoint
	var snapshotedTables, snapshotChunks, queryWatermarks []byte

	_, dbSpan := tracer.Start(ctx, "checkpoint.pg SELECT", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		semconv.PeerService("postgres"),
		attribute.String("db.system", "postgresql"),
	))
	err := s.pool.QueryRow(ctx, `
		SELECT version, checksum, written_by, revision, mode, lsn, watermark,
		       snapshot_complete, snapshoted_tables, snapshot_chunks,
		       query_watermarks, updated_at
		FROM _pg2iceberg.checkpoints
		WHERE pipeline_id = $1
	`, pipelineID).Scan(
		&cp.Version, &cp.Checksum, &cp.WrittenBy, &cp.Revision, &cp.Mode, &cp.LSN, &cp.Watermark,
		&cp.SnapshotComplete, &snapshotedTables, &snapshotChunks,
		&queryWatermarks, &cp.UpdatedAt,
	)
	dbSpan.End()

	if err == pgx.ErrNoRows {
		return &Checkpoint{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}

	// Unmarshal JSONB columns for map fields.
	if len(snapshotedTables) > 0 {
		json.Unmarshal(snapshotedTables, &cp.SnapshotedTables)
	}
	if len(snapshotChunks) > 0 {
		json.Unmarshal(snapshotChunks, &cp.SnapshotChunks)
	}
	if len(queryWatermarks) > 0 {
		json.Unmarshal(queryWatermarks, &cp.QueryWatermarks)
	}

	if err := cp.Verify(); err != nil {
		return nil, err
	}
	return &cp, nil
}

// Save upserts the checkpoint for a pipeline.
func (s *PgCheckpointStore) Save(ctx context.Context, pipelineID string, cp *Checkpoint) error {
	ctx, span := tracer.Start(ctx, "checkpoint.Save", trace.WithAttributes(attribute.String("pipeline.id", pipelineID)))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Truncate to microsecond precision to match PostgreSQL's TIMESTAMPTZ,
	// which discards nanoseconds. Without this, the checksum computed here
	// (with nanoseconds) won't match the checksum recomputed after Load
	// (without nanoseconds).
	cp.UpdatedAt = time.Now().Truncate(time.Microsecond)
	cp.Seal()

	snapshotedTables, _ := json.Marshal(cp.SnapshotedTables)
	snapshotChunks, _ := json.Marshal(cp.SnapshotChunks)
	queryWatermarks, _ := json.Marshal(cp.QueryWatermarks)

	expectedRevision := cp.Revision - 1 // Seal() already incremented

	if expectedRevision == 0 {
		// First save — insert.
		_, dbSpan := tracer.Start(ctx, "checkpoint.pg UPSERT", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
			semconv.PeerService("postgres"),
			attribute.String("db.system", "postgresql"),
		))
		_, err := s.pool.Exec(ctx, `
			INSERT INTO _pg2iceberg.checkpoints (
				pipeline_id, version, checksum, written_by, revision, mode, lsn, watermark,
				snapshot_complete, snapshoted_tables, snapshot_chunks,
				query_watermarks, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (pipeline_id) DO UPDATE SET
				version = $2, checksum = $3, written_by = $4, revision = $5,
				mode = $6, lsn = $7, watermark = $8,
				snapshot_complete = $9, snapshoted_tables = $10, snapshot_chunks = $11,
				query_watermarks = $12, updated_at = $13
			WHERE _pg2iceberg.checkpoints.revision = 0 OR _pg2iceberg.checkpoints.revision IS NULL
		`, pipelineID, cp.Version, cp.Checksum, cp.WrittenBy, cp.Revision,
			cp.Mode, cp.LSN, cp.Watermark,
			cp.SnapshotComplete, snapshotedTables, snapshotChunks, queryWatermarks,
			cp.UpdatedAt)
		dbSpan.End()
		if err != nil {
			return fmt.Errorf("save checkpoint: %w", err)
		}
		return nil
	}

	// Subsequent saves — optimistic concurrency check.
	_, dbSpan := tracer.Start(ctx, "checkpoint.pg UPDATE", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		semconv.PeerService("postgres"),
		attribute.String("db.system", "postgresql"),
	))
	result, err := s.pool.Exec(ctx, `
		UPDATE _pg2iceberg.checkpoints SET
			version = $2, checksum = $3, written_by = $4, revision = $5,
			mode = $6, lsn = $7, watermark = $8,
			snapshot_complete = $9, snapshoted_tables = $10, snapshot_chunks = $11,
			query_watermarks = $12, updated_at = $13
		WHERE pipeline_id = $1 AND revision = $14
	`, pipelineID, cp.Version, cp.Checksum, cp.WrittenBy, cp.Revision,
		cp.Mode, cp.LSN, cp.Watermark,
		cp.SnapshotComplete, snapshotedTables, snapshotChunks, queryWatermarks,
		cp.UpdatedAt, expectedRevision)
	dbSpan.End()
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrConcurrentUpdate
	}
	return nil
}

// Close releases the connection pool.
func (s *PgCheckpointStore) Close() {
	s.pool.Close()
}
