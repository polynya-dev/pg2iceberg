package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
				"If the database user lacks CREATE privileges, run this manually:\n"+
				"  CREATE SCHEMA IF NOT EXISTS _pg2iceberg;\n"+
				"  CREATE TABLE IF NOT EXISTS _pg2iceberg.checkpoints (\n"+
				"      pipeline_id TEXT PRIMARY KEY,\n"+
				"      checkpoint  JSONB NOT NULL,\n"+
				"      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()\n"+
				"  );", err)
	}

	_, err = s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _pg2iceberg.checkpoints (
			pipeline_id TEXT PRIMARY KEY,
			checkpoint  JSONB NOT NULL,
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("create checkpoint table: %w", err)
	}

	return nil
}

// Load reads the checkpoint for a pipeline. Returns a zero Checkpoint if no row exists.
func (s *PgCheckpointStore) Load(pipelineID string) (*Checkpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var data []byte
	err := s.pool.QueryRow(ctx,
		`SELECT checkpoint FROM _pg2iceberg.checkpoints WHERE pipeline_id = $1`,
		pipelineID,
	).Scan(&data)

	if err == pgx.ErrNoRows {
		return &Checkpoint{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}

	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	return &cp, nil
}

// Save upserts the checkpoint for a pipeline.
func (s *PgCheckpointStore) Save(pipelineID string, cp *Checkpoint) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cp.UpdatedAt = time.Now()

	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO _pg2iceberg.checkpoints (pipeline_id, checkpoint, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (pipeline_id) DO UPDATE SET checkpoint = $2, updated_at = now()
	`, pipelineID, data)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// Close releases the connection pool.
func (s *PgCheckpointStore) Close() {
	s.pool.Close()
}
