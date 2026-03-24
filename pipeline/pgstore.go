package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hasyimibhar/pg2iceberg/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore implements PipelineStore using a PostgreSQL database.
type PgStore struct {
	pool *pgxpool.Pool
}

func NewPgStore(ctx context.Context, dsn string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to pipeline store: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pipeline store: %w", err)
	}

	s := &PgStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate pipeline store: %w", err)
	}

	return s, nil
}

func (s *PgStore) Close() {
	s.pool.Close()
}

func (s *PgStore) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pipelines (
			id         TEXT PRIMARY KEY,
			config     JSONB NOT NULL,
			status     TEXT NOT NULL DEFAULT 'stopped',
			error      TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	return err
}

func (s *PgStore) Save(id string, cfg *config.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO pipelines (id, config, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (id) DO UPDATE SET config = $2, updated_at = now()
	`, id, data)
	if err != nil {
		return fmt.Errorf("save pipeline: %w", err)
	}
	return nil
}

func (s *PgStore) Delete(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx, `DELETE FROM pipelines WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	return nil
}

func (s *PgStore) Load(id string) (*config.Config, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var data []byte
	err := s.pool.QueryRow(ctx, `SELECT config FROM pipelines WHERE id = $1`, id).Scan(&data)
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("load pipeline: %w", err)
	}

	return config.LoadJSON(data)
}

func (s *PgStore) List() (map[string]*config.Config, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx, `SELECT id, config FROM pipelines`)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*config.Config)
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			continue
		}
		cfg, err := config.LoadJSON(data)
		if err != nil {
			continue
		}
		result[id] = cfg
	}
	return result, nil
}

// UpdateStatus persists pipeline status to the database.
func (s *PgStore) UpdateStatus(id string, status Status, errMsg string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx, `
		UPDATE pipelines SET status = $2, error = $3, updated_at = now()
		WHERE id = $1
	`, id, string(status), errMsg)
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}
