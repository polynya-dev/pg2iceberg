// Package pipeline provides shared pipeline infrastructure used by both
// the logical and query mode pipelines: status types, metrics structs,
// and checkpoint store construction.
package pipeline

import (
	"context"

	"github.com/pg2iceberg/pg2iceberg/config"
)

// Status represents the current state of a Pipeline.
type Status string

const (
	StatusStarting     Status = "starting"
	StatusSnapshotting Status = "snapshotting"
	StatusRunning      Status = "running"
	StatusStopping     Status = "stopping"
	StatusStopped      Status = "stopped"
	StatusError        Status = "error"
)

// Pipeline is the common interface implemented by both logical.Pipeline and
// query.Pipeline.
type Pipeline interface {
	Done() <-chan struct{}
	Status() (Status, error)
	Metrics() Metrics
}

// Metrics holds pipeline metrics exposed via the /metrics endpoint.
type Metrics struct {
	Status         Status `json:"status"`
	BufferedRows   int    `json:"buffered_rows"`
	BufferedBytes  int64  `json:"buffered_bytes"`
	RowsProcessed  int64  `json:"rows_processed"`
	BytesProcessed int64  `json:"bytes_processed"`
	LSN            uint64 `json:"lsn,omitempty"`
	LastFlushAt    string `json:"last_flush_at,omitempty"`
	Uptime         string `json:"uptime"`
}

// NewCheckpointStore creates a CheckpointStore from config. It uses a file store
// if a path is configured, otherwise falls back to Postgres.
func NewCheckpointStore(ctx context.Context, cfg *config.Config) (CheckpointStore, error) {
	// Explicit file path: use file store (local dev).
	if cfg.State.Path != "" {
		return NewFileCheckpointStore(cfg.State.Path), nil
	}

	// Explicit postgres URL, or fall back to source postgres.
	url := cfg.State.PostgresURL
	if url == "" {
		url = cfg.Source.Postgres.DSN()
	}
	return NewPgCheckpointStore(ctx, url)
}
