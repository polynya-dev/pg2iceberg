package source

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/metrics"
	"github.com/pg2iceberg/pg2iceberg/schema"
	"github.com/pg2iceberg/pg2iceberg/worker"
)

// TxFactory creates a new transaction for snapshot queries. Each table gets
// its own transaction so tables can be snapshotted in parallel. The caller
// controls isolation level and snapshot pinning inside the factory.
// The returned cleanup function must be called when the transaction is no
// longer needed (typically closes the underlying connection).
type TxFactory func(ctx context.Context) (pgx.Tx, func(context.Context), error)

// SnapshotTable describes a table to be snapshotted.
type SnapshotTable struct {
	Name   string
	Schema *schema.TableSchema
}

// Snapshotter performs bulk SELECT * table copies using per-table transactions
// obtained from a TxFactory. Tables are snapshotted concurrently via a WorkerPool.
type Snapshotter struct {
	tables     []SnapshotTable
	txFactory  TxFactory
	pool       *worker.Pool
	pipelineID string
}

// NewSnapshotter creates a Snapshotter for the given tables.
// Concurrency controls how many tables are snapshotted in parallel.
func NewSnapshotter(tables []SnapshotTable, txFactory TxFactory, concurrency int, pipelineID ...string) *Snapshotter {
	if concurrency <= 0 {
		concurrency = 1
	}
	// Don't create more workers than tables.
	if concurrency > len(tables) {
		concurrency = len(tables)
	}
	pid := ""
	if len(pipelineID) > 0 {
		pid = pipelineID[0]
	}
	return &Snapshotter{
		tables:     tables,
		txFactory:  txFactory,
		pool:       worker.NewPool(concurrency),
		pipelineID: pid,
	}
}

// Workers returns a snapshot of all worker statuses.
func (s *Snapshotter) Workers() []worker.Status {
	return s.pool.Workers()
}

// Run snapshots all tables using the worker pool, emitting rows as ChangeEvents.
// After each table completes, an OpSnapshotTableComplete event is sent.
func (s *Snapshotter) Run(ctx context.Context, events chan<- ChangeEvent) ([]worker.Result, error) {
	tasks := make([]worker.Task, len(s.tables))
	for i, tbl := range s.tables {
		tbl := tbl
		tasks[i] = worker.Task{
			Name: tbl.Name,
			Fn: func(ctx context.Context, progress *worker.Progress) error {
				if err := s.snapshotOneTable(ctx, tbl, events, progress); err != nil {
					return err
				}

				// Signal that this table's snapshot is complete.
				select {
				case events <- ChangeEvent{
					Table:     tbl.Name,
					Operation: OpSnapshotTableComplete,
				}:
				case <-ctx.Done():
					return ctx.Err()
				}

				return nil
			},
		}
	}

	results, err := s.pool.Run(ctx, tasks)
	for _, r := range results {
		if r.Err == nil {
			log.Printf("[snapshot] emitted %d rows for %s", r.Rows, r.Task)
		}
	}
	return results, err
}

// snapshotOneTable copies all rows from a single table. It obtains its own
// transaction from the TxFactory. This is the future hook point for CTID
// range splitting and parallel workers.
func (s *Snapshotter) snapshotOneTable(ctx context.Context, tbl SnapshotTable, events chan<- ChangeEvent, progress *worker.Progress) error {
	tx, cleanup, err := s.txFactory(ctx)
	if err != nil {
		return fmt.Errorf("create tx for %s: %w", tbl.Name, err)
	}
	defer cleanup(ctx)
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s", tbl.Name))
	if err != nil {
		return fmt.Errorf("snapshot query %s: %w", tbl.Name, err)
	}

	descs := rows.FieldDescriptions()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			rows.Close()
			return fmt.Errorf("snapshot scan %s: %w", tbl.Name, err)
		}

		row := make(map[string]any, len(descs))
		for i, desc := range descs {
			row[string(desc.Name)] = pgValueToString(values[i])
		}

		now := time.Now()
		select {
		case events <- ChangeEvent{
			Table:              tbl.Name,
			Operation:          OpInsert,
			After:              row,
			PK:                 tbl.Schema.PK,
			SourceTimestamp:     now,
			ProcessingTimestamp: now,
		}:
		case <-ctx.Done():
			rows.Close()
			return ctx.Err()
		}
		progress.Add(1)
		metrics.SnapshotRowsTotal.WithLabelValues(s.pipelineID, tbl.Name).Inc()
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return fmt.Errorf("snapshot rows %s: %w", tbl.Name, err)
	}

	return nil
}
