package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hasyimibhar/pg2iceberg/config"
	"github.com/hasyimibhar/pg2iceberg/schema"
	"github.com/hasyimibhar/pg2iceberg/sink"
	"github.com/hasyimibhar/pg2iceberg/source"
	"github.com/hasyimibhar/pg2iceberg/state"
	"github.com/jackc/pgx/v5"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)
		cancel()
	}()

	if err := run(ctx, cfg); err != nil && err != context.Canceled {
		log.Fatalf("fatal: %v", err)
	}
}

func run(ctx context.Context, cfg *config.Config) error {
	// Load checkpoint
	store := state.NewStore(cfg.State.Path)
	cp, err := store.Load()
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}

	// Discover schemas via a regular PG connection
	pgConn, err := pgx.Connect(ctx, cfg.Source.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}

	tables := getTables(cfg)
	schemas := make(map[string]*schema.TableSchema)
	for _, table := range tables {
		ts, err := schema.DiscoverSchema(ctx, pgConn, table)
		if err != nil {
			return fmt.Errorf("discover schema for %s: %w", table, err)
		}
		// Override PK from config if using query mode
		if cfg.Source.Mode == "query" {
			for _, qt := range cfg.Source.Query.Tables {
				if qt.Name == table && len(qt.PrimaryKey) > 0 {
					ts.PK = qt.PrimaryKey
				}
			}
		}
		schemas[table] = ts
		log.Printf("discovered schema for %s: %d columns, pk=%v", table, len(ts.Columns), ts.PK)
	}
	pgConn.Close(ctx)

	// Initialize sink
	s, err := sink.NewSink(cfg.Sink)
	if err != nil {
		return fmt.Errorf("create sink: %w", err)
	}

	for _, ts := range schemas {
		if err := s.RegisterTable(ctx, ts); err != nil {
			return fmt.Errorf("register table %s: %w", ts.Table, err)
		}
	}

	// Initialize source
	var src source.Source
	switch cfg.Source.Mode {
	case "query":
		qs := source.NewQuerySource(cfg.Source.Postgres, cfg.Source.Query)
		if cp.Mode == "query" && cp.Watermark != "" {
			t, err := time.Parse(time.RFC3339Nano, cp.Watermark)
			if err == nil {
				for _, qt := range cfg.Source.Query.Tables {
					qs.SetWatermark(qt.Name, t)
				}
				log.Printf("restored query watermark: %v", t)
			}
		}
		src = qs

	case "logical":
		ls := source.NewLogicalSource(cfg.Source.Postgres, cfg.Source.Logical)
		if cp.Mode == "logical" && cp.LSN > 0 {
			ls.SetStartLSN(cp.LSN)
			log.Printf("restored logical LSN: %d", cp.LSN)
		}
		src = ls

	default:
		return fmt.Errorf("unknown source mode: %s", cfg.Source.Mode)
	}

	defer func() {
		if err := src.Close(); err != nil {
			log.Printf("source close error: %v", err)
		}
	}()

	// Start capture
	events := make(chan source.ChangeEvent, 1000)
	errCh := make(chan error, 1)

	go func() {
		errCh <- src.Capture(ctx, events)
	}()

	// Main loop: consume events, flush periodically
	flushInterval := cfg.Sink.FlushDuration()
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	flushRows := cfg.Sink.FlushRows
	eventCount := 0

	log.Printf("pg2iceberg started (mode=%s, flush_interval=%s, flush_rows=%d)",
		cfg.Source.Mode, flushInterval, flushRows)

	for {
		select {
		case event, ok := <-events:
			if !ok {
				// Channel closed, flush remaining
				return flush(ctx, s, src, cfg, store, cp)
			}

			if err := s.Write(event); err != nil {
				log.Printf("write error: %v", err)
				continue
			}
			eventCount++

			// Flush on row count threshold
			if s.TotalBuffered() >= flushRows {
				if err := flush(ctx, s, src, cfg, store, cp); err != nil {
					log.Printf("flush error: %v", err)
				}
				eventCount = 0
			}

		case <-flushTicker.C:
			if s.ShouldFlush() {
				if err := flush(ctx, s, src, cfg, store, cp); err != nil {
					log.Printf("flush error: %v", err)
				}
				eventCount = 0
			}

		case err := <-errCh:
			// Source exited. Flush any remaining data.
			if s.ShouldFlush() {
				if flushErr := flush(ctx, s, src, cfg, store, cp); flushErr != nil {
					log.Printf("final flush error: %v", flushErr)
				}
			}
			return err
		}
	}
}

func flush(ctx context.Context, s *sink.Sink, src source.Source, cfg *config.Config, store *state.Store, cp *state.Checkpoint) error {
	if err := s.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	// Update checkpoint
	cp.Mode = cfg.Source.Mode
	switch cfg.Source.Mode {
	case "logical":
		if ls, ok := src.(*source.LogicalSource); ok {
			cp.LSN = ls.ConfirmedLSN()
		}
	}

	if err := store.Save(cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	return nil
}

func getTables(cfg *config.Config) []string {
	switch cfg.Source.Mode {
	case "query":
		tables := make([]string, len(cfg.Source.Query.Tables))
		for i, t := range cfg.Source.Query.Tables {
			tables[i] = t.Name
		}
		return tables
	case "logical":
		return cfg.Source.Logical.Tables
	default:
		return nil
	}
}
