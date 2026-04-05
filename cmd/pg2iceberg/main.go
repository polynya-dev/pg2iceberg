package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/query"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to config file")

	flag.Parse()

	if os.Getenv("PPROF") != "" {
		go func() {
			log.Println("pprof listening on :6060")
			http.ListenAndServe(":6060", nil)
		}()
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

	err := runSingle(ctx, *configPath)
	if err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

// runner is the common interface for both pipeline modes.
type runner interface {
	Done() <-chan struct{}
	Status() (pipeline.Status, error)
	Metrics() pipeline.Metrics
}

func runSingle(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	var r runner

	switch cfg.Source.Mode {
	case "query":
		qp, err := query.BuildPipeline(ctx, "default", cfg)
		if err != nil {
			return fmt.Errorf("build query pipeline: %w", err)
		}
		if err := qp.Start(ctx); err != nil {
			return err
		}
		r = qp

	default: // "logical" or unspecified — use the existing pipeline
		p, err := pipeline.BuildPipeline(ctx, "default", cfg)
		if err != nil {
			return fmt.Errorf("build pipeline: %w", err)
		}
		if err := p.Start(ctx); err != nil {
			return err
		}
		r = p
	}

	// Start a lightweight metrics server.
	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	startMetricsServer(ctx, metricsAddr, r)

	<-r.Done()
	if status, err := r.Status(); status == pipeline.StatusError && err != nil {
		return err
	}
	return nil
}

func startMetricsServer(ctx context.Context, addr string, r runner) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r2 *http.Request) {
		status, _ := r.Status()
		if status == pipeline.StatusError {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(r.Metrics())
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	go func() {
		log.Printf("[metrics] listening on %s", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("[metrics] server error: %v", err)
		}
	}()
}

