package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pg2iceberg/pg2iceberg/api"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to config file")
	serverMode := flag.Bool("server", false, "run in multi-tenant API server mode")
	listenAddr := flag.String("listen", ":8080", "API server listen address")
	storeDSN := flag.String("store-url", "", "postgres URL for pipeline store, e.g. postgresql://user:pass@host:5432/db (server mode)")
	storeDir := flag.String("store-dir", "./pipelines", "file-based pipeline store directory (server mode, used if -store-dsn is not set)")

	flag.Parse()

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

	var err error
	if *serverMode {
		err = runServer(ctx, *listenAddr, *storeDSN, *storeDir)
	} else {
		err = runSingle(ctx, *configPath)
	}
	if err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func runSingle(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	p, err := pipeline.BuildPipeline(ctx, "default", cfg)
	if err != nil {
		return fmt.Errorf("build pipeline: %w", err)
	}
	if err := p.Start(ctx); err != nil {
		return err
	}

	// Start a lightweight metrics server.
	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	startMetricsServer(ctx, metricsAddr, p)

	<-p.Done()
	if status, err := p.Status(); status == pipeline.StatusError && err != nil {
		return err
	}
	return nil
}

func startMetricsServer(ctx context.Context, addr string, p *pipeline.Pipeline) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.Metrics())
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

func runServer(ctx context.Context, listenAddr, storeDSN, storeDir string) error {
	var store pipeline.PipelineStore

	if storeDSN != "" {
		pgStore, err := pipeline.NewPgStore(ctx, storeDSN)
		if err != nil {
			return fmt.Errorf("connect to pipeline store: %w", err)
		}
		defer pgStore.Close()
		store = pgStore
		log.Printf("using postgres pipeline store")
	} else {
		store = pipeline.NewFileStore(storeDir)
		log.Printf("using file-based pipeline store at %s", storeDir)
	}

	mgr := pipeline.NewManager(ctx, store)

	if err := mgr.RestoreAll(); err != nil {
		return fmt.Errorf("restore pipelines: %w", err)
	}

	srv := api.NewServer(mgr, listenAddr)

	go func() {
		<-ctx.Done()
		mgr.StopAll()
	}()

	return srv.Run(ctx)
}
