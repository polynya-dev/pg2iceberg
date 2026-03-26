package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pg2iceberg/pg2iceberg/api"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
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

	if *serverMode {
		runServer(ctx, *listenAddr, *storeDSN, *storeDir)
	} else {
		runSingle(ctx, *configPath)
	}
}

func runSingle(ctx context.Context, configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	p := pipeline.NewPipeline("default", cfg)
	if err := p.Start(ctx); err != nil {
		log.Fatalf("fatal: %v", err)
	}

	<-p.Done()
	if status, err := p.Status(); status == pipeline.StatusError && err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func runServer(ctx context.Context, listenAddr, storeDSN, storeDir string) {
	var store pipeline.PipelineStore

	if storeDSN != "" {
		pgStore, err := pipeline.NewPgStore(ctx, storeDSN)
		if err != nil {
			log.Fatalf("connect to pipeline store: %v", err)
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
		log.Fatalf("restore pipelines: %v", err)
	}

	srv := api.NewServer(mgr, listenAddr)

	go func() {
		<-ctx.Done()
		mgr.StopAll()
	}()

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
