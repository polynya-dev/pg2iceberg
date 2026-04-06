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

	"github.com/jackc/pgx/v5"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/iceberg"
	"github.com/pg2iceberg/pg2iceberg/logical"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/pg2iceberg/pg2iceberg/postgres"
	"github.com/pg2iceberg/pg2iceberg/query"
	"github.com/pg2iceberg/pg2iceberg/snapshot"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "", "path to config file")

	// Connection
	postgresURL := flag.String("postgres-url", "", "PostgreSQL connection URL (env: POSTGRES_URL)")

	// Replication
	tables := flag.String("tables", "", "comma-separated list of tables (env: TABLES)")
	mode := flag.String("mode", "", "replication mode: logical or query (env: MODE)")
	slotName := flag.String("slot-name", "", "logical replication slot name (env: SLOT_NAME)")
	pubName := flag.String("publication-name", "", "logical replication publication name (env: PUBLICATION_NAME)")

	// Iceberg / S3
	icebergCatalogURL := flag.String("iceberg-catalog-url", "", "Iceberg REST catalog URL (env: ICEBERG_CATALOG_URL)")
	warehouse := flag.String("warehouse", "", "Iceberg warehouse path (env: WAREHOUSE)")
	namespace := flag.String("namespace", "", "Iceberg namespace (env: NAMESPACE)")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint URL (env: S3_ENDPOINT)")
	s3AccessKey := flag.String("s3-access-key", "", "S3 access key (env: S3_ACCESS_KEY)")
	s3SecretKey := flag.String("s3-secret-key", "", "S3 secret key (env: S3_SECRET_KEY)")
	s3Region := flag.String("s3-region", "", "S3 region (env: S3_REGION)")

	// Behavior
	snapshotOnly := flag.Bool("snapshot-only", false, "exit after initial snapshot completes (env: SNAPSHOT_ONLY)")

	// State & metrics
	statePostgresURL := flag.String("state-postgres-url", "", "PostgreSQL URL for state storage (env: STATE_POSTGRES_URL)")
	metricsAddr := flag.String("metrics-addr", "", "metrics server address (env: METRICS_ADDR)")

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

	// Build config: YAML (optional) → env vars → CLI flags → defaults → validate
	cfg, err := buildConfig(*configPath, cliOverrides{
		postgresURL:       *postgresURL,
		tables:            *tables,
		mode:              *mode,
		slotName:          *slotName,
		pubName:           *pubName,
		icebergCatalogURL: *icebergCatalogURL,
		warehouse:         *warehouse,
		namespace:         *namespace,
		s3Endpoint:        *s3Endpoint,
		s3AccessKey:       *s3AccessKey,
		s3SecretKey:       *s3SecretKey,
		s3Region:          *s3Region,
		snapshotOnly:      *snapshotOnly,
		statePostgresURL:  *statePostgresURL,
		metricsAddr:       *metricsAddr,
	})
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}

	var runErr error
	if cfg.SnapshotOnly {
		runErr = runSnapshotOnly(ctx, cfg)
	} else {
		runErr = runPipeline(ctx, cfg)
	}
	if runErr != nil {
		log.Printf("fatal: %v", runErr)
		os.Exit(1)
	}
}

type cliOverrides struct {
	postgresURL       string
	tables            string
	mode              string
	slotName          string
	pubName           string
	icebergCatalogURL string
	warehouse         string
	namespace         string
	s3Endpoint        string
	s3AccessKey       string
	s3SecretKey       string
	s3Region          string
	snapshotOnly      bool
	statePostgresURL  string
	metricsAddr       string
}

func buildConfig(configPath string, cli cliOverrides) (*config.Config, error) {
	var cfg config.Config

	// 1. Load YAML base config if provided.
	if configPath != "" {
		loaded, err := config.LoadYAML(configPath)
		if err != nil {
			return nil, fmt.Errorf("load config: %w", err)
		}
		cfg = *loaded
	}

	// 2. Apply environment variable overrides.
	if err := cfg.ApplyEnv(); err != nil {
		return nil, fmt.Errorf("apply env: %w", err)
	}

	// 3. Apply CLI flag overrides (highest priority).
	if cli.postgresURL != "" {
		pg, err := config.ParsePostgresURL(cli.postgresURL)
		if err != nil {
			return nil, fmt.Errorf("parse --postgres-url: %w", err)
		}
		cfg.Source.Postgres = pg
	}
	if cli.tables != "" {
		cfg.Tables = config.ParseTables(cli.tables)
	}
	if cli.mode != "" {
		cfg.Source.Mode = cli.mode
	}
	if cli.slotName != "" {
		cfg.Source.Logical.SlotName = cli.slotName
	}
	if cli.pubName != "" {
		cfg.Source.Logical.PublicationName = cli.pubName
	}
	if cli.icebergCatalogURL != "" {
		cfg.Sink.CatalogURI = cli.icebergCatalogURL
	}
	if cli.warehouse != "" {
		cfg.Sink.Warehouse = cli.warehouse
	}
	if cli.namespace != "" {
		cfg.Sink.Namespace = cli.namespace
	}
	if cli.s3Endpoint != "" {
		cfg.Sink.S3Endpoint = cli.s3Endpoint
	}
	if cli.s3AccessKey != "" {
		cfg.Sink.S3AccessKey = cli.s3AccessKey
	}
	if cli.s3SecretKey != "" {
		cfg.Sink.S3SecretKey = cli.s3SecretKey
	}
	if cli.s3Region != "" {
		cfg.Sink.S3Region = cli.s3Region
	}
	if cli.snapshotOnly {
		cfg.SnapshotOnly = true
	}
	if cli.statePostgresURL != "" {
		cfg.State.PostgresURL = cli.statePostgresURL
	}
	if cli.metricsAddr != "" {
		cfg.MetricsAddr = cli.metricsAddr
	}

	// 4. Apply defaults and validate.
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func runPipeline(ctx context.Context, cfg *config.Config) error {
	var r pipeline.Pipeline

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

	default: // "logical" or unspecified
		lp, err := logical.BuildPipeline(ctx, "default", cfg)
		if err != nil {
			return fmt.Errorf("build logical pipeline: %w", err)
		}
		if err := lp.Start(ctx); err != nil {
			return err
		}
		r = lp
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

// runSnapshotOnly performs a one-shot snapshot of all configured tables and exits.
// It bypasses both logical and query pipelines — no replication slot, no WAL
// streaming, no watermark polling.
func runSnapshotOnly(ctx context.Context, cfg *config.Config) error {
	log.Println("[snapshot] starting snapshot-only mode")

	// Checkpoint store.
	cpStore, err := pipeline.NewCheckpointStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create checkpoint store: %w", err)
	}
	defer cpStore.Close()

	// Check if snapshot already completed.
	cp, err := cpStore.Load("default")
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	if cp.SnapshotComplete {
		log.Println("[snapshot] snapshot already completed (per checkpoint), nothing to do")
		return nil
	}

	// Discover schemas.
	pgConn, err := pgx.Connect(ctx, cfg.Source.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}

	schemas := make(map[string]*postgres.TableSchema)
	for _, tc := range cfg.Tables {
		ts, err := postgres.DiscoverSchema(ctx, pgConn, tc.Name)
		if err != nil {
			pgConn.Close(ctx)
			return fmt.Errorf("discover schema for %s: %w", tc.Name, err)
		}
		if err := ts.Validate(); err != nil {
			pgConn.Close(ctx)
			return err
		}
		schemas[tc.Name] = ts
		log.Printf("[snapshot] discovered schema for %s: %d columns, pk=%v", tc.Name, len(ts.Columns), ts.PK)
	}
	pgConn.Close(ctx)

	// Iceberg catalog and S3.
	var httpClient *http.Client
	if cfg.Sink.CatalogAuth == "sigv4" {
		transport, err := iceberg.NewSigV4Transport(cfg.Sink.S3Region)
		if err != nil {
			return fmt.Errorf("create sigv4 transport: %w", err)
		}
		httpClient = &http.Client{Transport: transport}
	}
	catalog := iceberg.NewCatalogClient(cfg.Sink.CatalogURI, httpClient)

	s3Client, err := iceberg.NewS3Client(cfg.Sink.S3Endpoint, cfg.Sink.S3AccessKey, cfg.Sink.S3SecretKey, cfg.Sink.S3Region, cfg.Sink.Warehouse)
	if err != nil {
		return fmt.Errorf("create s3 client: %w", err)
	}

	// Ensure namespace and create/load Iceberg tables.
	if err := catalog.EnsureNamespace(cfg.Sink.Namespace); err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}

	for pgTable, ts := range schemas {
		icebergName := postgres.TableToIceberg(pgTable)

		var partExprs []string
		for _, tc := range cfg.Tables {
			if tc.Name == pgTable {
				partExprs = tc.Iceberg.Partition
				break
			}
		}
		partSpec, err := iceberg.BuildPartitionSpec(partExprs, ts)
		if err != nil {
			return fmt.Errorf("build partition spec for %s: %w", pgTable, err)
		}

		matTm, err := catalog.LoadTable(cfg.Sink.Namespace, icebergName)
		if err != nil {
			return fmt.Errorf("load table %s: %w", icebergName, err)
		}
		if matTm == nil {
			location := fmt.Sprintf("%s%s.db/%s", cfg.Sink.Warehouse, cfg.Sink.Namespace, icebergName)
			if _, err := catalog.CreateTable(cfg.Sink.Namespace, icebergName, ts, location, partSpec); err != nil {
				return fmt.Errorf("create table %s: %w", icebergName, err)
			}
			log.Printf("[snapshot] created table %s.%s", cfg.Sink.Namespace, icebergName)
		}
	}

	// Build snapshot tables.
	var tables []snapshot.Table
	for _, tc := range cfg.Tables {
		ts := schemas[tc.Name]
		if ts == nil {
			continue
		}
		tables = append(tables, snapshot.Table{Name: tc.Name, Schema: ts})
	}

	dsn := cfg.Source.Postgres.DSN()
	txFactory := func(ctx context.Context) (pgx.Tx, func(context.Context), error) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("snapshot connect: %w", err)
		}
		cleanup := func(ctx context.Context) { conn.Close(ctx) }
		tx, err := conn.Begin(ctx)
		if err != nil {
			cleanup(ctx)
			return nil, nil, fmt.Errorf("begin snapshot tx: %w", err)
		}
		if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			tx.Rollback(ctx)
			cleanup(ctx)
			return nil, nil, fmt.Errorf("set isolation level: %w", err)
		}
		return tx, cleanup, nil
	}

	deps := snapshot.Deps{
		Catalog:    catalog,
		S3:         s3Client,
		SinkCfg:    cfg.Sink,
		LogicalCfg: cfg.Source.Logical,
		TableCfgs:  cfg.Tables,
		Schemas:    schemas,
		Store:      cpStore,
		PipelineID: "default",
	}

	concurrency := cfg.Source.Logical.SnapshotConcurrencyOrDefault()
	snap := snapshot.NewSnapshotter(tables, txFactory, concurrency, deps)
	if _, err := snap.Run(ctx); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}

	// Mark snapshot complete.
	cp, err = cpStore.Load("default")
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	cp.SnapshotComplete = true
	cp.SnapshotedTables = nil
	cp.SnapshotChunks = nil
	if err := cpStore.Save("default", cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	log.Println("[snapshot] snapshot-only complete")
	return nil
}

func startMetricsServer(ctx context.Context, addr string, r pipeline.Pipeline) {
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
