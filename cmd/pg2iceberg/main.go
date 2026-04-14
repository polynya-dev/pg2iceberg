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
	"github.com/pg2iceberg/pg2iceberg/stream"
	"github.com/pg2iceberg/pg2iceberg/snapshot"
	"github.com/pg2iceberg/pg2iceberg/tracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Set at build time via: go build -ldflags "-X main.commitSHA=$(git rev-parse --short HEAD)"
var commitSHA string

func main() {
	pipeline.CommitSHA = commitSHA

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
	catalogAuth := flag.String("catalog-auth", "", "catalog auth mode: none, sigv4, bearer (env: CATALOG_AUTH)")
	catalogToken := flag.String("catalog-token", "", "Bearer token for catalog auth (env: CATALOG_TOKEN)")
	credentialMode := flag.String("credential-mode", "", "credential mode: static or vended (env: CREDENTIAL_MODE)")
	warehouse := flag.String("warehouse", "", "Iceberg warehouse path (env: WAREHOUSE)")
	namespace := flag.String("namespace", "", "Iceberg namespace (env: NAMESPACE)")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint URL (env: S3_ENDPOINT)")
	s3AccessKey := flag.String("s3-access-key", "", "S3 access key (env: S3_ACCESS_KEY)")
	s3SecretKey := flag.String("s3-secret-key", "", "S3 secret key (env: S3_SECRET_KEY)")
	s3Region := flag.String("s3-region", "", "S3 region (env: S3_REGION)")

	// Behavior
	snapshotOnly := flag.Bool("snapshot-only", false, "exit after initial snapshot completes (env: SNAPSHOT_ONLY)")
	compactOnly := flag.Bool("compact", false, "run one compaction pass on all tables and exit")
	maintainOnly := flag.Bool("maintain", false, "run one maintenance pass (snapshot expiry + orphan cleanup) and exit")
	cleanupOnly := flag.Bool("cleanup", false, "drop replication slot, publication, and _pg2iceberg schema, then exit")
	materializerOnly := flag.Bool("materializer-only", false, "run materializer worker only (no WAL capture; requires --materializer-worker-id)")
	streamOnly := flag.Bool("stream-only", false, "run WAL writer only (no materializer)")
	materializerWorkerID := flag.String("materializer-worker-id", "", "worker ID for distributed materializer (env: MATERIALIZER_WORKER_ID)")

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
		postgresURL:          *postgresURL,
		tables:               *tables,
		mode:                 *mode,
		slotName:             *slotName,
		pubName:              *pubName,
		icebergCatalogURL:    *icebergCatalogURL,
		catalogAuth:          *catalogAuth,
		catalogToken:         *catalogToken,
		credentialMode:       *credentialMode,
		warehouse:            *warehouse,
		namespace:            *namespace,
		s3Endpoint:           *s3Endpoint,
		s3AccessKey:          *s3AccessKey,
		s3SecretKey:          *s3SecretKey,
		s3Region:             *s3Region,
		snapshotOnly:         *snapshotOnly,
		statePostgresURL:     *statePostgresURL,
		metricsAddr:          *metricsAddr,
		materializerWorkerID: *materializerWorkerID,
	})
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}

	shutdownTracing, err := tracing.Init(ctx, "pg2iceberg", commitSHA)
	if err != nil {
		log.Printf("warning: failed to initialize tracing: %v", err)
	} else {
		defer shutdownTracing(ctx)
	}

	var runErr error
	if *cleanupOnly {
		runErr = runCleanup(ctx, cfg)
	} else if *maintainOnly {
		runErr = runMaintain(ctx, cfg)
	} else if *compactOnly {
		runErr = runCompact(ctx, cfg)
	} else if cfg.SnapshotOnly {
		runErr = runSnapshotOnly(ctx, cfg)
	} else if *materializerOnly {
		if cfg.Sink.MaterializerWorkerID == "" {
			log.Fatalf("fatal: --materializer-only requires --materializer-worker-id")
		}
		runErr = runMaterializerOnly(ctx, cfg)
	} else if *streamOnly {
		runErr = runStreamOnly(ctx, cfg)
	} else {
		runErr = runPipeline(ctx, cfg)
	}
	if runErr != nil {
		log.Printf("fatal: %v", runErr)
		os.Exit(1)
	}
}

type cliOverrides struct {
	postgresURL          string
	tables               string
	mode                 string
	slotName             string
	pubName              string
	icebergCatalogURL    string
	catalogAuth          string
	catalogToken         string
	credentialMode       string
	warehouse            string
	namespace            string
	s3Endpoint           string
	s3AccessKey          string
	s3SecretKey          string
	s3Region             string
	snapshotOnly         bool
	statePostgresURL     string
	metricsAddr          string
	materializerWorkerID string
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
	if cli.catalogAuth != "" {
		cfg.Sink.CatalogAuth = cli.catalogAuth
	}
	if cli.catalogToken != "" {
		cfg.Sink.CatalogToken = cli.catalogToken
	}
	if cli.credentialMode != "" {
		cfg.Sink.CredentialMode = cli.credentialMode
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
	if cli.materializerWorkerID != "" {
		cfg.Sink.MaterializerWorkerID = cli.materializerWorkerID
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

// runStreamOnly runs the WAL writer (logical replication) without a materializer.
// Use this when materializer workers run as separate processes.
func runStreamOnly(ctx context.Context, cfg *config.Config) error {
	log.Println("[stream-only] starting WAL writer only (no materializer)")

	// Disable the materializer.
	cfg.Sink.MaterializerInterval = "0"

	lp, err := logical.BuildPipeline(ctx, "default", cfg)
	if err != nil {
		return fmt.Errorf("build logical pipeline: %w", err)
	}
	if err := lp.Start(ctx); err != nil {
		return err
	}

	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	startMetricsServer(ctx, metricsAddr, lp)

	<-lp.Done()
	if status, err := lp.Status(); status == pipeline.StatusError && err != nil {
		return err
	}
	return nil
}

// runMaterializerOnly runs a standalone materializer worker. It reads staged
// events from the Stream (S3 + PG coordinator) and writes to Iceberg.
// No WAL capture — the WAL writer runs as a separate process.
func runMaterializerOnly(ctx context.Context, cfg *config.Config) error {
	workerID := cfg.Sink.MaterializerWorkerID
	log.Printf("[materializer-only] starting worker %s", workerID)

	// Connect to source PG to discover schemas.
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
		schemas[tc.Name] = ts
		log.Printf("[materializer-only] discovered schema for %s: %d columns, pk=%v", tc.Name, len(ts.Columns), ts.PK)
	}
	pgConn.Close(ctx)

	// Create Iceberg clients.
	clients, err := iceberg.NewClients(cfg.Sink)
	if err != nil {
		return fmt.Errorf("create iceberg clients: %w", err)
	}

	// Create coordinator.
	coord, err := stream.NewPgCoordinatorWithSchema(ctx, cfg.Source.Postgres.DSN(), cfg.State.CoordinatorSchema)
	if err != nil {
		return fmt.Errorf("create coordinator: %w", err)
	}
	defer coord.Close()

	if err := coord.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate coordinator: %w", err)
	}

	// Register tables in a temporary sink to get tableSink state.
	snk := logical.NewSink(cfg.Sink, cfg.Tables, workerID, clients.S3, clients.Catalog)

	// Ensure S3 is initialized (vended credentials).
	if clients.S3 == nil && len(cfg.Tables) > 0 {
		firstTable := postgres.TableToIceberg(cfg.Tables[0].Name)
		if err := clients.EnsureStorage(ctx, cfg.Sink.Namespace, firstTable); err != nil {
			return fmt.Errorf("ensure storage: %w", err)
		}
		snk.SetS3(clients.S3)
	}

	str := stream.NewStream(coord, snk.S3(), cfg.Sink.Namespace)
	snk.SetStream(str)

	for _, ts := range schemas {
		if err := snk.RegisterTable(ctx, ts); err != nil {
			return fmt.Errorf("register table %s: %w", ts.Table, err)
		}
	}

	// Ensure cursors for the consumer group (scoped by publication name).
	group := cfg.Source.Logical.PublicationName
	for pgTable := range schemas {
		if err := coord.EnsureCursor(ctx, group, pgTable); err != nil {
			return fmt.Errorf("ensure cursor for %s: %w", pgTable, err)
		}
	}

	// Create and run materializer.
	mat := logical.NewMaterializer(cfg.Sink, snk.Catalog(), snk.S3(), snk.Tables(), str)
	mat.WorkerID = workerID
	mat.ConsumerGroup = cfg.Source.Logical.PublicationName

	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
		mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		})
		srv := &http.Server{Addr: metricsAddr, Handler: mux}
		go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("[materializer-only] metrics server error: %v", err)
		}
	}()

	log.Printf("[materializer-only] worker %s running (interval=%s)", workerID, cfg.Sink.MaterializerDuration())
	mat.Run(ctx)

	log.Printf("[materializer-only] worker %s stopped", workerID)
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
	cp, err := cpStore.Load(ctx, "default")
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
	clients, err := iceberg.NewClients(cfg.Sink)
	if err != nil {
		return fmt.Errorf("create iceberg clients: %w", err)
	}
	catalog := clients.Catalog

	// Ensure namespace and create/load Iceberg tables.
	if err := catalog.EnsureNamespace(ctx, cfg.Sink.Namespace); err != nil {
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

		matTm, err := catalog.LoadTable(ctx, cfg.Sink.Namespace, icebergName)
		if err != nil {
			return fmt.Errorf("load table %s: %w", icebergName, err)
		}
		if matTm == nil {
			var location string
			if wh := clients.Warehouse(); wh != "" {
				location = fmt.Sprintf("%s%s.db/%s", wh, cfg.Sink.Namespace, icebergName)
			}
			if _, err := catalog.CreateTable(ctx, cfg.Sink.Namespace, icebergName, ts, location, partSpec); err != nil {
				return fmt.Errorf("create table %s: %w", icebergName, err)
			}
			log.Printf("[snapshot] created table %s.%s", cfg.Sink.Namespace, icebergName)
		}

		// In vended mode, initialize storage after first LoadTable/CreateTable.
		if clients.S3 == nil {
			if err := clients.EnsureStorage(ctx, cfg.Sink.Namespace, icebergName); err != nil {
				return fmt.Errorf("ensure storage: %w", err)
			}
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
		S3:         clients.S3,
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
	cp, err = cpStore.Load(ctx, "default")
	if err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	cp.SnapshotComplete = true
	cp.SnapshotedTables = nil
	cp.SnapshotChunks = nil
	if err := cpStore.Save(ctx, "default", cp); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	log.Println("[snapshot] snapshot-only complete")
	return nil
}

// runCompact performs a single compaction pass on all configured tables.
func runCompact(ctx context.Context, cfg *config.Config) error {
	log.Println("[compact] starting compaction")

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
		schemas[tc.Name] = ts
	}
	pgConn.Close(ctx)

	// Iceberg clients.
	clients, err := iceberg.NewClients(cfg.Sink)
	if err != nil {
		return fmt.Errorf("create iceberg clients: %w", err)
	}
	catalog := clients.Catalog

	// Ensure storage is available.
	if clients.S3 == nil && len(cfg.Tables) > 0 {
		firstTable := postgres.TableToIceberg(cfg.Tables[0].Name)
		if err := clients.EnsureStorage(ctx, cfg.Sink.Namespace, firstTable); err != nil {
			return fmt.Errorf("ensure storage: %w", err)
		}
	}

	cc := iceberg.CompactionConfig{
		DataFileThreshold:   cfg.Sink.CompactionDataFilesOrDefault(),
		DeleteFileThreshold: cfg.Sink.CompactionDeleteFilesOrDefault(),
	}

	var compacted int
	for _, tc := range cfg.Tables {
		ts := schemas[tc.Name]
		icebergName := postgres.TableToIceberg(tc.Name)

		var partExprs []string
		if len(tc.Iceberg.Partition) > 0 {
			partExprs = tc.Iceberg.Partition
		}
		partSpec, err := iceberg.BuildPartitionSpec(partExprs, ts)
		if err != nil {
			return fmt.Errorf("build partition spec for %s: %w", tc.Name, err)
		}

		// Load table to get schema ID.
		matTm, err := catalog.LoadTable(ctx, cfg.Sink.Namespace, icebergName)
		if err != nil {
			return fmt.Errorf("load table %s: %w", icebergName, err)
		}
		if matTm == nil {
			log.Printf("[compact] table %s does not exist, skipping", icebergName)
			continue
		}

		tw := iceberg.NewTableWriter(iceberg.TableWriteConfig{
			Namespace:   cfg.Sink.Namespace,
			IcebergName: icebergName,
			SrcSchema:   ts,
			PartSpec:    partSpec,
			SchemaID:    matTm.Metadata.CurrentSchemaID,
			TargetSize:  cfg.Sink.MaterializerTargetFileSizeOrDefault(),
			Concurrency: cfg.Sink.MaterializerConcurrencyOrDefault(),
		}, catalog, clients.S3)

		prepared, err := tw.Compact(ctx, ts.PK, cc)
		if err != nil {
			log.Printf("[compact] error compacting %s: %v", icebergName, err)
			continue
		}
		if prepared == nil {
			log.Printf("[compact] %s: below thresholds, skipping", icebergName)
			continue
		}

		if err := catalog.CommitSnapshot(ctx, cfg.Sink.Namespace, prepared.IcebergName, prepared.PrevSnapshotID, prepared.Commit); err != nil {
			log.Printf("[compact] commit error for %s: %v", icebergName, err)
			continue
		}

		log.Printf("[compact] %s: %d data + %d delete files -> %d files (%d rows deleted)",
			icebergName, prepared.DataCount+prepared.DeleteCount, prepared.DeleteCount, prepared.BucketCount, prepared.DeleteRowCount)
		compacted++
	}

	log.Printf("[compact] done, %d table(s) compacted", compacted)
	return nil
}

// runMaintain performs a single maintenance pass on all configured tables.
func runMaintain(ctx context.Context, cfg *config.Config) error {
	log.Println("[maintain] starting maintenance")

	clients, err := iceberg.NewClients(cfg.Sink)
	if err != nil {
		return fmt.Errorf("create iceberg clients: %w", err)
	}
	catalog, ok := clients.Catalog.(*iceberg.MetadataStore)
	if !ok {
		return fmt.Errorf("maintenance requires MetadataStore catalog")
	}

	// Ensure storage is available.
	if clients.S3 == nil && len(cfg.Tables) > 0 {
		firstTable := postgres.TableToIceberg(cfg.Tables[0].Name)
		if err := clients.EnsureStorage(ctx, cfg.Sink.Namespace, firstTable); err != nil {
			return fmt.Errorf("ensure storage: %w", err)
		}
	}

	mc := iceberg.MaintenanceConfig{
		SnapshotRetention: cfg.Sink.MaintenanceRetentionOrDefault(),
		OrphanGracePeriod: cfg.Sink.MaintenanceGraceOrDefault(),
	}

	log.Printf("[maintain] retention=%s, grace=%s", mc.SnapshotRetention, mc.OrphanGracePeriod)

	for _, tc := range cfg.Tables {
		icebergName := postgres.TableToIceberg(tc.Name)

		// Maintain materialized table.
		if err := iceberg.MaintainTable(ctx, catalog, clients.S3, cfg.Sink.Namespace, icebergName, mc); err != nil {
			log.Printf("[maintain] error on %s: %v", icebergName, err)
		}

		// In logical mode, also maintain events table.
		if cfg.Source.Mode == "logical" {
			eventsName := iceberg.EventsTableName(icebergName)
			if err := iceberg.MaintainTable(ctx, catalog, clients.S3, cfg.Sink.Namespace, eventsName, mc); err != nil {
				log.Printf("[maintain] error on %s: %v", eventsName, err)
			}
		}
	}

	log.Println("[maintain] done")
	return nil
}

func runCleanup(ctx context.Context, cfg *config.Config) error {
	dsn := cfg.Source.Postgres.DSN()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(ctx)

	schema := "_pg2iceberg"
	if cfg.State.CoordinatorSchema != "" {
		schema = cfg.State.CoordinatorSchema
	}

	return logical.Cleanup(ctx, conn, cfg.Source.Logical.SlotName, cfg.Source.Logical.PublicationName, schema)
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
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r2 *http.Request) {
		status, _ := r.Status()
		w.Header().Set("Content-Type", "application/json")
		if status != pipeline.StatusRunning {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(r.Metrics())
	})
	mux.HandleFunc("/tables", func(w http.ResponseWriter, r2 *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if tl, ok := r.(pipeline.TableLister); ok {
			json.NewEncoder(w).Encode(tl.Tables())
		} else {
			w.WriteHeader(http.StatusNotImplemented)
			json.NewEncoder(w).Encode(map[string]string{"error": "table listing not available"})
		}
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
