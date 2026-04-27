//! Phase C integration: full `LogicalLifecycle` end-to-end against a
//! real container stack.
//!
//! Stack:
//!   - source Postgres (logical replication enabled)
//!   - MinIO (S3-compatible object store)
//!   - Apache iceberg-rest-fixture (REST catalog, SQLite-backed)
//!
//! All four containers share a per-test Docker network so iceberg-rest
//! can reach MinIO via `http://minio:9000`. The host accesses each
//! container via mapped ports.
//!
//! Gated behind `--features integration`. Run with the env vars
//! documented in the integration_coord file's header doc.
//!
//! This is a slow test (≈30–60s of container startup per test
//! invocation). We keep it to one focused scenario — INSERT N rows in
//! PG, drive the lifecycle, verify N rows land in Iceberg with
//! correct contents — to keep CI cost reasonable. Negative scenarios
//! (failure injection, partition transforms, schema evolution) are
//! covered by sim DST already.

#![cfg(feature = "integration")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pg2iceberg::config::{
    Config, LogicalConfig, PostgresConfig, SinkConfig, SourceConfig, StateConfig, TableConfig,
};
use pg2iceberg::run::build_rest_catalog;
use pg2iceberg_core::{ColumnName, Namespace, PgValue, TableIdent};
use pg2iceberg_iceberg::{prod::IcebergRustCatalog, read_materialized_state};
use pg2iceberg_stream::prod::ObjectStoreBlobStore;
use testcontainers::core::IntoContainerPort;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers::core::WaitFor;

/// Create a bucket by exec'ing `mc` inside a transient minio/mc
/// container attached to the same docker network as the MinIO
/// container. This is the canonical pattern from the Go example's
/// docker-compose — and avoids dragging the AWS SDK into our
/// test-only dep tree.
async fn create_bucket_via_mc(network: &str, bucket: &str) {
    use testcontainers::core::wait::ExitWaitStrategy;
    let cmd = format!(
        "until mc alias set local http://minio:9000 minioadmin minioadmin; \
         do sleep 0.5; done; \
         mc mb --ignore-existing local/{bucket}"
    );
    let _container = GenericImage::new("minio/mc", "latest")
        .with_wait_for(WaitFor::Exit(ExitWaitStrategy::new().with_exit_code(0)))
        .with_entrypoint("/bin/sh")
        .with_network(network)
        .with_cmd(["-c".to_string(), cmd])
        .start()
        .await
        .expect("mc create-bucket");
}

fn uniq() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Bring up the full container stack on a fresh per-test network.
/// Returns the source-PG DSN, REST-catalog URL, MinIO host:port,
/// and live container handles (kept alive by the caller).
struct Stack {
    network: String,
    pg_dsn: String,
    rest_url: String,
    minio_host: String,
    minio_port: u16,
    bucket: String,
    _src_pg: ContainerAsync<Postgres>,
    _minio: ContainerAsync<MinIO>,
    _rest: ContainerAsync<GenericImage>,
}

async fn bring_up_stack() -> Stack {
    let network = format!("p2i-it-{}", uniq());
    let bucket = "warehouse".to_string();

    // ── MinIO ────────────────────────────────────────────────────
    let minio: ContainerAsync<MinIO> = MinIO::default()
        .with_network(&network)
        .with_hostname("minio")
        .start()
        .await
        .expect("start minio");
    let minio_host = minio.get_host().await.expect("minio host").to_string();
    let minio_port = minio
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");

    // Create the warehouse bucket via `mc`. Sidecar exits 0 on
    // success; AsyncRunner blocks until the wait_for resolves.
    create_bucket_via_mc(&network, &bucket).await;

    // ── iceberg-rest ─────────────────────────────────────────────
    // SQLite-in-memory keeps the test self-contained. Auth is
    // disabled (no CATALOG_AUTH_* envs) so the client connects
    // anonymously.
    let rest = GenericImage::new("apache/iceberg-rest-fixture", "latest")
        .with_exposed_port(8181.tcp())
        // The image ships a built-in HEALTHCHECK that probes
        // /v1/config — the cleanest readiness signal we have.
        .with_wait_for(WaitFor::healthcheck())
        .with_network(&network)
        .with_hostname("iceberg-rest")
        .with_env_var("AWS_ACCESS_KEY_ID", "minioadmin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("CATALOG_WAREHOUSE", format!("s3://{bucket}/"))
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", "http://minio:9000")
        .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .start()
        .await
        .expect("start iceberg-rest");
    let rest_host = rest.get_host().await.expect("rest host").to_string();
    let rest_port = rest
        .get_host_port_ipv4(8181)
        .await
        .expect("rest port");
    let rest_url = format!("http://{rest_host}:{rest_port}");

    // ── Source PG ────────────────────────────────────────────────
    let src_pg: ContainerAsync<Postgres> = Postgres::default()
        .with_cmd([
            "-c",
            "fsync=off",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ])
        .with_network(&network)
        .with_hostname("source-pg")
        .start()
        .await
        .expect("start source pg");
    let pg_host = src_pg.get_host().await.expect("pg host").to_string();
    let pg_port = src_pg
        .get_host_port_ipv4(5432)
        .await
        .expect("pg port");
    let pg_dsn = format!(
        "host={pg_host} port={pg_port} user=postgres password=postgres dbname=postgres"
    );

    Stack {
        network,
        pg_dsn,
        rest_url,
        minio_host,
        minio_port,
        bucket,
        _src_pg: src_pg,
        _minio: minio,
        _rest: rest,
    }
}

async fn regular_client(dsn: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("regular connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// Build a `Config` that points at the live containers.
fn config_for_stack(stack: &Stack, table: &str, slot: &str, publication: &str) -> Config {
    let pg_host = stack.pg_dsn.split_whitespace().find_map(|kv| {
        kv.strip_prefix("host=").map(str::to_string)
    }).unwrap();
    let pg_port: u16 = stack.pg_dsn.split_whitespace().find_map(|kv| {
        kv.strip_prefix("port=").and_then(|p| p.parse().ok())
    }).unwrap();
    Config {
        tables: vec![TableConfig {
            name: format!("public.{table}"),
            skip_snapshot: false,
            primary_key: vec![],
            watermark_column: String::new(),
            columns: vec![],
            iceberg: Default::default(),
        }],
        source: SourceConfig {
            mode: "logical".into(),
            postgres: PostgresConfig {
                host: pg_host,
                port: pg_port,
                database: "postgres".into(),
                user: "postgres".into(),
                password: "postgres".into(),
                sslmode: "disable".into(),
            },
            logical: LogicalConfig {
                publication_name: publication.into(),
                slot_name: slot.into(),
                standby_interval: String::new(),
            },
            query: Default::default(),
        },
        sink: SinkConfig {
            catalog_uri: stack.rest_url.clone(),
            catalog_auth: String::new(),
            catalog_token: String::new(),
            catalog_client_id: String::new(),
            catalog_client_secret: String::new(),
            credential_mode: "static".into(),
            warehouse: format!("s3://{}/", stack.bucket),
            namespace: "public".into(),
            s3_endpoint: format!("http://{}:{}", stack.minio_host, stack.minio_port),
            s3_access_key: "minioadmin".into(),
            s3_secret_key: "minioadmin".into(),
            s3_region: "us-east-1".into(),
            flush_interval: String::new(),
            // 1000 mirrors the Go reference's default. Sink::new
            // asserts `flush_threshold > 0`, so 0 would crash the
            // pipeline on startup.
            flush_rows: 1000,
            compaction_data_files: 8,
            compaction_delete_files: 4,
            target_file_size: 0, // disable compaction for this test
            maintenance_retention: String::new(),
            maintenance_grace: "30m".into(),
            materialized_prefix: "materialized/".into(),
            catalog_props: BTreeMap::new(),
            meta_namespace: String::new(),
        },
        state: StateConfig {
            path: String::new(),
            postgres_url: String::new(), // coord lives in source PG
            coordinator_schema: format!("_pg2iceberg_{}", uniq()),
            group: "default".into(),
        },
        metrics_addr: String::new(),
        snapshot_only: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_inserts_propagate_pg_to_iceberg() {
    // Surface lifecycle / pipeline / catalog tracing into test output
    // (only when RUST_LOG is set). `try_init` so multiple test
    // invocations in the same process don't conflict.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let stack = bring_up_stack().await;

    let table = format!("accounts_{}", uniq());
    let slot = format!("s_{}", uniq());
    let publication = format!("p_{}", uniq());

    // Create + seed the source table BEFORE starting the lifecycle.
    // The snapshot phase will pick up these rows; the CDC phase
    // picks up anything inserted after `start_replication`.
    let src = regular_client(&stack.pg_dsn).await;
    src.batch_execute(&format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, balance INT NOT NULL)"
    ))
    .await
    .expect("create table");
    for i in 1..=3 {
        src.execute(
            &format!("INSERT INTO {table} (id, balance) VALUES ($1, $2)"),
            &[&i, &(i * 100)],
        )
        .await
        .expect("seed insert");
    }

    let cfg = config_for_stack(&stack, &table, &slot, &publication);

    // Construct the lifecycle the same way the binary does. We build
    // two catalog handles against the same REST server: one is
    // consumed by `build_logical_lifecycle` (which moves the catalog
    // into a private Arc inside the lifecycle), the other we keep on
    // the test side to read materialized state for assertions.
    let rest_for_lifecycle = build_rest_catalog(&cfg).await.expect("rest catalog");
    let lifecycle_catalog = IcebergRustCatalog::new(Arc::new(rest_for_lifecycle));
    let rest_for_assertions = build_rest_catalog(&cfg)
        .await
        .expect("rest catalog (assertions)");
    let assert_catalog = IcebergRustCatalog::new(Arc::new(rest_for_assertions));
    // Build blob through the binary's helper so we exercise the
    // exact prod path. Re-importing it here would cycle the dep
    // graph, so we replicate the small wiring inline — the only
    // thing we'd lose differential-test coverage on is the
    // bucket/prefix parsing, which is unit-tested in run.rs.
    let s3 = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(&stack.bucket)
        .with_region("us-east-1")
        .with_endpoint(format!("http://{}:{}", stack.minio_host, stack.minio_port))
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .build()
        .expect("AmazonS3Builder");
    let blob: Arc<dyn pg2iceberg_stream::BlobStore> =
        Arc::new(ObjectStoreBlobStore::new(Arc::new(s3)));

    let blob_namer: Arc<dyn pg2iceberg_logical::pipeline::BlobNamer> =
        Arc::new(UuidBlobNamer::new("staged"));

    let lifecycle = pg2iceberg::setup::build_logical_lifecycle(
        &cfg,
        lifecycle_catalog,
        blob.clone(),
        blob_namer,
    )
    .await
    .expect("build lifecycle");

    // Drive the lifecycle inline (rather than via tokio::spawn —
    // tracing's `Arguments<'_>` / `dyn Value` aren't Send so the
    // future can't migrate between worker threads). Uses a oneshot
    // for shutdown and `tokio::join!` to interleave the lifecycle
    // with the test workload concurrently on the same task.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown_fut = Box::pin(async move {
        let _ = shutdown_rx.await;
    });
    let lifecycle_fut =
        pg2iceberg_validate::run_logical_lifecycle(lifecycle, shutdown_fut);

    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    let schema = pg2iceberg_core::TableSchema {
        ident: ident.clone(),
        columns: vec![
            pg2iceberg_core::ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: pg2iceberg_core::IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            pg2iceberg_core::ColumnSchema {
                name: "balance".into(),
                field_id: 2,
                ty: pg2iceberg_core::IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: vec![],
    };
    let pk_cols = vec![ColumnName("id".into())];

    let test_work = async {
        // Brief delay so the lifecycle's snapshot phase has a chance
        // to grab the consistent_point before we start writing more
        // rows.
        tokio::time::sleep(Duration::from_secs(2)).await;
        for i in 4..=6 {
            src.execute(
                &format!("INSERT INTO {table} (id, balance) VALUES ($1, $2)"),
                &[&i, &(i * 100)],
            )
            .await
            .expect("cdc insert");
        }

        // Poll Iceberg until all 6 rows materialize, or time out.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut last_count = 0usize;
        let visible = loop {
            let attempt = read_materialized_state(
                &assert_catalog,
                blob.as_ref(),
                &ident,
                &schema,
                &pk_cols,
            )
            .await;
            match attempt {
                Ok(rows) if rows.len() >= 6 => break rows,
                Ok(rows) => last_count = rows.len(),
                Err(_) => {} // table may not exist yet
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "timed out waiting for Iceberg materialization; last_count={last_count}"
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        let mut by_id: BTreeMap<i32, i32> = BTreeMap::new();
        for row in &visible {
            let id = match row.get(&ColumnName("id".into())) {
                Some(PgValue::Int4(v)) => *v,
                other => panic!("unexpected id value: {other:?}"),
            };
            let balance = match row.get(&ColumnName("balance".into())) {
                Some(PgValue::Int4(v)) => *v,
                other => panic!("unexpected balance value: {other:?}"),
            };
            by_id.insert(id, balance);
        }
        let expected: BTreeMap<i32, i32> = (1..=6).map(|i| (i, i * 100)).collect();
        assert_eq!(by_id, expected, "PG state must equal Iceberg state");

        let _ = shutdown_tx.send(());
    };

    // `tokio::select!` (not `join!`) so a lifecycle error surfaces
    // immediately instead of waiting on a 2-minute test_work timeout
    // poll-loop. Branch order: if lifecycle returns first, that's a
    // real exit (clean or error) we want to report; if test_work
    // returns first, it's signaled shutdown so lifecycle should be
    // exiting too — drain it under a short timeout.
    tokio::pin!(lifecycle_fut);
    tokio::pin!(test_work);
    tokio::select! {
        result = &mut lifecycle_fut => {
            // If we got here before test_work finished, the lifecycle
            // exited unexpectedly. Surface the underlying error.
            panic!("lifecycle exited before test_work could observe it: {result:?}");
        }
        () = &mut test_work => {
            // test_work signaled shutdown; let the lifecycle drain.
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                lifecycle_fut,
            )
            .await
            .expect("lifecycle drained within 30s");
            result.expect("lifecycle ran to clean shutdown");
        }
    }
}

/// Trivial UUID-based blob namer for the lifecycle. The binary uses
/// a real-IO version that pulls in `RealIdGen`; we don't need that
/// here — any unique-name function works.
struct UuidBlobNamer {
    prefix: String,
}

impl UuidBlobNamer {
    fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }
}

#[async_trait]
impl pg2iceberg_logical::pipeline::BlobNamer for UuidBlobNamer {
    async fn next_blob_path(&self, table: &str) -> String {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        format!(
            "{}/{}/{}.parquet",
            self.prefix.trim_end_matches('/'),
            table,
            suffix
        )
    }
}
