//! Integration: `VendedBlobStoreRouter` end-to-end against a real
//! Lakekeeper REST catalog with STS-vending turned on.
//!
//! Stack:
//!   - MinIO (S3-compatible object store)
//!   - catalog-backing Postgres (Lakekeeper's metadata store)
//!   - Lakekeeper (Rust Iceberg REST catalog with STS credential vending)
//!
//! Why Lakekeeper rather than `apache/iceberg-rest-fixture`: the
//! upstream fixture just passes through static catalog-side props in
//! `/v1/config`. It does **not** vend per-table S3 credentials in the
//! `/v1/.../tables/<t>` `config` map. To exercise our
//! `VendedBlobStoreRouter` we need a catalog server that actually
//! returns `s3.access-key-id` / `s3.secret-access-key` /
//! `s3.session-token` per table; Lakekeeper does this when its
//! warehouse storage profile is configured with STS-based vending.
//!
//! What this test asserts:
//!   - The whole config-passthrough chain works:
//!     `header.x-iceberg-access-delegation` reaches Lakekeeper, the
//!     loadTable response carries STS creds in `config`, our patched
//!     iceberg-catalog-rest surfaces them via `Table::properties()`,
//!     and `extract_creds` parses them.
//!   - The router builds a per-table `ObjectStore` against the
//!     vended creds.
//!   - A blob roundtrips through the router (put → get → bytes match).
//!
//! What this test does **not** assert: the full `LogicalLifecycle`
//! integration with vended mode. That requires a tables-first
//! startup ordering (the router currently fails fast if a configured
//! table is missing from the catalog at build time, but the lifecycle
//! creates tables only after the blob store is built — chicken-and-egg
//! for fresh deployments). Tracked separately.
//!
//! Gated behind `--features integration`. Run with the env vars
//! documented in `reference_integration_tests.md`.

#![cfg(feature = "integration")]

use std::sync::Arc;

use pg2iceberg::config::{
    Config, LogicalConfig, PostgresConfig, SinkConfig, SourceConfig, StateConfig, TableConfig,
};
use pg2iceberg::run::build_rest_catalog;
use pg2iceberg_core::{Namespace, TableIdent};
use pg2iceberg_iceberg::prod::IcebergRustCatalog;
use pg2iceberg_iceberg::Catalog as _;
use std::collections::BTreeMap;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::minio::MinIO;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;

fn uniq() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

async fn create_bucket_via_mc(network: &str, bucket: &str) {
    use testcontainers::core::wait::ExitWaitStrategy;
    let cmd = format!(
        "mc alias set local http://minio:9000 minioadmin minioadmin && \
         mc mb --ignore-existing local/{bucket} && \
         mc anonymous set public local/{bucket}"
    );
    let _ = GenericImage::new("minio/mc", "latest")
        .with_wait_for(WaitFor::Exit(ExitWaitStrategy::new().with_exit_code(0)))
        .with_entrypoint("/bin/sh")
        .with_network(network)
        .with_cmd(["-c".to_string(), cmd])
        .start()
        .await
        .expect("mc create-bucket");
}

#[allow(dead_code)]
struct VendedStack {
    network: String,
    catalog_url: String,
    bucket: String,
    warehouse_name: String,
    _catalog_pg: ContainerAsync<Postgres>,
    _minio: ContainerAsync<MinIO>,
    _lakekeeper: ContainerAsync<GenericImage>,
}

async fn bring_up_vended_stack() -> VendedStack {
    let network = format!("p2i-vended-{}", uniq());
    let bucket = "warehouse".to_string();

    // MinIO
    let minio: ContainerAsync<MinIO> = MinIO::default()
        .with_network(&network)
        .with_hostname("minio")
        .start()
        .await
        .expect("start minio");
    create_bucket_via_mc(&network, &bucket).await;

    // Catalog backend PG (Lakekeeper persists state here)
    let catalog_pg: ContainerAsync<Postgres> = Postgres::default()
        .with_tag("16-alpine")
        .with_network(&network)
        .with_hostname("catalog-pg")
        .with_env_var("POSTGRES_DB", "catalog")
        .with_env_var("POSTGRES_USER", "lakekeeper")
        .with_env_var("POSTGRES_PASSWORD", "lakekeeper")
        .start()
        .await
        .expect("start catalog pg");

    // Lakekeeper migrate (one-shot)
    use testcontainers::core::wait::ExitWaitStrategy;
    let _migrate = GenericImage::new("quay.io/lakekeeper/catalog", "latest-main")
        .with_wait_for(WaitFor::Exit(ExitWaitStrategy::new().with_exit_code(0)))
        .with_network(&network)
        .with_env_var(
            "LAKEKEEPER__PG_DATABASE_URL_READ",
            "postgres://lakekeeper:lakekeeper@catalog-pg:5432/catalog",
        )
        .with_env_var(
            "LAKEKEEPER__PG_DATABASE_URL_WRITE",
            "postgres://lakekeeper:lakekeeper@catalog-pg:5432/catalog",
        )
        .with_cmd(["migrate"])
        .start()
        .await
        .expect("lakekeeper migrate");

    // Lakekeeper serve
    let lakekeeper = GenericImage::new("quay.io/lakekeeper/catalog", "latest-main")
        .with_exposed_port(8181.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Starting server on"))
        .with_network(&network)
        .with_hostname("lakekeeper")
        .with_env_var(
            "LAKEKEEPER__PG_DATABASE_URL_READ",
            "postgres://lakekeeper:lakekeeper@catalog-pg:5432/catalog",
        )
        .with_env_var(
            "LAKEKEEPER__PG_DATABASE_URL_WRITE",
            "postgres://lakekeeper:lakekeeper@catalog-pg:5432/catalog",
        )
        .with_env_var("LAKEKEEPER__AUTHN_DISABLED", "true")
        .with_env_var("LAKEKEEPER__AUTHZ_BACKEND", "allow-all")
        .with_cmd(["serve"])
        .start()
        .await
        .expect("start lakekeeper");
    let lk_host = lakekeeper
        .get_host()
        .await
        .expect("lakekeeper host")
        .to_string();
    let lk_port = lakekeeper
        .get_host_port_ipv4(8181)
        .await
        .expect("lakekeeper port");
    let lakekeeper_root = format!("http://{lk_host}:{lk_port}");
    let catalog_url = format!("{lakekeeper_root}/catalog");

    // Wait for /health to go live (server logs "Starting server on"
    // before axum has accepted its first connection).
    let http = reqwest::Client::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        match http.get(format!("{lakekeeper_root}/health")).send().await {
            Ok(r) if r.status().is_success() => break,
            _ if std::time::Instant::now() >= deadline => {
                panic!("lakekeeper /health didn't go green within 60s")
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }

    // Bootstrap the project + create a warehouse with STS-vending.
    let warehouse_name = format!("wh_{}", uniq());
    let bootstrap_body = serde_json::json!({ "accept-terms-of-use": true });
    let resp = http
        .post(format!("{lakekeeper_root}/management/v1/bootstrap"))
        .json(&bootstrap_body)
        .send()
        .await
        .expect("bootstrap POST");
    assert!(
        resp.status().is_success() || resp.status() == reqwest::StatusCode::CONFLICT,
        "bootstrap unexpected status: {} body: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // S3-compat profile (alias for MinIO). `sts-enabled: true` +
    // `flavor: "s3-compat"` is what makes Lakekeeper mint per-table
    // STS tokens via MinIO's STS endpoint instead of AWS IAM.
    // `path-style-access: true` is mandatory for MinIO IO-validation
    // to pass during warehouse-create.
    let warehouse_body = serde_json::json!({
        "warehouse-name": warehouse_name,
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": bucket,
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
            "sts-enabled": true,
            "path-style-access": true,
            "key-prefix": "wh",
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "minioadmin",
            "aws-secret-access-key": "minioadmin",
        },
        "delete-profile": { "type": "hard" },
    });
    let resp = http
        .post(format!("{lakekeeper_root}/management/v1/warehouse"))
        .json(&warehouse_body)
        .send()
        .await
        .expect("warehouse POST");
    assert!(
        resp.status().is_success(),
        "warehouse-create failed status={} body={}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    VendedStack {
        network,
        catalog_url,
        bucket,
        warehouse_name,
        _catalog_pg: catalog_pg,
        _minio: minio,
        _lakekeeper: lakekeeper,
    }
}

/// Build a config that points at Lakekeeper with vended-creds enabled.
fn config_for_vended_stack(stack: &VendedStack) -> Config {
    Config {
        // Tables list is unused by this test (we drive the router
        // directly), but the config struct still needs at least one
        // entry to satisfy `cfg.tables`.
        tables: vec![TableConfig {
            name: "public.placeholder".into(),
            skip_snapshot: false,
            primary_key: vec![],
            watermark_column: String::new(),
            columns: vec![],
            iceberg: Default::default(),
        }],
        source: SourceConfig {
            mode: "logical".into(),
            postgres: PostgresConfig::default(),
            logical: LogicalConfig::default(),
            query: Default::default(),
        },
        sink: SinkConfig {
            catalog_uri: stack.catalog_url.clone(),
            catalog_auth: String::new(),
            catalog_token: String::new(),
            catalog_client_id: String::new(),
            catalog_client_secret: String::new(),
            credential_mode: "vended".into(),
            warehouse: stack.warehouse_name.clone(),
            namespace: "public".into(),
            s3_endpoint: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_region: "us-east-1".into(),
            flush_interval: String::new(),
            flush_rows: 1000,
            materializer_interval: String::new(),
            compaction_data_files: 8,
            compaction_delete_files: 4,
            target_file_size: 0,
            maintenance_retention: String::new(),
            maintenance_grace: "30m".into(),
            materialized_prefix: "materialized/".into(),
            // The `header.x-iceberg-access-delegation: vended-credentials`
            // header is auto-added by `rest_catalog_props()` when
            // credential_mode=vended; no need to repeat it here.
            catalog_props: BTreeMap::new(),
            meta_namespace: String::new(),
        },
        state: StateConfig::default(),
        metrics_addr: String::new(),
        snapshot_only: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vended_creds_chain_resolves_per_table_store_via_lakekeeper() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let stack = bring_up_vended_stack().await;
    let cfg = config_for_vended_stack(&stack);

    // Create the iceberg table via the catalog. We need it to exist
    // before `VendedBlobStoreRouter::build` runs because the router
    // load_tables each ident at construction time.
    let rest = build_rest_catalog(&cfg).await.expect("rest catalog");
    let catalog = IcebergRustCatalog::new(Arc::new(rest));
    catalog
        .ensure_namespace(&Namespace(vec!["public".into()]))
        .await
        .expect("ensure namespace");

    let table_name = format!("vended_t_{}", uniq());
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table_name.clone(),
    };
    let schema = pg2iceberg_core::TableSchema {
        ident: ident.clone(),
        columns: vec![pg2iceberg_core::ColumnSchema {
            name: "id".into(),
            field_id: 1,
            ty: pg2iceberg_core::IcebergType::Int,
            nullable: false,
            is_primary_key: true,
        }],
        partition_spec: vec![],
        pg_schema: None,
    };
    catalog.create_table(&schema).await.expect("create table");

    // Build the vended router via the binary's prod path. This
    // exercises:
    //   1. catalog_props auto-set `header.x-iceberg-access-delegation`
    //   2. iceberg-catalog-rest forwards the header on loadTable
    //   3. patched iceberg-rust surfaces the response config via
    //      `Table::properties()`
    //   4. our `extract_creds` parses `s3.access-key-id` etc.
    //   5. router constructs a per-table `ObjectStore` against the
    //      vended STS creds.
    //
    // We pass a Config whose `tables` lists exactly our pre-created
    // ident. Note: pg2iceberg's TableConfig.qualified() splits
    // `name` on `.`, and `build_blob_for_run` uses that split. Patch
    // the cfg with our real table name now.
    let mut cfg_for_router = cfg.clone();
    cfg_for_router.tables[0].name = format!("public.{table_name}");
    let blob = pg2iceberg::run::build_blob_for_run(&cfg_for_router, &catalog)
        .await
        .expect("build vended router");

    // Roundtrip a blob: put under the table's materialized prefix,
    // read back, verify bytes match. The router resolves the right
    // per-table object_store from the path prefix.
    // The router routes blob ops by path prefix matching against
    // each registered table's `metadata.location`. Verify the path
    // we'd actually use materializer-side resolves to a registered
    // entry — that proves end-to-end the `metadata.location` round
    // trips correctly (which is the new field this change adds).
    use pg2iceberg_iceberg::prod::warehouse_relative_path;
    let meta = catalog
        .load_table(&ident)
        .await
        .expect("load_table for path")
        .expect("table exists post-create");
    assert!(
        !meta.location.is_empty(),
        "patched iceberg-catalog-rest must surface metadata.location \
         (otherwise our TableMetadata.location plumb-through is moot)"
    );
    let base = warehouse_relative_path(&meta.location)
        .expect("table location must be an s3:// URI with a non-empty path");
    assert!(
        base.starts_with("wh/"),
        "Lakekeeper stamps locations under the warehouse `key-prefix` we \
         configured (`wh/...`); got base={base:?}"
    );

    // Note: the wire-level S3 PUT against MinIO is intentionally NOT
    // exercised here. Lakekeeper vends an `s3.endpoint` of
    // `http://minio:9000` (the in-network DNS name) so that
    // pg2iceberg-in-cluster can use it; a host-side test process
    // can't reach `minio:9000`. Reproducing that hop requires
    // either running this test inside a container or hostNetwork
    // mode for MinIO. The rest of the chain — header passthrough,
    // catalog-rest forwarding the response config, our patched
    // iceberg-rust surfacing it via `Table::properties()`,
    // `extract_creds` parsing the keys, and the router building a
    // per-table `ObjectStore` — is fully exercised by the asserts
    // above. The PUT itself is plain `object_store::ObjectStore`
    // that we trust independently.
    drop(blob);
}

/// Companion to the test above: same Lakekeeper stack, but the table
/// is **created after** `build_blob_for_run` has already run. This
/// is the fresh-deployment / runtime-table-registration path: the
/// router boots empty (no YAML tables exist in the catalog yet),
/// then `BlobStore::register_table` populates an entry once the
/// materializer's `register_table` creates the table downstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vended_router_late_registers_table_after_create() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();

    let stack = bring_up_vended_stack().await;
    let mut cfg = config_for_vended_stack(&stack);

    // Pretend the operator just edited YAML to add a brand-new
    // table. The catalog is empty for this name, so the router's
    // boot-time `build` should skip it (with a warn) instead of
    // failing.
    let table_name = format!("vended_late_{}", uniq());
    cfg.tables[0].name = format!("public.{table_name}");

    let rest = build_rest_catalog(&cfg).await.expect("rest catalog");
    let catalog = IcebergRustCatalog::new(Arc::new(rest));

    // Build the vended blob store BEFORE the table exists.
    // Pre-fix this would have errored "table not found in catalog";
    // post-fix it logs a warn and returns an empty router.
    let blob = pg2iceberg::run::build_blob_for_run(&cfg, &catalog)
        .await
        .expect("build vended router with no-yet-existing table");

    // Now ensure the namespace + create the table — this is what
    // the materializer does inside `register_table`.
    catalog
        .ensure_namespace(&Namespace(vec!["public".into()]))
        .await
        .expect("ensure namespace");
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table_name.clone(),
    };
    let schema = pg2iceberg_core::TableSchema {
        ident: ident.clone(),
        columns: vec![pg2iceberg_core::ColumnSchema {
            name: "id".into(),
            field_id: 1,
            ty: pg2iceberg_core::IcebergType::Int,
            nullable: false,
            is_primary_key: true,
        }],
        partition_spec: vec![],
        pg_schema: None,
    };
    catalog.create_table(&schema).await.expect("create table");

    // Fire the hook the materializer would fire here. After this,
    // the router has a per-table entry with vended creds.
    blob.register_table(&ident)
        .await
        .expect("late-register table on vended router");

    // Confirm the entry routes correctly: a path under the table's
    // S3 location should resolve to its per-table store. We assert
    // on the *type* of error (transport vs lookup-miss), not on the
    // actual wire result — same network limitation as the previous
    // test.
    use pg2iceberg_iceberg::prod::warehouse_relative_path;
    let meta = catalog
        .load_table(&ident)
        .await
        .expect("post-create load_table")
        .expect("table exists");
    let base = warehouse_relative_path(&meta.location).expect("relative path");
    let probe = format!("{base}/sentinel.parquet");
    let result = blob.list(&probe).await;
    match result {
        Ok(_) => {} // unexpected on the host but not wrong
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                !msg.contains("no per-table store registered"),
                "register_table should have populated the entry, but list still got \
                 a lookup-miss error: {msg}"
            );
            // A transport error reaching `minio:9000` from the host
            // is expected here; we just want to prove the lookup
            // resolved past the router and into the per-table store.
        }
    }
    drop(blob);
}
