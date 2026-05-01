//! Vended-credentials S3 router.
//!
//! Mirrors `pg2iceberg/iceberg/s3_vended.go` + `s3_vended_router.go`.
//! Polaris / Snowflake / Tabular catalogs return per-table temporary
//! S3 credentials in the `loadTable` response when the request carries
//! the `X-Iceberg-Access-Delegation: vended-credentials` header. These
//! creds are scoped to that table's S3 prefix only — a single shared
//! S3 client can't write across tables. The router below dispatches
//! each blob op to the correct per-table client by longest-prefix
//! match on the table's `Location`, refreshing creds before they
//! expire.
//!
//! The pieces a binary needs from this module:
//!
//! - [`extract_creds`] — pulls `s3.access-key-id`, `s3.secret-access-key`,
//!   `s3.session-token` (etc.) from a [`TableMetadata::config`] map
//!   the polynya-patches fork populates from REST `loadTable`.
//! - [`build_per_table_object_store`] — turns those creds into an
//!   `Arc<dyn ObjectStore>` scoped to the table's bucket+prefix.
//! - [`VendedBlobStoreRouter`] — implements `BlobStore` by routing
//!   each op to the correct per-table store.
//! - [`VendedMaterializerNamer`] / [`VendedBlobNamer`] — produce
//!   table-scoped paths under each table's S3 location so the router
//!   can match on prefix.
//!
//! Refresh is opportunistic: every blob op checks if the entry is
//! within `refresh_buffer` of expiry and, if so, calls
//! `Catalog::load_table` to fetch fresh creds. Concurrent ops
//! racing to refresh deduplicate via a single `tokio::sync::RwLock`
//! around the per-table state.

use crate::{Catalog, IcebergError, Result as IcebergResult};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use pg2iceberg_core::TableIdent;
use pg2iceberg_stream::{BlobInfo, BlobStore, Result as StreamResult, StreamError};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Default validity window for vended credentials when the catalog
/// doesn't include an explicit expiry. Mirrors Go's `defaultVendedTTL`.
pub const DEFAULT_VENDED_TTL: Duration = Duration::from_secs(55 * 60);

/// How far in advance of expiry a refresh kicks in. Mirrors Go's
/// `refreshBuffer`.
pub const DEFAULT_REFRESH_BUFFER: Duration = Duration::from_secs(5 * 60);

/// Per-table credentials extracted from the catalog response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VendedCreds {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
}

/// Pulls the S3 creds out of the `config` map populated by the
/// polynya-patches fork from the REST `loadTable` response. Returns
/// `None` when the catalog didn't return creds — the catalog likely
/// isn't running in vended-credentials mode, or didn't see the
/// access-delegation header.
pub fn extract_creds(config: &BTreeMap<String, String>) -> Option<VendedCreds> {
    let ak = config.get("s3.access-key-id")?.clone();
    let sk = config.get("s3.secret-access-key")?.clone();
    if ak.is_empty() || sk.is_empty() {
        return None;
    }
    Some(VendedCreds {
        access_key_id: ak,
        secret_access_key: sk,
        session_token: config.get("s3.session-token").cloned(),
        region: config.get("s3.region").cloned(),
        endpoint: config.get("s3.endpoint").cloned(),
    })
}

/// Build an `object_store::ObjectStore` scoped to a single table's
/// vended creds. The store talks to one S3 bucket; the caller is
/// responsible for routing keys that fall outside this table's
/// prefix to a different store.
///
/// `bucket` and `region` are usually pre-extracted from the table's
/// `Location` (an `s3://bucket/...` URI); the catalog's region prop,
/// when present, takes precedence over the operator's default.
pub fn build_per_table_object_store(
    creds: &VendedCreds,
    bucket: &str,
    default_region: &str,
) -> IcebergResult<Arc<dyn ObjectStore>> {
    let region = creds
        .region
        .clone()
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| default_region.to_string());
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&region)
        .with_access_key_id(&creds.access_key_id)
        .with_secret_access_key(&creds.secret_access_key)
        // Vended creds usually target non-AWS S3-compatible endpoints
        // (Polaris, MinIO, etc.); path-style addressing is the safe
        // default. Real AWS accepts both.
        .with_virtual_hosted_style_request(false);
    if let Some(t) = &creds.session_token {
        if !t.is_empty() {
            b = b.with_token(t);
        }
    }
    if let Some(ep) = creds.endpoint.as_deref().filter(|s| !s.is_empty()) {
        b = b.with_endpoint(ep);
    }
    let store = b
        .build()
        .map_err(|e| IcebergError::Other(format!("AmazonS3Builder for vended creds: {e}")))?;
    Ok(Arc::new(store))
}

/// Strip the `s3://bucket/` prefix from a table location, leaving the
/// warehouse-relative path. The router uses this as the basePath for
/// longest-prefix matching.
///
/// `s3://my-bucket/warehouse/db/orders/` → `warehouse/db/orders`.
/// Trailing slashes are normalized off so prefix matches don't
/// accidentally exclude the directory itself.
pub fn warehouse_relative_path(location: &str) -> Option<String> {
    let stripped = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))?;
    let mut parts = stripped.splitn(2, '/');
    parts.next()?; // bucket
    let path = parts.next()?.trim_end_matches('/');
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

/// Extract the bucket name from `s3://bucket/...` URIs. Mirrors Go's
/// `url.Parse(...).Host`.
pub fn bucket_from_location(location: &str) -> Option<String> {
    let stripped = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))?;
    let bucket = stripped.split('/').next()?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket.to_string())
    }
}

/// One per-table entry in the router. The `inner` is behind a lock so
/// the refresh path can swap the underlying store atomically without
/// blocking concurrent reads.
struct VendedEntry {
    ident: TableIdent,
    /// Warehouse-relative prefix, no trailing slash. Used for
    /// longest-prefix matching against incoming op paths.
    base_path: String,
    inner: RwLock<EntryInner>,
}

struct EntryInner {
    object_store: Arc<dyn ObjectStore>,
    expires_at: SystemTime,
}

/// Configuration for the router. `default_ttl` and `refresh_buffer`
/// have sensible defaults that mirror Go's behavior; the bucket /
/// region / endpoint are operator-supplied at startup.
#[derive(Clone, Debug)]
pub struct VendedRouterConfig {
    pub default_ttl: Duration,
    pub refresh_buffer: Duration,
    /// Default S3 region used when the catalog doesn't return one in
    /// the per-table config.
    pub default_region: String,
}

impl Default for VendedRouterConfig {
    fn default() -> Self {
        Self {
            default_ttl: DEFAULT_VENDED_TTL,
            refresh_buffer: DEFAULT_REFRESH_BUFFER,
            default_region: "us-east-1".to_string(),
        }
    }
}

/// `BlobStore` impl that dispatches each op to the correct per-table
/// `ObjectStore` based on which table's S3 location prefixes the path.
///
/// Construction is async because we have to load each table from the
/// catalog to obtain creds; once built, the router is `Send + Sync`
/// and shareable.
pub struct VendedBlobStoreRouter {
    entries: Vec<Arc<VendedEntry>>,
    catalog: Arc<dyn Catalog>,
    cfg: VendedRouterConfig,
}

impl VendedBlobStoreRouter {
    /// Build a router by loading each `(ident, location)` from the
    /// catalog and extracting creds from the response. Errors out if
    /// any table is missing creds — the binary's startup expects
    /// vended mode to be fully wired before the lifecycle runs, so
    /// failing fast surfaces config issues immediately.
    pub async fn build(
        catalog: Arc<dyn Catalog>,
        idents: &[TableIdent],
        cfg: VendedRouterConfig,
    ) -> IcebergResult<Self> {
        let mut entries: Vec<Arc<VendedEntry>> = Vec::with_capacity(idents.len());
        for ident in idents {
            let meta = catalog.load_table(ident).await?.ok_or_else(|| {
                IcebergError::NotFound(format!(
                    "vended-router: table {ident} not found in catalog (cannot vend creds)"
                ))
            })?;
            let creds = extract_creds(&meta.config).ok_or_else(|| {
                IcebergError::Other(format!(
                    "vended-router: catalog returned no S3 credentials for {ident}; \
                     check that the catalog is a Polaris/Tabular/Snowflake-style server \
                     and that `header.x-iceberg-access-delegation: vended-credentials` is \
                     set in catalog_props"
                ))
            })?;
            // Pull the table's S3 location from `metadata.location` —
            // we don't have direct access, but the catalog config
            // typically also carries `s3.location`. Fall back to
            // `meta.config.get("s3.location")` for a clean error if
            // missing.
            let location = meta
                .config
                .get("location")
                .or_else(|| meta.config.get("s3.location"))
                .cloned()
                .ok_or_else(|| {
                    IcebergError::Other(format!(
                        "vended-router: no `location` returned for {ident}; \
                         catalog must include `location` in the loadTable config"
                    ))
                })?;
            let bucket = bucket_from_location(&location).ok_or_else(|| {
                IcebergError::Other(format!(
                    "vended-router: location {location:?} for {ident} is not an s3:// URI"
                ))
            })?;
            let base_path = warehouse_relative_path(&location).unwrap_or_default();
            let object_store = build_per_table_object_store(&creds, &bucket, &cfg.default_region)?;
            let expires_at = SystemTime::now() + cfg.default_ttl;
            entries.push(Arc::new(VendedEntry {
                ident: ident.clone(),
                base_path,
                inner: RwLock::new(EntryInner {
                    object_store,
                    expires_at,
                }),
            }));
        }
        // Sort longest base_path first so longest-prefix-match works
        // with a simple linear scan. Two tables can't share a base
        // path, so stable order doesn't matter.
        entries.sort_by(|a, b| b.base_path.len().cmp(&a.base_path.len()));
        Ok(Self {
            entries,
            catalog,
            cfg,
        })
    }

    /// Locate the entry whose `base_path` is a prefix of `key`. Returns
    /// `None` for keys that fall outside every registered table. Empty
    /// `base_path` entries (table at bucket root) match everything;
    /// they're sorted last so they only catch keys that didn't match
    /// any more-specific entry.
    fn lookup(&self, key: &str) -> Option<Arc<VendedEntry>> {
        for e in &self.entries {
            if e.base_path.is_empty() || key.starts_with(&e.base_path) {
                return Some(Arc::clone(e));
            }
        }
        None
    }

    /// Return the active store for `entry`, refreshing creds if
    /// expiry is within `refresh_buffer`. The refresh path
    /// double-checks under the write lock so concurrent ops only
    /// trigger one refresh.
    async fn fresh_store(&self, entry: &VendedEntry) -> IcebergResult<Arc<dyn ObjectStore>> {
        // Read-fast path.
        {
            let g = entry.inner.read().await;
            if SystemTime::now() + self.cfg.refresh_buffer < g.expires_at {
                return Ok(Arc::clone(&g.object_store));
            }
        }
        // Slow path: take the write lock, double-check, refresh.
        let mut g = entry.inner.write().await;
        if SystemTime::now() + self.cfg.refresh_buffer < g.expires_at {
            return Ok(Arc::clone(&g.object_store));
        }
        let meta = self
            .catalog
            .load_table(&entry.ident)
            .await?
            .ok_or_else(|| {
                IcebergError::NotFound(format!(
                    "vended-router: refresh load_table for {} returned None",
                    entry.ident
                ))
            })?;
        let creds = extract_creds(&meta.config).ok_or_else(|| {
            IcebergError::Other(format!(
                "vended-router: refresh produced no creds for {}",
                entry.ident
            ))
        })?;
        let bucket = meta
            .config
            .get("location")
            .or_else(|| meta.config.get("s3.location"))
            .and_then(|loc| bucket_from_location(loc))
            .ok_or_else(|| {
                IcebergError::Other(format!(
                    "vended-router: refresh location missing for {}",
                    entry.ident
                ))
            })?;
        let store = build_per_table_object_store(&creds, &bucket, &self.cfg.default_region)?;
        g.object_store = Arc::clone(&store);
        g.expires_at = SystemTime::now() + self.cfg.default_ttl;
        tracing::info!(table = %entry.ident, "vended creds refreshed");
        Ok(store)
    }

    async fn store_for(&self, key: &str) -> StreamResult<Arc<dyn ObjectStore>> {
        let entry = self.lookup(key).ok_or_else(|| {
            StreamError::Io(format!(
                "vended-router: no per-table store registered for path {key:?} \
                 — ensure every materialized blob writes under a registered table's \
                 S3 location"
            ))
        })?;
        self.fresh_store(&entry)
            .await
            .map_err(|e| StreamError::Io(format!("vended-router: refresh: {e}")))
    }
}

fn parse_path(input: &str) -> StreamResult<Path> {
    Path::parse(input).map_err(|e| StreamError::Io(format!("invalid object path {input:?}: {e}")))
}

#[async_trait]
impl BlobStore for VendedBlobStoreRouter {
    async fn put(&self, path: &str, bytes: Bytes) -> StreamResult<()> {
        let store = self.store_for(path).await?;
        let p = parse_path(path)?;
        store
            .put(&p, bytes.into())
            .await
            .map_err(|e| StreamError::Io(format!("vended put {path:?}: {e}")))?;
        Ok(())
    }

    async fn get(&self, path: &str) -> StreamResult<Bytes> {
        let store = self.store_for(path).await?;
        let p = parse_path(path)?;
        let result = store
            .get(&p)
            .await
            .map_err(|e| StreamError::Io(format!("vended get {path:?}: {e}")))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| StreamError::Io(format!("vended get bytes {path:?}: {e}")))?;
        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> StreamResult<Vec<BlobInfo>> {
        let store = self.store_for(prefix).await?;
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(parse_path(prefix.trim_end_matches('/'))?)
        };
        let mut stream = store.list(prefix_path.as_ref());
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item.map_err(|e| StreamError::Io(format!("vended list {prefix:?}: {e}")))?;
            out.push(BlobInfo {
                path: meta.location.to_string(),
                size: meta.size as u64,
                last_modified_ms: meta.last_modified.timestamp_millis(),
            });
        }
        Ok(out)
    }

    async fn delete(&self, path: &str) -> StreamResult<()> {
        let store = self.store_for(path).await?;
        let p = parse_path(path)?;
        match store.delete(&p).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(StreamError::Io(format!("vended delete {path:?}: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        IcebergError, PreparedCommit, PreparedCompaction, SchemaChange, Snapshot, TableMetadata,
    };
    use pg2iceberg_core::{Namespace, TableSchema};
    use std::sync::Mutex;

    /// Tiny stub Catalog that holds a fixed map of `(ident → metadata)`.
    /// Used by router tests to plant per-table config (i.e. fake
    /// vended creds) without standing up a real REST server.
    /// `load_table` increments a counter so refresh tests can verify
    /// they actually re-loaded.
    struct StubCatalog {
        tables: Mutex<BTreeMap<TableIdent, TableMetadata>>,
        load_count: Mutex<usize>,
    }

    impl StubCatalog {
        fn new() -> Self {
            Self {
                tables: Mutex::new(BTreeMap::new()),
                load_count: Mutex::new(0),
            }
        }
        fn add(&self, ident: TableIdent, meta: TableMetadata) {
            self.tables.lock().unwrap().insert(ident, meta);
        }
        fn loads(&self) -> usize {
            *self.load_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl Catalog for StubCatalog {
        async fn ensure_namespace(&self, _ns: &Namespace) -> crate::Result<()> {
            Ok(())
        }
        async fn load_table(&self, ident: &TableIdent) -> crate::Result<Option<TableMetadata>> {
            *self.load_count.lock().unwrap() += 1;
            Ok(self.tables.lock().unwrap().get(ident).cloned())
        }
        async fn create_table(&self, _schema: &TableSchema) -> crate::Result<TableMetadata> {
            Err(IcebergError::Other("stub: create_table".into()))
        }
        async fn commit_snapshot(&self, _prepared: PreparedCommit) -> crate::Result<TableMetadata> {
            Err(IcebergError::Other("stub: commit_snapshot".into()))
        }
        async fn commit_compaction(
            &self,
            _prepared: PreparedCompaction,
        ) -> crate::Result<TableMetadata> {
            Err(IcebergError::Other("stub: commit_compaction".into()))
        }
        async fn evolve_schema(
            &self,
            _ident: &TableIdent,
            _changes: Vec<SchemaChange>,
        ) -> crate::Result<TableMetadata> {
            Err(IcebergError::Other("stub: evolve_schema".into()))
        }
        async fn snapshots(&self, _ident: &TableIdent) -> crate::Result<Vec<Snapshot>> {
            Ok(Vec::new())
        }
    }

    fn ident(name: &str) -> TableIdent {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: name.into(),
        }
    }

    fn dummy_schema(ident: TableIdent) -> TableSchema {
        TableSchema {
            ident,
            columns: vec![pg2iceberg_core::ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: pg2iceberg_core::IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            }],
            partition_spec: vec![],
        }
    }

    fn meta_with(location: &str, ident: TableIdent, has_creds: bool) -> TableMetadata {
        let mut config = BTreeMap::new();
        config.insert("location".into(), location.into());
        if has_creds {
            config.insert("s3.access-key-id".into(), "ASIAFAKE".into());
            config.insert("s3.secret-access-key".into(), "fakesecret".into());
            config.insert("s3.session-token".into(), "faketoken".into());
        }
        TableMetadata {
            ident: ident.clone(),
            schema: dummy_schema(ident),
            current_snapshot_id: None,
            config,
        }
    }

    #[test]
    fn extract_creds_from_full_config() {
        let mut cfg = BTreeMap::new();
        cfg.insert("s3.access-key-id".into(), "ASIATEST".into());
        cfg.insert("s3.secret-access-key".into(), "secret".into());
        cfg.insert("s3.session-token".into(), "session".into());
        cfg.insert("s3.region".into(), "us-west-2".into());
        cfg.insert("s3.endpoint".into(), "http://minio:9000".into());
        let c = extract_creds(&cfg).unwrap();
        assert_eq!(c.access_key_id, "ASIATEST");
        assert_eq!(c.secret_access_key, "secret");
        assert_eq!(c.session_token.as_deref(), Some("session"));
        assert_eq!(c.region.as_deref(), Some("us-west-2"));
        assert_eq!(c.endpoint.as_deref(), Some("http://minio:9000"));
    }

    #[test]
    fn extract_creds_returns_none_when_missing() {
        let cfg = BTreeMap::new();
        assert!(extract_creds(&cfg).is_none());
        let mut cfg = BTreeMap::new();
        cfg.insert("s3.access-key-id".into(), "x".into());
        // missing secret → no creds
        assert!(extract_creds(&cfg).is_none());
        let mut cfg = BTreeMap::new();
        cfg.insert("s3.access-key-id".into(), "".into());
        cfg.insert("s3.secret-access-key".into(), "x".into());
        // empty access key → no creds
        assert!(extract_creds(&cfg).is_none());
    }

    #[test]
    fn warehouse_relative_path_strips_scheme_and_bucket() {
        assert_eq!(
            warehouse_relative_path("s3://my-bucket/warehouse/db/orders"),
            Some("warehouse/db/orders".into())
        );
        assert_eq!(
            warehouse_relative_path("s3://my-bucket/warehouse/db/orders/"),
            Some("warehouse/db/orders".into()),
            "trailing slash normalized off"
        );
        assert_eq!(
            warehouse_relative_path("s3a://b/p"),
            Some("p".into()),
            "s3a:// scheme also accepted"
        );
        // No path component → None.
        assert_eq!(warehouse_relative_path("s3://my-bucket"), None);
        assert_eq!(warehouse_relative_path("s3://my-bucket/"), None);
        // Non-s3 scheme rejected.
        assert!(warehouse_relative_path("gs://x/y").is_none());
    }

    #[test]
    fn bucket_from_location_extracts_host() {
        assert_eq!(
            bucket_from_location("s3://my-bucket/path"),
            Some("my-bucket".into())
        );
        assert_eq!(
            bucket_from_location("s3://my-bucket"),
            Some("my-bucket".into())
        );
        assert!(bucket_from_location("s3://").is_none());
        assert!(bucket_from_location("file:///tmp").is_none());
    }

    #[tokio::test]
    async fn router_build_extracts_per_table_creds() {
        let cat = Arc::new(StubCatalog::new());
        let a = ident("orders");
        let b = ident("users");
        cat.add(
            a.clone(),
            meta_with("s3://bucket/wh/db/orders", a.clone(), true),
        );
        cat.add(
            b.clone(),
            meta_with("s3://bucket/wh/db/users", b.clone(), true),
        );

        let router = VendedBlobStoreRouter::build(
            cat,
            &[a.clone(), b.clone()],
            VendedRouterConfig::default(),
        )
        .await
        .expect("build succeeds with full creds");

        // Two entries, sorted longest base_path first. Both paths
        // are equal length so order is implementation-defined; we
        // just check both are present.
        assert_eq!(router.entries.len(), 2);
        let bps: std::collections::BTreeSet<&str> = router
            .entries
            .iter()
            .map(|e| e.base_path.as_str())
            .collect();
        assert!(bps.contains("wh/db/orders"));
        assert!(bps.contains("wh/db/users"));
    }

    #[tokio::test]
    async fn router_build_fails_when_creds_missing() {
        let cat = Arc::new(StubCatalog::new());
        let a = ident("orders");
        // has_creds=false → s3.access-key-id is absent
        cat.add(
            a.clone(),
            meta_with("s3://bucket/wh/orders", a.clone(), false),
        );

        let err = match VendedBlobStoreRouter::build(cat, &[a], VendedRouterConfig::default()).await
        {
            Ok(_) => panic!("expected build to fail with missing creds"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("no S3 credentials") && msg.contains("access-delegation"),
            "error must point operator at the access-delegation header; got: {msg}"
        );
    }

    #[tokio::test]
    async fn router_build_fails_when_table_missing() {
        let cat = Arc::new(StubCatalog::new());
        // Don't add any tables → load_table returns None.
        let err = match VendedBlobStoreRouter::build(
            cat,
            &[ident("ghost")],
            VendedRouterConfig::default(),
        )
        .await
        {
            Ok(_) => panic!("expected build to fail with missing table"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(msg.contains("not found"));
    }

    #[tokio::test]
    async fn router_lookup_uses_longest_prefix_match() {
        let cat = Arc::new(StubCatalog::new());
        // Two tables where one's path is a prefix of the other —
        // ensures longest-prefix wins (the more specific table).
        let parent = ident("data");
        let child = ident("orders");
        cat.add(
            parent.clone(),
            meta_with("s3://bucket/wh/data", parent.clone(), true),
        );
        cat.add(
            child.clone(),
            meta_with("s3://bucket/wh/data/orders", child.clone(), true),
        );

        let router = VendedBlobStoreRouter::build(
            cat,
            &[parent.clone(), child.clone()],
            VendedRouterConfig::default(),
        )
        .await
        .unwrap();

        // Path under the child's prefix → child entry.
        let hit = router.lookup("wh/data/orders/file.parquet").unwrap();
        assert_eq!(hit.ident, child);
        // Path only under the parent's prefix → parent entry.
        let hit = router.lookup("wh/data/sibling.parquet").unwrap();
        assert_eq!(hit.ident, parent);
        // Path outside both → no match.
        assert!(router.lookup("other/path.parquet").is_none());
    }

    #[tokio::test]
    async fn router_refresh_reloads_from_catalog_when_near_expiry() {
        let cat = Arc::new(StubCatalog::new());
        let a = ident("orders");
        cat.add(
            a.clone(),
            meta_with("s3://bucket/wh/orders", a.clone(), true),
        );

        // Build with a tiny TTL + huge refresh buffer so every
        // `fresh_store` call sees expiry-near-now and triggers a
        // reload.
        let cfg = VendedRouterConfig {
            default_ttl: Duration::from_secs(0),
            refresh_buffer: Duration::from_secs(10),
            default_region: "us-east-1".into(),
        };
        let router = VendedBlobStoreRouter::build(
            Arc::clone(&cat) as Arc<dyn Catalog>,
            std::slice::from_ref(&a),
            cfg,
        )
        .await
        .unwrap();
        let initial_loads = cat.loads();
        // Read-fast path won't satisfy us — every `fresh_store` will
        // refresh.
        let entry = router.lookup("wh/orders/x.parquet").unwrap();
        let _ = router.fresh_store(&entry).await.unwrap();
        let _ = router.fresh_store(&entry).await.unwrap();
        // Both calls should have triggered a load_table refresh.
        assert!(
            cat.loads() >= initial_loads + 2,
            "every near-expiry op should refresh; loads went from {} to {}",
            initial_loads,
            cat.loads()
        );
    }

    #[tokio::test]
    async fn router_refresh_skipped_when_within_ttl() {
        let cat = Arc::new(StubCatalog::new());
        let a = ident("orders");
        cat.add(
            a.clone(),
            meta_with("s3://bucket/wh/orders", a.clone(), true),
        );

        // Long TTL + short refresh buffer → no refresh on subsequent
        // lookups within the window.
        let cfg = VendedRouterConfig {
            default_ttl: Duration::from_secs(3600),
            refresh_buffer: Duration::from_secs(60),
            default_region: "us-east-1".into(),
        };
        let router = VendedBlobStoreRouter::build(
            Arc::clone(&cat) as Arc<dyn Catalog>,
            std::slice::from_ref(&a),
            cfg,
        )
        .await
        .unwrap();
        let after_build_loads = cat.loads();
        let entry = router.lookup("wh/orders/x.parquet").unwrap();
        let _ = router.fresh_store(&entry).await.unwrap();
        let _ = router.fresh_store(&entry).await.unwrap();
        // No additional loads — still on the credentials from build().
        assert_eq!(
            cat.loads(),
            after_build_loads,
            "in-TTL ops must not refresh"
        );
    }
}
