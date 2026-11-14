//! Production [`SnapshotSource`] implementation.
//!
//! Opens a *separate* non-replication-mode `tokio_postgres::Client`,
//! starts a `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`
//! transaction, captures the current WAL position as the snapshot LSN,
//! and serves chunked `read_chunk` calls by issuing
//!
//! ```sql
//! COPY (
//!   SELECT col1::int8, col2::text, ...
//!   FROM "schema"."table"
//!   [ WHERE (pk_cols) > (literals) ]   -- when after_pk_key is supplied
//!   ORDER BY pk_cols ASC
//!   LIMIT N
//! ) TO STDOUT (FORMAT binary);
//! ```
//!
//! against the in-flight transaction. All chunks see a consistent view
//! because the tx stays open for the lifetime of the source. Values come
//! in their binary form, each column cast to the type whose form its
//! Iceberg type's decoder reads ([`crate::snapshot_copy`]).
//!
//! To read several tables at once, the source opens more connections:
//! each imports the first's snapshot (`pg_export_snapshot()`, then `SET
//! TRANSACTION SNAPSHOT`), so a chunk sees the same rows whichever reads
//! it.
//!
//! A large table is split into ranges of its key's first column, in its
//! index's order, read at once ([`SnapshotSource::split`]): at the bounds
//! of the column's histogram in `pg_stats`, of rows about as many each —
//! or, without one, of an integer column, evenly between its least and
//! greatest values. A bound is the column's value as text, which
//! Postgres takes as one of the column's type in `"c" >= '<bound>'`.
//!
//! `pk_cols` are in the primary key index's order, so the index serves
//! each chunk; in another order Postgres scans and sorts the whole table
//! for every chunk.
//!
//! ## Why a separate connection
//!
//! The replication-mode connection that drives the slot can't run
//! parameterized queries, can't open named cursors, and can't easily
//! sit inside a long REPEATABLE READ tx without interfering with the
//! `START_REPLICATION` command we eventually issue. So replication
//! and snapshot reads each get their own independent connection.
//!
//! ## What's deferred
//!
//! - **Marker-row fence** (the snapshot↔CDC handoff pattern documented
//!   in `pg2iceberg-snapshot::lib.rs`). Rows inserted between slot
//!   creation (LSN K) and the snapshot tx's view (LSN N) appear in
//!   *both* the snapshot read and the eventual replication stream,
//!   producing duplicate WAL events. The materializer's PK-keyed
//!   equality-delete deduplication handles this *correctness-wise* (the
//!   second insert becomes an Update which voids the first), but it
//!   means an interleaved snapshot is not optimal. For
//!   first-deployment use cases on quiesced tables this isn't a
//!   problem; hot tables need a real marker-row fence to land first.
//! - **Resumable mid-snapshot crash recovery.** The
//!   [`pg2iceberg_snapshot::Snapshotter`] surface persists per-table
//!   PK-key progress; we honor `after_pk_key` correctly so it works,
//!   but the binary today only invokes [`run_snapshot`] (the
//!   single-pass free function), which restarts from chunk 0 on
//!   crash. The pipeline replay is idempotent because identical source
//!   rows produce identical staged Parquet.
//! - **Shared snapshot ID** (`CREATE_REPLICATION_SLOT … USE_SNAPSHOT`
//!   coordinated with `SET TRANSACTION SNAPSHOT '<id>'`). We use a
//!   plain REPEATABLE READ tx, which gives a *self-consistent* view
//!   but not necessarily the one anchored at the slot's
//!   `consistent_point`. Combined with the marker fence this would
//!   close the duplicate-event window completely.

use crate::config::PostgresConfig;
use crate::snapshot_copy;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use pg2iceberg_coord::prod::{connect_with as coord_connect_with, TlsMode as CoordTls};
use pg2iceberg_core::value::{
    DaysSinceEpoch, Decimal as CoreDecimal, PgValue, TimeMicros, TimestampMicros,
};
use pg2iceberg_core::{ColumnName, ColumnSchema, Columns, Lsn, Row, TableIdent, TableSchema};
use pg2iceberg_iceberg::pk_key;
use pg2iceberg_pg::prod::value_decode::decode_text;
use pg2iceberg_snapshot::{KeyRange, Result as SnapshotResult, SnapshotError, SnapshotSource};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::AbortHandle;
use tokio_postgres::{Client, SimpleQueryMessage};

/// Per-table state cached at construction so `read_chunk` can build a
/// SELECT without re-resolving column types each call.
#[derive(Clone)]
struct CachedSchema {
    schema: TableSchema,
    /// The primary key in the schema's column order: the order of the
    /// canonical PK key, and of a cursor from before chunks were read in
    /// `index_order`.
    pk_cols: Vec<ColumnName>,
    /// The primary key in its index's order, when that isn't `pk_cols`'s.
    /// Chunks are read in this order, which the index serves; in any other
    /// order each chunk scans and sorts the whole table.
    index_order: Option<Vec<ColumnName>>,
    /// The column names its rows share, and where each of the schema's
    /// columns goes.
    row_columns: Columns,
    places: Vec<usize>,
    /// The columns a chunk selects, each cast to be read in binary.
    select: String,
}

/// The cursor of a table read in its index order: the last row's key, in
/// that order. A cursor that's a bare list of values is in `pk_cols`'s.
#[derive(Serialize, Deserialize)]
struct OrderedCursor {
    order: Vec<ColumnName>,
    after: Vec<PgValue>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Cursor {
    Ordered(OrderedCursor),
    Canonical(Vec<PgValue>),
}

pub struct PgSnapshotSource {
    /// REPEATABLE READ tx is open on these clients, all in the same
    /// snapshot, for the lifetime of the source. A read takes one that's
    /// free: `tokio_postgres` queues a client's queries, one at a time.
    clients: Vec<Mutex<Client>>,
    /// One per client: a read holds one while it takes a free client.
    readers: Semaphore,
    _conns: Vec<AbortHandle>,
    snapshot_lsn: Lsn,
    schemas: BTreeMap<TableIdent, CachedSchema>,
}

impl PgSnapshotSource {
    /// Open the snapshot over `connections` connections, so as many
    /// chunks can be read at once.
    pub async fn open(
        pg_cfg: &PostgresConfig,
        schemas: &[TableSchema],
        connections: usize,
    ) -> Result<Self> {
        let tls = match pg_cfg.tls_label() {
            "webpki" => CoordTls::Webpki,
            _ => CoordTls::Disable,
        };
        let conn = coord_connect_with(&pg_cfg.dsn(), tls)
            .await
            .context("connect snapshot source")?;
        // **Snapshot↔CDC fence (the "marker" LSN).**
        //
        // Capture `pg_current_wal_lsn()` *before* BEGIN. The
        // snapshot-view boundary `B` set by BEGIN is >= this LSN.
        // We use this LSN as the replication start point. The
        // tradeoff:
        //
        // - Events in `[snap_lsn, B]` (committed between this query
        //   and BEGIN) are visible in the snapshot view *and* in
        //   replication → duplicates. The materializer's PK-keyed
        //   equality-delete dedup absorbs them; the cost is some
        //   extra delete files on hot tables.
        // - Events in `[B, ∞)` are visible only via replication →
        //   correctly emitted exactly once.
        //
        // The opposite order (BEGIN first, then `pg_current_wal_lsn`)
        // produces snap_lsn > `B` in the presence of concurrent
        // commits, which would *lose* events in `[B, snap_lsn]` that
        // are neither in the snapshot view nor (with start LSN
        // = snap_lsn) replicated. The principle: accept duplicates
        // (the materializer's PK-keyed dedup absorbs them), never
        // lose events.
        let lsn_text = simple_query_one_value(&conn.client, "SELECT pg_current_wal_lsn()::text")
            .await
            .context("pg_current_wal_lsn (pre-BEGIN fence)")?
            .ok_or_else(|| anyhow::anyhow!("pg_current_wal_lsn returned no rows"))?;
        let snapshot_lsn =
            parse_lsn(&lsn_text).with_context(|| format!("parse LSN {lsn_text:?}"))?;
        // BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY locks in a
        // consistent view for every read_chunk call. Any tx that
        // committed in `[snap_lsn, BEGIN-time]` is in the view.
        conn.client
            .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .context("BEGIN snapshot tx")?;

        let mut by_ident = BTreeMap::new();
        for s in schemas {
            let pk_cols: Vec<ColumnName> = s
                .primary_key_columns()
                .map(|c| ColumnName(c.name.clone()))
                .collect();
            if pk_cols.is_empty() {
                anyhow::bail!(
                    "table {} has no primary key; snapshot needs PK columns to keyset-paginate",
                    s.ident
                );
            }
            let index_order = pk_index_order(&conn.client, &s.pg_ident())
                .await
                .with_context(|| format!("primary key index of {}", s.pg_ident()))?;
            // A configured key that isn't the table's primary key has no
            // index of its order to read in.
            let index_order = index_order.filter(|order| {
                *order != pk_cols
                    && order.len() == pk_cols.len()
                    && order.iter().all(|c| pk_cols.contains(c))
            });
            let (row_columns, places) = Columns::sorted(
                s.columns
                    .iter()
                    .map(|c| ColumnName(c.name.clone()))
                    .collect(),
            )
            .with_context(|| format!("table {} names a column twice", s.ident))?;
            // Key by PG-side ident — `read_chunk` is called with the
            // PG identifier (the snapshotter passes `s.pg_ident()`),
            // and our SELECT issues against PG. The Iceberg-side
            // `s.ident` is irrelevant here.
            let pg_types = column_types(&conn.client, &s.pg_ident())
                .await
                .with_context(|| format!("column types of {}", s.pg_ident()))?;
            let select = s
                .columns
                .iter()
                .map(|c| {
                    let pg_type = pg_types
                        .get(&c.name)
                        .map(|(name, output)| (name.as_str(), output.as_str()));
                    snapshot_copy::select(&c.name, c.ty, pg_type)
                })
                .collect::<Vec<_>>()
                .join(", ");
            by_ident.insert(
                s.pg_ident(),
                CachedSchema {
                    schema: s.clone(),
                    pk_cols,
                    index_order,
                    row_columns,
                    places,
                    select,
                },
            );
        }

        let mut clients = vec![Mutex::new(conn.client)];
        let mut conns = vec![conn.abort];
        if connections > 1 {
            // Exported while this transaction stays open; queried already,
            // it has taken its snapshot.
            let first = clients[0].get_mut();
            let id = simple_query_one_value(first, "SELECT pg_export_snapshot()")
                .await
                .context("pg_export_snapshot")?
                .ok_or_else(|| anyhow::anyhow!("pg_export_snapshot returned no rows"))?;
            for _ in 1..connections {
                let conn = coord_connect_with(&pg_cfg.dsn(), tls)
                    .await
                    .context("connect snapshot reader")?;
                conn.client
                    .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .await
                    .context("BEGIN snapshot reader tx")?;
                conn.client
                    .simple_query(&format!("SET TRANSACTION SNAPSHOT '{id}'"))
                    .await
                    .with_context(|| format!("import snapshot {id}"))?;
                clients.push(Mutex::new(conn.client));
                conns.push(conn.abort);
            }
        }

        Ok(Self {
            readers: Semaphore::new(clients.len()),
            clients,
            _conns: conns,
            snapshot_lsn,
            schemas: by_ident,
        })
    }

    pub fn snapshot_lsn(&self) -> Lsn {
        self.snapshot_lsn
    }

    fn cached(&self, ident: &TableIdent) -> SnapshotResult<&CachedSchema> {
        self.schemas
            .get(ident)
            .ok_or_else(|| SnapshotError::Source(format!("unknown table: {ident}")))
    }

    /// What `copy`, a `COPY … TO STDOUT`, outputs, run on a client free, in
    /// the snapshot.
    async fn copy_out(&self, copy: &str) -> SnapshotResult<Vec<u8>> {
        let _reader = self
            .readers
            .acquire()
            .await
            .map_err(|e| SnapshotError::Source(format!("snapshot readers: {e}")))?;
        // Each reader holds at most one client, so one is free.
        let client = self
            .clients
            .iter()
            .find_map(|c| c.try_lock().ok())
            .expect("a free client per reader");
        let stream = client
            .copy_out(copy)
            .await
            .map_err(|e| SnapshotError::Source(format!("{copy:?}: {e}")))?;
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(data) = stream.next().await {
            out.extend_from_slice(
                &data.map_err(|e| SnapshotError::Source(format!("{copy:?}: {e}")))?,
            );
        }
        Ok(out)
    }

    /// Run `q` on a client free, in the snapshot.
    async fn query(&self, q: &str) -> SnapshotResult<Vec<SimpleQueryMessage>> {
        let _reader = self
            .readers
            .acquire()
            .await
            .map_err(|e| SnapshotError::Source(format!("snapshot readers: {e}")))?;
        // Each reader holds at most one client, so one is free.
        let client = self
            .clients
            .iter()
            .find_map(|c| c.try_lock().ok())
            .expect("a free client per reader");
        client
            .simple_query(q)
            .await
            .map_err(|e| SnapshotError::Source(format!("{q:?}: {e}")))
    }

    /// The first column of each row `q` returns.
    async fn query_column(&self, q: &str) -> SnapshotResult<Vec<Option<String>>> {
        let mut values = Vec::new();
        for msg in self.query(q).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let value: Option<&str> = row
                    .try_get(0)
                    .map_err(|e| SnapshotError::Source(format!("{q:?}: {e}")))?;
                values.push(value.map(String::from));
            }
        }
        Ok(values)
    }

    /// The rows of `ident` past `after_pk_key`, of `range` if given — a
    /// chunk of up to `chunk_size`, in the cursor's order.
    async fn read(
        &self,
        ident: &TableIdent,
        range: Option<&KeyRange>,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> SnapshotResult<Vec<Row>> {
        let cached = self.cached(ident)?;

        // Read on in the cursor's order; a first chunk in the index's.
        let (order, after) = match after_pk_key {
            None => (cached.read_order().to_vec(), None),
            Some(after) => {
                let cursor = serde_json::from_str(after).map_err(|e| {
                    SnapshotError::Source(format!("decode after_pk_key {after:?}: {e}"))
                })?;
                let (order, values) = match cursor {
                    Cursor::Canonical(values) => (cached.pk_cols.clone(), values),
                    Cursor::Ordered(c) => (c.order, c.after),
                };
                if order.len() != cached.pk_cols.len()
                    || !order.iter().all(|c| cached.pk_cols.contains(c))
                    || values.len() != order.len()
                {
                    return Err(SnapshotError::Source(format!(
                        "after_pk_key {after:?} isn't a key of the primary key {:?}",
                        cached.pk_cols
                    )));
                }
                (order, Some(values))
            }
        };
        let pk_csv = order
            .iter()
            .map(|c| quote_ident(&c.0))
            .collect::<Vec<_>>()
            .join(", ");

        let table_sql = qualified_table(ident);

        let mut conditions = Vec::new();
        if let Some(values) = after {
            let lits: Vec<String> = values
                .iter()
                .map(pg_value_to_sql_literal)
                .collect::<std::result::Result<_, _>>()
                .map_err(SnapshotError::Source)?;
            conditions.push(format!(
                "({pk}) > ({vals})",
                pk = pk_csv,
                vals = lits.join(", ")
            ));
        }
        if let Some(range) = range {
            let lead = quote_ident(&cached.read_order()[0].0);
            if let Some(from) = &range.from {
                conditions.push(format!("{lead} >= {}", quote_lit(from)));
            }
            if let Some(to) = &range.to {
                conditions.push(format!("{lead} < {}", quote_lit(to)));
            }
        }
        let where_sql = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let q = format!(
            "COPY (SELECT {cols} FROM {tbl}{where_clause} ORDER BY {pk} ASC LIMIT {limit}) \
             TO STDOUT (FORMAT binary)",
            cols = cached.select,
            tbl = table_sql,
            where_clause = where_sql,
            pk = pk_csv,
            limit = chunk_size,
        );

        let copy = self
            .copy_out(&q)
            .await
            .map_err(|e| SnapshotError::Source(format!("snapshot read of {ident}: {e}")))?;

        let columns = &cached.schema.columns;
        let malformed = |e: String| SnapshotError::Source(format!("snapshot read of {ident}: {e}"));
        let mut tuples = snapshot_copy::Tuples::new(&copy, columns.len()).map_err(malformed)?;
        let mut fields = Vec::with_capacity(columns.len());
        let mut out = Vec::with_capacity(chunk_size.min(64));
        while tuples.next_into(&mut fields).map_err(malformed)? {
            let mut values = vec![PgValue::Null; cached.places.len()];
            for ((col, &place), field) in columns.iter().zip(&cached.places).zip(&fields) {
                if let Some(bytes) = field {
                    values[place] = snapshot_copy::decode(col.ty, bytes).map_err(|e| {
                        SnapshotError::Source(format!(
                            "decode column {} ({:?}): {e}",
                            col.name, col.ty
                        ))
                    })?;
                }
            }
            out.push(Row::with_columns(cached.row_columns.clone(), values));
        }
        Ok(out)
    }

    /// About how many rows `ident` holds, as of its last `ANALYZE`, its
    /// partitions' included; none if never analyzed.
    async fn estimated_rows(&self, ident: &TableIdent) -> SnapshotResult<f64> {
        let q = format!(
            "SELECT GREATEST(c.reltuples, 0) + COALESCE((                 SELECT sum(GREATEST(k.reltuples, 0)) FROM pg_inherits i                 JOIN pg_class k ON k.oid = i.inhrelid WHERE i.inhparent = c.oid), 0)              FROM pg_class c WHERE c.oid = {table}::regclass",
            table = quote_lit(&qualified_table(ident)),
        );
        let rows = self.query_column(&q).await?;
        Ok(rows
            .into_iter()
            .flatten()
            .next()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0.0))
    }

    /// The bounds of `column`'s histogram in `pg_stats`, least first: of
    /// as many rows between each two, about — but for its most common
    /// values. Empty without one.
    async fn histogram(&self, ident: &TableIdent, column: &str) -> SnapshotResult<Vec<String>> {
        let q = format!(
            "SELECT unnest(histogram_bounds::text::text[]) FROM (                 SELECT histogram_bounds FROM pg_stats                 WHERE schemaname = {schema} AND tablename = {table} AND attname = {column}                 ORDER BY inherited DESC LIMIT 1) s",
            schema = quote_lit(&ident.namespace.0.join(".")),
            table = quote_lit(&ident.name),
            column = quote_lit(column),
        );
        Ok(self.query_column(&q).await?.into_iter().flatten().collect())
    }

    /// `parts - 1` integers evenly between `column`'s least and greatest
    /// values, ascending, those that differ. Empty if it holds none.
    async fn integer_bounds(
        &self,
        ident: &TableIdent,
        column: &str,
        parts: usize,
    ) -> SnapshotResult<Vec<String>> {
        let column = quote_ident(column);
        let q = format!(
            "SELECT min({column})::text FROM {table} UNION ALL SELECT max({column})::text FROM {table}",
            table = qualified_table(ident),
        );
        let ends: Vec<i128> = self
            .query_column(&q)
            .await?
            .into_iter()
            .flatten()
            .filter_map(|v| v.parse().ok())
            .collect();
        let [lo, hi] = ends[..] else {
            return Ok(Vec::new());
        };
        let parts = parts as i128;
        let mut bounds: Vec<String> = (1..parts)
            .map(|p| lo + (hi - lo) * p / parts)
            .filter(|&b| b > lo)
            .map(|b| b.to_string())
            .collect();
        bounds.dedup();
        Ok(bounds)
    }
}

impl CachedSchema {
    /// The order a table's first chunk is read in: its index's.
    fn read_order(&self) -> &[ColumnName] {
        self.index_order.as_deref().unwrap_or(&self.pk_cols)
    }
}

#[async_trait]
impl SnapshotSource for PgSnapshotSource {
    async fn snapshot_lsn(&self) -> SnapshotResult<Lsn> {
        Ok(self.snapshot_lsn)
    }

    async fn read_chunk(
        &self,
        ident: &TableIdent,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> SnapshotResult<Vec<Row>> {
        self.read(ident, None, chunk_size, after_pk_key).await
    }

    async fn split(
        &self,
        ident: &TableIdent,
        parts: usize,
        min_rows: usize,
    ) -> SnapshotResult<Vec<KeyRange>> {
        let cached = self.cached(ident)?;
        let rows = self.estimated_rows(ident).await?;
        let parts = parts.min((rows / min_rows.max(1) as f64) as usize);
        if parts < 2 {
            return Ok(Vec::new());
        }
        let lead = &cached.read_order()[0].0;
        let histogram = self.histogram(ident, lead).await?;
        let mut bounds: Vec<String> = if histogram.len() > 2 {
            // Every so many of its inner bounds, as many rows apart.
            let buckets = histogram.len() - 1;
            let parts = parts.min(buckets);
            (1..parts)
                .map(|p| histogram[p * buckets / parts].clone())
                .collect()
        } else {
            let integer = cached
                .schema
                .columns
                .iter()
                .find(|c| c.name == *lead)
                .is_some_and(|c| {
                    matches!(
                        c.ty,
                        pg2iceberg_core::IcebergType::Int | pg2iceberg_core::IcebergType::Long
                    )
                });
            if !integer {
                return Ok(Vec::new());
            }
            self.integer_bounds(ident, lead, parts).await?
        };
        bounds.dedup();
        if bounds.is_empty() {
            return Ok(Vec::new());
        }
        let ends: Vec<Option<String>> = std::iter::once(None)
            .chain(bounds.into_iter().map(Some))
            .chain(std::iter::once(None))
            .collect();
        Ok(ends
            .windows(2)
            .map(|w| KeyRange {
                from: w[0].clone(),
                to: w[1].clone(),
            })
            .collect())
    }

    async fn read_range(
        &self,
        ident: &TableIdent,
        range: &KeyRange,
        chunk_size: usize,
        after_pk_key: Option<&str>,
    ) -> SnapshotResult<Vec<Row>> {
        self.read(ident, Some(range), chunk_size, after_pk_key)
            .await
    }

    fn cursor(
        &self,
        ident: &TableIdent,
        after: Option<&str>,
        last: &Row,
        pk_cols: &[ColumnName],
    ) -> String {
        // The order `last`'s chunk was read in (see `read_chunk`).
        let order = match after.map(serde_json::from_str::<Cursor>) {
            Some(Ok(Cursor::Ordered(c))) => Some(c.order),
            Some(_) => None,
            None => self.schemas.get(ident).and_then(|c| c.index_order.clone()),
        };
        match order {
            Some(order) => serde_json::to_string(&OrderedCursor {
                after: order.iter().filter_map(|c| last.get(c).cloned()).collect(),
                order,
            })
            .expect("PgValue is Serializable"),
            None => pk_key(last, pk_cols),
        }
    }
}

/// Each of `ident`'s columns' type's name and output function.
async fn column_types(
    client: &Client,
    ident: &TableIdent,
) -> Result<BTreeMap<String, (String, String)>> {
    let q = format!(
        "SELECT a.attname, t.typname, t.typoutput::regproc::text \
         FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid \
         WHERE a.attrelid = {table}::regclass AND a.attnum > 0 AND NOT a.attisdropped",
        table = quote_lit(&qualified_table(ident)),
    );
    let mut types = BTreeMap::new();
    for msg in client
        .simple_query(&q)
        .await
        .with_context(|| format!("simple_query {q:?}"))?
    {
        if let SimpleQueryMessage::Row(row) = msg {
            let get = |i| -> Result<String> {
                Ok(row
                    .try_get(i)
                    .context("pg_attribute")?
                    .unwrap_or_default()
                    .to_string())
            };
            types.insert(get(0)?, (get(1)?, get(2)?));
        }
    }
    Ok(types)
}

/// The columns of `ident`'s primary key in its index's order, or `None`
/// without one.
async fn pk_index_order(client: &Client, ident: &TableIdent) -> Result<Option<Vec<ColumnName>>> {
    let q = format!(
        "SELECT a.attname \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = {table}::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey, a.attnum)",
        table = quote_lit(&qualified_table(ident)),
    );
    let mut cols = Vec::new();
    for msg in client
        .simple_query(&q)
        .await
        .with_context(|| format!("simple_query {q:?}"))?
    {
        if let SimpleQueryMessage::Row(row) = msg {
            let name: Option<&str> = row.try_get(0).context("pg_attribute.attname")?;
            cols.extend(name.map(|n| ColumnName(n.to_string())));
        }
    }
    Ok((!cols.is_empty()).then_some(cols))
}

/// A value of `col` in its text form: what its binary form, which chunks
/// are read in, decodes to as well.
pub fn decode_column(col: &ColumnSchema, raw_text: &str) -> SnapshotResult<PgValue> {
    let pg_type = iceberg_to_pg_type_for_decode(col.ty);
    decode_text(pg_type, raw_text.as_bytes()).map_err(|e| {
        SnapshotError::Source(format!(
            "decode column {} ({:?}) from {raw_text:?}: {e}",
            col.name, col.ty
        ))
    })
}

/// `decode_text` takes a [`pg2iceberg_core::PgType`] but our cached
/// schema is in [`pg2iceberg_core::IcebergType`] form. Map back to a
/// suitable PgType for text-format decoding. The mapping is lossy in
/// theory (PG `int2` and `int4` both map to Iceberg `Int`), but
/// `decode_text` for `Int4` parses the same text representation as
/// `Int2` would, so it's lossless in practice.
fn iceberg_to_pg_type_for_decode(ty: pg2iceberg_core::IcebergType) -> pg2iceberg_core::PgType {
    use pg2iceberg_core::{IcebergType, PgType};
    match ty {
        IcebergType::Boolean => PgType::Bool,
        IcebergType::Int => PgType::Int4,
        IcebergType::Long => PgType::Int8,
        IcebergType::Float => PgType::Float4,
        IcebergType::Double => PgType::Float8,
        IcebergType::Decimal { precision, scale } => PgType::Numeric {
            precision: Some(precision),
            scale: Some(scale),
        },
        IcebergType::String => PgType::Text,
        IcebergType::Binary => PgType::Bytea,
        IcebergType::Date => PgType::Date,
        IcebergType::Time => PgType::Time,
        IcebergType::Timestamp => PgType::Timestamp,
        IcebergType::TimestampTz => PgType::TimestampTz,
        IcebergType::Uuid => PgType::Uuid,
    }
}

/// Convert a [`PgValue`] to a Postgres-source-compatible SQL literal
/// suitable for embedding in a `WHERE (cols) > (literals)` row
/// comparison. Used only for keyset-pagination cursor reseeking; the
/// scope is small so we can afford to be exhaustive.
pub(crate) fn pg_value_to_sql_literal(v: &PgValue) -> std::result::Result<String, String> {
    Ok(match v {
        PgValue::Null => "NULL".into(),
        PgValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        PgValue::Int2(n) => n.to_string(),
        PgValue::Int4(n) => n.to_string(),
        PgValue::Int8(n) => n.to_string(),
        PgValue::Float4(n) => format_float(*n as f64),
        PgValue::Float8(n) => format_float(*n),
        PgValue::Numeric(d) => {
            let s = format_decimal(d).map_err(|e| format!("encode numeric: {e}"))?;
            format!("{}::numeric", quote_lit(&s))
        }
        PgValue::Text(s) => quote_lit(s),
        PgValue::Bytea(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("'\\x{hex}'::bytea")
        }
        PgValue::Date(DaysSinceEpoch(days)) => {
            // Format as ISO date.
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("epoch")
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| format!("date out of range: days={days}"))?;
            format!("{}::date", quote_lit(&date.format("%Y-%m-%d").to_string()))
        }
        PgValue::Time(TimeMicros(micros)) => {
            let s = format_time_of_day(*micros);
            format!("{}::time", quote_lit(&s))
        }
        PgValue::TimeTz {
            time: TimeMicros(micros),
            zone_secs,
        } => {
            let mut s = format_time_of_day(*micros);
            s.push_str(&format_offset(*zone_secs));
            format!("{}::timetz", quote_lit(&s))
        }
        PgValue::Timestamp(TimestampMicros(micros)) => {
            let s = format_timestamp(*micros)?;
            format!("{}::timestamp", quote_lit(&s))
        }
        PgValue::TimestampTz(TimestampMicros(micros)) => {
            // PG accepts ISO with offset; we emit UTC+00.
            let s = format!("{}+00", format_timestamp(*micros)?);
            format!("{}::timestamptz", quote_lit(&s))
        }
        PgValue::Uuid(bytes) => {
            let s = uuid::Uuid::from_bytes(*bytes).hyphenated().to_string();
            format!("{}::uuid", quote_lit(&s))
        }
        PgValue::Json(s) => format!("{}::json", quote_lit(s)),
        PgValue::Jsonb(s) => format!("{}::jsonb", quote_lit(s)),
    })
}

fn format_float(n: f64) -> String {
    if n.is_nan() {
        "'NaN'::float8".into()
    } else if n.is_infinite() {
        if n.is_sign_positive() {
            "'Infinity'::float8".into()
        } else {
            "'-Infinity'::float8".into()
        }
    } else {
        format!("{n}")
    }
}

fn format_decimal(d: &CoreDecimal) -> std::result::Result<String, String> {
    // Reverse of `decode_numeric`: render unscaled BE bytes + scale as
    // a textual numeric literal. We reuse `rust_decimal` (already a dep
    // of pg2iceberg-pg) at runtime, but it's not a dep of the binary.
    // Reach for `i128` instead — Iceberg caps decimals at 38 digits so
    // this suffices.
    if d.unscaled_be_bytes.is_empty() {
        return Ok(format!("0e-{}", d.scale));
    }
    if d.unscaled_be_bytes.len() > 16 {
        return Err(format!(
            "numeric > 128-bit unscaled (len={})",
            d.unscaled_be_bytes.len()
        ));
    }
    // Sign-extend to 16 bytes for two's-complement i128.
    let mut buf = [0u8; 16];
    let sign = d.unscaled_be_bytes[0] & 0x80;
    let pad = if sign != 0 { 0xFF } else { 0x00 };
    buf.fill(pad);
    let off = 16 - d.unscaled_be_bytes.len();
    buf[off..].copy_from_slice(&d.unscaled_be_bytes);
    let unscaled = i128::from_be_bytes(buf);
    let scale = d.scale as usize;
    if scale == 0 {
        return Ok(unscaled.to_string());
    }
    let abs = unscaled.unsigned_abs();
    let mut s = abs.to_string();
    while s.len() <= scale {
        s.insert(0, '0');
    }
    let split = s.len() - scale;
    let formatted = format!("{}.{}", &s[..split], &s[split..]);
    if unscaled < 0 {
        Ok(format!("-{formatted}"))
    } else {
        Ok(formatted)
    }
}

fn format_time_of_day(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let frac_us = (micros % 1_000_000) as u32;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    if frac_us == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{h:02}:{m:02}:{s:02}.{frac_us:06}")
    }
}

fn format_offset(zone_secs: i32) -> String {
    let sign = if zone_secs < 0 { '-' } else { '+' };
    let abs = zone_secs.unsigned_abs();
    let h = abs / 3600;
    let m = (abs % 3600) / 60;
    if m == 0 {
        format!("{sign}{h:02}")
    } else {
        format!("{sign}{h:02}:{m:02}")
    }
}

fn format_timestamp(micros: i64) -> std::result::Result<String, String> {
    let secs = micros.div_euclid(1_000_000);
    let frac_us = micros.rem_euclid(1_000_000) as u32;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| format!("timestamp out of range: micros={micros}"))?;
    let naive = dt.naive_utc();
    if frac_us == 0 {
        Ok(naive.format("%Y-%m-%d %H:%M:%S").to_string())
    } else {
        Ok(format!(
            "{}.{frac_us:06}",
            naive.format("%Y-%m-%d %H:%M:%S")
        ))
    }
}

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_lit(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

fn qualified_table(ident: &TableIdent) -> String {
    if ident.namespace.0.is_empty() {
        quote_ident(&ident.name)
    } else {
        format!(
            "{}.{}",
            quote_ident(&ident.namespace.0.join(".")),
            quote_ident(&ident.name)
        )
    }
}

fn parse_lsn(s: &str) -> Result<Lsn> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("malformed LSN: {s}"))?;
    let hi = u64::from_str_radix(hi, 16).map_err(|_| anyhow::anyhow!("malformed LSN hi: {s}"))?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| anyhow::anyhow!("malformed LSN lo: {s}"))?;
    Ok(Lsn((hi << 32) | lo))
}

async fn simple_query_one_value(client: &Client, q: &str) -> Result<Option<String>> {
    let messages = client
        .simple_query(q)
        .await
        .with_context(|| format!("simple_query {q:?}"))?;
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let v: Option<&str> = row
                .try_get(0)
                .with_context(|| format!("row.try_get(0) for {q:?}"))?;
            return Ok(v.map(String::from));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_doubles_quotes() {
        assert_eq!(quote_ident("foo"), "\"foo\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn quote_lit_doubles_quotes() {
        assert_eq!(quote_lit("foo"), "'foo'");
        assert_eq!(quote_lit("a'b"), "'a''b'");
    }

    #[test]
    fn pg_value_to_sql_literal_int_text_uuid() {
        assert_eq!(pg_value_to_sql_literal(&PgValue::Int4(42)).unwrap(), "42");
        assert_eq!(
            pg_value_to_sql_literal(&PgValue::Text("o'rly".into())).unwrap(),
            "'o''rly'"
        );
        let u = PgValue::Uuid([
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ]);
        let sql = pg_value_to_sql_literal(&u).unwrap();
        assert!(sql.starts_with("'550e8400-"), "got {sql}");
        assert!(sql.ends_with("::uuid"), "got {sql}");
    }

    #[test]
    fn pg_value_to_sql_literal_null_bool_bytea() {
        assert_eq!(pg_value_to_sql_literal(&PgValue::Null).unwrap(), "NULL");
        assert_eq!(
            pg_value_to_sql_literal(&PgValue::Bool(true)).unwrap(),
            "TRUE"
        );
        assert_eq!(
            pg_value_to_sql_literal(&PgValue::Bytea(vec![0xde, 0xad])).unwrap(),
            "'\\xdead'::bytea"
        );
    }

    #[test]
    fn format_decimal_preserves_scale() {
        // 12345 unscaled with scale 2 = 123.45
        let d = CoreDecimal {
            unscaled_be_bytes: vec![0x30, 0x39],
            scale: 2,
        };
        assert_eq!(format_decimal(&d).unwrap(), "123.45");

        // Negative: -123 unscaled with scale 0
        let neg = CoreDecimal {
            unscaled_be_bytes: vec![0xFF, 0x85], // -123 in 16-bit two's complement
            scale: 0,
        };
        assert_eq!(format_decimal(&neg).unwrap(), "-123");
    }

    #[test]
    fn parse_lsn_round_trips() {
        assert_eq!(parse_lsn("0/0").unwrap().0, 0);
        assert_eq!(parse_lsn("1/0").unwrap().0, 0x1_0000_0000);
        assert_eq!(
            parse_lsn("12345678/9ABCDEF0").unwrap().0,
            0x1234_5678_9ABC_DEF0
        );
        assert!(parse_lsn("oops").is_err());
    }
}
