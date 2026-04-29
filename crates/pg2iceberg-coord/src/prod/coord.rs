//! [`PostgresCoordinator`]: production [`Coordinator`] backed by `tokio-postgres`.
//!
//! Every method maps to one of the SQL statements in [`crate::sql`].
//! `claim_offsets` runs inside a single PG transaction so the
//! ensure-row → claim-sequence → insert-log-index sequence commits
//! atomically — that's the durability invariant the
//! [`crate::CoordCommitReceipt`] type encodes.
//!
//! # Lifetime
//!
//! `PostgresCoordinator` owns a `tokio_postgres::Client` plus the
//! `AbortHandle` of its background connection task. Drop the
//! coordinator to drop the connection.

use crate::prod::connect::PgConn;
use crate::schema::CoordSchema;
use crate::sql;
use crate::{
    receipt, CommitBatch, CoordCommitReceipt, CoordError, Coordinator, LogEntry, MarkerInfo,
    OffsetGrant, Result,
};
use async_trait::async_trait;
use crate::TableSnapshotState;
use pg2iceberg_core::{Lsn, TableIdent, WorkerId};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tokio_postgres::Client;

/// `claim_offsets` opens a transaction, which `tokio-postgres` only
/// exposes via `&mut Client`. We wrap the client in a `tokio::Mutex`
/// so the trait's `&self` methods can still acquire the unique borrow
/// they need. In our use case all coord ops are serialized through the
/// materializer cycle anyway, so the mutex isn't a perf concern.
pub struct PostgresCoordinator {
    client: Mutex<Client>,
    schema: CoordSchema,
    _abort: AbortHandle,
}

impl PostgresCoordinator {
    /// Wrap an existing connection. The caller is responsible for
    /// having opened the connection in regular (non-replication) mode.
    pub fn new(conn: PgConn, schema: CoordSchema) -> Self {
        Self {
            client: Mutex::new(conn.client),
            schema,
            _abort: conn.abort,
        }
    }

    /// Run the schema migration. Idempotent — every statement uses
    /// `CREATE TABLE IF NOT EXISTS`. Caller is expected to invoke this
    /// once at startup before any other coord method.
    pub async fn migrate(&self) -> Result<()> {
        let client = self.client.lock().await;
        client
            .batch_execute(&sql::create_schema(&self.schema))
            .await
            .map_err(pg)?;
        for stmt in sql::migrate(&self.schema) {
            client.batch_execute(&stmt).await.map_err(pg)?;
        }
        Ok(())
    }

    /// `DROP SCHEMA … CASCADE` — wipe every coordinator table this
    /// instance owns. Used by the `cleanup` subcommand to tear down
    /// pipeline state ahead of a re-bootstrap. Idempotent: a missing
    /// schema is silently ignored.
    ///
    /// Aborts the in-process connection task on completion is
    /// **not** done here — the caller drops `self` to do that. We
    /// intentionally don't close the connection so callers can chain
    /// teardown calls if needed.
    pub async fn teardown(&self) -> Result<()> {
        let client = self.client.lock().await;
        client
            .batch_execute(&sql::drop_schema(&self.schema))
            .await
            .map_err(pg)?;
        Ok(())
    }
}

impl Drop for PostgresCoordinator {
    fn drop(&mut self) {
        self._abort.abort();
    }
}

fn pg(e: tokio_postgres::Error) -> CoordError {
    CoordError::Pg(e.to_string())
}

/// `TableIdent` → the canonical text key used in the `_pg2iceberg`
/// tables. Mirrors the Go reference: namespace and table joined by `.`,
/// or just `table` if no namespace.
fn table_key(t: &TableIdent) -> String {
    if t.namespace.0.is_empty() {
        t.name.clone()
    } else {
        format!("{}.{}", t.namespace.0.join("."), t.name)
    }
}

/// Format a `Duration` as a Postgres `INTERVAL` text input. PG accepts
/// the cast `'<n> seconds'::interval` cleanly down to microsecond
/// resolution, which is enough for our consumer/lock TTLs.
fn duration_as_interval(d: Duration) -> String {
    // `as_micros()` returns u128; cap at i64::MAX to keep the cast safe.
    let micros = d.as_micros().min(i64::MAX as u128);
    format!("{micros} microseconds")
}

/// `Lsn` → BIGINT for storage. PG's BIGINT is i64; LSNs are u64. We
/// reinterpret the bits — LSNs in practice fit comfortably in i64 (PG
/// itself uses int8 internally).
fn lsn_to_i64(lsn: Lsn) -> i64 {
    lsn.0 as i64
}

/// Inverse of [`lsn_to_i64`].
fn i64_to_lsn(v: i64) -> Lsn {
    Lsn(v as u64)
}

#[async_trait]
impl Coordinator for PostgresCoordinator {
    async fn claim_offsets(&self, batch: &CommitBatch) -> Result<CoordCommitReceipt> {
        // Marker-only flush (no log_index rows but markers to record).
        // Persists markers atomically inside its own short tx.
        if batch.claims.is_empty() {
            if !batch.markers.is_empty() {
                let mut client = self.client.lock().await;
                let tx = client.transaction().await.map_err(pg)?;
                let insert_marker = sql::insert_pending_marker(&self.schema);
                for m in &batch.markers {
                    let lsn_i64 = lsn_to_i64(m.commit_lsn);
                    tx.execute(&insert_marker, &[&m.uuid, &lsn_i64])
                        .await
                        .map_err(pg)?;
                }
                tx.commit().await.map_err(pg)?;
            }
            return Ok(receipt::mint(batch.flushable_lsn, Vec::new()));
        }

        // Aggregate per-table totals so we hit `log_seq` once per table
        // rather than once per claim (matches Go: tableOffsets map).
        let mut totals: BTreeMap<String, u64> = BTreeMap::new();
        for c in &batch.claims {
            let key = table_key(&c.table);
            *totals.entry(key).or_default() += c.record_count;
        }

        // Single transaction: ensure log_seq rows, claim ranges, insert
        // log_index rows, write markers. Mints the receipt only after
        // COMMIT returns. The atomicity is what makes the durability
        // invariant work — pipeline either sees its log_index rows AND
        // its markers persisted together, or neither.
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(pg)?;

        let ensure_q = sql::ensure_log_seq_row(&self.schema);
        let claim_q = sql::claim_log_seq(&self.schema);
        let insert_q = sql::insert_log_index(&self.schema);

        let mut ends: BTreeMap<String, i64> = BTreeMap::new();
        for (key, total) in &totals {
            tx.execute(&ensure_q, &[key]).await.map_err(pg)?;
            // `log_seq.next_offset` is BIGINT (i64) on the PG side; we
            // claim `total` records and PG returns the new high-water
            // mark.
            let total_i64 =
                i64::try_from(*total).map_err(|_| CoordError::Other("claim total > i64".into()))?;
            let row = tx
                .query_one(&claim_q, &[key, &total_i64])
                .await
                .map_err(pg)?;
            let new_end: i64 = row.get(0);
            ends.insert(key.clone(), new_end);
        }

        // Walk claims in input order, computing per-claim (start, end)
        // by counting backwards from the per-table high-water mark. We
        // know each claim's record_count so we step backwards, but
        // keeping a running offset forward is simpler — same approach
        // as the sim.
        let mut start_offsets: BTreeMap<String, i64> = ends
            .iter()
            .map(|(k, e)| {
                let total = totals[k] as i64;
                (k.clone(), *e - total)
            })
            .collect();
        let flushable_lsn_i64 = lsn_to_i64(batch.flushable_lsn);
        let mut grants: Vec<OffsetGrant> = Vec::with_capacity(batch.claims.len());
        for c in &batch.claims {
            let key = table_key(&c.table);
            let start = *start_offsets.get(&key).unwrap();
            let end = start + c.record_count as i64;
            start_offsets.insert(key.clone(), end);

            let record_count_i32 = i32::try_from(c.record_count)
                .map_err(|_| CoordError::Other(format!("record_count > i32 for {}", c.table)))?;
            let byte_size_i64 = i64::try_from(c.byte_size)
                .map_err(|_| CoordError::Other(format!("byte_size > i64 for {}", c.table)))?;
            tx.execute(
                &insert_q,
                &[
                    &key,
                    &end,
                    &start,
                    &c.s3_path,
                    &record_count_i32,
                    &byte_size_i64,
                    &flushable_lsn_i64,
                ],
            )
            .await
            .map_err(pg)?;

            grants.push(OffsetGrant {
                table: c.table.clone(),
                start_offset: start as u64,
                end_offset: end as u64,
                s3_path: c.s3_path.clone(),
            });
        }

        // Markers ride the same transaction as the log_index rows.
        if !batch.markers.is_empty() {
            let insert_marker = sql::insert_pending_marker(&self.schema);
            for m in &batch.markers {
                let lsn_i64 = lsn_to_i64(m.commit_lsn);
                tx.execute(&insert_marker, &[&m.uuid, &lsn_i64])
                    .await
                    .map_err(pg)?;
            }
        }

        tx.commit().await.map_err(pg)?;
        Ok(receipt::mint(batch.flushable_lsn, grants))
    }

    async fn read_log(
        &self,
        table: &TableIdent,
        after_offset: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        let key = table_key(table);
        let after_i64 = i64::try_from(after_offset)
            .map_err(|_| CoordError::Other("after_offset > i64".into()))?;
        let limit_i64 =
            i64::try_from(limit).map_err(|_| CoordError::Other("limit > i64".into()))?;
        let client = self.client.lock().await;
        let rows = client
            .query(
                &sql::read_log(&self.schema),
                &[&key, &after_i64, &limit_i64],
            )
            .await
            .map_err(pg)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let start: i64 = r.get("start_offset");
            let end: i64 = r.get("end_offset");
            let path: String = r.get("s3_path");
            let rc: i32 = r.get("record_count");
            let bs: i64 = r.get("byte_size");
            let flushable_lsn_raw: i64 = r.get("flushable_lsn");
            out.push(LogEntry {
                table: table.clone(),
                start_offset: start as u64,
                end_offset: end as u64,
                s3_path: path,
                record_count: rc as u64,
                byte_size: bs as u64,
                flushable_lsn: i64_to_lsn(flushable_lsn_raw),
            });
        }
        Ok(out)
    }

    async fn truncate_log(&self, table: &TableIdent, before_offset: u64) -> Result<Vec<String>> {
        let key = table_key(table);
        let before_i64 = i64::try_from(before_offset)
            .map_err(|_| CoordError::Other("before_offset > i64".into()))?;
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::truncate_log(&self.schema), &[&key, &before_i64])
            .await
            .map_err(pg)?;
        Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
    }

    async fn ensure_cursor(&self, group: &str, table: &TableIdent) -> Result<()> {
        let key = table_key(table);
        let client = self.client.lock().await;
        client
            .execute(&sql::ensure_cursor(&self.schema), &[&group, &key])
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn get_cursor(&self, group: &str, table: &TableIdent) -> Result<Option<i64>> {
        let key = table_key(table);
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::get_cursor(&self.schema), &[&group, &key])
            .await
            .map_err(pg)?;
        Ok(rows.first().map(|r| r.get::<_, i64>(0)))
    }

    async fn set_cursor(&self, group: &str, table: &TableIdent, to_offset: i64) -> Result<()> {
        let key = table_key(table);
        let client = self.client.lock().await;
        let n = client
            .execute(&sql::set_cursor(&self.schema), &[&group, &key, &to_offset])
            .await
            .map_err(pg)?;
        if n == 0 {
            return Err(CoordError::NotFound(format!("cursor for {group}/{table}")));
        }
        Ok(())
    }

    async fn register_consumer(&self, group: &str, worker: &WorkerId, ttl: Duration) -> Result<()> {
        let interval = duration_as_interval(ttl);
        let client = self.client.lock().await;
        client
            .execute(
                &sql::register_consumer(&self.schema),
                &[&group, &worker.0, &interval],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn unregister_consumer(&self, group: &str, worker: &WorkerId) -> Result<()> {
        let client = self.client.lock().await;
        client
            .execute(
                &sql::unregister_consumer(&self.schema),
                &[&group, &worker.0],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn active_consumers(&self, group: &str) -> Result<Vec<WorkerId>> {
        // Sweep expired rows first (matches Go's ActiveConsumers).
        let client = self.client.lock().await;
        client
            .execute(&sql::expire_consumers(&self.schema), &[])
            .await
            .map_err(pg)?;
        let rows = client
            .query(&sql::active_consumers(&self.schema), &[&group])
            .await
            .map_err(pg)?;
        Ok(rows
            .into_iter()
            .map(|r| WorkerId(r.get::<_, String>(0)))
            .collect())
    }

    async fn try_lock(&self, table: &TableIdent, worker: &WorkerId, ttl: Duration) -> Result<bool> {
        let key = table_key(table);
        // Sweep stale lock for this table first so a dead holder
        // doesn't block a live worker.
        let client = self.client.lock().await;
        client
            .execute(&sql::expire_locks(&self.schema), &[&key])
            .await
            .map_err(pg)?;
        let interval = duration_as_interval(ttl);
        let n = client
            .execute(&sql::try_lock(&self.schema), &[&key, &worker.0, &interval])
            .await
            .map_err(pg)?;
        Ok(n == 1)
    }

    async fn renew_lock(
        &self,
        table: &TableIdent,
        worker: &WorkerId,
        ttl: Duration,
    ) -> Result<bool> {
        let key = table_key(table);
        let interval = duration_as_interval(ttl);
        let client = self.client.lock().await;
        let n = client
            .execute(
                &sql::renew_lock(&self.schema),
                &[&key, &worker.0, &interval],
            )
            .await
            .map_err(pg)?;
        Ok(n == 1)
    }

    async fn release_lock(&self, table: &TableIdent, worker: &WorkerId) -> Result<()> {
        let key = table_key(table);
        let client = self.client.lock().await;
        client
            .execute(&sql::release_lock(&self.schema), &[&key, &worker.0])
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn pipeline_system_identifier(&self) -> Result<u64> {
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::select_pipeline_meta(&self.schema), &[])
            .await
            .map_err(pg)?;
        match rows.first() {
            Some(r) => {
                let v: i64 = r.get(0);
                Ok(v as u64)
            }
            None => Ok(0),
        }
    }

    async fn set_pipeline_system_identifier(&self, sysid: u64) -> Result<()> {
        // Read-modify-write the row inside one transaction so a
        // concurrent stamper can't race us into a silent overwrite.
        // The CHECK (id = 1) constraint plus the SELECT-then-UPSERT
        // means there's at most one writer that wins; everyone else
        // either matches the stored value (no-op) or returns
        // SystemIdMismatch.
        let mut client = self.client.lock().await;
        let tx = client.transaction().await.map_err(pg)?;
        let stored: u64 = match tx
            .query(&sql::select_pipeline_meta(&self.schema), &[])
            .await
            .map_err(pg)?
            .first()
        {
            Some(r) => {
                let v: i64 = r.get(0);
                v as u64
            }
            None => 0,
        };
        if stored != 0 && stored != sysid {
            // Don't write the conflicting value. Surface the
            // mismatch and let the caller decide (typically: exit
            // with a clear error pointing at the DSN).
            return Err(CoordError::SystemIdMismatch {
                stored,
                connected: sysid,
            });
        }
        let v: i64 = sysid as i64;
        tx.execute(&sql::upsert_pipeline_meta(&self.schema), &[&v])
            .await
            .map_err(pg)?;
        tx.commit().await.map_err(pg)?;
        Ok(())
    }

    async fn flushed_lsn(&self) -> Result<Lsn> {
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::select_flushed_lsn(&self.schema), &[])
            .await
            .map_err(pg)?;
        match rows.first() {
            Some(r) => {
                let v: i64 = r.get(0);
                Ok(i64_to_lsn(v))
            }
            None => Ok(Lsn::ZERO),
        }
    }

    async fn set_flushed_lsn(&self, lsn: Lsn) -> Result<()> {
        let v = lsn_to_i64(lsn);
        let client = self.client.lock().await;
        client
            .execute(&sql::upsert_flushed_lsn(&self.schema), &[&v])
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn table_state(&self, ident: &TableIdent) -> Result<Option<TableSnapshotState>> {
        let key = table_key(ident);
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::select_table_state(&self.schema), &[&key])
            .await
            .map_err(pg)?;
        match rows.first() {
            Some(r) => {
                let pg_oid: i64 = r.get(0);
                let snapshot_complete: bool = r.get(1);
                let snapshot_lsn: i64 = r.get(2);
                let completed_micros: Option<f64> = r.try_get(3).ok();
                Ok(Some(TableSnapshotState {
                    pg_oid: pg_oid as u32,
                    snapshot_complete,
                    snapshot_lsn: i64_to_lsn(snapshot_lsn),
                    completed_at_micros: completed_micros.map(|f| f as i64),
                }))
            }
            None => Ok(None),
        }
    }

    async fn mark_table_snapshot_complete(
        &self,
        ident: &TableIdent,
        pg_oid: u32,
        snapshot_lsn: Lsn,
    ) -> Result<()> {
        let key = table_key(ident);
        let oid_i64: i64 = pg_oid as i64;
        let lsn_i64 = lsn_to_i64(snapshot_lsn);
        let client = self.client.lock().await;
        client
            .execute(
                &sql::mark_table_complete(&self.schema),
                &[&key, &oid_i64, &lsn_i64],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn snapshot_progress(&self, ident: &TableIdent) -> Result<Option<String>> {
        let key = table_key(ident);
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::select_snapshot_progress(&self.schema), &[&key])
            .await
            .map_err(pg)?;
        Ok(rows.first().map(|r| r.get::<_, String>(0)))
    }

    async fn set_snapshot_progress(
        &self,
        ident: &TableIdent,
        last_pk_key: &str,
    ) -> Result<()> {
        let key = table_key(ident);
        let client = self.client.lock().await;
        client
            .execute(
                &sql::upsert_snapshot_progress(&self.schema),
                &[&key, &last_pk_key],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn clear_snapshot_progress(&self, ident: &TableIdent) -> Result<()> {
        let key = table_key(ident);
        let client = self.client.lock().await;
        client
            .execute(&sql::delete_snapshot_progress(&self.schema), &[&key])
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn query_watermark(&self, ident: &TableIdent) -> Result<Option<pg2iceberg_core::PgValue>> {
        let key = table_key(ident);
        let client = self.client.lock().await;
        let rows = client
            .query(&sql::select_query_watermark(&self.schema), &[&key])
            .await
            .map_err(pg)?;
        match rows.first() {
            Some(r) => {
                let payload: serde_json::Value = r.get(0);
                let v: pg2iceberg_core::PgValue = serde_json::from_value(payload)
                    .map_err(|e| CoordError::Other(format!("query_watermark deserialize: {e}")))?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    async fn set_query_watermark(
        &self,
        ident: &TableIdent,
        watermark: &pg2iceberg_core::PgValue,
    ) -> Result<()> {
        let key = table_key(ident);
        let payload = serde_json::to_value(watermark)
            .map_err(|e| CoordError::Other(format!("query_watermark serialize: {e}")))?;
        let client = self.client.lock().await;
        client
            .execute(
                &sql::upsert_query_watermark(&self.schema),
                &[&key, &payload],
            )
            .await
            .map_err(pg)?;
        Ok(())
    }

    async fn pending_markers_for_table(
        &self,
        table: &TableIdent,
        cursor: i64,
    ) -> Result<Vec<MarkerInfo>> {
        // Per the trait doc: a marker is eligible iff every
        // log_index entry for `table` with
        // `flushable_lsn <= marker.commit_lsn` has
        // `end_offset <= cursor` (i.e. processed past the marker's
        // WAL point), and the marker hasn't already been emitted
        // for this table.
        //
        // The SQL in `sql::pending_markers_eligible` joins
        // `pending_markers` with `marker_emissions` (NOT EXISTS) and
        // `log_index` (NOT EXISTS for unprocessed). The qualified
        // table key (e.g. `"public.accounts"`) is built in PG by
        // `$1 || '.' || $2`.
        let namespace = table.namespace.0.join(".");
        let q = sql::pending_markers_eligible(&self.schema);
        let client = self.client.lock().await;
        let rows = client
            .query(&q, &[&namespace, &table.name, &cursor])
            .await
            .map_err(pg)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let uuid: String = r.get("uuid");
            let lsn_i64: i64 = r.get("commit_lsn");
            out.push(MarkerInfo {
                uuid,
                commit_lsn: i64_to_lsn(lsn_i64),
            });
        }
        Ok(out)
    }

    async fn record_marker_emitted(
        &self,
        uuid: &str,
        table: &TableIdent,
    ) -> Result<()> {
        let namespace = table.namespace.0.join(".");
        let q = sql::insert_marker_emission(&self.schema);
        let client = self.client.lock().await;
        client
            .execute(&q, &[&uuid, &namespace, &table.name])
            .await
            .map_err(pg)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::Namespace;

    #[test]
    fn table_key_no_namespace() {
        let id = TableIdent {
            namespace: Namespace(Vec::new()),
            name: "orders".into(),
        };
        assert_eq!(table_key(&id), "orders");
    }

    #[test]
    fn table_key_with_namespace() {
        let id = TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "orders".into(),
        };
        assert_eq!(table_key(&id), "public.orders");

        let id = TableIdent {
            namespace: Namespace(vec!["a".into(), "b".into()]),
            name: "t".into(),
        };
        assert_eq!(table_key(&id), "a.b.t");
    }

    #[test]
    fn duration_as_interval_uses_microseconds() {
        assert_eq!(
            duration_as_interval(Duration::from_secs(1)),
            "1000000 microseconds"
        );
        assert_eq!(
            duration_as_interval(Duration::from_millis(500)),
            "500000 microseconds"
        );
        assert_eq!(
            duration_as_interval(Duration::from_micros(7)),
            "7 microseconds"
        );
    }
}
