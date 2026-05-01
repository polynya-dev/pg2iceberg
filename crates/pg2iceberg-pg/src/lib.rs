//! Postgres client + replication trait surface.
//!
//! Production impls live in [`prod`] (gated behind the `prod` feature),
//! built on `tokio-postgres` + `postgres-replication`. The trait surface
//! is sim-friendly: nothing outside `prod` pulls a network dep.

#[cfg(feature = "prod")]
pub mod prod;

use async_trait::async_trait;
use pg2iceberg_core::{ChangeEvent, Lsn, TableIdent};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotId(pub String);

/// PG replication slot WAL retention state. Mirrors the
/// `pg_replication_slots.wal_status` column added in PG 13.
///
/// State machine: `reserved` → `extended` → `unreserved` → `lost`.
/// A slot can transition from `unreserved` back to `reserved`/`extended`
/// if the consumer catches up, but `lost` is terminal — the WAL
/// behind the slot's `restart_lsn` is gone.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum WalStatus {
    /// WAL pinned by this slot is within `max_wal_size`. Healthy.
    Reserved,
    /// Past `max_wal_size` but within `max_slot_wal_keep_size`. Normal under load.
    Extended,
    /// Past `max_slot_wal_keep_size`; WAL will be removed at the next checkpoint.
    /// Operator's last chance to fix consumer lag before data loss.
    Unreserved,
    /// WAL has been recycled. The slot is unrecoverable — `START_REPLICATION`
    /// will fail because `restart_lsn` no longer exists in the WAL stream.
    /// The only recovery is to drop the slot, drop the Iceberg tables,
    /// and re-snapshot.
    Lost,
}

impl WalStatus {
    /// Parse from the text form returned by `pg_replication_slots.wal_status::text`.
    /// PG 13+ only — older versions simply don't have this column.
    pub fn parse(s: &str) -> std::result::Result<Self, PgError> {
        match s {
            "reserved" => Ok(WalStatus::Reserved),
            "extended" => Ok(WalStatus::Extended),
            "unreserved" => Ok(WalStatus::Unreserved),
            "lost" => Ok(WalStatus::Lost),
            other => Err(PgError::Protocol(format!(
                "unknown pg_replication_slots.wal_status value: {other:?}"
            ))),
        }
    }
}

/// Combined view of a replication slot's health, suitable for
/// startup validation and the invariant watcher's per-tick probe.
/// Built by [`PgClient::slot_health`] in one round-trip rather than
/// the previous N round-trips for the individual fields.
#[derive(Clone, Debug)]
pub struct SlotHealth {
    pub exists: bool,
    pub restart_lsn: Lsn,
    pub confirmed_flush_lsn: Lsn,
    /// `Some` on PG 13+; `None` on older versions where the column
    /// doesn't exist (the prod query uses `to_jsonb` to degrade
    /// gracefully). The startup `wal_status = lost` check is a
    /// no-op on older PGs.
    pub wal_status: Option<WalStatus>,
    /// PG 14+. `false` on older versions. `true` indicates the slot
    /// was killed by a physical-replication conflict during recovery
    /// — also unrecoverable.
    pub conflicting: bool,
    /// PG 13+. Bytes until the slot crosses into `unreserved`. Negative
    /// when already past. `None` when the column doesn't exist or is
    /// NULL. Useful for metrics / dashboards; not directly consumed
    /// by validation today.
    pub safe_wal_size: Option<i64>,
}

#[derive(Clone, Debug, Error)]
pub enum PgError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("replication slot {0} not found")]
    SlotNotFound(String),
    #[error("publication {0} not found")]
    PublicationNotFound(String),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, PgError>;

/// One column observed in a pgoutput Relation message. Pre-mapped
/// to our [`pg2iceberg_core::IcebergType`] so the materializer
/// doesn't need to redo the OID-to-type mapping on its hot path.
///
/// pgoutput Relation messages don't carry nullability info — only
/// name, type-id, type-modifier, and a flag indicating REPLICA
/// IDENTITY membership. We stamp `nullable = true` on every
/// non-PK column for safety; that matches what real PG accepts on
/// `ALTER TABLE ADD COLUMN` without a `DEFAULT` clause, and
/// downstream readers tolerate an over-permissive nullable flag.
#[derive(Clone, Debug)]
pub struct RelationColumn {
    pub name: String,
    pub ty: pg2iceberg_core::IcebergType,
    /// `true` when pgoutput's column flags include the
    /// REPLICA-IDENTITY-key bit (typically: PK columns + columns
    /// in a `USING INDEX` index).
    pub is_primary_key: bool,
    pub nullable: bool,
}

/// Decoded pgoutput message. Matches the variants in `logical/decode.go:41-58`.
#[derive(Clone, Debug)]
pub enum DecodedMessage {
    Begin {
        final_lsn: Lsn,
        xid: u32,
    },
    Commit {
        commit_lsn: Lsn,
        xid: u32,
    },
    /// Schema for a relation. Sent before any change events for that
    /// relation, and re-sent after `ALTER TABLE` invalidates PG's
    /// cache. The lifecycle compares incoming columns against the
    /// materializer's registered schema; differences become
    /// `SchemaChange::AddColumn` / `DropColumn` and trigger
    /// `Catalog::evolve_schema`.
    Relation {
        ident: TableIdent,
        columns: Vec<RelationColumn>,
    },
    Change(ChangeEvent),
    /// Periodic primary keepalive; carries the server's WAL end position.
    Keepalive {
        wal_end: Lsn,
        reply_requested: bool,
    },
}

#[async_trait]
pub trait ReplicationStream: Send {
    async fn recv(&mut self) -> Result<DecodedMessage>;

    /// Send standby status. `flushed` is the LSN we've durably committed via
    /// the coordinator; PG can recycle WAL up to this point.
    async fn send_standby(&mut self, flushed: Lsn, applied: Lsn) -> Result<()>;
}

/// Read-only slot inspection. Smaller than [`PgClient`] so non-PG
/// sources (sim, query-mode reads) can supply confirmed_flush_lsn to
/// the invariant watcher without needing to fake the whole replication
/// surface. Blanket-impl'd for any `PgClient`.
#[async_trait]
pub trait SlotMonitor: Send + Sync {
    async fn confirmed_flush_lsn(&self, slot: &str) -> Result<Option<Lsn>>;
    /// Full health probe — used by the watcher to flag
    /// `wal_status = unreserved` (last warning before WAL loss) and
    /// to surface `safe_wal_size` as a metric. Default impl falls
    /// back to a minimal record built from `confirmed_flush_lsn` so
    /// non-PG monitor backends keep working.
    async fn slot_health_for_watcher(&self, slot: &str) -> Result<Option<SlotHealth>> {
        let cfl = self.confirmed_flush_lsn(slot).await?;
        Ok(cfl.map(|lsn| SlotHealth {
            exists: true,
            restart_lsn: Lsn::ZERO,
            confirmed_flush_lsn: lsn,
            wal_status: None,
            conflicting: false,
            safe_wal_size: None,
        }))
    }
}

#[async_trait]
impl<P: PgClient + ?Sized> SlotMonitor for P {
    async fn confirmed_flush_lsn(&self, slot: &str) -> Result<Option<Lsn>> {
        PgClient::slot_confirmed_flush_lsn(self, slot).await
    }
    async fn slot_health_for_watcher(&self, slot: &str) -> Result<Option<SlotHealth>> {
        PgClient::slot_health(self, slot).await
    }
}

#[async_trait]
pub trait PgClient: Send + Sync {
    async fn create_publication(&self, name: &str, tables: &[TableIdent]) -> Result<()>;
    async fn create_slot(&self, slot: &str) -> Result<Lsn>;
    async fn slot_exists(&self, slot: &str) -> Result<bool>;
    async fn slot_restart_lsn(&self, slot: &str) -> Result<Option<Lsn>>;
    /// `confirmed_flush_lsn` is the LSN downstream has acked. The watcher
    /// uses it to enforce invariant 1 (`pipeline.flushed_lsn ≤
    /// slot.confirmed_flush_lsn`). Returns `None` when the slot doesn't
    /// exist; returns `Some(Lsn(_))` when the slot exists. PG itself
    /// initializes the column to the slot's consistent_point at
    /// creation time, so a freshly-created slot returns
    /// `Some(consistent_point)`, not `Some(Lsn::ZERO)`. The latter
    /// is reserved as the mapping for SQL-NULL — a state PG doesn't
    /// surface for live slots in current versions, but the impl
    /// preserves it as a graceful fallback.
    async fn slot_confirmed_flush_lsn(&self, slot: &str) -> Result<Option<Lsn>>;

    async fn export_snapshot(&self) -> Result<SnapshotId>;

    /// Look up a table's `pg_class.oid` for the given
    /// `(namespace, name)`. Returns `None` if the table doesn't
    /// exist. Used by startup validation to detect identity
    /// changes — `DROP TABLE` + recreate gives a new oid even with
    /// the same schema, and the pre-drop Iceberg data is now
    /// stale.
    async fn table_oid(&self, namespace: &str, name: &str) -> Result<Option<u32>>;

    /// List the tables currently bound to `publication_name`.
    /// Empty vec for an empty / non-existent publication. Used by
    /// startup validation to detect the case where an operator
    /// removed a tracked table from the publication mid-run — any
    /// DML during the gap was filtered by the slot, so re-adding
    /// the table requires a full re-snapshot.
    async fn publication_tables(&self, publication_name: &str) -> Result<Vec<TableIdent>>;

    /// Combined slot health probe. Returns the `restart_lsn`,
    /// `confirmed_flush_lsn`, `wal_status`, `conflicting`, and
    /// `safe_wal_size` columns of `pg_replication_slots` in a single
    /// round-trip. Returns `None` when the slot doesn't exist.
    ///
    /// Pre-PG 13 doesn't have `wal_status` / `safe_wal_size`; pre-PG 14
    /// doesn't have `conflicting`. The prod impl uses `to_jsonb` to
    /// degrade gracefully on older versions — those fields surface as
    /// `None` / `false`.
    async fn slot_health(&self, slot: &str) -> Result<Option<SlotHealth>>;

    /// PostgreSQL cluster system identifier — unique per `initdb`,
    /// survives replication/clone, differs between blue and green
    /// even when green was bootstrapped from blue's snapshot. Used
    /// by the lifecycle to fingerprint the source cluster and
    /// stamp/verify it in the checkpoint, so an accidental DSN swap
    /// or stale-LSN-on-different-cluster resume can't silently
    /// corrupt downstream Iceberg state. Sourced from the
    /// `IDENTIFY_SYSTEM` replication command.
    async fn identify_system_id(&self) -> Result<u64>;

    async fn start_replication(
        &self,
        slot: &str,
        start: Lsn,
        publication: &str,
    ) -> Result<Box<dyn ReplicationStream>>;

    /// Drop the named replication slot. Idempotent — returns `Ok(())`
    /// if the slot doesn't exist. Errors if the slot is currently
    /// `active` (some consumer is connected); the caller must stop
    /// the consumer first. Used by the operational `cleanup`
    /// subcommand to tear down a pipeline's PG-side state.
    async fn drop_slot(&self, slot: &str) -> Result<()>;

    /// Drop the named publication. Idempotent — runs `DROP PUBLICATION
    /// IF EXISTS`, which is a noop on a missing publication. Used by
    /// the operational `cleanup` subcommand alongside [`Self::drop_slot`].
    async fn drop_publication(&self, name: &str) -> Result<()>;
}
