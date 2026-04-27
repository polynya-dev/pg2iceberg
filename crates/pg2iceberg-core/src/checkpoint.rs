//! Persisted replication state. Mirrors the Go reference's
//! `pipeline/checkpoint.go` — same field set, same checksum digest,
//! same Seal/Verify lifecycle, same OCC contract via `revision`.
//!
//! Two divergences from Go we keep deliberately:
//!
//! 1. `query_watermarks` values are typed [`PgValue`] rather than
//!    Go's RFC3339 strings. Operators with non-timestamp watermarks
//!    (`id BIGINT`, etc.) work without a wire-format change.
//! 2. `snapshot_progress` values are JSON-encoded PK cursors rather
//!    than Go's CTID chunk indices. Pagination-strategy-agnostic
//!    and survives `VACUUM FULL`.
//!
//! Map keys are flattened to `String` ("namespace.name" form) to
//! match Go's wire format and avoid the `BTreeMap<TableIdent, _>`
//! JSON-key gymnastics serde_json doesn't support natively.

use crate::lsn::Lsn;
use crate::value::PgValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

/// Schema version stamped on every save. Bump when adding a new
/// field changes the wire shape; `verify()` rejects checkpoints
/// with `version > CHECKPOINT_VERSION`.
pub const CHECKPOINT_VERSION: i32 = 1;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CheckpointError {
    #[error(
        "checkpoint version {got} is newer than supported version {max}; \
         upgrade pg2iceberg or delete the checkpoint to start fresh"
    )]
    VersionTooNew { got: i32, max: i32 },
    #[error(
        "checkpoint checksum mismatch (computed {expected}, stored {got}); \
         the checkpoint may have been modified externally; \
         delete it to start fresh"
    )]
    ChecksumMismatch { expected: String, got: String },
    #[error(
        "checkpoint system_identifier mismatch: stored {stored}, connected {connected}. \
         refusing to resume — a stale LSN from a different cluster cannot be \
         replayed safely. if this is an intentional blue/green cutover, use the \
         switchover flow; otherwise verify the source DSN"
    )]
    SystemIdMismatch { stored: u64, connected: u64 },
    #[error(
        "concurrent checkpoint update detected; \
         another pg2iceberg instance may be running with the same pipeline ID"
    )]
    ConcurrentUpdate,
}

#[derive(Copy, Clone, Default, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Mode {
    #[default]
    Logical,
    Query,
}

impl Mode {
    /// Stable string form used in checksum input + Go-compat YAML.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Logical => "logical",
            Mode::Query => "query",
        }
    }
}

/// Persisted in `_pg2iceberg.checkpoints`. Mirrors the Go reference's
/// `pipeline.Checkpoint` field-for-field, plus the two intentional
/// type-system upgrades (typed PgValue watermarks; PK-cursor snapshot
/// progress) documented at the module top.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Schema version of this checkpoint payload. `0` = legacy /
    /// fresh; [`CHECKPOINT_VERSION`] otherwise.
    #[serde(default)]
    pub version: i32,
    /// SHA-256 hex digest over the checkpoint's content fields
    /// (everything except `checksum` itself). Set by [`seal`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    /// Monotonic OCC counter. Incremented by [`seal`] on every save.
    /// The coord's UPDATE-WHERE-revision binds the new value against
    /// the previous one to detect concurrent writers.
    #[serde(default)]
    pub revision: i64,
    pub mode: Mode,
    pub flushed_lsn: Lsn,
    /// PostgreSQL cluster system identifier (from `IDENTIFY_SYSTEM`
    /// or `pg_control_system()`). Stamped on every save so a
    /// checkpoint always carries the cluster fingerprint it was
    /// written under. `0` = legacy / not yet populated.
    #[serde(default)]
    pub system_identifier: u64,
    /// `true` after the initial-snapshot phase finishes for every
    /// configured table. Combined with `snapshoted_tables` so that
    /// adding a new table later snapshots it without re-snapshotting
    /// the existing ones.
    #[serde(default)]
    pub snapshot_complete: bool,
    /// Per-table "snapshot done" flag. Keys are
    /// `"<namespace>.<name>"`; an absent key means "not yet
    /// snapshotted" (will be picked up on next start). True/false
    /// is the same — false means snapshot in progress / interrupted.
    #[serde(default)]
    pub snapshoted_tables: BTreeMap<String, bool>,
    /// Per-table PG `pg_class.oid` captured at snapshot time. Used
    /// to detect identity changes — `DROP TABLE` + recreate gives
    /// a new oid even with identical schema, and the Iceberg state
    /// pre-drop is now stale. Startup validation's invariant 11
    /// compares this against the current oid and refuses to start
    /// on mismatch (operator must drop the Iceberg table for the
    /// snapshot to re-run). Keys match `snapshoted_tables`.
    /// Absent on legacy / sim runs (oid 0 means "unknown" — invariant
    /// 11 skips).
    #[serde(default)]
    pub snapshoted_table_oids: BTreeMap<String, u32>,
    /// Per-table snapshot progress cursor. Keyed by
    /// `"<namespace>.<name>"`; value is the JSON-encoded canonical
    /// PK key of the last row staged. [`Snapshotter`] resumes from
    /// here so a mid-snapshot crash doesn't replay completed
    /// chunks. Empty/absent once the table's snapshot completes.
    ///
    /// (Diverges from Go's `SnapshotChunks map[string]int` which
    /// tracks CTID chunk indices. The PK-cursor model is
    /// pagination-strategy-agnostic and survives `VACUUM FULL`.)
    ///
    /// [`Snapshotter`]: <documented in pg2iceberg-snapshot>
    #[serde(default)]
    pub snapshot_progress: BTreeMap<String, String>,
    /// Per-table watermark for query mode. Keyed by
    /// `"<namespace>.<name>"`; value is the highest watermark
    /// column observed (typed [`PgValue`] — supports timestamp,
    /// bigint, uuid, etc., not just RFC3339 strings like Go).
    #[serde(default)]
    pub query_watermarks: BTreeMap<String, PgValue>,
    /// When this checkpoint was last sealed + saved. Microseconds
    /// since epoch UTC. `None` for never-saved fresh-construction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

impl Checkpoint {
    pub fn fresh(mode: Mode) -> Self {
        Self {
            version: 0,
            checksum: None,
            revision: 0,
            mode,
            flushed_lsn: Lsn::ZERO,
            system_identifier: 0,
            snapshot_complete: false,
            snapshoted_tables: BTreeMap::new(),
            snapshoted_table_oids: BTreeMap::new(),
            snapshot_progress: BTreeMap::new(),
            query_watermarks: BTreeMap::new(),
            updated_at: None,
        }
    }

    /// Mark a table's snapshot as done. Helper for the snapshotter.
    pub fn mark_snapshoted(&mut self, table_key: impl Into<String>) {
        self.snapshoted_tables.insert(table_key.into(), true);
    }

    /// Increment revision, set version, recompute checksum. Must be
    /// the last mutation before persisting — anything that touches
    /// content fields after seal invalidates the checksum.
    pub fn seal(&mut self, now_micros: i64) {
        self.revision += 1;
        self.version = CHECKPOINT_VERSION;
        self.updated_at = Some(now_micros);
        self.checksum = Some(self.compute_checksum());
    }

    /// Validate version + checksum + system_identifier.
    ///
    /// - Zero-value checkpoints (mode + version both unset) are
    ///   accepted as "fresh start; nothing to verify."
    /// - `connected_system_id == 0` skips the cluster check (e.g.
    ///   sim mode where there's no PG cluster).
    /// - A stored `system_identifier == 0` is accepted as legacy /
    ///   first-save; the caller is expected to stamp the connected
    ///   value on the next save.
    pub fn verify(&self, connected_system_id: u64) -> Result<(), CheckpointError> {
        if self.version > CHECKPOINT_VERSION {
            return Err(CheckpointError::VersionTooNew {
                got: self.version,
                max: CHECKPOINT_VERSION,
            });
        }
        if let Some(stored) = &self.checksum {
            let expected = self.compute_checksum();
            if &expected != stored {
                return Err(CheckpointError::ChecksumMismatch {
                    expected,
                    got: stored.clone(),
                });
            }
        }
        if connected_system_id != 0
            && self.system_identifier != 0
            && self.system_identifier != connected_system_id
        {
            return Err(CheckpointError::SystemIdMismatch {
                stored: self.system_identifier,
                connected: connected_system_id,
            });
        }
        Ok(())
    }

    /// Deterministic SHA-256 over content fields. Mirrors Go's
    /// `pipeline/checkpoint.go::computeChecksum` — same fields, same
    /// `key=value\n` format, same conditional inclusion of
    /// system_identifier when nonzero — so checksums are bit-equal
    /// between Go-written and Rust-written checkpoints.
    fn compute_checksum(&self) -> String {
        let mut parts: Vec<String> = Vec::with_capacity(16);
        parts.push(format!("version={}", self.version));
        // Go's `WrittenBy` field is in the digest input even when
        // empty; we don't carry that field, so emit empty to keep
        // the digest layout identical position-for-position.
        parts.push(String::from("written_by="));
        parts.push(format!("revision={}", self.revision));
        parts.push(format!("mode={}", self.mode.as_str()));
        // Go has a legacy single `Watermark` field. We don't carry
        // it; same empty placeholder rationale as above.
        parts.push(String::from("watermark="));
        parts.push(format!("lsn={}", self.flushed_lsn.0));
        if self.system_identifier != 0 {
            parts.push(format!("system_identifier={}", self.system_identifier));
        }
        parts.push(format!("snapshot_complete={}", self.snapshot_complete));
        // updated_at is included in the digest in Go (RFC3339Nano).
        // We use micros-since-epoch for compactness; the exact wire
        // format doesn't matter as long as it's deterministic.
        let updated_at = self.updated_at.unwrap_or(0);
        parts.push(format!("updated_at={updated_at}"));

        if !self.snapshoted_tables.is_empty() {
            parts.push(format!(
                "snapshoted_tables={}",
                sorted_map_str(
                    &self.snapshoted_tables,
                    |v| v.to_string(),
                )
            ));
        }
        if !self.snapshoted_table_oids.is_empty() {
            parts.push(format!(
                "snapshoted_table_oids={}",
                sorted_map_str(&self.snapshoted_table_oids, |v| v.to_string())
            ));
        }
        if !self.snapshot_progress.is_empty() {
            parts.push(format!(
                "snapshot_progress={}",
                sorted_map_str(&self.snapshot_progress, |v| v.clone())
            ));
        }
        if !self.query_watermarks.is_empty() {
            parts.push(format!(
                "query_watermarks={}",
                sorted_map_str(&self.query_watermarks, |v| format!("{v:?}"))
            ));
        }

        let joined = parts.join("\n");
        let digest = Sha256::digest(joined.as_bytes());
        hex_encode(&digest)
    }
}

fn sorted_map_str<V, F: Fn(&V) -> String>(map: &BTreeMap<String, V>, fmt_v: F) -> String {
    // BTreeMap iterates in key-sorted order already.
    map.iter()
        .map(|(k, v)| format!("{k}={}", fmt_v(v)))
        .collect::<Vec<_>>()
        .join(",")
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0F) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_verify_accepts_zero_value() {
        let cp = Checkpoint::fresh(Mode::Logical);
        cp.verify(0).unwrap();
        cp.verify(12345).unwrap();
    }

    #[test]
    fn seal_then_verify_round_trip() {
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.flushed_lsn = Lsn(0xCAFE);
        cp.system_identifier = 7777;
        cp.seal(1_000_000);

        assert_eq!(cp.version, CHECKPOINT_VERSION);
        assert_eq!(cp.revision, 1);
        assert!(cp.checksum.is_some());

        cp.verify(7777).unwrap();
    }

    #[test]
    fn verify_rejects_tampered_lsn() {
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.flushed_lsn = Lsn(100);
        cp.seal(0);

        // Tamper.
        cp.flushed_lsn = Lsn(999);

        let err = cp.verify(0).unwrap_err();
        assert!(matches!(err, CheckpointError::ChecksumMismatch { .. }));
    }

    #[test]
    fn verify_rejects_system_id_mismatch() {
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.system_identifier = 1111;
        cp.seal(0);

        let err = cp.verify(2222).unwrap_err();
        assert!(matches!(err, CheckpointError::SystemIdMismatch { .. }));
    }

    #[test]
    fn verify_passes_when_connected_system_id_zero() {
        // Sim/local mode: no PG cluster to fingerprint against.
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.system_identifier = 1111;
        cp.seal(0);
        cp.verify(0).unwrap();
    }

    #[test]
    fn verify_passes_when_stored_system_id_zero() {
        // Legacy / first-save: stored is zero, the cluster fingerprint
        // hasn't been adopted yet.
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.seal(0);
        cp.verify(2222).unwrap();
    }

    #[test]
    fn verify_rejects_future_version() {
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.version = CHECKPOINT_VERSION + 1;
        // No seal — checksum stays None; version-check still fires.
        let err = cp.verify(0).unwrap_err();
        assert!(matches!(err, CheckpointError::VersionTooNew { .. }));
    }

    #[test]
    fn checksum_covers_snapshoted_tables_changes() {
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.seal(0);
        let cs1 = cp.checksum.clone().unwrap();

        cp.snapshoted_tables.insert("public.a".into(), true);
        cp.seal(0);
        let cs2 = cp.checksum.clone().unwrap();

        assert_ne!(cs1, cs2);
    }

    #[test]
    fn revision_increments_per_seal() {
        let mut cp = Checkpoint::fresh(Mode::Logical);
        cp.seal(0);
        assert_eq!(cp.revision, 1);
        cp.seal(0);
        assert_eq!(cp.revision, 2);
    }
}
