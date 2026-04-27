use crate::lsn::Lsn;
use crate::schema::{Namespace, TableIdent};
use crate::value::PgValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Custom serde for `BTreeMap<TableIdent, V>` that flattens the
/// `TableIdent` keys to strings of the form `"<namespace>.<name>"`
/// (or just `"<name>"` when namespace is empty).
///
/// JSON requires map keys to be strings; the default
/// `Serialize`/`Deserialize` for `TableIdent` produces a struct,
/// which `serde_json` rejects as a map key (`"key must be a
/// string"`). Surfaced by the testcontainers integration test
/// against a real PG coord, where `save_checkpoint` JSON-encodes
/// the [`Checkpoint`] for storage in `_pg2iceberg.checkpoints.payload`.
mod table_ident_map {
    use super::{Namespace, TableIdent};
    use serde::de::Deserializer;
    use serde::ser::{SerializeMap, Serializer};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    pub fn serialize<S, V>(
        map: &BTreeMap<TableIdent, V>,
        s: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        V: Serialize,
    {
        let mut m = s.serialize_map(Some(map.len()))?;
        for (k, v) in map {
            m.serialize_entry(&k.to_string(), v)?;
        }
        m.end()
    }

    pub fn deserialize<'de, D, V>(d: D) -> Result<BTreeMap<TableIdent, V>, D::Error>
    where
        D: Deserializer<'de>,
        V: Deserialize<'de>,
    {
        let raw: BTreeMap<String, V> = BTreeMap::deserialize(d)?;
        let mut out = BTreeMap::new();
        for (k, v) in raw {
            out.insert(parse_table_ident(&k), v);
        }
        Ok(out)
    }

    fn parse_table_ident(s: &str) -> TableIdent {
        match s.rsplit_once('.') {
            Some((ns, name)) => TableIdent {
                namespace: Namespace(ns.split('.').map(String::from).collect()),
                name: name.to_string(),
            },
            None => TableIdent {
                namespace: Namespace(Vec::new()),
                name: s.to_string(),
            },
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum Mode {
    Logical,
    Query,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum SnapshotState {
    NotStarted,
    InProgress,
    Complete,
}

/// Persisted in `_pg2iceberg.checkpoints`. Mirrors `pipeline/checkpoint.go`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub mode: Mode,
    pub flushed_lsn: Lsn,
    pub snapshot_state: SnapshotState,
    /// Per-table watermark for query mode. The value is the highest
    /// watermark column observed; `query` mode resumes from here on the
    /// next poll.
    #[serde(with = "table_ident_map")]
    pub query_watermarks: BTreeMap<TableIdent, PgValue>,
    /// Per-table snapshot progress. The value is the canonical PK key
    /// (JSON-encoded) of the last row staged through the pipeline. Used
    /// by [`Snapshotter`] to resume mid-snapshot after a crash without
    /// re-staging already-completed chunks. `Complete` snapshots clear
    /// their entry; resumable snapshots keep one until the table is done.
    #[serde(with = "table_ident_map")]
    pub snapshot_progress: BTreeMap<TableIdent, String>,
    /// Tables we've seen at least once; startup validation uses this to detect
    /// removed tables.
    pub tracked_tables: Vec<TableIdent>,
}

impl Checkpoint {
    pub fn fresh(mode: Mode) -> Self {
        Self {
            mode,
            flushed_lsn: Lsn::ZERO,
            snapshot_state: SnapshotState::NotStarted,
            query_watermarks: BTreeMap::new(),
            snapshot_progress: BTreeMap::new(),
            tracked_tables: Vec::new(),
        }
    }
}
