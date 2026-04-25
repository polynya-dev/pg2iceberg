use crate::lsn::Lsn;
use crate::schema::TableIdent;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Per-table watermark (RFC3339 string, kept opaque so we don't
    /// commit to a timestamp parser at this layer).
    pub query_watermarks: BTreeMap<TableIdent, String>,
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
            tracked_tables: Vec::new(),
        }
    }
}
