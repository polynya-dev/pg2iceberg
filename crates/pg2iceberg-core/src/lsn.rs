use serde::{Deserialize, Serialize};
use std::fmt;

/// Postgres log sequence number. Monotonic per-cluster.
#[derive(
    Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

impl From<u64> for Lsn {
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}
