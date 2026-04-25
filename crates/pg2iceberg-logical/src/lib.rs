//! Logical-replication pipeline.
//!
//! Consumes `DecodedMessage`s from a source, buffers per-tx, on commit moves
//! them onto a flush queue, and on flush:
//!   1. routes events into per-table [`RollingWriter`]s,
//!   2. encodes Parquet chunks via the codec,
//!   3. uploads to a [`BlobStore`],
//!   4. atomically claims offsets in the [`Coordinator`],
//!   5. **only then** advances `flushedLSN` — gated by [`CoordCommitReceipt`].
//!
//! Step 5 is enforced by the type system: [`Pipeline::advance_flushed_lsn`]
//! consumes a receipt by value, and the receipt cannot be constructed outside
//! a coord impl. See the compile_fail doctest below.

pub mod materializer;
pub mod pipeline;
pub mod sink;

pub use materializer::{
    CounterMaterializerNamer, Materializer, MaterializerError, MaterializerNamer,
};
pub use pipeline::{Pipeline, PipelineError};
pub use sink::{Sink, SinkError};

/// **Compile-time durability proof.**
///
/// Outside `pg2iceberg-coord`, `CoordCommitReceipt` is `#[non_exhaustive]` so
/// it cannot be constructed via struct literal. Combined with `advance_flushed_lsn`
/// taking it by value, this means there is no way to advance the slot
/// without the coord having actually committed.
///
/// ```compile_fail
/// use pg2iceberg_coord::CoordCommitReceipt;
/// use pg2iceberg_core::Lsn;
///
/// // Cannot construct CoordCommitReceipt outside its defining crate.
/// let _ = CoordCommitReceipt {
///     flushable_lsn: Lsn(0),
///     grants: vec![],
/// };
/// ```
#[allow(dead_code)]
fn _compile_time_proof_only() {}
