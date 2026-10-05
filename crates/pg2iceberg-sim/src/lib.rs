//! Simulated PG, simulated catalog, fault injector.
//!
//! In-memory implementations of the production trait surfaces:
//! [`coord::MemoryCoordinator`] mirrors `PgCoordinator` semantics so
//! DST and unit tests exercise invariants without testcontainers;
//! [`postgres::SimPostgres`] models WAL + slot semantics.

pub mod blob;
pub mod catalog;
pub mod clock;
pub mod coord;
pub mod fault;
pub mod id;
pub mod oracle;
pub mod pgoutput;
pub mod postgres;
