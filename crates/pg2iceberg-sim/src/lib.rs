//! Simulated PG, simulated catalog, fault injector.
//!
//! Phase 3 lands the in-memory [`coord::MemoryCoordinator`] (mirrors the Go
//! `PgCoordinator` semantics so DST and unit tests can exercise invariants
//! without testcontainers). Phase 4 adds `SimPostgres` (WAL + slot semantics).

pub mod blob;
pub mod catalog;
pub mod clock;
pub mod coord;
pub mod fault;
pub mod postgres;
