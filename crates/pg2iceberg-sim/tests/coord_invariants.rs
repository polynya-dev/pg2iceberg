//! Invariant tests for `MemoryCoordinator`.
//!
//! These tests pin behaviors that any correct `Coordinator` impl must satisfy
//! — the future PG impl will be tested against the same suite via a shared
//! `coord_conformance` test crate.

use pg2iceberg_coord::{schema::CoordSchema, CommitBatch, Coordinator, OffsetClaim};
use pg2iceberg_core::{Lsn, Namespace, TableIdent, WorkerId};
use pg2iceberg_sim::{clock::TestClock, coord::MemoryCoordinator};
use pollster::block_on;
use std::sync::Arc;
use std::time::Duration;

fn ident(name: &str) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: name.into(),
    }
}

fn coord_at_t0() -> (MemoryCoordinator, TestClock) {
    let clock = TestClock::at(0);
    let arc: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock.clone());
    (
        MemoryCoordinator::new(CoordSchema::default_name(), arc),
        clock,
    )
}

fn claim(table: &str, n: u64, path: &str) -> OffsetClaim {
    OffsetClaim {
        table: ident(table),
        record_count: n,
        byte_size: 100,
        s3_path: path.into(),
    }
}

// ---------- claim_offsets atomicity & monotonicity ----------

#[test]
fn empty_batch_returns_receipt_with_no_grants() {
    let (c, _) = coord_at_t0();
    let r = block_on(c.claim_offsets(&CommitBatch {
        claims: vec![],
        flushable_lsn: Lsn(42),
        markers: vec![],
    }))
    .unwrap();
    assert_eq!(r.flushable_lsn, Lsn(42));
    assert!(r.grants.is_empty());
}

#[test]
fn first_claim_starts_at_zero_and_grants_match() {
    let (c, _) = coord_at_t0();
    let r = block_on(c.claim_offsets(&CommitBatch {
        claims: vec![claim("a", 5, "s3://a/0.parquet")],
        flushable_lsn: Lsn(100),
        markers: vec![],
    }))
    .unwrap();
    assert_eq!(r.flushable_lsn, Lsn(100));
    assert_eq!(r.grants.len(), 1);
    assert_eq!(r.grants[0].start_offset, 0);
    assert_eq!(r.grants[0].end_offset, 5);
    assert_eq!(r.grants[0].s3_path, "s3://a/0.parquet");
}

#[test]
fn successive_claims_form_contiguous_offset_ranges() {
    let (c, _) = coord_at_t0();
    block_on(c.claim_offsets(&CommitBatch {
        claims: vec![claim("a", 3, "p0")],
        flushable_lsn: Lsn(1),
        markers: vec![],
    }))
    .unwrap();
    let r = block_on(c.claim_offsets(&CommitBatch {
        claims: vec![claim("a", 7, "p1")],
        flushable_lsn: Lsn(2),
        markers: vec![],
    }))
    .unwrap();
    assert_eq!(r.grants[0].start_offset, 3);
    assert_eq!(r.grants[0].end_offset, 10);
}

#[test]
fn batches_with_multiple_tables_assign_independently() {
    let (c, _) = coord_at_t0();
    let r = block_on(c.claim_offsets(&CommitBatch {
        claims: vec![claim("a", 4, "pa"), claim("b", 6, "pb")],
        flushable_lsn: Lsn(1),
        markers: vec![],
    }))
    .unwrap();
    let by_table: std::collections::BTreeMap<_, _> = r
        .grants
        .iter()
        .map(|g| (g.table.name.clone(), (g.start_offset, g.end_offset)))
        .collect();
    assert_eq!(by_table.get("a"), Some(&(0, 4)));
    assert_eq!(by_table.get("b"), Some(&(0, 6)));
}

#[test]
fn multiple_claims_for_same_table_in_one_batch_chain_offsets() {
    // Mirrors Go: per-batch totals are aggregated, then the running offset is
    // carried across claims in input order.
    let (c, _) = coord_at_t0();
    let r = block_on(c.claim_offsets(&CommitBatch {
        claims: vec![
            claim("a", 2, "p0"),
            claim("a", 3, "p1"),
            claim("a", 1, "p2"),
        ],
        flushable_lsn: Lsn(1),
        markers: vec![],
    }))
    .unwrap();
    assert_eq!(r.grants[0].start_offset, 0);
    assert_eq!(r.grants[0].end_offset, 2);
    assert_eq!(r.grants[1].start_offset, 2);
    assert_eq!(r.grants[1].end_offset, 5);
    assert_eq!(r.grants[2].start_offset, 5);
    assert_eq!(r.grants[2].end_offset, 6);
}

#[test]
fn read_log_filters_by_offset_and_orders_ascending() {
    let (c, _) = coord_at_t0();
    block_on(c.claim_offsets(&CommitBatch {
        claims: vec![
            claim("a", 5, "p0"),
            claim("a", 5, "p1"),
            claim("a", 5, "p2"),
        ],
        flushable_lsn: Lsn(1),
        markers: vec![],
    }))
    .unwrap();
    // After offset 5, expect the latter two entries.
    let entries = block_on(c.read_log(&ident("a"), 5, 100)).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].end_offset, 10);
    assert_eq!(entries[1].end_offset, 15);
}

#[test]
fn read_log_respects_limit() {
    let (c, _) = coord_at_t0();
    let claims: Vec<_> = (0..5).map(|i| claim("a", 1, &format!("p{i}"))).collect();
    block_on(c.claim_offsets(&CommitBatch {
        claims,
        flushable_lsn: Lsn(1),
        markers: vec![],
    }))
    .unwrap();
    let entries = block_on(c.read_log(&ident("a"), 0, 3)).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[2].end_offset, 3);
}

#[test]
fn truncate_log_returns_paths_and_drops_rows() {
    let (c, _) = coord_at_t0();
    block_on(c.claim_offsets(&CommitBatch {
        claims: vec![
            claim("a", 3, "p0"),
            claim("a", 3, "p1"),
            claim("a", 3, "p2"),
        ],
        flushable_lsn: Lsn(1),
        markers: vec![],
    }))
    .unwrap();
    let dropped = block_on(c.truncate_log(&ident("a"), 6)).unwrap();
    assert_eq!(dropped, vec!["p0".to_string(), "p1".to_string()]);
    let remaining = block_on(c.read_log(&ident("a"), 0, 100)).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].s3_path, "p2");
}

// ---------- cursor semantics ----------

#[test]
fn ensure_cursor_creates_at_minus_one_and_get_returns_some() {
    let (c, _) = coord_at_t0();
    assert_eq!(
        block_on(c.get_cursor("default", &ident("a"))).unwrap(),
        None
    );
    block_on(c.ensure_cursor("default", &ident("a"))).unwrap();
    assert_eq!(
        block_on(c.get_cursor("default", &ident("a"))).unwrap(),
        Some(-1)
    );
}

#[test]
fn ensure_cursor_does_not_overwrite_existing() {
    let (c, _) = coord_at_t0();
    block_on(c.ensure_cursor("default", &ident("a"))).unwrap();
    block_on(c.set_cursor("default", &ident("a"), 42)).unwrap();
    block_on(c.ensure_cursor("default", &ident("a"))).unwrap();
    assert_eq!(
        block_on(c.get_cursor("default", &ident("a"))).unwrap(),
        Some(42)
    );
}

#[test]
fn set_cursor_on_unensured_row_is_not_found() {
    let (c, _) = coord_at_t0();
    let err = block_on(c.set_cursor("default", &ident("a"), 10)).unwrap_err();
    assert!(matches!(err, pg2iceberg_coord::CoordError::NotFound(_)));
}

// ---------- consumer heartbeats with TTL ----------

#[test]
fn registered_consumer_appears_in_active_until_ttl_expires() {
    let (c, clock) = coord_at_t0();
    let w = WorkerId("worker-1".into());
    block_on(c.register_consumer("default", &w, Duration::from_secs(30))).unwrap();
    assert_eq!(
        block_on(c.active_consumers("default")).unwrap(),
        vec![w.clone()]
    );

    // Just before expiry → still active.
    clock.advance(Duration::from_secs(29));
    assert_eq!(
        block_on(c.active_consumers("default")).unwrap(),
        vec![w.clone()]
    );

    // Past TTL → swept out (matches Go ActiveConsumers line 343 sweep).
    clock.advance(Duration::from_secs(2));
    assert!(block_on(c.active_consumers("default")).unwrap().is_empty());
}

#[test]
fn active_consumers_sorted_by_id() {
    let (c, _) = coord_at_t0();
    for w in ["c", "a", "b"] {
        block_on(c.register_consumer("default", &WorkerId(w.into()), Duration::from_secs(60)))
            .unwrap();
    }
    let active = block_on(c.active_consumers("default")).unwrap();
    let ids: Vec<&str> = active.iter().map(|w| w.0.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

// ---------- locks ----------

#[test]
fn try_lock_returns_true_then_false_until_release() {
    let (c, _) = coord_at_t0();
    let w1 = WorkerId("w1".into());
    let w2 = WorkerId("w2".into());
    assert!(block_on(c.try_lock(&ident("a"), &w1, Duration::from_secs(30))).unwrap());
    assert!(!block_on(c.try_lock(&ident("a"), &w2, Duration::from_secs(30))).unwrap());
    block_on(c.release_lock(&ident("a"), &w1)).unwrap();
    assert!(block_on(c.try_lock(&ident("a"), &w2, Duration::from_secs(30))).unwrap());
}

#[test]
fn expired_lock_can_be_reclaimed() {
    let (c, clock) = coord_at_t0();
    let w1 = WorkerId("w1".into());
    let w2 = WorkerId("w2".into());
    assert!(block_on(c.try_lock(&ident("a"), &w1, Duration::from_secs(10))).unwrap());
    clock.advance(Duration::from_secs(11));
    assert!(block_on(c.try_lock(&ident("a"), &w2, Duration::from_secs(10))).unwrap());
}

#[test]
fn renew_lock_only_succeeds_for_holder() {
    let (c, _) = coord_at_t0();
    let w1 = WorkerId("w1".into());
    let w2 = WorkerId("w2".into());
    block_on(c.try_lock(&ident("a"), &w1, Duration::from_secs(10))).unwrap();
    assert!(block_on(c.renew_lock(&ident("a"), &w1, Duration::from_secs(20))).unwrap());
    assert!(!block_on(c.renew_lock(&ident("a"), &w2, Duration::from_secs(20))).unwrap());
}

#[test]
fn release_lock_by_non_holder_is_noop() {
    let (c, _) = coord_at_t0();
    let w1 = WorkerId("w1".into());
    let w2 = WorkerId("w2".into());
    block_on(c.try_lock(&ident("a"), &w1, Duration::from_secs(10))).unwrap();
    block_on(c.release_lock(&ident("a"), &w2)).unwrap();
    // Lock still held by w1.
    assert!(!block_on(c.try_lock(&ident("a"), &w2, Duration::from_secs(10))).unwrap());
}
