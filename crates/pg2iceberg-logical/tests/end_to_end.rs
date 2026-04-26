//! End-to-end logical-replication pipeline test against SimPostgres.
//!
//! Wires the trait surfaces together for the first time:
//!
//!   SimPostgres → SimReplicationStream → Pipeline.process → Sink
//!     → encode_chunk → MemoryBlobStore.put → MemoryCoordinator.claim_offsets
//!     → CoordCommitReceipt → Pipeline.advance_flushed_lsn
//!
//! The receipt-gated LSN advance is the load-bearing invariant; this test is
//! the runtime check, paired with the compile_fail doctest in `lib.rs`.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::IcebergType;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Lsn, Namespace, Op, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::Pipeline;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::postgres::SimPostgres;
use pg2iceberg_stream::codec::decode_chunk;
use pg2iceberg_stream::BlobStore;
use pollster::block_on;
use std::collections::BTreeMap;
use std::sync::Arc;

fn ident(name: &str) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: name.into(),
    }
}

fn orders_schema() -> TableSchema {
    TableSchema {
        ident: ident("orders"),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "qty".into(),
                field_id: 2,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
        partition_spec: Vec::new(),
    }
}

fn row(pairs: &[(&str, PgValue)]) -> Row {
    let mut r = BTreeMap::new();
    for (k, v) in pairs {
        r.insert(ColumnName((*k).into()), v.clone());
    }
    r
}

struct Harness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<MemoryBlobStore>,
    pipeline: Pipeline<MemoryCoordinator>,
    stream: pg2iceberg_sim::postgres::SimReplicationStream,
}

fn boot() -> Harness {
    let db = SimPostgres::new();
    db.create_table(orders_schema()).unwrap();
    db.create_publication("pub1", &[ident("orders")]).unwrap();
    db.create_slot("slot1", "pub1").unwrap();

    let clock = TestClock::at(0);
    let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock);
    let coord = Arc::new(MemoryCoordinator::new(
        CoordSchema::default_name(),
        arc_clock,
    ));
    let blob_store = Arc::new(MemoryBlobStore::new());
    let namer = Arc::new(CounterBlobNamer::new("s3://stage"));
    let pipeline = Pipeline::new(coord.clone(), blob_store.clone(), namer, 1000);
    let stream = db.start_replication("slot1").unwrap();

    Harness {
        db,
        coord,
        blob_store,
        pipeline,
        stream,
    }
}

/// Drain the replication stream into the pipeline. Holds the same stream
/// across calls so the cursor advances naturally — matches a long-running
/// production pipeline rather than reconnect-on-every-drive.
fn drive_until_caught_up(h: &mut Harness) {
    while let Some(msg) = h.stream.recv() {
        block_on(h.pipeline.process(msg)).unwrap();
    }
}

/// Flush + ack: after the pipeline writes through to the coord and advances
/// its `flushed_lsn`, that LSN gets sent back to the slot. Tests that don't
/// model reconnect can omit this; tests that do must call it for the slot
/// to know what's been confirmed.
fn flush_and_ack(h: &mut Harness) -> Option<Lsn> {
    let advanced = block_on(h.pipeline.flush()).unwrap();
    h.stream.send_standby(h.pipeline.flushed_lsn());
    advanced
}

#[test]
fn flushed_lsn_matches_commit_lsn_after_single_tx() {
    let mut h = boot();

    let mut tx = h.db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    let commit_lsn = tx.commit(Timestamp(123_456_789)).unwrap();

    drive_until_caught_up(&mut h);
    let advanced = flush_and_ack(&mut h);
    assert_eq!(advanced, Some(commit_lsn));
    assert_eq!(h.pipeline.flushed_lsn(), commit_lsn);

    // Coord state: one log_index row for "orders" at offset [0, 1).
    let entries = block_on(h.coord.read_log(&ident("orders"), 0, 100)).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].start_offset, 0);
    assert_eq!(entries[0].end_offset, 1);
    assert_eq!(entries[0].record_count, 1);

    // Blob store has the staged file; it round-trips via the codec.
    let paths = h.blob_store.paths();
    assert_eq!(paths.len(), 1);
    assert_eq!(entries[0].s3_path, paths[0]);
    let bytes = block_on(h.blob_store.get(&paths[0])).unwrap();
    let mat = decode_chunk(&bytes).unwrap();
    assert_eq!(mat.len(), 1);
    assert_eq!(mat[0].op, Op::Insert);
    assert_eq!(mat[0].lsn, change_lsn_before_commit(commit_lsn));
}

/// In a single-row tx the WAL is `Begin / Insert / Commit` with consecutive
/// LSNs; the change LSN is `commit_lsn - 1`. Captures that fact for the
/// assertion above without baking magic numbers.
fn change_lsn_before_commit(commit: Lsn) -> Lsn {
    Lsn(commit.0 - 1)
}

#[test]
fn flushed_lsn_advances_to_max_across_multiple_txns() {
    let mut h = boot();

    let mut last_commit = Lsn::ZERO;
    for i in 1..=3 {
        let mut tx = h.db.begin_tx();
        tx.insert(
            &ident("orders"),
            row(&[("id", PgValue::Int4(i)), ("qty", PgValue::Int4(i * 10))]),
        );
        last_commit = tx.commit(Timestamp(0)).unwrap();
    }

    drive_until_caught_up(&mut h);
    let advanced = flush_and_ack(&mut h);
    assert_eq!(advanced, Some(last_commit));
    assert_eq!(h.pipeline.flushed_lsn(), last_commit);

    // One chunk covers all three rows: [0, 3).
    let entries = block_on(h.coord.read_log(&ident("orders"), 0, 100)).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!((entries[0].start_offset, entries[0].end_offset), (0, 3));
}

#[test]
fn rolled_back_tx_does_not_advance_flushed_lsn_or_create_blobs() {
    let mut h = boot();

    let mut tx = h.db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(99)), ("qty", PgValue::Int4(1))]),
    );
    tx.rollback();

    drive_until_caught_up(&mut h);
    let advanced = flush_and_ack(&mut h);
    assert_eq!(advanced, None);
    assert_eq!(h.pipeline.flushed_lsn(), Lsn::ZERO);
    assert!(h.blob_store.paths().is_empty());
}

#[test]
fn flushed_lsn_only_moves_after_coord_commit() {
    // Sanity check on the durability gate: before flush, even with all events
    // received, flushed_lsn stays at 0. Only after `flush()` (which goes
    // through claim_offsets) does it move.
    let mut h = boot();

    let mut tx = h.db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(7)), ("qty", PgValue::Int4(7))]),
    );
    let commit = tx.commit(Timestamp(0)).unwrap();

    drive_until_caught_up(&mut h);
    // Pipeline has all events; coord has nothing yet.
    assert_eq!(h.pipeline.flushed_lsn(), Lsn::ZERO);
    assert!(block_on(h.coord.read_log(&ident("orders"), 0, 10))
        .unwrap()
        .is_empty());

    block_on(h.pipeline.flush()).unwrap();
    assert_eq!(h.pipeline.flushed_lsn(), commit);
}

#[test]
fn second_flush_after_more_txns_appends_offsets_contiguously() {
    let mut h = boot();

    let mut tx = h.db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))]),
    );
    let _ = tx.commit(Timestamp(0)).unwrap();
    drive_until_caught_up(&mut h);
    block_on(h.pipeline.flush()).unwrap();

    let mut tx = h.db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(2))]),
    );
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(3)), ("qty", PgValue::Int4(3))]),
    );
    let last = tx.commit(Timestamp(0)).unwrap();
    drive_until_caught_up(&mut h);
    block_on(h.pipeline.flush()).unwrap();

    let entries = block_on(h.coord.read_log(&ident("orders"), 0, 100)).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!((entries[0].start_offset, entries[0].end_offset), (0, 1));
    assert_eq!((entries[1].start_offset, entries[1].end_offset), (1, 3));
    assert_eq!(h.pipeline.flushed_lsn(), last);
}
