//! Invariant tests for `SimPostgres`.
//!
//! These pin behaviors any "model of Postgres" must satisfy: monotonic LSNs,
//! atomic transactions, publication filtering, replay-from-restart_lsn after
//! reconnect, and rollback producing no WAL trace.

use pg2iceberg_core::{
    typemap::IcebergType, ChangeEvent, ColumnName, ColumnSchema, Lsn, Namespace, Op, PgValue, Row,
    TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_sim::postgres::{SimError, SimPostgres};
use std::collections::BTreeMap;

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
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            ColumnSchema {
                name: "qty".into(),
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
    }
}

fn other_schema() -> TableSchema {
    TableSchema {
        ident: ident("ledger"),
        columns: vec![ColumnSchema {
            name: "id".into(),
            ty: IcebergType::Int,
            nullable: false,
            is_primary_key: true,
        }],
    }
}

fn row(pairs: &[(&str, PgValue)]) -> Row {
    let mut r = BTreeMap::new();
    for (k, v) in pairs {
        r.insert(ColumnName((*k).into()), v.clone());
    }
    r
}

fn drain(stream: &mut pg2iceberg_sim::postgres::SimReplicationStream) -> Vec<DecodedMessage> {
    let mut out = Vec::new();
    while let Some(msg) = stream.recv() {
        out.push(msg);
    }
    out
}

fn boot() -> SimPostgres {
    let db = SimPostgres::new();
    db.create_table(orders_schema()).unwrap();
    db.create_table(other_schema()).unwrap();
    db.create_publication("pub1", &[ident("orders")]).unwrap();
    db.create_slot("slot1", "pub1").unwrap();
    db
}

// ---------- LSN monotonicity & tx atomicity ----------

#[test]
fn current_lsn_advances_on_each_wal_event() {
    let db = SimPostgres::new();
    let l0 = db.current_lsn();
    db.create_table(orders_schema()).unwrap();
    let l1 = db.current_lsn();
    assert!(l1 > l0, "create_table must advance LSN");
}

#[test]
fn tx_commit_produces_begin_changes_commit_in_order_with_consecutive_lsns() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(20))]),
    );
    let commit_lsn = tx.commit(Timestamp(123)).unwrap();

    let mut stream = db.start_replication("slot1").unwrap();
    let msgs = drain(&mut stream);
    // Relation events from create_table predate the slot, so they're not in
    // its WAL window. The slot sees just Begin + 2 Change + Commit.
    assert_eq!(msgs.len(), 4, "got: {msgs:?}");
    assert!(matches!(msgs[0], DecodedMessage::Begin { .. }));
    assert!(matches!(msgs[1], DecodedMessage::Change(_)));
    assert!(matches!(msgs[2], DecodedMessage::Change(_)));
    assert!(matches!(msgs[3], DecodedMessage::Commit { .. }));

    // LSNs are strictly monotonic and the Commit LSN matches the value
    // returned from `commit()` (i.e. what the pipeline will hand to the coord).
    let lsns: Vec<Lsn> = msgs
        .iter()
        .map(|m| match m {
            DecodedMessage::Begin { final_lsn, .. } => *final_lsn,
            DecodedMessage::Commit { commit_lsn, .. } => *commit_lsn,
            DecodedMessage::Change(ce) => ce.lsn,
            _ => panic!(),
        })
        .collect();
    for w in lsns.windows(2) {
        assert!(w[0] < w[1], "non-monotonic: {:?}", lsns);
    }
    assert_eq!(*lsns.last().unwrap(), commit_lsn);
}

#[test]
fn rollback_produces_no_wal_visible_to_stream() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(99)), ("qty", PgValue::Int4(1))]),
    );
    tx.rollback();

    let mut stream = db.start_replication("slot1").unwrap();
    assert!(
        stream.recv().is_none(),
        "rolled-back tx must not appear in WAL"
    );

    // And table state is untouched.
    let rows = db.read_table(&ident("orders")).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn dropping_uncommitted_tx_implicitly_rolls_back() {
    let db = boot();
    {
        let mut tx = db.begin_tx();
        tx.insert(
            &ident("orders"),
            row(&[("id", PgValue::Int4(7)), ("qty", PgValue::Int4(5))]),
        );
        // No commit, no explicit rollback — let it drop.
    }
    let mut stream = db.start_replication("slot1").unwrap();
    assert!(stream.recv().is_none());
}

#[test]
fn duplicate_pk_insert_aborts_whole_tx() {
    let db = boot();
    // Seed.
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    // Conflicting tx attempts insert + duplicate.
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(20))]),
    );
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(99))]),
    );
    let err = tx.commit(Timestamp(0)).unwrap_err();
    assert!(matches!(err, SimError::PkConflict { .. }));

    // Neither insert from the failed tx is visible — the second insert
    // wasn't applied, and atomicity says the first must be rolled back too.
    let rows = db.read_table(&ident("orders")).unwrap();
    assert_eq!(rows.len(), 1, "only the seeded row should remain");
}

// ---------- publication filtering ----------

#[test]
fn changes_to_unpublished_tables_are_skipped() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    tx.insert(&ident("ledger"), row(&[("id", PgValue::Int4(1))]));
    tx.commit(Timestamp(0)).unwrap();

    let mut stream = db.start_replication("slot1").unwrap();
    let msgs = drain(&mut stream);
    // Begin, one Change (orders only), Commit.
    assert_eq!(msgs.len(), 3);
    let change = match &msgs[1] {
        DecodedMessage::Change(c) => c,
        _ => panic!(),
    };
    assert_eq!(change.table, ident("orders"));
}

// ---------- reconnect / replay from restart_lsn ----------

#[test]
fn reconnect_at_restart_lsn_yields_messages_after_last_ack() {
    let db = boot();

    // Tx1.
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    let commit1 = tx.commit(Timestamp(0)).unwrap();

    // Tx2.
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(2)), ("qty", PgValue::Int4(20))]),
    );
    let _commit2 = tx.commit(Timestamp(0)).unwrap();

    // First connection: reads tx1 + tx2, acks only up to commit1, then
    // disconnects.
    {
        let mut s = db.start_replication("slot1").unwrap();
        let _ = drain(&mut s);
        s.send_standby(commit1);
    }

    // Reconnect: should replay everything strictly after commit1, i.e. tx2.
    let mut s = db.start_replication("slot1").unwrap();
    let msgs = drain(&mut s);
    assert_eq!(msgs.len(), 3, "Begin, Change, Commit for tx2 only");
    if let DecodedMessage::Change(c) = &msgs[1] {
        let id = c
            .after
            .as_ref()
            .unwrap()
            .get(&ColumnName("id".into()))
            .unwrap();
        assert_eq!(*id, PgValue::Int4(2));
    } else {
        panic!("expected Change");
    }
}

#[test]
fn send_standby_only_advances_forward() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    let commit = tx.commit(Timestamp(0)).unwrap();

    let mut s = db.start_replication("slot1").unwrap();
    let _ = drain(&mut s);
    s.send_standby(commit);
    let after = db.slot_state("slot1").unwrap();
    assert_eq!(after.confirmed_flush_lsn, commit);
    assert_eq!(after.restart_lsn, commit);

    // A backwards ack must NOT pull restart_lsn back. (Mirror, not CDC: no
    // rewinding.)
    s.send_standby(Lsn(0));
    let still = db.slot_state("slot1").unwrap();
    assert_eq!(still.confirmed_flush_lsn, commit);
    assert_eq!(still.restart_lsn, commit);
}

#[test]
fn slot_created_after_existing_data_does_not_replay_history() {
    let db = SimPostgres::new();
    db.create_table(orders_schema()).unwrap();
    db.create_publication("pub1", &[ident("orders")]).unwrap();

    // Pre-slot tx: should not appear to a slot created afterwards.
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(0)), ("qty", PgValue::Int4(0))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    db.create_slot("late", "pub1").unwrap();

    // Post-slot tx: should appear.
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(1))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    let mut s = db.start_replication("late").unwrap();
    let msgs = drain(&mut s);
    assert_eq!(msgs.len(), 3, "only the post-slot tx is visible");
    if let DecodedMessage::Change(c) = &msgs[1] {
        let id = c
            .after
            .as_ref()
            .unwrap()
            .get(&ColumnName("id".into()))
            .unwrap();
        assert_eq!(*id, PgValue::Int4(1));
    }
}

// ---------- update / delete semantics ----------

#[test]
fn update_emits_change_with_before_and_after() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    let mut tx = db.begin_tx();
    tx.update(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(99))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    let mut s = db.start_replication("slot1").unwrap();
    let msgs = drain(&mut s);
    // tx1: Begin/Change/Commit, tx2: Begin/Change/Commit
    let updates: Vec<&ChangeEvent> = msgs
        .iter()
        .filter_map(|m| match m {
            DecodedMessage::Change(c) if c.op == Op::Update => Some(c),
            _ => None,
        })
        .collect();
    assert_eq!(updates.len(), 1);
    let u = updates[0];
    assert_eq!(
        u.before.as_ref().unwrap().get(&ColumnName("qty".into())),
        Some(&PgValue::Int4(10))
    );
    assert_eq!(
        u.after.as_ref().unwrap().get(&ColumnName("qty".into())),
        Some(&PgValue::Int4(99))
    );
}

#[test]
fn delete_emits_change_with_before_only_and_removes_row() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.insert(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    let mut tx = db.begin_tx();
    tx.delete(
        &ident("orders"),
        row(&[("id", PgValue::Int4(1)), ("qty", PgValue::Int4(10))]),
    );
    tx.commit(Timestamp(0)).unwrap();

    assert!(db.read_table(&ident("orders")).unwrap().is_empty());

    let mut s = db.start_replication("slot1").unwrap();
    let msgs = drain(&mut s);
    let deletes: Vec<&ChangeEvent> = msgs
        .iter()
        .filter_map(|m| match m {
            DecodedMessage::Change(c) if c.op == Op::Delete => Some(c),
            _ => None,
        })
        .collect();
    assert_eq!(deletes.len(), 1);
    assert!(deletes[0].after.is_none());
    assert!(deletes[0].before.is_some());
}

#[test]
fn delete_of_nonexistent_row_aborts_tx() {
    let db = boot();
    let mut tx = db.begin_tx();
    tx.delete(
        &ident("orders"),
        row(&[("id", PgValue::Int4(404)), ("qty", PgValue::Int4(0))]),
    );
    let err = tx.commit(Timestamp(0)).unwrap_err();
    assert!(matches!(err, SimError::RowNotFound { op: "delete", .. }));
}
