//! `PgSnapshotSource` against a real Postgres: what the sim's snapshot
//! source can't show — the SQL each chunk runs, and how Postgres plans it.
//!
//! Gated behind `--features integration`. Run command in the
//! integration_coord file's header doc applies here too.

#![cfg(feature = "integration")]

use std::collections::BTreeSet;
use std::time::Duration;

use pg2iceberg::config::PostgresConfig;
use pg2iceberg::snapshot_src::PgSnapshotSource;
use pg2iceberg_core::{ColumnName, PgValue, Row, TableSchema};
use pg2iceberg_iceberg::pk_key;
use pg2iceberg_pg::prod::{PgClientImpl, TlsMode};
use pg2iceberg_snapshot::SnapshotSource;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

const ROWS: i32 = 20_000;
const CHUNK: usize = 500;

struct Fixture {
    _container: ContainerAsync<Postgres>,
    dsn: String,
    regular: tokio_postgres::Client,
    schema: TableSchema,
}

/// A table whose primary key's index order, `(a, b, c)`, isn't its
/// columns' order, `c, a, b` — as TPC-C's are.
async fn fixture() -> Fixture {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .with_cmd(["-c", "fsync=off"])
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (regular, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("regular connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    // `c` alone is unique, so the rows are told apart by it.
    regular
        .batch_execute(&format!(
            "CREATE TABLE lines (
                c INT NOT NULL,
                payload TEXT NOT NULL,
                a INT NOT NULL,
                b INT NOT NULL,
                PRIMARY KEY (a, b, c)
            );
            INSERT INTO lines
                SELECT i, repeat('x', 100), i % 10, i % 13 FROM generate_series(1, {ROWS}) i;
            ANALYZE lines;"
        ))
        .await
        .expect("create table");
    let client = PgClientImpl::connect_with(&dsn, TlsMode::Disable)
        .await
        .expect("connect");
    let schema = client
        .discover_schema("public", "lines")
        .await
        .expect("discover");
    Fixture {
        _container: container,
        dsn,
        regular,
        schema,
    }
}

async fn open(f: &Fixture) -> PgSnapshotSource {
    let cfg = PostgresConfig {
        url: f.dsn.clone(),
        ..Default::default()
    };
    PgSnapshotSource::open(&cfg, std::slice::from_ref(&f.schema))
        .await
        .expect("open source")
}

fn pk_cols(schema: &TableSchema) -> Vec<ColumnName> {
    schema
        .primary_key_columns()
        .map(|c| ColumnName(c.name.clone()))
        .collect()
}

fn int(row: &Row, col: &str) -> i32 {
    match row.get(&ColumnName(col.into())) {
        Some(PgValue::Int4(n)) => *n,
        other => panic!("{col}: {other:?}"),
    }
}

/// Read the rest of the table from `after`, as the snapshotter does.
async fn read_from(
    source: &PgSnapshotSource,
    schema: &TableSchema,
    mut after: Option<String>,
) -> (Vec<Row>, usize) {
    let ident = schema.pg_ident();
    let pk = pk_cols(schema);
    let mut rows = Vec::new();
    let mut chunks = 0;
    loop {
        let chunk = source
            .read_chunk(&ident, CHUNK, after.as_deref())
            .await
            .expect("read chunk");
        let Some(last) = chunk.last() else {
            return (rows, chunks);
        };
        after = Some(source.cursor(&ident, after.as_deref(), last, &pk));
        chunks += 1;
        rows.extend(chunk);
    }
}

/// Sequential and index scans of `lines` so far, once the backends that
/// ran them have exited and reported them.
async fn scans(regular: &tokio_postgres::Client, at_least: i64) -> (i64, i64) {
    for _ in 0..100 {
        let row = regular
            .query_one(
                "SELECT seq_scan, coalesce(idx_scan, 0) FROM pg_stat_user_tables
                 WHERE relname = 'lines'",
                &[],
            )
            .await
            .expect("pg_stat_user_tables");
        let (seq, idx): (i64, i64) = (row.get(0), row.get(1));
        if seq + idx >= at_least {
            return (seq, idx);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("scans of lines never reached {at_least}");
}

/// Each chunk is read through the primary key's index, in its order —
/// in the columns' order, Postgres scans and sorts the whole table for
/// every chunk, and a snapshot takes time quadratic in the table's size.
#[tokio::test]
async fn chunks_are_read_through_the_primary_key_index() {
    let f = fixture().await;
    let (seq_before, idx_before) = scans(&f.regular, 0).await;

    let source = open(&f).await;
    let (rows, chunks) = read_from(&source, &f.schema, None).await;
    drop(source);

    assert_eq!(rows.len(), ROWS as usize);
    let keys: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|r| (int(r, "a"), int(r, "b"), int(r, "c")))
        .collect();
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "rows aren't in the index's order, each once"
    );

    let (seq, idx) = scans(&f.regular, seq_before + idx_before + chunks as i64).await;
    assert_eq!(seq - seq_before, 0, "chunks scanned the table");
    assert!(idx - idx_before >= chunks as i64);
}

/// A snapshot an earlier version began, whose cursor is in the columns'
/// order, carries on in that order: in the index's, it would skip the
/// rows that order puts before the cursor.
#[tokio::test]
async fn a_cursor_in_the_columns_order_resumes_in_it() {
    let f = fixture().await;
    let pk = pk_cols(&f.schema);
    assert_eq!(pk, ["c", "a", "b"].map(|c| ColumnName(c.into())));

    // Where an earlier version stopped: a third of the way through, in
    // the columns' order.
    let stopped = f
        .regular
        .query(
            &format!(
                "SELECT c, a, b FROM lines ORDER BY c, a, b LIMIT {}",
                ROWS / 3
            ),
            &[],
        )
        .await
        .expect("read the first third");
    let staged: BTreeSet<i32> = stopped.iter().map(|r| r.get::<_, i32>(0)).collect();
    let last = stopped.last().expect("rows");
    let last: Row = ["c", "a", "b"]
        .iter()
        .enumerate()
        .map(|(i, c)| (ColumnName(c.to_string()), PgValue::Int4(last.get(i))))
        .collect();

    let source = open(&f).await;
    let (rows, _) = read_from(&source, &f.schema, Some(pk_key(&last, &pk))).await;

    let resumed: BTreeSet<i32> = rows.iter().map(|r| int(r, "c")).collect();
    assert_eq!(rows.len(), resumed.len(), "a row read twice");
    assert!(staged.is_disjoint(&resumed), "a row read twice");
    assert_eq!(staged.len() + resumed.len(), ROWS as usize, "rows skipped");
}
