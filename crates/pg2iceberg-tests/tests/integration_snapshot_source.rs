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
use pg2iceberg_snapshot::{KeyRange, SnapshotSource};
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
    PgSnapshotSource::open(&cfg, std::slice::from_ref(&f.schema), 1)
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

/// The rows of `range`, read as the snapshotter reads them.
async fn read_range(source: &PgSnapshotSource, schema: &TableSchema, range: &KeyRange) -> Vec<Row> {
    let ident = schema.pg_ident();
    let pk = pk_cols(schema);
    let mut rows: Vec<Row> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let chunk = source
            .read_range(&ident, range, CHUNK, after.as_deref())
            .await
            .expect("read range");
        let Some(last) = chunk.last() else {
            return rows;
        };
        after = Some(source.cursor(&ident, after.as_deref(), last, &pk));
        rows.extend(chunk);
    }
}

/// `sql`'s table, `name`, as pg2iceberg finds it.
async fn table(f: &Fixture, name: &str, sql: &str) -> TableSchema {
    f.regular.batch_execute(sql).await.expect("create table");
    PgClientImpl::connect_with(&f.dsn, TlsMode::Disable)
        .await
        .expect("connect")
        .discover_schema("public", name)
        .await
        .expect("discover")
}

/// `schema`'s table split into at most `parts` ranges, each read.
async fn split_and_read(
    f: &Fixture,
    schema: &TableSchema,
    parts: usize,
    min_rows: usize,
) -> Vec<Vec<Row>> {
    let cfg = PostgresConfig {
        url: f.dsn.clone(),
        ..Default::default()
    };
    let source = PgSnapshotSource::open(&cfg, std::slice::from_ref(schema), parts)
        .await
        .expect("open source");
    let ranges = source
        .split(&schema.pg_ident(), parts, min_rows)
        .await
        .expect("split");
    let mut read = Vec::new();
    for range in &ranges {
        read.push(read_range(&source, schema, range).await);
    }
    read
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

/// Over several connections, the source reads one snapshot: rows changed
/// after it opened are as they were on every connection.
#[tokio::test]
async fn every_connection_reads_the_same_snapshot() {
    let f = fixture().await;
    let cfg = PostgresConfig {
        url: f.dsn.clone(),
        ..Default::default()
    };
    let source = PgSnapshotSource::open(&cfg, std::slice::from_ref(&f.schema), 3)
        .await
        .expect("open source");
    f.regular
        .batch_execute(&format!(
            "INSERT INTO lines
                SELECT i, 'late', i % 10, i % 13 FROM generate_series({}, {}) i;
            DELETE FROM lines WHERE c <= 100;",
            ROWS + 1,
            ROWS + 1000
        ))
        .await
        .expect("change the table");

    // Three readers at once: each read takes a connection of its own.
    let (a, b, c) = tokio::join!(
        read_from(&source, &f.schema, None),
        read_from(&source, &f.schema, None),
        read_from(&source, &f.schema, None),
    );
    for (rows, _) in [a, b, c] {
        let seen: BTreeSet<i32> = rows.iter().map(|r| int(r, "c")).collect();
        assert_eq!(seen, (1..=ROWS).collect::<BTreeSet<_>>());
    }
}

/// A table is split at its key's first column — here of ten values, none
/// in a histogram, being as many as are common: evenly between its least
/// and greatest, an integer. Each row is read once, by one range.
#[tokio::test]
async fn a_table_splits_evenly_between_its_first_columns_ends() {
    let f = fixture().await;

    let ranges = split_and_read(&f, &f.schema, 4, 1000).await;

    assert_eq!(ranges.len(), 4, "ranges: {}", ranges.len());
    let mut seen = BTreeSet::new();
    for (i, rows) in ranges.iter().enumerate() {
        let a: BTreeSet<i32> = rows.iter().map(|r| int(r, "a")).collect();
        // Bounds at 2, 4 and 6: between 0 and 9.
        let expect: BTreeSet<i32> = match i {
            0 => (0..2).collect(),
            1 => (2..4).collect(),
            2 => (4..6).collect(),
            _ => (6..10).collect(),
        };
        assert_eq!(a, expect, "range {i}");
        for r in rows {
            assert!(seen.insert(int(r, "c")), "row {} read twice", int(r, "c"));
        }
    }
    assert_eq!(seen.len(), ROWS as usize, "rows skipped");
}

/// A table whose key's first column has a histogram is split at its
/// bounds, into ranges of about as many rows — whatever its type, given
/// as text — and each row is read once.
#[tokio::test]
async fn a_table_splits_at_its_histograms_bounds() {
    let f = fixture().await;
    let cases = [
        ("TEXT", "md5(i::text) || ''''"),
        (
            "TIMESTAMPTZ",
            "timestamptz '2026-01-01 00:00:00+08' + i * interval '1 second'",
        ),
        ("UUID", "md5(i::text)::uuid"),
    ];
    for (ty, value) in cases {
        let schema = table(
            &f,
            "keyed",
            &format!(
                "DROP TABLE IF EXISTS keyed;
                CREATE TABLE keyed (k {ty} NOT NULL, i INT NOT NULL, PRIMARY KEY (k, i));
                INSERT INTO keyed SELECT {value}, i FROM generate_series(1, {ROWS}) i;
                ANALYZE keyed;"
            ),
        )
        .await;

        let ranges = split_and_read(&f, &schema, 8, 1000).await;

        assert_eq!(ranges.len(), 8, "{ty}: ranges: {}", ranges.len());
        let mut seen = BTreeSet::new();
        for rows in &ranges {
            // A histogram's buckets hold as many rows, of a sample.
            assert!(
                (1000..5000).contains(&rows.len()),
                "{ty}: a range of {} rows",
                rows.len()
            );
            for r in rows {
                assert!(
                    seen.insert(int(r, "i")),
                    "{ty}: row {} read twice",
                    int(r, "i")
                );
            }
        }
        assert_eq!(seen.len(), ROWS as usize, "{ty}: rows skipped");
    }
}

/// A table never analyzed — of no rows Postgres knows of — or of too few,
/// is read whole.
#[tokio::test]
async fn a_table_of_too_few_rows_known_is_not_split() {
    let f = fixture().await;
    let unanalyzed = table(
        &f,
        "fresh",
        &format!(
            "CREATE TABLE fresh (k INT PRIMARY KEY) WITH (autovacuum_enabled = false);
            INSERT INTO fresh SELECT generate_series(1, {ROWS});"
        ),
    )
    .await;
    let cfg = PostgresConfig {
        url: f.dsn.clone(),
        ..Default::default()
    };
    let source = PgSnapshotSource::open(&cfg, &[unanalyzed.clone(), f.schema.clone()], 1)
        .await
        .expect("open source");

    let fresh = source.split(&unanalyzed.pg_ident(), 4, 1000).await.unwrap();
    let small = source
        .split(&f.schema.pg_ident(), 4, ROWS as usize)
        .await
        .unwrap();

    assert_eq!(fresh, Vec::new(), "a table never analyzed was split");
    assert_eq!(small, Vec::new(), "a table of a range's rows was split");
}

/// A chunk is read in binary: every type, at its edges, decodes to what
/// its text form does — which chunks were read in before.
#[tokio::test]
async fn binary_reads_decode_as_text_reads_do() {
    let f = fixture().await;
    let schema = table(
        &f,
        "typed",
        "CREATE TYPE mood AS ENUM ('sad', 'ok');
        CREATE TABLE typed (
            id INT PRIMARY KEY,
            small INT2, big INT8, f4 FLOAT4, f8 FLOAT8,
            num NUMERIC(20, 4), num_free NUMERIC, num_big NUMERIC(38, 0),
            t TEXT, vc VARCHAR(10), bp CHAR(4), j JSONB, js JSON, b BYTEA,
            d DATE, tm TIME, ts TIMESTAMP, tstz TIMESTAMPTZ, u UUID, flag BOOL,
            e mood, ip INET, iv INTERVAL, xs INT[]
        );
        INSERT INTO typed (id) VALUES (1);
        INSERT INTO typed VALUES (
            2, -32768, -9223372036854775808, -1.5e-40, 1.7976931348623157e308,
            -1234567890123456.7891, 0.000000001, 99999999999999999999999999999999999999,
            'ünïcødé ''quoted'' \\ tab	end', 'short', 'ab', '{\"a\": [1, 2.5, null]}',
            '{ \"b\" : 1 }', '\\x00ff10', '1999-12-31', '23:59:59.999999',
            '1970-01-01 00:00:00.000001', '2000-01-01 00:00:00+08',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', true, 'ok', '10.0.0.1/8',
            '1 year 2 days 00:00:01', '{1,NULL,3}'
        ), (
            3, 32767, 9223372036854775807, 3.4028235e38, -0.0, 0, -0.5, -1, '', '', '',
            '[]', 'null', '\\x', '0001-01-01', '00:00:00', '1969-12-31 23:59:59.5',
            '2262-04-11 23:47:16.854775+00', '00000000-0000-0000-0000-000000000000',
            false, 'sad', '::1', '-1 seconds', '{}'
        );",
    )
    .await;
    let cfg = PostgresConfig {
        url: f.dsn.clone(),
        ..Default::default()
    };
    let source = PgSnapshotSource::open(&cfg, std::slice::from_ref(&schema), 1)
        .await
        .expect("open source");

    let binary = source
        .read_chunk(&schema.pg_ident(), 10, None)
        .await
        .expect("read in binary");

    let cols: Vec<String> = schema
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect();
    let text: Vec<Row> = f
        .regular
        .simple_query(&format!(
            "SELECT {} FROM typed ORDER BY id",
            cols.join(", ")
        ))
        .await
        .expect("read in text")
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .map(|row| {
            schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let value = match row.get(i) {
                        Some(raw) => pg2iceberg::snapshot_src::decode_column(c, raw)
                            .unwrap_or_else(|e| panic!("{}: {e}", c.name)),
                        None => PgValue::Null,
                    };
                    (ColumnName(c.name.clone()), value)
                })
                .collect()
        })
        .collect();
    assert_eq!(binary.len(), 3);
    // Debug forms: a NaN isn't equal to itself.
    for (b, t) in binary.iter().zip(&text) {
        for c in &schema.columns {
            let name = ColumnName(c.name.clone());
            assert_eq!(
                format!("{:?}", b.get(&name)),
                format!("{:?}", t.get(&name)),
                "{} ({:?})",
                c.name,
                c.ty
            );
        }
    }
}
