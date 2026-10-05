//! Conformance: the simulated Postgres against real Postgres.
//!
//! The DST is only as faithful as `SimPostgres`. Each scenario here runs
//! the same transactions against a real Postgres (testcontainers) and the
//! sim, reads what each one's walsender would send — pgoutput from
//! `pg_logical_slot_get_binary_changes` on one side, the sim's
//! `recv_wire` on the other — parses both with the same parser, and
//! compares them message by message: relation messages (types, key
//! flags, replica identity), old-tuple kinds, text-format values,
//! unchanged-TOAST markers, TRUNCATE, transaction boundaries.
//! Identifiers that legitimately differ (LSNs, xids, relation OIDs) are
//! compared by role, not value.
//!
//! The same comparison runs one level up, on what the DST without wire
//! encoding consumes: production's `PgoutputDecoder` over Postgres's
//! bytes against the sim's decoded `recv`. Scenarios are hand-written
//! (each aimed at one protocol rule) and seeded random workloads.
//!
//! Separately, values the sim doesn't model (domains, enums, arrays,
//! special floats and timestamps, session time zones) go from Postgres
//! through production's decoder and are checked against the type
//! production's schema discovery gives the column.
//!
//! Gated behind `--features integration` (needs Docker).
#![cfg(feature = "integration")]

use bytes::Bytes;
use pg2iceberg_core::typemap::{IcebergType, PgType};
use pg2iceberg_core::value::{DaysSinceEpoch, Decimal, TimeMicros, TimestampMicros};
use pg2iceberg_core::Lsn;
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, Op as ChangeOp, PgValue, Row, TableIdent, TableSchema,
};
use pg2iceberg_pg::prod::{PgClientImpl, PgoutputDecoder, TlsMode};
use pg2iceberg_pg::{DecodedMessage, PgClient};
use pg2iceberg_sim::pgoutput::{pg_text, ReplicaIdentity};
use pg2iceberg_sim::postgres::{SimPostgres, WireMessage};
use postgres_replication::protocol::{
    LogicalReplicationMessage as Msg, ReplicaIdentity as WireIdentity, Tuple, TupleData,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

// ---------- scenarios ----------

#[derive(Clone)]
struct Col {
    name: &'static str,
    sql: &'static str,
    pg: PgType,
    ice: IcebergType,
}

fn col(name: &'static str, sql: &'static str, pg: PgType, ice: IcebergType) -> Col {
    Col { name, sql, pg, ice }
}

/// Every table is keyed by `id int4`.
#[derive(Clone)]
struct Table {
    name: &'static str,
    cols: Vec<Col>,
    identity: ReplicaIdentity,
    /// In the publication.
    published: bool,
    /// Columns stored out of line uncompressed (`STORAGE EXTERNAL`), so a
    /// large value is TOASTed deterministically.
    external: Vec<&'static str>,
}

fn table(name: &'static str, cols: Vec<Col>) -> Table {
    Table {
        name,
        cols,
        identity: ReplicaIdentity::Default,
        published: true,
        external: Vec::new(),
    }
}

#[derive(Clone)]
enum Op {
    Insert(&'static str, Vec<(&'static str, PgValue)>),
    /// `UPDATE t SET … WHERE id = k`.
    Update(&'static str, i32, Vec<(&'static str, PgValue)>),
    /// `UPDATE t SET id = to WHERE id = from`.
    ChangeKey(&'static str, i32, i32),
    Delete(&'static str, i32),
    /// One `TRUNCATE` of several tables.
    Truncate(Vec<&'static str>),
    AddColumn(&'static str, Col),
    DropColumn(&'static str, &'static str),
    /// Something that invalidates the table's relation cache entry
    /// without changing its columns (`CREATE INDEX`).
    Invalidate(&'static str),
}

struct Tx {
    ops: Vec<Op>,
    rollback: bool,
}

fn tx(ops: Vec<Op>) -> Tx {
    Tx {
        ops,
        rollback: false,
    }
}

struct Scenario {
    name: String,
    tables: Vec<Table>,
    txs: Vec<Tx>,
}

fn int(n: i32) -> PgValue {
    PgValue::Int4(n)
}

fn text(s: &str) -> PgValue {
    PgValue::Text(s.into())
}

/// Larger than the TOAST threshold: with `STORAGE EXTERNAL` it is stored
/// out of line, so an UPDATE that leaves it alone sends a marker.
fn big(tag: &str) -> PgValue {
    PgValue::Text(format!("{tag}:{}", "x".repeat(8000)))
}

/// A value Postgres TOASTs, by the same rule the sim's callers apply.
fn toasted(v: &PgValue) -> bool {
    matches!(v, PgValue::Text(s) if s.len() > 2000)
}

fn orders() -> Vec<Col> {
    vec![
        col("id", "int4", PgType::Int4, IcebergType::Int),
        col("note", "text", PgType::Text, IcebergType::String),
        col("qty", "int4", PgType::Int4, IcebergType::Int),
    ]
}

fn scenarios() -> Vec<Scenario> {
    let ins = |t: &'static str, id: i32, note: PgValue, qty: i32| {
        Op::Insert(t, vec![("id", int(id)), ("note", note), ("qty", int(qty))])
    };
    let full = |name: &'static str| Table {
        identity: ReplicaIdentity::Full,
        ..table(name, orders())
    };
    let toast = |name: &'static str, identity: ReplicaIdentity| Table {
        identity,
        external: vec!["note"],
        ..table(name, orders())
    };
    vec![
        Scenario {
            name: "dml_default_identity".into(),
            tables: vec![table("t", orders())],
            txs: vec![
                tx(vec![ins("t", 1, text("a"), 10), ins("t", 2, text("b"), 20)]),
                tx(vec![Op::Update("t", 1, vec![("qty", int(11))])]),
                tx(vec![Op::Delete("t", 2)]),
            ],
        },
        Scenario {
            name: "dml_full_identity".into(),
            tables: vec![full("t")],
            txs: vec![
                tx(vec![
                    ins("t", 1, text("a"), 10),
                    ins("t", 2, PgValue::Null, 20),
                ]),
                tx(vec![Op::Update("t", 1, vec![("qty", int(11))])]),
                tx(vec![Op::Delete("t", 2)]),
            ],
        },
        Scenario {
            name: "key_change".into(),
            tables: vec![table("d", orders()), full("f")],
            txs: vec![
                tx(vec![ins("d", 1, text("a"), 10), ins("f", 1, text("a"), 10)]),
                tx(vec![Op::ChangeKey("d", 1, 2), Op::ChangeKey("f", 1, 2)]),
            ],
        },
        Scenario {
            name: "toast_unchanged".into(),
            tables: vec![
                toast("d", ReplicaIdentity::Default),
                toast("f", ReplicaIdentity::Full),
            ],
            txs: vec![
                tx(vec![ins("d", 1, big("d1"), 10), ins("f", 1, big("f1"), 10)]),
                tx(vec![
                    Op::Update("d", 1, vec![("qty", int(11))]),
                    Op::Update("f", 1, vec![("qty", int(11))]),
                ]),
                tx(vec![Op::ChangeKey("d", 1, 2), Op::ChangeKey("f", 1, 2)]),
                tx(vec![Op::Delete("d", 2), Op::Delete("f", 2)]),
            ],
        },
        Scenario {
            name: "truncate".into(),
            tables: vec![table("a", orders()), table("b", orders())],
            txs: vec![
                tx(vec![ins("a", 1, text("a"), 1), ins("b", 1, text("b"), 1)]),
                tx(vec![Op::Truncate(vec!["a"])]),
                tx(vec![
                    Op::Truncate(vec!["a", "b"]),
                    ins("a", 2, text("a2"), 2),
                ]),
            ],
        },
        Scenario {
            name: "truncate_then_insert".into(),
            tables: vec![table("a", orders())],
            txs: vec![
                tx(vec![ins("a", 1, text("a"), 1)]),
                tx(vec![Op::Truncate(vec!["a"])]),
                tx(vec![ins("a", 2, text("a2"), 2)]),
                tx(vec![ins("a", 3, text("a3"), 3)]),
            ],
        },
        Scenario {
            name: "invalidation".into(),
            tables: vec![table("t", orders())],
            txs: vec![
                tx(vec![ins("t", 1, text("a"), 1)]),
                tx(vec![Op::Invalidate("t")]),
                tx(vec![ins("t", 2, text("b"), 2)]),
            ],
        },
        Scenario {
            name: "schema_change".into(),
            tables: vec![table("t", orders())],
            txs: vec![
                tx(vec![ins("t", 1, text("a"), 1)]),
                tx(vec![Op::DropColumn("t", "note")]),
                tx(vec![Op::Insert("t", vec![("id", int(2)), ("qty", int(2))])]),
                tx(vec![Op::AddColumn(
                    "t",
                    col("extra", "int8", PgType::Int8, IcebergType::Long),
                )]),
                tx(vec![Op::Update("t", 1, vec![("extra", PgValue::Int8(7))])]),
            ],
        },
        Scenario {
            name: "skipped_transactions".into(),
            tables: vec![
                table("t", orders()),
                Table {
                    published: false,
                    ..table("other", orders())
                },
            ],
            txs: vec![
                Tx {
                    ops: vec![ins("t", 1, text("a"), 1)],
                    rollback: true,
                },
                tx(vec![ins("other", 1, text("o"), 1)]),
                tx(vec![
                    ins("t", 2, text("b"), 2),
                    ins("other", 2, text("o"), 2),
                ]),
            ],
        },
        Scenario {
            name: "types".into(),
            tables: vec![table(
                "t",
                vec![
                    col("id", "int4", PgType::Int4, IcebergType::Int),
                    col("s", "int2", PgType::Int2, IcebergType::Int),
                    col("l", "int8", PgType::Int8, IcebergType::Long),
                    col(
                        "n",
                        "numeric(10,2)",
                        PgType::Numeric {
                            precision: Some(10),
                            scale: Some(2),
                        },
                        IcebergType::Decimal {
                            precision: 10,
                            scale: 2,
                        },
                    ),
                    col("b", "bool", PgType::Bool, IcebergType::Boolean),
                    col("f", "float8", PgType::Float8, IcebergType::Double),
                    col("by", "bytea", PgType::Bytea, IcebergType::Binary),
                    col("d", "date", PgType::Date, IcebergType::Date),
                    col("ts", "timestamp", PgType::Timestamp, IcebergType::Timestamp),
                    col(
                        "tz",
                        "timestamptz",
                        PgType::TimestampTz,
                        IcebergType::TimestampTz,
                    ),
                    col("u", "uuid", PgType::Uuid, IcebergType::Uuid),
                    col("j", "jsonb", PgType::Jsonb, IcebergType::String),
                ],
            )],
            txs: vec![tx(vec![Op::Insert(
                "t",
                vec![
                    ("id", int(1)),
                    ("s", PgValue::Int2(-3)),
                    ("l", PgValue::Int8(9_000_000_000)),
                    (
                        "n",
                        PgValue::Numeric(Decimal {
                            unscaled_be_bytes: 12_345i128.to_be_bytes().to_vec(),
                            scale: 2,
                        }),
                    ),
                    ("b", PgValue::Bool(true)),
                    ("f", PgValue::Float8(1.5)),
                    ("by", PgValue::Bytea(vec![0xde, 0xad])),
                    ("d", PgValue::Date(DaysSinceEpoch(19_723))),
                    (
                        "ts",
                        PgValue::Timestamp(TimestampMicros(1_704_067_200_000_001)),
                    ),
                    (
                        "tz",
                        PgValue::TimestampTz(TimestampMicros(1_704_067_200_000_000)),
                    ),
                    ("u", PgValue::Uuid([0x12; 16])),
                    ("j", PgValue::Jsonb(r#"{"a": 1}"#.into())),
                ],
            )])],
        },
    ]
}

// ---------- real Postgres ----------

static PG: OnceCell<(ContainerAsync<Postgres>, String)> = OnceCell::const_new();

/// Held by each test for its whole run: one test at a time. Another
/// test's publication DDL, committed while a slot decodes, invalidates
/// every relation in pgoutput's cache, and the slot resends Relation
/// messages mid-stream — real Postgres behavior that no scenario
/// models.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn dsn() -> &'static str {
    let (_, dsn) = PG
        .get_or_init(|| async {
            let c = Postgres::default()
                .with_tag("16-alpine")
                .with_cmd([
                    "-c",
                    "fsync=off",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=32",
                ])
                .start()
                .await
                .expect("start postgres");
            let host = c.get_host().await.expect("host");
            let port = c.get_host_port_ipv4(5432).await.expect("port");
            let dsn =
                format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
            (c, dsn)
        })
        .await;
    dsn
}

fn literal(v: &PgValue, sql_type: &str) -> String {
    match pg_text(v) {
        None => "NULL".into(),
        Some(t) => format!("'{}'::{sql_type}", t.replace('\'', "''")),
    }
}

/// Run `sc` against real Postgres in schema `ns`; returns the pgoutput
/// messages its slot decodes.
async fn run_real(sc: &Scenario, ns: &str) -> Vec<Bytes> {
    let (client, conn) = tokio_postgres::connect(dsn().await, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
        .batch_execute(&format!("CREATE SCHEMA {ns}"))
        .await
        .unwrap();
    let mut types: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for t in &sc.tables {
        let cols: Vec<String> = t
            .cols
            .iter()
            .map(|c| {
                types.insert((t.name, c.name), c.sql);
                if c.name == "id" {
                    format!("{} {} PRIMARY KEY", c.name, c.sql)
                } else {
                    format!("{} {}", c.name, c.sql)
                }
            })
            .collect();
        client
            .batch_execute(&format!(
                "CREATE TABLE {ns}.{} ({})",
                t.name,
                cols.join(", ")
            ))
            .await
            .unwrap();
        if t.identity == ReplicaIdentity::Full {
            client
                .batch_execute(&format!(
                    "ALTER TABLE {ns}.{} REPLICA IDENTITY FULL",
                    t.name
                ))
                .await
                .unwrap();
        }
        for c in &t.external {
            client
                .batch_execute(&format!(
                    "ALTER TABLE {ns}.{} ALTER COLUMN {c} SET STORAGE EXTERNAL",
                    t.name
                ))
                .await
                .unwrap();
        }
    }
    let published: Vec<String> = sc
        .tables
        .iter()
        .filter(|t| t.published)
        .map(|t| format!("{ns}.{}", t.name))
        .collect();
    client
        .batch_execute(&format!(
            "CREATE PUBLICATION {ns}_pub FOR TABLE {}",
            published.join(", ")
        ))
        .await
        .unwrap();
    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{ns}_slot', 'pgoutput')"
        ))
        .await
        .unwrap();
    for t in &sc.txs {
        let mut stmts = Vec::new();
        let ddl = t.ops.iter().any(|o| {
            matches!(
                o,
                Op::AddColumn(..) | Op::DropColumn(..) | Op::Invalidate(..)
            )
        });
        for op in &t.ops {
            stmts.push(match op {
                Op::Insert(tb, vals) => {
                    let names: Vec<&str> = vals.iter().map(|(c, _)| *c).collect();
                    let lits: Vec<String> = vals
                        .iter()
                        .map(|(c, v)| literal(v, types[&(*tb, *c)]))
                        .collect();
                    format!(
                        "INSERT INTO {ns}.{tb} ({}) VALUES ({})",
                        names.join(", "),
                        lits.join(", ")
                    )
                }
                Op::Update(tb, id, set) => {
                    let sets: Vec<String> = set
                        .iter()
                        .map(|(c, v)| format!("{c} = {}", literal(v, types[&(*tb, *c)])))
                        .collect();
                    format!("UPDATE {ns}.{tb} SET {} WHERE id = {id}", sets.join(", "))
                }
                Op::ChangeKey(tb, from, to) => {
                    format!("UPDATE {ns}.{tb} SET id = {to} WHERE id = {from}")
                }
                Op::Delete(tb, id) => format!("DELETE FROM {ns}.{tb} WHERE id = {id}"),
                Op::Truncate(tbs) => {
                    let names: Vec<String> = tbs.iter().map(|t| format!("{ns}.{t}")).collect();
                    format!("TRUNCATE {}", names.join(", "))
                }
                Op::AddColumn(tb, c) => {
                    types.insert((tb, c.name), c.sql);
                    format!("ALTER TABLE {ns}.{tb} ADD COLUMN {} {}", c.name, c.sql)
                }
                Op::DropColumn(tb, c) => format!("ALTER TABLE {ns}.{tb} DROP COLUMN {c}"),
                Op::Invalidate(tb) => format!("CREATE INDEX ON {ns}.{tb} (qty)"),
            });
        }
        let body = stmts.join("; ");
        let script = if ddl {
            body
        } else if t.rollback {
            format!("BEGIN; {body}; ROLLBACK")
        } else {
            format!("BEGIN; {body}; COMMIT")
        };
        client.batch_execute(&script).await.unwrap();
    }
    let rows = client
        .query(
            &format!(
                "SELECT data FROM pg_logical_slot_get_binary_changes('{ns}_slot', NULL, NULL, \
                 'proto_version', '1', 'publication_names', '{ns}_pub')"
            ),
            &[],
        )
        .await
        .unwrap();
    client
        .batch_execute(&format!("SELECT pg_drop_replication_slot('{ns}_slot')"))
        .await
        .unwrap();
    rows.iter()
        .map(|r| Bytes::from(r.get::<_, Vec<u8>>(0)))
        .collect()
}

// ---------- the sim ----------

fn sim_schema(ns: &str, t: &Table) -> TableSchema {
    TableSchema {
        ident: TableIdent {
            namespace: Namespace(vec![ns.into()]),
            name: t.name.into(),
        },
        columns: t
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnSchema {
                name: c.name.into(),
                field_id: i as i32 + 1,
                ty: c.ice,
                nullable: c.name != "id",
                is_primary_key: c.name == "id",
            })
            .collect(),
        partition_spec: Vec::new(),
        pg_schema: None,
    }
}

/// Run `sc` against the sim; returns the pgoutput messages its
/// walsender sends, and the messages its stream decodes.
fn run_sim(sc: &Scenario, ns: &str) -> (Vec<Bytes>, Vec<DecodedMessage>) {
    let db = SimPostgres::new();
    let ident = |t: &str| TableIdent {
        namespace: Namespace(vec![ns.into()]),
        name: t.into(),
    };
    let mut col_types: BTreeMap<(String, String), PgType> = BTreeMap::new();
    for t in &sc.tables {
        db.create_table(sim_schema(ns, t)).unwrap();
        db.set_replica_identity(&ident(t.name), t.identity);
        for c in &t.cols {
            db.set_pg_type(&ident(t.name), c.name, c.pg);
            col_types.insert((t.name.into(), c.name.into()), c.pg);
        }
    }
    let published: Vec<TableIdent> = sc
        .tables
        .iter()
        .filter(|t| t.published)
        .map(|t| ident(t.name))
        .collect();
    db.create_publication("pub", &published).unwrap();
    db.create_slot("slot", "pub").unwrap();

    // Rows as the source holds them, to build full rows for UPDATEs.
    let mut state: BTreeMap<(String, i32), Row> = BTreeMap::new();
    let key = |r: &Row| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => *n,
        other => panic!("key {other:?}"),
    };
    for t in &sc.txs {
        let mut next = state.clone();
        let mut handle = db.begin_tx();
        for op in &t.ops {
            match op {
                Op::Insert(tb, vals) => {
                    let row: Row = vals
                        .iter()
                        .map(|(c, v)| (ColumnName((*c).into()), v.clone()))
                        .collect();
                    next.insert((tb.to_string(), key(&row)), row.clone());
                    handle.insert(&ident(tb), row);
                }
                Op::Update(tb, id, set) => {
                    let old = next[&(tb.to_string(), *id)].clone();
                    let mut new = old.clone();
                    for (c, v) in set {
                        new.insert(ColumnName((*c).into()), v.clone());
                    }
                    let unchanged: Vec<ColumnName> = old
                        .iter()
                        .filter(|(c, v)| !set.iter().any(|(s, _)| *s == c.0) && toasted(v))
                        .map(|(c, _)| c.clone())
                        .collect();
                    next.insert((tb.to_string(), *id), new.clone());
                    handle.update_with_unchanged(&ident(tb), new, unchanged);
                }
                Op::ChangeKey(tb, from, to) => {
                    let old = next.remove(&(tb.to_string(), *from)).expect("row to move");
                    let mut new = old.clone();
                    new.insert(ColumnName("id".into()), int(*to));
                    let unchanged: Vec<ColumnName> = old
                        .iter()
                        .filter(|(_, v)| toasted(v))
                        .map(|(c, _)| c.clone())
                        .collect();
                    next.insert((tb.to_string(), *to), new.clone());
                    handle.update_with_pk_change_unchanged(&ident(tb), old, new, unchanged);
                }
                Op::Delete(tb, id) => {
                    next.remove(&(tb.to_string(), *id));
                    let pk: Row = [(ColumnName("id".into()), int(*id))].into();
                    handle.delete(&ident(tb), pk);
                }
                Op::Truncate(tbs) => {
                    for tb in tbs {
                        next.retain(|(t, _), _| t != tb);
                    }
                    let idents: Vec<TableIdent> = tbs.iter().map(|t| ident(t)).collect();
                    handle.truncate_all(&idents);
                }
                Op::AddColumn(tb, c) => {
                    db.alter_add_column(
                        &ident(tb),
                        ColumnSchema {
                            name: c.name.into(),
                            field_id: 0,
                            ty: c.ice,
                            nullable: true,
                            is_primary_key: false,
                        },
                    )
                    .unwrap();
                    db.set_pg_type(&ident(tb), c.name, c.pg);
                }
                Op::Invalidate(tb) => db.invalidate_relation(&ident(tb)).unwrap(),
                Op::DropColumn(tb, c) => {
                    db.alter_drop_column(&ident(tb), c).unwrap();
                    for ((t, _), row) in next.iter_mut() {
                        if t == tb {
                            row.remove(&ColumnName((*c).into()));
                        }
                    }
                }
            }
        }
        if t.rollback {
            handle.rollback();
        } else {
            handle.commit(pg2iceberg_core::Timestamp(0)).unwrap();
            state = next;
        }
    }
    let mut stream = db.start_replication("slot").unwrap();
    let mut wire = Vec::new();
    while let Some(m) = stream.recv_wire() {
        if let WireMessage::Pgoutput(b) = m {
            wire.push(b);
        }
    }
    let mut stream = db.start_replication("slot").unwrap();
    let decoded = std::iter::from_fn(|| stream.recv()).collect();
    (wire, decoded)
}

// ---------- comparison ----------

/// A message with identifiers replaced by their role.
#[derive(Debug, PartialEq)]
enum Norm {
    Begin {
        tx: usize,
    },
    Commit {
        tx: usize,
    },
    Relation {
        name: String,
        identity: &'static str,
        /// `(name, type oid, type modifier, part of the replica identity)`
        cols: Vec<(String, i32, i32, bool)>,
    },
    Insert {
        rel: String,
        new: Vec<Val>,
    },
    Update {
        rel: String,
        old: Option<(&'static str, Vec<Val>)>,
        new: Vec<Val>,
    },
    Delete {
        rel: String,
        old: (&'static str, Vec<Val>),
    },
    Truncate {
        rels: Vec<String>,
        options: i8,
    },
    Other,
}

#[derive(PartialEq)]
enum Val {
    Null,
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

impl std::fmt::Debug for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Null => f.write_str("null"),
            Val::Unchanged => f.write_str("<unchanged>"),
            Val::Text(s) if s.len() > 24 => write!(f, "{:?}…({} bytes)", &s[..16], s.len()),
            Val::Text(s) => write!(f, "{s:?}"),
            Val::Binary(b) => write!(f, "binary({} bytes)", b.len()),
        }
    }
}

fn vals(t: &Tuple) -> Vec<Val> {
    t.tuple_data()
        .iter()
        .map(|d| match d {
            TupleData::Null => Val::Null,
            TupleData::UnchangedToast => Val::Unchanged,
            TupleData::Text(b) => Val::Text(String::from_utf8_lossy(b).into_owned()),
            TupleData::Binary(b) => Val::Binary(b.to_vec()),
        })
        .collect()
}

/// Parse and normalize a stream; also checks each transaction's Begin
/// names its Commit's LSN.
fn normalize(stream: &[Bytes]) -> Result<Vec<Norm>, String> {
    let mut rels: BTreeMap<u32, String> = BTreeMap::new();
    let mut tx = 0usize;
    let mut final_lsn = 0u64;
    let mut out = Vec::new();
    let rel = |rels: &BTreeMap<u32, String>, id: u32| {
        rels.get(&id)
            .cloned()
            .ok_or_else(|| format!("change for relation {id} before its Relation message"))
    };
    for b in stream {
        let m = Msg::parse(b).map_err(|e| format!("parse: {e}"))?;
        out.push(match m {
            Msg::Begin(b) => {
                tx += 1;
                final_lsn = b.final_lsn();
                Norm::Begin { tx }
            }
            Msg::Commit(c) => {
                if c.commit_lsn() != final_lsn {
                    return Err(format!(
                        "tx {tx}: Begin final_lsn {final_lsn} != Commit commit_lsn {}",
                        c.commit_lsn()
                    ));
                }
                Norm::Commit { tx }
            }
            Msg::Relation(r) => {
                let name = r.name().map_err(|e| e.to_string())?.to_string();
                rels.insert(r.rel_id(), name.clone());
                Norm::Relation {
                    name,
                    identity: match r.replica_identity() {
                        WireIdentity::Default => "default",
                        WireIdentity::Nothing => "nothing",
                        WireIdentity::Full => "full",
                        WireIdentity::Index => "index",
                    },
                    cols: r
                        .columns()
                        .iter()
                        .map(|c| {
                            Ok((
                                c.name()?.to_string(),
                                c.type_id(),
                                c.type_modifier(),
                                c.flags() & 1 != 0,
                            ))
                        })
                        .collect::<std::io::Result<_>>()
                        .map_err(|e| e.to_string())?,
                }
            }
            Msg::Insert(i) => Norm::Insert {
                rel: rel(&rels, i.rel_id())?,
                new: vals(i.tuple()),
            },
            Msg::Update(u) => Norm::Update {
                rel: rel(&rels, u.rel_id())?,
                old: match (u.key_tuple(), u.old_tuple()) {
                    (Some(k), _) => Some(("key", vals(k))),
                    (_, Some(o)) => Some(("old", vals(o))),
                    _ => None,
                },
                new: vals(u.new_tuple()),
            },
            Msg::Delete(d) => Norm::Delete {
                rel: rel(&rels, d.rel_id())?,
                old: match (d.key_tuple(), d.old_tuple()) {
                    (Some(k), _) => ("key", vals(k)),
                    (_, Some(o)) => ("old", vals(o)),
                    _ => return Err("DELETE without an old tuple".into()),
                },
            },
            Msg::Truncate(t) => Norm::Truncate {
                rels: t
                    .rel_ids()
                    .iter()
                    .map(|id| rel(&rels, *id))
                    .collect::<Result<_, _>>()?,
                options: t.options(),
            },
            _ => Norm::Other,
        });
    }
    Ok(out)
}

/// A decoded message without what legitimately differs (LSNs, xids,
/// commit timestamps). Keepalives are dropped.
#[derive(PartialEq)]
enum Decoded {
    Begin,
    Commit,
    Relation {
        ident: String,
        /// `(name, type, is_primary_key, nullable)`
        cols: Vec<(String, IcebergType, bool, bool)>,
    },
    Change {
        table: String,
        op: ChangeOp,
        before: Option<Row>,
        after: Option<Row>,
        unchanged: Vec<ColumnName>,
    },
}

impl Debug for Decoded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Decoded::Begin => f.write_str("Begin"),
            Decoded::Commit => f.write_str("Commit"),
            Decoded::Relation { ident, cols } => write!(f, "Relation {ident} {cols:?}"),
            Decoded::Change {
                table,
                op,
                before,
                after,
                unchanged,
            } => write!(
                f,
                "{op:?} {table} before={} after={} unchanged={:?}",
                show_row(before),
                show_row(after),
                unchanged.iter().map(|c| c.0.as_str()).collect::<Vec<_>>()
            ),
        }
    }
}

fn show_row(row: &Option<Row>) -> String {
    let Some(row) = row else {
        return "-".into();
    };
    let cols: Vec<String> = row
        .iter()
        .map(|(c, v)| match v {
            PgValue::Text(s) if s.len() > 24 => {
                format!("{}: Text({:?}…{} bytes)", c.0, &s[..16], s.len())
            }
            v => format!("{}: {v:?}", c.0),
        })
        .collect();
    format!("{{{}}}", cols.join(", "))
}

fn strip(msgs: Vec<DecodedMessage>) -> Vec<Decoded> {
    msgs.into_iter()
        .filter_map(|m| {
            Some(match m {
                DecodedMessage::Begin { .. } => Decoded::Begin,
                DecodedMessage::Commit { .. } => Decoded::Commit,
                DecodedMessage::Relation { ident, columns } => Decoded::Relation {
                    ident: ident.to_string(),
                    cols: columns
                        .into_iter()
                        .map(|c| (c.name, c.ty, c.is_primary_key, c.nullable))
                        .collect(),
                },
                DecodedMessage::Change(e) => Decoded::Change {
                    table: e.table.to_string(),
                    op: e.op,
                    before: e.before,
                    after: e.after,
                    unchanged: e.unchanged_cols,
                },
                DecodedMessage::Keepalive { .. } => return None,
            })
        })
        .collect()
}

/// `stream` through production's decoder: what it decodes, and every
/// message it rejects, with why.
fn decode(stream: &[Bytes]) -> (Vec<DecodedMessage>, Vec<String>) {
    let mut decoder = PgoutputDecoder::new();
    let mut out = Vec::new();
    let mut errors = Vec::new();
    for b in stream {
        match decoder.decode(b) {
            Ok(msgs) => out.extend(msgs),
            Err(e) => errors.push(e.to_string()),
        }
    }
    (out, errors)
}

/// Where `sim` first departs from `real`, shown from just before there
/// to the end of both: scenarios are short, and later differences help
/// triage.
fn diff<T: PartialEq + Debug>(real: &[T], sim: &[T]) -> Option<String> {
    if real == sim {
        return None;
    }
    let at = real
        .iter()
        .zip(sim)
        .position(|(r, s)| r != s)
        .unwrap_or(real.len().min(sim.len()));
    let show = |v: &[T]| {
        v.iter()
            .enumerate()
            .skip(at.saturating_sub(2))
            .map(|(i, m)| format!("    {}{i}: {m:?}", if i == at { "> " } else { "  " }))
            .collect::<Vec<_>>()
            .join("\n")
    };
    Some(format!(
        "at message {at}:\n  postgres:\n{}\n  sim:\n{}",
        show(real),
        show(sim)
    ))
}

/// A fresh schema name per run of a scenario.
fn namespace(name: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!("c{}_{name}", NEXT.fetch_add(1, Ordering::Relaxed))
}

async fn check(sc: &Scenario) -> Result<(), String> {
    let ns = namespace(&sc.name);
    let real_bytes = run_real(sc, &ns).await;
    let (sim_bytes, sim_decoded) = run_sim(sc, &ns);
    let mut problems = Vec::new();
    let (real_decoded, rejected) = decode(&real_bytes);
    for e in rejected {
        problems.push(format!("production's decoder rejects real pgoutput: {e}"));
    }
    let real = normalize(&real_bytes).map_err(|e| format!("[{}] real: {e}", sc.name))?;
    let sim = normalize(&sim_bytes).map_err(|e| format!("[{}] sim: {e}", sc.name))?;
    if let Some(d) = diff(&real, &sim) {
        problems.push(format!("sim's pgoutput diverges from Postgres {d}"));
    }
    if let Some(d) = diff(&strip(real_decoded), &strip(sim_decoded)) {
        problems.push(format!(
            "sim's decoded stream diverges from production decoding Postgres {d}"
        ));
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(format!("[{}]\n{}", sc.name, problems.join("\n")))
    }
}

#[tokio::test]
async fn sim_matches_postgres() {
    let _serial = SERIAL.lock().await;
    let mut failures = Vec::new();
    for sc in scenarios() {
        if let Err(e) = check(&sc).await {
            failures.push(e);
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

// ---------- random workloads ----------

/// xorshift64*: deterministic, and enough for picking ops.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n as u64) as usize
    }

    fn chance(&mut self, pct: usize) -> bool {
        self.below(100) < pct
    }
}

/// Random transactions over a DEFAULT-identity and a FULL-identity
/// table, both with out-of-line TOAST, and an unpublished table: every
/// kind of DML, multi-table TRUNCATE, rollbacks, relation invalidations.
fn random_scenario(seed: u64) -> Scenario {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let toasting = |t: Table| Table {
        external: vec!["note"],
        ..t
    };
    let tables = vec![
        toasting(table("a", orders())),
        toasting(Table {
            identity: ReplicaIdentity::Full,
            ..table("b", orders())
        }),
        Table {
            published: false,
            ..table("c", orders())
        },
    ];
    const NAMES: [&str; 3] = ["a", "b", "c"];
    // Committed rows: (table, id) → (note, qty).
    let mut rows: BTreeMap<(&str, i32), (PgValue, i32)> = BTreeMap::new();
    let mut next_id = 1;
    let mut txs = Vec::new();
    for n in 0..12 {
        if rng.chance(8) {
            txs.push(tx(vec![Op::Invalidate(NAMES[rng.below(3)])]));
            continue;
        }
        let mut live = rows.clone();
        let mut ops = Vec::new();
        for _ in 0..1 + rng.below(4) {
            let tb = NAMES[rng.below(3)];
            let keys: Vec<i32> = live
                .keys()
                .filter(|(t, _)| *t == tb)
                .map(|(_, k)| *k)
                .collect();
            let note = match rng.below(4) {
                0 => PgValue::Null,
                // Only the publishing tables store it out of line.
                1 if tb != "c" => big(&format!("{n}.{}", rng.below(100))),
                _ => text(&format!("n{}", rng.below(100))),
            };
            let qty = rng.below(1000) as i32;
            let choice = if keys.is_empty() { 0 } else { rng.below(9) };
            let key = if keys.is_empty() {
                0
            } else {
                keys[rng.below(keys.len())]
            };
            match choice {
                0..=2 => {
                    let id = next_id;
                    next_id += 1;
                    live.insert((tb, id), (note.clone(), qty));
                    ops.push(Op::Insert(
                        tb,
                        vec![("id", int(id)), ("note", note), ("qty", int(qty))],
                    ));
                }
                3 => {
                    live.get_mut(&(tb, key)).unwrap().1 = qty;
                    ops.push(Op::Update(tb, key, vec![("qty", int(qty))]));
                }
                4 => {
                    live.get_mut(&(tb, key)).unwrap().0 = note.clone();
                    ops.push(Op::Update(tb, key, vec![("note", note)]));
                }
                5 => {
                    // Sets a column to what it already is: still an UPDATE.
                    let same = live[&(tb, key)].1;
                    ops.push(Op::Update(tb, key, vec![("qty", int(same))]));
                }
                6 => {
                    let id = next_id;
                    next_id += 1;
                    let row = live.remove(&(tb, key)).unwrap();
                    live.insert((tb, id), row);
                    ops.push(Op::ChangeKey(tb, key, id));
                }
                7 => {
                    live.remove(&(tb, key));
                    ops.push(Op::Delete(tb, key));
                }
                _ => {
                    let mut tbs = vec![tb];
                    let other = NAMES[rng.below(3)];
                    if other != tb && rng.chance(50) {
                        tbs.push(other);
                    }
                    live.retain(|(t, _), _| !tbs.contains(t));
                    ops.push(Op::Truncate(tbs));
                }
            }
        }
        let rollback = rng.chance(10);
        if !rollback {
            rows = live;
        }
        txs.push(Tx { ops, rollback });
    }
    Scenario {
        name: format!("random_{seed}"),
        tables,
        txs,
    }
}

/// `CONFORMANCE_SEEDS` (default 24) random workloads.
#[tokio::test]
async fn sim_matches_postgres_on_random_workloads() {
    let _serial = SERIAL.lock().await;
    let seeds: u64 = std::env::var("CONFORMANCE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    let mut failures = Vec::new();
    for seed in 0..seeds {
        if let Err(e) = check(&random_scenario(seed)).await {
            failures.push(e);
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

// ---------- values production decodes ----------

struct Probe {
    name: &'static str,
    /// Run first, with `{ns}` replaced by the probe's schema.
    setup: &'static str,
    /// The column's type (`{ns}` replaced).
    ty: &'static str,
    /// SQL for the value.
    value: &'static str,
    /// Run in the session that decodes the slot.
    session: &'static str,
    /// What production should decode, or `None` where there is no
    /// clearly right answer (the outcome is printed instead).
    expect: Option<PgValue>,
}

fn probe(name: &'static str, ty: &'static str, value: &'static str, expect: PgValue) -> Probe {
    Probe {
        name,
        setup: "",
        ty,
        value,
        session: "",
        expect: Some(expect),
    }
}

fn probes() -> Vec<Probe> {
    let ts = |micros: i64| PgValue::TimestampTz(TimestampMicros(micros));
    vec![
        // Discovery reads `information_schema.columns.udt_name`, which
        // for a domain is its base type…
        Probe {
            setup: "CREATE DOMAIN {ns}.posint AS int4 CHECK (VALUE > 0)",
            ..probe("int_domain", "{ns}.posint", "5", PgValue::Int4(5))
        },
        Probe {
            setup: "CREATE DOMAIN {ns}.price AS numeric(10,2)",
            ..probe(
                "numeric_domain",
                "{ns}.price",
                "'12.34'",
                PgValue::Numeric(Decimal {
                    unscaled_be_bytes: 1234i128.to_be_bytes().to_vec(),
                    scale: 2,
                }),
            )
        },
        Probe {
            setup: "CREATE DOMAIN {ns}.email AS text",
            ..probe("text_domain", "{ns}.email", "'a@b.c'", text("a@b.c"))
        },
        // …and maps every type it doesn't know to text.
        Probe {
            setup: "CREATE TYPE {ns}.color AS ENUM ('red', 'green')",
            ..probe("enum", "{ns}.color", "'red'", text("red"))
        },
        probe("int_array", "int4[]", "'{1,2}'", text("{1,2}")),
        probe(
            "text_array",
            "text[]",
            r#"'{a,"b c"}'"#,
            text(r#"{a,"b c"}"#),
        ),
        probe(
            "interval",
            "interval",
            "'1 day 2 hours'",
            text("1 day 02:00:00"),
        ),
        probe("money", "money", "'12.34'", text("$12.34")),
        probe("point", "point", "'(1,2)'", text("(1,2)")),
        probe("inet", "inet", "'10.0.0.1'", text("10.0.0.1")),
        probe("bpchar", "char(5)", "'ab'", text("ab   ")),
        probe(
            "text_escapes",
            "text",
            r"E'line1\nline2 \\ ''q'' é'",
            text("line1\nline2 \\ 'q' é"),
        ),
        probe(
            "jsonb_normalized",
            "jsonb",
            r#"'{"b": 1, "a": [1,2]}'"#,
            PgValue::Jsonb(r#"{"a": [1, 2], "b": 1}"#.into()),
        ),
        probe(
            "json_verbatim",
            "json",
            r#"'{"b":1,  "a":2}'"#,
            PgValue::Json(r#"{"b":1,  "a":2}"#.into()),
        ),
        Probe {
            session: "SET TimeZone = 'Asia/Kolkata'",
            ..probe(
                "timestamptz_session_zone",
                "timestamptz",
                "'2024-01-01 00:00:00+00'",
                ts(1_704_067_200_000_000),
            )
        },
        probe(
            "timestamptz_pre_epoch",
            "timestamptz",
            "'1969-12-31 23:59:59.5+00'",
            ts(-500_000),
        ),
        probe(
            "time_max",
            "time",
            "'23:59:59.999999'",
            PgValue::Time(TimeMicros(86_399_999_999)),
        ),
        probe(
            "date_year_one",
            "date",
            "'0001-01-01'",
            PgValue::Date(DaysSinceEpoch(-719_162)),
        ),
        probe("float_nan", "float8", "'NaN'", PgValue::Float8(f64::NAN)),
        probe(
            "float_infinity",
            "float8",
            "'-Infinity'",
            PgValue::Float8(f64::NEG_INFINITY),
        ),
        Probe {
            expect: None,
            ..probe("numeric_nan", "numeric", "'NaN'", PgValue::Null)
        },
        Probe {
            expect: None,
            ..probe(
                "numeric_unconstrained",
                "numeric",
                "'123.4500'",
                PgValue::Null,
            )
        },
        Probe {
            expect: None,
            ..probe(
                "timestamp_infinity",
                "timestamp",
                "'infinity'",
                PgValue::Null,
            )
        },
        Probe {
            expect: None,
            ..probe("date_minus_infinity", "date", "'-infinity'", PgValue::Null)
        },
        Probe {
            expect: None,
            ..probe("timetz", "timetz", "'12:00:00+05:30'", PgValue::Null)
        },
        // Discovery types oid as a 32-bit int; oids are unsigned.
        probe(
            "oid_large",
            "oid",
            "4000000000",
            PgValue::Int8(4_000_000_000),
        ),
    ]
}

/// What production makes of `p`: the type discovery gives its column,
/// and the value (or error) its decoder produces from Postgres's
/// pgoutput for an INSERT of it.
async fn run_probe(p: &Probe) -> (String, Result<PgValue, String>) {
    let ns = namespace(p.name);
    let (client, conn) = tokio_postgres::connect(dsn().await, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    let ty = p.ty.replace("{ns}", &ns);
    // Separately: a batch is one transaction, and a slot can't be
    // created in one that has written.
    let mut setup = vec![format!("CREATE SCHEMA {ns}")];
    if !p.setup.is_empty() {
        setup.push(p.setup.replace("{ns}", &ns));
    }
    setup.extend([
        format!("CREATE TABLE {ns}.t (id int4 PRIMARY KEY, v {ty})"),
        format!("CREATE PUBLICATION {ns}_pub FOR TABLE {ns}.t"),
        format!("SELECT pg_create_logical_replication_slot('{ns}_slot', 'pgoutput')"),
        format!("INSERT INTO {ns}.t VALUES (1, ({})::{ty})", p.value),
    ]);
    for sql in setup {
        client.batch_execute(&sql).await.unwrap();
    }

    // Production's view of the source: the column's type from discovery,
    // and the domains its decoder types columns with.
    let source = PgClientImpl::connect(dsn().await).await.expect("connect");
    let discovered = match source.discover_schema(&ns, "t").await {
        Ok(schema) => schema
            .columns
            .iter()
            .find(|c| c.name == "v")
            .map_or("no column v".into(), |c| format!("{:?}", c.ty)),
        Err(e) => format!("discovery fails: {e}"),
    };
    let domains = source.domains().await.expect("domains");

    if !p.session.is_empty() {
        client.batch_execute(p.session).await.unwrap();
    }
    let rows = client
        .query(
            &format!(
                "SELECT data FROM pg_logical_slot_get_binary_changes('{ns}_slot', NULL, NULL, \
                 'proto_version', '1', 'publication_names', '{ns}_pub')"
            ),
            &[],
        )
        .await
        .unwrap();
    client
        .batch_execute(&format!("SELECT pg_drop_replication_slot('{ns}_slot')"))
        .await
        .unwrap();
    let mut decoder = PgoutputDecoder::with_domains(domains);
    for r in &rows {
        let msgs = match decoder.decode(&Bytes::from(r.get::<_, Vec<u8>>(0))) {
            Ok(m) => m,
            Err(e) => return (discovered, Err(e.to_string())),
        };
        for m in msgs {
            if let DecodedMessage::Change(e) = m {
                let v = e
                    .after
                    .and_then(|mut row| row.remove(&ColumnName("v".into())))
                    .ok_or("no value for v".to_string());
                return (discovered, v);
            }
        }
    }
    (discovered, Err("no change decoded".into()))
}

/// Run `probes`; every one with an expectation must meet it.
async fn check_probes(probes: impl IntoIterator<Item = Probe>) {
    let mut failures = Vec::new();
    for p in probes {
        let (discovered, got) = run_probe(&p).await;
        let shown = match &got {
            Ok(v) => format!("{v:?}"),
            Err(e) => format!("error: {e}"),
        };
        match &p.expect {
            // Debug, so NaN equals NaN.
            Some(want) if Ok(format!("{want:?}")) != got.as_ref().map(|v| format!("{v:?}")) => {
                failures.push(format!(
                    "{} ({}, discovered as {discovered}): want {want:?}, got {shown}",
                    p.name, p.ty
                ))
            }
            Some(_) => {}
            None => eprintln!(
                "observed: {} ({}, discovered as {discovered}): {shown}",
                p.name, p.ty
            ),
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Values the sim doesn't model, from Postgres through production's
/// decoder. Each must decode to a value of the type discovery gave its
/// column; anything discovery accepts but the decoder rejects stops
/// replication at the first change to the table.
#[tokio::test]
async fn production_decodes_postgres_values() {
    let _serial = SERIAL.lock().await;
    check_probes(probes().into_iter().filter(|p| p.name != "oid_large")).await;
}

/// An oid above `i32::MAX` must not wrap negative.
#[tokio::test]
async fn production_keeps_large_oids() {
    let _serial = SERIAL.lock().await;
    check_probes(probes().into_iter().filter(|p| p.name == "oid_large")).await;
}

// ---------- where a restarted stream resumes ----------

/// The ids a stream sends for its inserts, each with its transaction's
/// commit LSN.
fn inserted(msgs: impl IntoIterator<Item = DecodedMessage>) -> Vec<(i32, Lsn)> {
    let mut out = Vec::new();
    let mut id = None;
    for m in msgs {
        match m {
            DecodedMessage::Change(e) => {
                if let Some(PgValue::Int4(i)) = e
                    .after
                    .and_then(|r| r.get(&ColumnName("id".into())).cloned())
                {
                    id = Some(i);
                }
            }
            DecodedMessage::Commit { commit_lsn, .. } => {
                if let Some(i) = id.take() {
                    out.push((i, commit_lsn));
                }
            }
            _ => {}
        }
    }
    out
}

/// One `START_REPLICATION` session against Postgres: everything it sends
/// until it goes quiet, then (optionally) an ack before disconnecting.
async fn real_session(
    slot: &str,
    publication: &str,
    start: Lsn,
    ack: Option<Lsn>,
) -> Vec<(i32, Lsn)> {
    let client = PgClientImpl::connect_with(dsn().await, TlsMode::Disable)
        .await
        .expect("replication connect");
    let mut stream = client
        .start_replication(slot, start, publication)
        .await
        .expect("start_replication");
    let mut msgs = Vec::new();
    while let Ok(Ok(m)) =
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.recv()).await
    {
        msgs.push(m);
    }
    if let Some(lsn) = ack {
        stream.send_standby(lsn, lsn).await.expect("ack");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    inserted(msgs)
}

/// Which transactions a restarted stream sends, as `[ids]` for: starting
/// at the first commit's LSN, starting just past it, and reconnecting
/// after acking the second commit's LSN.
type Resumes = [Vec<i32>; 3];

async fn real_resumes() -> Resumes {
    let ns = namespace("resume");
    let (client, conn) = tokio_postgres::connect(dsn().await, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    for sql in [
        format!("CREATE SCHEMA {ns}"),
        format!("CREATE TABLE {ns}.t (id int4 PRIMARY KEY)"),
        format!("CREATE PUBLICATION {ns}_pub FOR TABLE {ns}.t"),
        format!("SELECT pg_create_logical_replication_slot('{ns}_slot', 'pgoutput')"),
    ] {
        client.batch_execute(&sql).await.unwrap();
    }
    for i in 1..=3 {
        client
            .batch_execute(&format!("INSERT INTO {ns}.t VALUES ({i})"))
            .await
            .unwrap();
    }
    let (slot, publication) = (format!("{ns}_slot"), format!("{ns}_pub"));
    let all = real_session(&slot, &publication, Lsn::ZERO, None).await;
    let (c1, c2) = (all[0].1, all[1].1);
    let ids = |v: Vec<(i32, Lsn)>| v.into_iter().map(|(i, _)| i).collect::<Vec<_>>();
    let at = ids(real_session(&slot, &publication, c1, None).await);
    let past = ids(real_session(&slot, &publication, Lsn(c1.0 + 1), None).await);
    real_session(&slot, &publication, Lsn::ZERO, Some(c2)).await;
    let after_ack = ids(real_session(&slot, &publication, Lsn::ZERO, None).await);
    client
        .batch_execute(&format!("SELECT pg_drop_replication_slot('{slot}')"))
        .await
        .unwrap();
    [at, past, after_ack]
}

fn sim_resumes() -> Resumes {
    let db = SimPostgres::new();
    let t = Table {
        cols: vec![col("id", "int4", PgType::Int4, IcebergType::Int)],
        ..table("t", Vec::new())
    };
    db.create_table(sim_schema("public", &t)).unwrap();
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "t".into(),
    };
    db.create_publication("pub", std::slice::from_ref(&ident))
        .unwrap();
    db.create_slot("slot", "pub").unwrap();
    for i in 1..=3 {
        let mut tx = db.begin_tx();
        tx.insert(&ident, [(ColumnName("id".into()), int(i))].into());
        tx.commit(pg2iceberg_core::Timestamp(0)).unwrap();
    }
    let session = |start: Lsn| {
        let mut stream = db.start_replication_at("slot", start).unwrap();
        let msgs: Vec<DecodedMessage> = std::iter::from_fn(|| stream.recv()).collect();
        (inserted(msgs), stream)
    };
    let (all, _) = session(Lsn::ZERO);
    let (c1, c2) = (all[0].1, all[1].1);
    let ids = |v: Vec<(i32, Lsn)>| v.into_iter().map(|(i, _)| i).collect::<Vec<_>>();
    let at = ids(session(c1).0);
    let past = ids(session(Lsn(c1.0 + 1)).0);
    session(Lsn::ZERO).1.send_standby(c2);
    let after_ack = ids(session(Lsn::ZERO).0);
    [at, past, after_ack]
}

/// Where a restarted stream resumes. Postgres skips the transactions
/// that committed before the start position and sends the rest — one
/// whose commit LSN *is* the start position comes again, so a consumer
/// acking commit LSNs sees its last acked transaction after every
/// reconnect. The DST only sees those replays if the sim agrees.
#[tokio::test]
async fn sim_resumes_replication_where_postgres_does() {
    let _serial = SERIAL.lock().await;
    let real = real_resumes().await;
    assert_eq!(real, [vec![1, 2, 3], vec![2, 3], vec![2, 3]], "Postgres");
    assert_eq!(sim_resumes(), real, "sim vs Postgres");
}
