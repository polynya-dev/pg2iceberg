//! Deterministic Simulation Test (DST) harness for the logical-replication
//! pipeline + materializer.
//!
//! For each randomly-generated workload, runs:
//!
//!   SimPostgres → SimReplicationStream → Pipeline.process → Sink → codec
//!     → MemoryBlobStore → MemoryCoordinator → CoordCommitReceipt → flushed_lsn
//!     → Materializer.cycle → fold → resolve_unchanged_cols → promote_re_inserts
//!     → TableWriter.prepare → MemoryCatalog.commit_snapshot → set_cursor
//!
//! Then asserts the plan §9 invariants:
//!
//! 1. Every `log_index.s3_path` resolves in the blob store.
//! 2. Per-table offsets are contiguous (start_i == end_{i-1}, first start = 0).
//! 3. `pipeline.flushed_lsn ≤ slot.confirmed_flush_lsn` after every ack.
//! 4. Staged events == committed WAL events (filtered to publication, sorted
//!    by LSN). This is the "no lost commits / no phantom commits" check.
//! 5. **PG ground truth == Iceberg materialized state at quiescence.** This
//!    is the headline correctness property of pg2iceberg: after a workload
//!    runs through the entire stack, `read_table(SimPostgres)` and
//!    `read_materialized_state(MemoryCatalog)` must be byte-equal (sorted
//!    by PK).
//! 6. **No WAL retention at quiescence:** `slot.confirmed_flush_lsn` ==
//!    end of WAL, even when the tail only touched unpublished tables.
//!
//! Workload generator interleaves `MaterializerCycle` with the pipeline
//! steps so the proptest exercises pipeline/materializer ordering, and
//! `CrashAndRestart` models pipeline-process crashes between flushes.
//!
//! `RestartMaterializer` crashes the materializer too: a new instance
//! rebuilds its FileIndex from catalog history, and must never reuse a
//! file path a committed snapshot references (the sim blob store refuses
//! overwrites).
//!
//! `Expire` and `CleanupOrphans` are the two halves of `maintain`.
//! Expiry drops snapshot metadata, never table state, so PG == Iceberg
//! is checked against the table as a query engine reads it
//! (`pg2iceberg_sim::oracle`: the table's real live files, columns by
//! field id, deletes scoped by partition and sequence number), and
//! invariant 12 checks that `Catalog::snapshots`, which the FileIndex
//! rebuild, compaction, orphan cleanup and `verify` replay, still matches
//! it.
//!
//! Each case runs on the sims or — under the `integration` feature — on
//! production's catalog and blob store (`IcebergRustCatalog` over
//! iceberg-rust's memory catalog, `ObjectStoreBlobStore` over
//! object_store's `InMemory`), so their translation layers are exercised
//! too.

use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::Coordinator;
use pg2iceberg_core::typemap::{IcebergType, PgType};
use pg2iceberg_core::{
    ColumnName, ColumnSchema, Namespace, Op, PgValue, Row, TableIdent, TableSchema, Timestamp,
};
use pg2iceberg_iceberg::CompactionConfig;
use pg2iceberg_iceberg::{
    Catalog, LogRange, PreparedCommit, PreparedCompaction, SchemaChange, Snapshot, TableMetadata,
};
use pg2iceberg_logical::materializer::{MaterializerNamer, UuidMaterializerNamer};
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::{replication_start_lsn, CachingCatalog, Materializer, Pipeline};
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::catalog::MemoryCatalog;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_sim::id::SeqIdGen;
use pg2iceberg_sim::oracle::{engine_read, live_files_from_history, LiveFile};
use pg2iceberg_sim::pgoutput::ReplicaIdentity;
use pg2iceberg_sim::postgres::{SimPostgres, SimReplicationStream};
use pg2iceberg_snapshot::Snapshotter;
use pg2iceberg_stream::codec::decode_chunk;
use pg2iceberg_stream::{object_key, BlobStore};
use pollster::block_on;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const TABLE_NAME: &str = "orders";
const PUB: &str = "pub1";
const SLOT: &str = "slot1";
/// Rows the pipeline may buffer before it must stage them. Tiny so
/// ordinary workloads produce transactions that span many chunks.
const FLUSH_ROWS: usize = 3;
/// Bound on change events held in memory, and on rows per staged
/// object: a not-yet-staged transaction tail plus committed rows
/// awaiting a flush, each below `FLUSH_ROWS`.
const MAX_BUFFERED_ROWS: usize = 2 * FLUSH_ROWS;
/// Materializer batch limit. Tiny so a large transaction spans several
/// batches — it must still become visible in one step (invariant 10).
const MAT_BATCH: usize = 2;

fn ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: TABLE_NAME.into(),
    }
}

fn schema() -> TableSchema {
    TableSchema {
        ident: ident(),
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            },
            // Stands in for a TOASTed column: `ToastUpdate` and `ChangePk`
            // leave it unchanged, so pgoutput sends a marker, not a value.
            // Ahead of `qty`, so dropping it renumbers `qty` for anything
            // that numbers columns by position.
            ColumnSchema {
                name: "note".into(),
                field_id: 2,
                ty: IcebergType::String,
                nullable: true,
                is_primary_key: false,
            },
            ColumnSchema {
                name: "qty".into(),
                field_id: 3,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: false,
            },
        ],
        // Partitioned by a non-key column, so an UPDATE can move a row
        // between partitions and a DELETE's key alone doesn't name one.
        partition_spec: if PARTITIONED.get() {
            vec![pg2iceberg_core::partition::PartitionField {
                source_column: "qty".into(),
                name: "qty_trunc".into(),
                transform: pg2iceberg_core::partition::Transform::Truncate(50),
            }]
        } else {
            Vec::new()
        },
        pg_schema: None,
    }
}

thread_local! {
    /// Whether this case's `id` column is a Postgres `smallint`. Its
    /// events carry `Int2`, but Iceberg stores it as `int`, so rows read
    /// back from data files carry `Int4`: anything keyed by PK must treat
    /// the two as the same key.
    static SMALLINT_PK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn id_value(id: i32) -> PgValue {
    if SMALLINT_PK.get() {
        PgValue::Int2(id.try_into().expect("DST ids fit in smallint"))
    } else {
        PgValue::Int4(id)
    }
}

/// A source row as Iceberg stores it (`smallint` → `int`), for
/// comparing PG state with materialized state.
fn stored(row: Row) -> Row {
    row.into_iter()
        .map(|(c, v)| match v {
            PgValue::Int2(n) => (c, PgValue::Int4(n.into())),
            v => (c, v),
        })
        .collect()
}

fn stored_rows(rows: Vec<Row>) -> Vec<Row> {
    rows.into_iter().map(stored).collect()
}

fn row(id: i32, qty: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), id_value(id));
    r.insert(ColumnName("qty".into()), PgValue::Int4(qty));
    if NOTE_PRESENT.get() {
        r.insert(note(), PgValue::Text(format!("note-{id}-{qty}")));
    }
    r
}

/// The table's schema as production discovers it at startup: the source
/// table's current columns, field ids numbered by position
/// (`discover.rs`), plus the configured partition spec.
fn discovered_schema(db: &SimPostgres) -> TableSchema {
    let current = db.table_schema(&ident()).expect("source table");
    TableSchema {
        columns: current
            .columns
            .into_iter()
            .enumerate()
            .map(|(i, c)| ColumnSchema {
                field_id: i as i32 + 1,
                ..c
            })
            .collect(),
        ..schema()
    }
}

/// `rows` restricted to the source table's current columns (a column
/// a row lacks reads as NULL): Iceberg keeps dropped columns, and a row
/// written before a column existed doesn't carry it.
fn on_source_columns(db: &SimPostgres, rows: Vec<Row>) -> Vec<Row> {
    let cols: Vec<String> = db
        .table_schema(&ident())
        .expect("source table")
        .columns
        .into_iter()
        .map(|c| c.name)
        .collect();
    on_columns(&cols, rows)
}

/// `rows` on just `cols`, NULL where a row lacks one.
fn on_columns(cols: &[String], rows: Vec<Row>) -> Vec<Row> {
    rows.into_iter()
        .map(|r| {
            cols.iter()
                .map(|c| {
                    let c = ColumnName(c.clone());
                    let v = r.get(&c).cloned().unwrap_or(PgValue::Null);
                    (c, v)
                })
                .collect()
        })
        .collect()
}

fn other_pg_ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["sales".into()]),
        name: if SECOND_TABLE.get() == 2 {
            "returns".into()
        } else {
            TABLE_NAME.into()
        },
    }
}

/// The second table's Iceberg name, mapped the way
/// `TableConfig::iceberg_ident` maps it: with `sink.namespace` set, the
/// PG schema is replaced and the table name kept. (Two tables mapped to
/// one name is a config error.)
fn other_ident() -> TableIdent {
    if SECOND_TABLE.get() == 2 {
        TableIdent {
            namespace: Namespace(vec!["public".into()]),
            name: "returns".into(),
        }
    } else {
        other_pg_ident()
    }
}

fn other_pg_schema() -> TableSchema {
    TableSchema {
        ident: other_pg_ident(),
        partition_spec: Vec::new(),
        ..schema()
    }
}

fn other_schema() -> TableSchema {
    TableSchema {
        ident: other_ident(),
        pg_schema: Some("sales".into()),
        partition_spec: Vec::new(),
        ..schema()
    }
}

fn other_row(id: i32, qty: i32) -> Row {
    BTreeMap::from([
        (ColumnName("id".into()), PgValue::Int4(id)),
        (ColumnName("qty".into()), PgValue::Int4(qty)),
        (note(), PgValue::Text(format!("sales-{id}-{qty}"))),
    ])
}

/// The publication's tables.
fn published() -> Vec<TableIdent> {
    let mut t = vec![ident()];
    if SECOND_TABLE.get() > 0 {
        t.push(other_pg_ident());
    }
    t
}

/// Register the second table with a materializer, as the lifecycle
/// does: its Iceberg schema, and its PG name's translation.
fn register_other(m: &mut Materializer<AuditedCatalog>) {
    if SECOND_TABLE.get() > 0 {
        block_on(m.register_table(other_schema())).unwrap();
    }
}

/// `note`'s default, when `AddNoteWithDefault` adds it.
const DEFAULT_NOTE: &str = "dflt";

fn note() -> ColumnName {
    ColumnName("note".into())
}

/// A table outside the publication. Its WAL never reaches the
/// pipeline, so it models every write the slot must not stay pinned
/// behind: other app tables, other databases on the cluster, and
/// pg2iceberg's own coord writes.
fn noise_ident() -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "noise".into(),
    }
}

fn noise_schema() -> TableSchema {
    TableSchema {
        ident: noise_ident(),
        ..schema()
    }
}

fn pk_only(id: i32) -> Row {
    let mut r = BTreeMap::new();
    r.insert(ColumnName("id".into()), id_value(id));
    r
}

// ---------- workload model ----------

#[derive(Clone, Debug)]
enum Step {
    /// `BEGIN; INSERT id, qty; COMMIT` — skipped if `id` already exists.
    Insert {
        id: i32,
        qty: i32,
    },
    /// `BEGIN; UPDATE id SET qty=N; COMMIT` — skipped if `id` is missing.
    Update {
        id: i32,
        qty: i32,
    },
    /// `UPDATE id SET qty=N` leaving the TOASTed `note` alone, so pgoutput
    /// sends an unchanged marker for it — skipped if `id` is missing.
    ToastUpdate {
        id: i32,
        qty: i32,
    },
    /// `UPDATE SET id=to WHERE id=from`, optionally with `note` TOASTed
    /// (unchanged marker) — skipped unless `from` exists and `to` doesn't.
    ChangePk {
        from: i32,
        to: i32,
        toast: bool,
    },
    /// `BEGIN; DELETE id; COMMIT` — skipped if `id` is missing.
    Delete {
        id: i32,
    },
    /// `BEGIN; INSERT id, qty; ROLLBACK`. Exercises the rollback path.
    RollbackInsert {
        id: i32,
        qty: i32,
    },
    /// `BEGIN; TRUNCATE; [INSERT id, qty;] COMMIT` — with the insert, the
    /// full-reload pattern. Rows written since the last materializer
    /// cycle share a fold step with the TRUNCATE.
    Truncate {
        reinsert: Option<(i32, i32)>,
    },
    /// `TRUNCATE orders, other` — one statement, so one WAL record and
    /// one pgoutput message naming both tables. Needs the second table.
    TruncateBoth,
    /// Something that invalidates the tables' relation cache entries
    /// without changing them (`CREATE INDEX`, `ANALYZE`): pgoutput resends
    /// each table's Relation, unchanged, before its next change.
    Invalidate,
    /// `BEGIN; INSERT INTO noise ...; COMMIT` — WAL for a table outside
    /// the publication. pgoutput skips the whole transaction, so only a
    /// keepalive tells the pipeline it can ack past it.
    UnpublishedWrite {
        qty: i32,
    },
    /// One transaction that updates every live row and inserts `inserts`
    /// fresh ones — routinely bigger than `FLUSH_ROWS`, so it must be
    /// staged in chunks.
    BigTx {
        inserts: usize,
        qty: i32,
    },
    /// Process at most `n` replication messages; may stop mid-transaction.
    DrivePartial {
        n: usize,
    },
    /// A flush tick + ack without draining the stream first.
    FlushTick,
    /// `DriveFlush` whose claim lands but whose slot ack doesn't: the
    /// process dies in between, and the next start replays from the
    /// slot what it already staged.
    DriveFlushWithoutAck,
    /// Hard crash: no drain, flush, or ack. Pipeline memory and any
    /// staged-but-unclaimed objects are lost; the slot replays from
    /// `restart_lsn`.
    CrashMidStream,
    /// The replication connection drops (Postgres restarts, the network
    /// cuts it, the walsender is terminated) and the lifecycle reopens
    /// the stream in-process: the pipeline's session is reset, the rest
    /// of it — and everything else — carries on.
    Reconnect,
    /// Drive replication + flush + ack: a complete pipeline cycle.
    DriveFlush,
    /// Run one materializer cycle for every registered table.
    MaterializerCycle,
    /// `DriveFlush` followed by dropping the pipeline + stream and rebuilding
    /// from the slot's `restart_lsn`. The coord, blob_store, catalog, and
    /// materializer (with its in-memory FileIndex) survive — those represent
    /// durable storage and the parallel materializer worker process. Phase
    /// 8.5 will extend this to also crash the materializer.
    CrashAndRestart,
    /// Materializer-process crash: a new materializer — fresh file namer,
    /// FileIndex rebuilt from the catalog — over the same durable coord,
    /// catalog, and blob store.
    RestartMaterializer,
    /// One compaction pass with an input budget so small it rewrites a
    /// single dirty file (or two clean ones) at a time, leaving older
    /// deletes and the rest of the table for later passes.
    Compact,
    /// An insert into the second table (`SECOND_TABLE`).
    OtherInsert {
        id: i32,
        qty: i32,
    },
    OtherUpdate {
        id: i32,
        qty: i32,
    },
    OtherDelete {
        id: i32,
    },
    /// Worker "b" runs a cycle (distributed mode).
    OtherWorkerCycle,
    /// Time moves past the heartbeat TTL: a worker that doesn't cycle
    /// next loses its tables to the one that does.
    ClockTick,
    /// Backfill one chunk (one row) of the initial snapshot, read as of
    /// the snapshot's LSN — rows may have changed since.
    BackfillChunk,
    /// `ALTER TABLE DROP COLUMN note` — Iceberg keeps it, renamed out of
    /// the way.
    DropNote,
    /// `ALTER TABLE ADD COLUMN note text` — a new column that happens to
    /// reuse a dropped one's name; its old values must not come back.
    AddNote,
    /// `ALTER TABLE ADD COLUMN note text DEFAULT 'dflt'`: rows already in
    /// the table read `'dflt'` — Postgres stores the value once instead of
    /// writing it into them, so the WAL carries nothing for them.
    AddNoteWithDefault,
    /// Another process (a `pg2iceberg compact` job) plans a compaction
    /// pass and writes its output files, but doesn't commit yet.
    ExternalCompactPlan,
    /// That process commits the pass it planned — after whatever happened
    /// to the table in between.
    ExternalCompactCommit,
    /// That process commits the pass while the materializer's next commit
    /// is in flight, which then lands on top of it (iceberg-rust retries a
    /// commit that lost the race).
    ExternalCompactMidCommit,
    /// The next catalog commit applies, then reports failure (its
    /// response is lost). The materializer must not lose or duplicate
    /// anything when it retries.
    LoseCommitResponse,
    /// The materializer process dies after a catalog commit lands, before
    /// it records its cursor: the restarted one reads the log from the
    /// old cursor, and must not apply what landed again.
    CrashAfterCommit,
    /// `maintain`'s first half: expire every snapshot but the current
    /// one. Readers still see the whole table; the files those snapshots
    /// added stay live.
    Expire,
    /// `maintain`'s second half: delete every blob in the table's
    /// directory that the table doesn't reference, with no grace period.
    CleanupOrphans,
}

fn step_strategy() -> impl Strategy<Value = Step> {
    // Small id space so collisions / valid Update / valid Delete are common.
    let id = 1i32..=6;
    let qty = 0i32..=100;
    prop_oneof![
        5 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::Insert { id, qty }),
        3 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::Update { id, qty }),
        2 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::ToastUpdate { id, qty }),
        1 => (id.clone(), id.clone(), any::<bool>())
            .prop_map(|(from, to, toast)| Step::ChangePk { from, to, toast }),
        2 => id.clone().prop_map(|id| Step::Delete { id }),
        1 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::RollbackInsert { id, qty }),
        1 => prop::option::of((id.clone(), qty.clone()))
            .prop_map(|reinsert| Step::Truncate { reinsert }),
        1 => Just(Step::TruncateBoth),
        1 => Just(Step::Invalidate),
        3 => qty.clone().prop_map(|qty| Step::UnpublishedWrite { qty }),
        2 => (1usize..=8, qty.clone()).prop_map(|(inserts, qty)| Step::BigTx { inserts, qty }),
        2 => (1usize..=6).prop_map(|n| Step::DrivePartial { n }),
        1 => Just(Step::FlushTick),
        1 => Just(Step::DriveFlushWithoutAck),
        1 => Just(Step::CrashMidStream),
        1 => Just(Step::Reconnect),
        3 => Just(Step::DriveFlush),
        2 => Just(Step::MaterializerCycle),
        1 => Just(Step::CrashAndRestart),
        1 => Just(Step::RestartMaterializer),
        2 => Just(Step::Compact),
        1 => Just(Step::Expire),
        1 => Just(Step::LoseCommitResponse),
        1 => Just(Step::CrashAfterCommit),
        2 => Just(Step::BackfillChunk),
        2 => Just(Step::OtherWorkerCycle),
        2 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::OtherInsert { id, qty }),
        1 => (id.clone(), qty.clone()).prop_map(|(id, qty)| Step::OtherUpdate { id, qty }),
        1 => id.clone().prop_map(|id| Step::OtherDelete { id }),
        1 => Just(Step::ClockTick),
        1 => Just(Step::DropNote),
        1 => Just(Step::AddNote),
        1 => Just(Step::AddNoteWithDefault),
        2 => Just(Step::ExternalCompactPlan),
        2 => Just(Step::ExternalCompactCommit),
        1 => Just(Step::ExternalCompactMidCommit),
        1 => Just(Step::CleanupOrphans),
    ]
}

/// Half the cases under the `integration` feature — which builds
/// production's catalog, blob store and pgoutput decoder — none without.
fn integration_only() -> impl Strategy<Value = bool> {
    if cfg!(feature = "integration") {
        any::<bool>().boxed()
    } else {
        Just(false).boxed()
    }
}

fn workload() -> impl Strategy<Value = Vec<Step>> {
    prop::collection::vec(step_strategy(), 1..=24)
}

// ---------- harness ----------

/// The production file namer: a fresh instance per materializer
/// incarnation, drawing from the shared UUID sequence.
fn mat_namer(id_gen: &Arc<SeqIdGen>) -> Arc<dyn MaterializerNamer> {
    Arc::new(UuidMaterializerNamer::new(id_gen.clone(), MAT_PREFIX))
}

/// One bucket, laid out like production: staged WAL chunks and
/// materialized data files under separate prefixes. Orphan cleanup must
/// only ever touch the latter.
const STAGE_PREFIX: &str = "s3://warehouse/staged";
const MAT_PREFIX: &str = "s3://warehouse/materialized";

thread_local! {
    /// Whether this case runs on production's catalog and blob store.
    static PROD_BACKEND: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether this case's replication stream goes over the wire: the sim
    /// encodes pgoutput and production's decoder decodes it.
    static WIRE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether the source table has its `note` column right now (the
    /// workload can drop and re-add it).
    static NOTE_PRESENT: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
    /// Whether `note` was added with a default since pg2iceberg last
    /// staged the stream (see `Step::DropNote`).
    static NOTE_DEFAULT_UNSTAGED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether this case starts with rows only an initial snapshot can
    /// deliver, backfilled chunk by chunk while changes stream in.
    static BACKFILL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether this case materializes with two distributed workers that
    /// hand the table back and forth as their heartbeats lapse.
    static DISTRIBUTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// A second table: 0 = none; 1 = `sales.orders`, the same name in
    /// another namespace, with `sink.namespace` unset (its Iceberg
    /// namespace is its PG schema); 2 = `sales.returns` with
    /// `sink.namespace = "public"`, which materializes it as
    /// `public.returns`.
    static SECOND_TABLE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
    /// Whether this case's table is partitioned (by a non-key column).
    static PARTITIONED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether this case's table has Postgres's default replica identity
    /// (a DELETE sends only the key) rather than FULL.
    static DEFAULT_IDENTITY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

thread_local! {
    /// What this thread's materializers record (see [`metrics`]).
    static METRICS: Arc<pg2iceberg_core::InMemoryMetrics> =
        Arc::new(pg2iceberg_core::InMemoryMetrics::new());
}

/// The metrics this thread's materializers record.
fn metrics() -> Arc<pg2iceberg_core::InMemoryMetrics> {
    METRICS.with(Arc::clone)
}

/// A replication session on the wire: production's pgoutput decoding of
/// what the sim's walsender encodes.
#[cfg(feature = "integration")]
struct Wire {
    decoder: pg2iceberg_pg::prod::PgoutputDecoder,
    decoded: std::collections::VecDeque<DecodedMessage>,
}

#[cfg(feature = "integration")]
impl Wire {
    /// A fresh session, or none unless this case runs on the wire.
    fn for_case() -> Option<Self> {
        WIRE.get().then(|| Self {
            decoder: pg2iceberg_pg::prod::PgoutputDecoder::new(),
            decoded: Default::default(),
        })
    }

    fn next(&mut self, stream: &mut SimReplicationStream) -> Option<DecodedMessage> {
        loop {
            if let Some(m) = self.decoded.pop_front() {
                return Some(m);
            }
            match stream.recv_wire()? {
                pg2iceberg_sim::postgres::WireMessage::Pgoutput(bytes) => self.decoded.extend(
                    self.decoder
                        .decode(&bytes)
                        .expect("production decodes the sim's pgoutput"),
                ),
                pg2iceberg_sim::postgres::WireMessage::Keepalive {
                    wal_end,
                    reply_requested,
                } => {
                    return Some(DecodedMessage::Keepalive {
                        wal_end,
                        reply_requested,
                    })
                }
            }
        }
    }
}

/// Where the materialized table lives, and the oracle that reads it as
/// a query engine would.
#[derive(Clone)]
struct Storage {
    catalog: Arc<dyn Catalog>,
    blob: Arc<dyn BlobStore>,
    backend: Backend,
}

#[derive(Clone)]
enum Backend {
    Sim {
        catalog: Arc<MemoryCatalog>,
        blob: Arc<MemoryBlobStore>,
    },
    #[cfg(feature = "integration")]
    Prod {
        iceberg: Arc<iceberg::memory::MemoryCatalog>,
    },
}

impl Storage {
    fn for_case() -> Self {
        if PROD_BACKEND.get() {
            #[cfg(feature = "integration")]
            return prod_backend::storage();
        }
        Self::sim()
    }

    fn sim() -> Self {
        let catalog = Arc::new(MemoryCatalog::new());
        let blob = Arc::new(MemoryBlobStore::new());
        Self {
            catalog: catalog.clone(),
            blob: blob.clone(),
            backend: Backend::Sim { catalog, blob },
        }
    }

    /// The table as a query engine reads it, sorted by PK: ground truth
    /// for the invariants. See `pg2iceberg_sim::oracle`.
    async fn engine_rows(&self, ident: &TableIdent) -> Result<Vec<Row>, String> {
        let files = self.live_files(ident).await?;
        let schema = self
            .catalog
            .load_table(ident)
            .await
            .map_err(|e| format!("load_table: {e}"))?
            .ok_or_else(|| format!("no table {ident}"))?
            .schema;
        let mut rows = engine_read(self.blob.as_ref(), &schema, &files).await?;
        sort_by_pk(&mut rows);
        Ok(rows)
    }

    /// The table's live files, from its real state rather than
    /// `Catalog::snapshots`.
    async fn live_files(&self, ident: &TableIdent) -> Result<Vec<LiveFile>, String> {
        match &self.backend {
            Backend::Sim { catalog, .. } => Ok(live_files_from_history(&catalog.history(ident))),
            #[cfg(feature = "integration")]
            Backend::Prod { iceberg } => prod_backend::live_files(iceberg, ident).await,
        }
    }

    /// The current snapshot's summary counts the files it holds. (The sim
    /// catalog writes no summaries.)
    #[cfg_attr(not(feature = "integration"), allow(unused_variables))]
    async fn summary_counts_live_files(&self, ident: &TableIdent) -> Result<(), String> {
        match &self.backend {
            Backend::Sim { .. } => Ok(()),
            #[cfg(feature = "integration")]
            Backend::Prod { iceberg } => {
                prod_backend::summary_counts_live_files(iceberg, ident).await
            }
        }
    }

    /// Object keys of every stored blob.
    async fn blob_keys(&self) -> Result<BTreeSet<String>, String> {
        Ok(self
            .blob
            .list("")
            .await
            .map_err(|e| format!("list blobs: {e}"))?
            .into_iter()
            .map(|b| object_key(&b.path).to_string())
            .collect())
    }

    /// The sim blob store, for tests that count its reads.
    fn sim_blob(&self) -> &MemoryBlobStore {
        match &self.backend {
            Backend::Sim { blob, .. } => blob,
            #[cfg(feature = "integration")]
            Backend::Prod { .. } => panic!("sim backend only"),
        }
    }
}

/// Production's catalog and blob store over in-memory backends.
#[cfg(feature = "integration")]
mod prod_backend {
    use super::*;
    use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
    use iceberg::spec::DataContentType;
    use iceberg::{CatalogBuilder, NamespaceIdent};
    use pg2iceberg_iceberg::prod::IcebergRustCatalog;
    use pg2iceberg_stream::ObjectStoreBlobStore;
    use std::collections::HashMap;

    pub fn storage() -> Storage {
        let iceberg = Arc::new(
            block_on(MemoryCatalogBuilder::default().load(
                "dst",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    "memory:///warehouse".to_string(),
                )]),
            ))
            .unwrap(),
        );
        let blob = Arc::new(WriteOnce(ObjectStoreBlobStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        ))));
        Storage {
            catalog: Arc::new(IcebergRustCatalog::new(iceberg.clone())),
            blob,
            backend: Backend::Prod { iceberg },
        }
    }

    /// The current snapshot's summary totals equal what its manifests
    /// hold. Engines take them as the table's size: ClickHouse 26.2
    /// answered `count()` with `total-records`.
    pub async fn summary_counts_live_files(
        iceberg: &iceberg::memory::MemoryCatalog,
        ident: &TableIdent,
    ) -> Result<(), String> {
        use iceberg::Catalog as _;
        let err = |e: iceberg::Error| e.to_string();
        let ns = NamespaceIdent::from_strs(&ident.namespace.0).map_err(err)?;
        let table = iceberg
            .load_table(&iceberg::TableIdent::new(ns, ident.name.clone()))
            .await
            .map_err(err)?;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(());
        };
        let list = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(err)?;
        let mut held: HashMap<&str, u64> = HashMap::new();
        for manifest_file in list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(err)?;
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                let df = entry.data_file();
                let (files, records) = match df.content_type() {
                    DataContentType::Data => ("total-data-files", "total-records"),
                    _ => ("total-delete-files", "total-equality-deletes"),
                };
                *held.entry(files).or_default() += 1;
                *held.entry(records).or_default() += df.record_count();
            }
        }
        let summary = &snapshot.summary().additional_properties;
        for key in [
            "total-records",
            "total-data-files",
            "total-delete-files",
            "total-equality-deletes",
        ] {
            let claims = summary.get(key).map(String::as_str).unwrap_or("none");
            let holds = held.get(key).copied().unwrap_or(0);
            if claims != holds.to_string() {
                return Err(format!(
                    "snapshot {} summary: {key} = {claims}, its files hold {holds}",
                    snapshot.sequence_number()
                ));
            }
        }
        Ok(())
    }

    /// The live files of the table's current snapshot, straight from
    /// its manifests.
    pub async fn live_files(
        iceberg: &iceberg::memory::MemoryCatalog,
        ident: &TableIdent,
    ) -> Result<Vec<LiveFile>, String> {
        use iceberg::Catalog as _;
        let err = |e: iceberg::Error| e.to_string();
        let ns = NamespaceIdent::from_strs(&ident.namespace.0).map_err(err)?;
        let table = iceberg
            .load_table(&iceberg::TableIdent::new(ns, ident.name.clone()))
            .await
            .map_err(err)?;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(Vec::new());
        };
        let list = table
            .manifest_list_reader(snapshot)
            .load()
            .await
            .map_err(err)?;
        let mut out = Vec::new();
        for manifest_file in list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(err)?;
            for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                let df = entry.data_file();
                let equality_ids = match df.content_type() {
                    DataContentType::Data => None,
                    DataContentType::EqualityDeletes => Some(df.equality_ids().unwrap_or_default()),
                    DataContentType::PositionDeletes => {
                        return Err(format!("unexpected position delete {}", df.file_path()))
                    }
                };
                out.push(LiveFile {
                    path: df.file_path().to_string(),
                    seq: entry
                        .sequence_number()
                        .ok_or_else(|| format!("{}: no sequence number", df.file_path()))?,
                    partition: df
                        .partition()
                        .iter()
                        .map(partition_literal)
                        .collect::<Result<_, _>>()?,
                    equality_ids,
                });
            }
        }
        Ok(out)
    }

    /// The harness partitions by `truncate(qty)`: an int, or null.
    fn partition_literal(
        value: Option<&iceberg::spec::Literal>,
    ) -> Result<pg2iceberg_core::partition::PartitionLiteral, String> {
        use iceberg::spec::{Literal, PrimitiveLiteral};
        use pg2iceberg_core::partition::PartitionLiteral;
        match value {
            None => Ok(PartitionLiteral::Null),
            Some(Literal::Primitive(PrimitiveLiteral::Int(n))) => Ok(PartitionLiteral::Int(*n)),
            Some(other) => Err(format!("unexpected partition value {other:?}")),
        }
    }

    /// Refuses to overwrite an object, like the sim blob store: every path
    /// pg2iceberg writes must be new.
    struct WriteOnce(ObjectStoreBlobStore);

    #[async_trait::async_trait]
    impl BlobStore for WriteOnce {
        async fn put(&self, path: &str, bytes: bytes::Bytes) -> pg2iceberg_stream::Result<()> {
            if self.0.get(path).await.is_ok() {
                return Err(pg2iceberg_stream::StreamError::Io(format!(
                    "refusing to overwrite existing blob {path}"
                )));
            }
            self.0.put(path, bytes).await
        }
        async fn get(&self, path: &str) -> pg2iceberg_stream::Result<bytes::Bytes> {
            self.0.get(path).await
        }
        async fn list(
            &self,
            prefix: &str,
        ) -> pg2iceberg_stream::Result<Vec<pg2iceberg_stream::BlobInfo>> {
            self.0.list(prefix).await
        }
        async fn delete(&self, path: &str) -> pg2iceberg_stream::Result<()> {
            self.0.delete(path).await
        }
    }
}

/// How long a distributed worker's heartbeat keeps its tables.
const WORKER_TTL: std::time::Duration = std::time::Duration::from_secs(30);

fn worker(name: &str) -> pg2iceberg_core::WorkerId {
    pg2iceberg_core::WorkerId(format!("worker-{name}"))
}

/// One materializer cycle; `None` if it failed because a commit's
/// response was lost — the lifecycle just runs the next cycle.
fn cycle(m: &mut Materializer<AuditedCatalog>) -> Option<usize> {
    match block_on(m.cycle()) {
        Ok(n) => Some(n),
        Err(e) if e.to_string().contains("response lost") => None,
        Err(e) => panic!("materializer cycle: {e}"),
    }
}

/// A pipeline set up the way production sets one up — including the
/// table's primary key, without which it can't split a key-changing
/// UPDATE into a Delete of the old key and an Update of the new one, and
/// `db`'s catalog for column defaults.
fn new_pipeline(
    coord: &Arc<MemoryCoordinator>,
    blob_store: &Arc<dyn BlobStore>,
    namer: &Arc<CounterBlobNamer>,
    db: &SimPostgres,
) -> Pipeline<MemoryCoordinator> {
    let mut pipeline = backfill_pipeline(coord, blob_store, namer);
    pipeline.track_replication();
    pipeline.read_column_defaults(Arc::new(db.clone()));
    pipeline
}

/// A pipeline for the harness's tables that doesn't consume the
/// replication stream: a mid-stream backfill's, as production runs one.
fn backfill_pipeline(
    coord: &Arc<MemoryCoordinator>,
    blob_store: &Arc<dyn BlobStore>,
    namer: &Arc<CounterBlobNamer>,
) -> Pipeline<MemoryCoordinator> {
    let mut pipeline = Pipeline::new(coord.clone(), blob_store.clone(), namer.clone(), FLUSH_ROWS);
    pipeline.register_primary_keys(ident(), vec![ColumnName("id".into())]);
    if SECOND_TABLE.get() > 0 {
        pipeline.register_primary_keys(other_ident(), vec![ColumnName("id".into())]);
        pipeline.register_table_translation(other_pg_ident(), other_ident());
    }
    pipeline
}

struct DstHarness {
    db: SimPostgres,
    coord: Arc<MemoryCoordinator>,
    blob_store: Arc<dyn BlobStore>,
    /// The backend behind `blob_store` and the catalog, and the oracle
    /// that reads it as a query engine would.
    storage: Storage,
    namer: Arc<CounterBlobNamer>,
    pipeline: Pipeline<MemoryCoordinator>,
    /// The backfill's own pipeline, in `BACKFILL` mode.
    backfill_pipeline: Option<Pipeline<MemoryCoordinator>>,
    materializer: Materializer<AuditedCatalog>,
    /// The materializer's catalog: checks invariant 10 after every commit.
    audited: Arc<AuditedCatalog>,
    stream: SimReplicationStream,
    /// The stream's wire session, when the case runs on the wire.
    #[cfg(feature = "integration")]
    wire: Option<Wire>,
    /// Mirror of which PK ids are currently live in the source DB. Used by
    /// the workload runner to pre-filter ops the proptest generator can't
    /// know about (state-dependent validity).
    live: BTreeSet<i32>,
    /// Next PK for `noise` inserts (always fresh, so they never conflict).
    noise_next_id: i32,
    /// Last PK handed out by `BigTx` inserts (kept clear of `1..=6`).
    next_bulk_id: i32,
    /// UUID source for materializer file names, shared by every
    /// materializer incarnation — like real UUIDs, never repeating.
    id_gen: Arc<SeqIdGen>,
    /// Whether the initial snapshot is still being backfilled.
    backfilling: bool,
    /// Ids live in the second table.
    other_live: BTreeSet<i32>,
    /// The coordinator's clock: heartbeats lapse when it moves on.
    clock: TestClock,
    /// In distributed mode, worker "b"; `materializer` is worker "a".
    other: Option<Materializer<AuditedCatalog>>,
    /// The WAL position when the slot was created: transactions committed
    /// before it reach Iceberg only through the snapshot.
    slot_start: pg2iceberg_core::Lsn,
    /// A compaction pass another process (a `pg2iceberg compact` job)
    /// has planned and written but not yet committed.
    pending_external: Option<PreparedCompaction>,
    /// The tokio runtime prod-backend cases run iceberg-rust on (see
    /// [`tokio_runtime`]), entered for the case. Dropped last.
    #[cfg(feature = "integration")]
    _tokio: Option<tokio::runtime::EnterGuard<'static>>,
}

/// iceberg-rust runs its work on a tokio runtime — the catalog defaults
/// to the current one, and spawns onto it — but the harness drives futures
/// with `pollster`. Prod-backend cases enter this runtime, whose worker
/// threads run what iceberg-rust spawns.
#[cfg(feature = "integration")]
fn tokio_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

impl DstHarness {
    /// Boot with a set of pre-existing rows. The seeds are committed BEFORE
    /// the publication + slot are created, so logical replication won't see
    /// them — they have to come in via the snapshot phase.
    fn boot_with_seeds(seeds: &[(i32, i32)]) -> Self {
        // A fresh table has its `note` column (proptest reuses the thread).
        NOTE_PRESENT.set(true);
        NOTE_DEFAULT_UNSTAGED.set(false);
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
        db.create_table(noise_schema()).unwrap();

        if !seeds.is_empty() {
            let mut tx = db.begin_tx();
            for (id, qty) in seeds {
                tx.insert(&ident(), row(*id, *qty));
            }
            tx.commit(Timestamp(0)).unwrap();
        }

        if SECOND_TABLE.get() > 0 {
            db.create_table(other_pg_schema()).unwrap();
        }
        db.create_publication(PUB, &published()).unwrap();
        db.create_slot(SLOT, PUB).unwrap();
        let slot_start = db.current_lsn();

        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock.clone());
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        #[cfg(feature = "integration")]
        let tokio = PROD_BACKEND.get().then(|| tokio_runtime().enter());
        let storage = Storage::for_case();
        let blob_store = storage.blob.clone();
        let catalog = storage.catalog.clone();
        let namer = Arc::new(CounterBlobNamer::new(STAGE_PREFIX));
        let pipeline = new_pipeline(&coord, &blob_store, &namer, &db);

        let id_gen = Arc::new(SeqIdGen::new());
        let mat_namer = mat_namer(&id_gen);
        let audited = Arc::new(AuditedCatalog {
            inner: catalog.clone(),
            storage: storage.clone(),
            db: db.clone(),
            violations: Mutex::new(Vec::new()),
            fail_next_commit: Default::default(),
            fail_commit_after_schema_change: Default::default(),
            audit_paused: Default::default(),
            lose_next_response: Default::default(),
            land_before_next_commit: Default::default(),
            coord: coord.clone() as Arc<dyn Coordinator>,
            applied: Default::default(),
        });
        let mut materializer = Materializer::with_metrics(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            audited.clone(),
            mat_namer,
            "default",
            MAT_BATCH,
            metrics(),
        );
        block_on(materializer.register_table(schema())).unwrap();
        register_other(&mut materializer);
        let other = DISTRIBUTED.get().then(|| {
            materializer.enable_distributed_mode(worker("a"), WORKER_TTL);
            let mut b = Materializer::with_metrics(
                coord.clone() as Arc<dyn Coordinator>,
                blob_store.clone(),
                audited.clone(),
                self::mat_namer(&id_gen),
                "default",
                MAT_BATCH,
                metrics(),
            );
            block_on(b.register_table(schema())).unwrap();
            register_other(&mut b);
            b.enable_distributed_mode(worker("b"), WORKER_TTL);
            b
        });

        if SMALLINT_PK.get() {
            db.set_pg_type(&ident(), "id", PgType::Int2);
        }
        if DEFAULT_IDENTITY.get() {
            db.set_replica_identity(&ident(), ReplicaIdentity::Default);
        }
        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            coord,
            blob_store,
            storage,
            namer,
            pipeline,
            materializer,
            audited,
            stream,
            #[cfg(feature = "integration")]
            wire: Wire::for_case(),
            live: seeds.iter().map(|(id, _)| *id).collect(),
            noise_next_id: 0,
            next_bulk_id: 1000,
            id_gen,
            pending_external: None,
            #[cfg(feature = "integration")]
            _tokio: tokio,
            backfilling: false,
            backfill_pipeline: None,
            other_live: BTreeSet::new(),
            slot_start,
            clock,
            other,
        }
    }

    fn boot() -> Self {
        if BACKFILL.get() {
            // Rows written before the slot existed; the snapshot reads
            // them as of now while the workload keeps changing them.
            let mut h = Self::boot_with_seeds(&[(1, 10), (2, 20), (3, 30), (4, 40)]);
            // The table's changes stream from when it joined the
            // publication, a little before the backfill's snapshot.
            let mut tx = h.db.begin_tx();
            tx.update(&ident(), row(4, 41));
            tx.commit(Timestamp(0)).unwrap();
            h.db.begin_snapshot();
            h.backfilling = true;
            // As production adds a table mid-stream: its backfill runs on
            // a pipeline of its own, and the table is gated — its changes
            // stay unmaterialized — until the backfill completes.
            h.backfill_pipeline = Some(backfill_pipeline(&h.coord, &h.blob_store, &h.namer));
            block_on(h.materializer.register_table_pending(schema())).unwrap();
            if let Some(b) = h.other.as_mut() {
                block_on(b.register_table_pending(schema())).unwrap();
            }
            // A half-loaded table matches no transaction boundary; that's
            // expected of a backfill, so audits wait until it's done.
            h.audited
                .audit_paused
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return h;
        }
        // A fresh table has its `note` column (proptest reuses the thread).
        NOTE_PRESENT.set(true);
        NOTE_DEFAULT_UNSTAGED.set(false);
        let db = SimPostgres::new();
        db.create_table(schema()).unwrap();
        db.create_table(noise_schema()).unwrap();
        if SECOND_TABLE.get() > 0 {
            db.create_table(other_pg_schema()).unwrap();
        }
        db.create_publication(PUB, &published()).unwrap();
        db.create_slot(SLOT, PUB).unwrap();
        let slot_start = db.current_lsn();

        let clock = TestClock::at(0);
        let arc_clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(clock.clone());
        let coord = Arc::new(MemoryCoordinator::new(
            CoordSchema::default_name(),
            arc_clock,
        ));
        #[cfg(feature = "integration")]
        let tokio = PROD_BACKEND.get().then(|| tokio_runtime().enter());
        let storage = Storage::for_case();
        let blob_store = storage.blob.clone();
        let catalog = storage.catalog.clone();
        let namer = Arc::new(CounterBlobNamer::new(STAGE_PREFIX));
        let pipeline = new_pipeline(&coord, &blob_store, &namer, &db);

        let id_gen = Arc::new(SeqIdGen::new());
        let mat_namer = mat_namer(&id_gen);
        let audited = Arc::new(AuditedCatalog {
            inner: catalog.clone(),
            storage: storage.clone(),
            db: db.clone(),
            violations: Mutex::new(Vec::new()),
            fail_next_commit: Default::default(),
            fail_commit_after_schema_change: Default::default(),
            audit_paused: Default::default(),
            lose_next_response: Default::default(),
            land_before_next_commit: Default::default(),
            coord: coord.clone() as Arc<dyn Coordinator>,
            applied: Default::default(),
        });
        let mut materializer = Materializer::with_metrics(
            coord.clone() as Arc<dyn Coordinator>,
            blob_store.clone(),
            audited.clone(),
            mat_namer,
            "default",
            MAT_BATCH,
            metrics(),
        );
        block_on(materializer.register_table(schema())).unwrap();
        register_other(&mut materializer);
        let other = DISTRIBUTED.get().then(|| {
            materializer.enable_distributed_mode(worker("a"), WORKER_TTL);
            let mut b = Materializer::with_metrics(
                coord.clone() as Arc<dyn Coordinator>,
                blob_store.clone(),
                audited.clone(),
                self::mat_namer(&id_gen),
                "default",
                MAT_BATCH,
                metrics(),
            );
            block_on(b.register_table(schema())).unwrap();
            register_other(&mut b);
            b.enable_distributed_mode(worker("b"), WORKER_TTL);
            b
        });

        if SMALLINT_PK.get() {
            db.set_pg_type(&ident(), "id", PgType::Int2);
        }
        if DEFAULT_IDENTITY.get() {
            db.set_replica_identity(&ident(), ReplicaIdentity::Default);
        }
        let stream = db.start_replication(SLOT).unwrap();

        Self {
            db,
            coord,
            blob_store,
            storage,
            namer,
            pipeline,
            materializer,
            audited,
            stream,
            #[cfg(feature = "integration")]
            wire: Wire::for_case(),
            live: BTreeSet::new(),
            noise_next_id: 0,
            next_bulk_id: 1000,
            id_gen,
            pending_external: None,
            #[cfg(feature = "integration")]
            _tokio: tokio,
            backfilling: false,
            backfill_pipeline: None,
            other_live: BTreeSet::new(),
            slot_start,
            clock,
            other,
        }
    }

    fn drive(&mut self) {
        while let Some(msg) = self.next_message() {
            self.process(msg);
        }
    }

    /// Process at most `n` messages — may stop mid-transaction.
    fn drive_partial(&mut self, n: usize) {
        for _ in 0..n {
            match self.next_message() {
                Some(msg) => self.process(msg),
                None => break,
            }
        }
    }

    fn next_message(&mut self) -> Option<DecodedMessage> {
        #[cfg(feature = "integration")]
        if let Some(wire) = self.wire.as_mut() {
            return wire.next(&mut self.stream);
        }
        self.stream.recv()
    }

    /// What the lifecycle does with a message: hand it to the pipeline,
    /// which stages schema changes in order with the rows.
    fn process(&mut self, msg: DecodedMessage) {
        block_on(self.pipeline.process(msg)).unwrap();
    }

    fn flush_and_ack(&mut self) {
        block_on(self.pipeline.flush()).unwrap();
        self.stream.send_standby(self.pipeline.flushed_lsn());
    }

    /// One materializer cycle; `None` if it failed because a commit's
    /// response was lost — the lifecycle just runs the next cycle.
    fn materialize(&mut self) -> Option<usize> {
        let before = self.current_snapshot();
        let n = cycle(&mut self.materializer);
        if self.current_snapshot() != before {
            if let Err(e) = file_index_matches_catalog(self, &self.materializer, true) {
                panic!("worker a: {e}");
            }
        }
        n
    }

    /// The table's current snapshot. A worker's FileIndex is checked once
    /// it has written the table: one that doesn't hold the table catches
    /// up only when it takes it over.
    fn current_snapshot(&self) -> Option<i64> {
        block_on(self.audited.load_table(&ident()))
            .unwrap()
            .and_then(|m| m.current_snapshot_id)
    }

    /// Worker "b"'s cycle, in distributed mode.
    /// `Some(0)` without one; `None` if its commit's response was lost.
    fn materialize_other(&mut self) -> Option<usize> {
        let Some(mut other) = self.other.take() else {
            return Some(0);
        };
        let before = self.current_snapshot();
        let n = cycle(&mut other);
        if self.current_snapshot() != before {
            if let Err(e) = file_index_matches_catalog(self, &other, true) {
                panic!("worker b: {e}");
            }
        }
        self.other = Some(other);
        n
    }

    fn backfill_chunk(&mut self) {
        if !self.backfilling {
            return;
        }
        let s = Snapshotter::new(self.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(1);
        let pipeline = self.backfill_pipeline.as_mut().expect("BACKFILL mode");
        block_on(s.run_chunks(&self.db, &[schema()], pipeline, Some(1))).unwrap();
        let done = block_on(self.coord.table_state(&ident()))
            .unwrap()
            .is_some_and(|t| t.snapshot_complete);
        if done {
            self.db.end_snapshot();
            self.backfilling = false;
            self.materializer.mark_snapshot_complete(&ident());
            if let Some(b) = self.other.as_mut() {
                b.mark_snapshot_complete(&ident());
            }
            self.audited
                .audit_paused
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Plan and write a compaction pass as a separate `compact` process
    /// would — its own FileIndex rebuilt from the catalog, its own file
    /// namer — and hold the commit for `ExternalCompactCommit`.
    fn external_compact_plan(&mut self) {
        // One job at a time.
        if self.pending_external.is_some()
            || self
                .audited
                .land_before_next_commit
                .lock()
                .unwrap()
                .is_some()
        {
            return;
        }
        let cfg = CompactionConfig {
            data_file_threshold: 1,
            delete_file_threshold: 1,
            target_size_bytes: 1024,
            max_input_bytes_per_pass: 1,
        };
        let pk_cols = [ColumnName("id".into())];
        let index = block_on(pg2iceberg_iceberg::rebuild_from_catalog(
            self.audited.as_ref(),
            self.blob_store.as_ref(),
            &ident(),
            &schema(),
            &pk_cols,
        ))
        .unwrap();
        let held = HeldCompaction {
            inner: self.audited.clone(),
            held: Mutex::new(None),
        };
        let namer = mat_namer(&self.id_gen);
        block_on(pg2iceberg_iceberg::compact_table(
            &held,
            self.blob_store.as_ref(),
            |t, _| {
                let namer = namer.clone();
                let t = t.clone();
                async move { namer.next_path(&t, "compact", "").await }
            },
            &ident(),
            &schema(),
            &pk_cols,
            Some(&index),
            &cfg,
        ))
        .unwrap();
        self.pending_external = held.held.into_inner().unwrap();
    }

    /// A partial compaction pass must not change what readers see. The
    /// audited catalog checks the commit against transaction boundaries;
    /// this checks the stronger before == after.
    fn compact(&mut self) {
        let cfg = CompactionConfig {
            data_file_threshold: 1,
            delete_file_threshold: 1,
            // Around the size of a few-row file, so the table mixes
            // small files with large ones only deletes make worth
            // rewriting.
            target_size_bytes: 1024,
            max_input_bytes_per_pass: 1,
        };
        let state = |h: &Self| {
            let mut rows = block_on(h.storage.engine_rows(&ident())).unwrap();
            sort_by_pk(&mut rows);
            rows
        };
        let before = state(self);
        // Compaction errors are non-fatal in the lifecycle; a pass whose
        // commit response was lost applied anyway, and one that lost to
        // another process's pass over the same files plans again next time.
        match block_on(self.materializer.compact_table(&ident(), &cfg)) {
            Ok(_) => {}
            Err(e) if e.to_string().contains("response lost") => {}
            Err(e)
                if e.to_string()
                    .contains("rewrite removes files no longer in the table") => {}
            Err(e) => panic!("compaction: {e}"),
        }
        // The materializer updates its FileIndex from what the pass
        // rewrote; it must match a rebuild from catalog history.
        if !DISTRIBUTED.get() {
            if let Err(e) = file_index_matches_catalog(self, &self.materializer, true) {
                panic!("after compaction: {e}");
            }
        }
        let after = state(self);
        if before != after {
            self.audited.violations.lock().unwrap().push(format!(
                "compaction changed the table: before {before:?}, after {after:?}"
            ));
        }
    }

    /// Run a Snapshotter-driven snapshot phase against the harness's source.
    /// Acks the slot to the snapshot LSN so live replication picks up
    /// strictly-later events.
    fn run_snapshot(&mut self) {
        let s = Snapshotter::new(self.coord.clone() as Arc<dyn Coordinator>);
        let snap_lsn = block_on(s.run(&self.db, &[schema()], &mut self.pipeline)).unwrap();
        self.stream.send_standby(snap_lsn);
    }

    /// Pipeline-process crash. Slot, coord, and blob store survive (durable
    /// storage); pipeline state and replication-stream cursor are lost.
    fn restart_materializer(&mut self) {
        let mut materializer = Materializer::with_metrics(
            self.coord.clone() as Arc<dyn Coordinator>,
            self.blob_store.clone(),
            self.audited.clone(),
            mat_namer(&self.id_gen),
            "default",
            MAT_BATCH,
            metrics(),
        );
        // As the lifecycle restarts: a table still backfilling is gated.
        if self.backfilling {
            block_on(materializer.register_table_pending(discovered_schema(&self.db))).unwrap();
        } else {
            block_on(materializer.register_table(discovered_schema(&self.db))).unwrap();
        }
        register_other(&mut materializer);
        if DISTRIBUTED.get() {
            materializer.enable_distributed_mode(worker("a"), WORKER_TTL);
        }
        self.materializer = materializer;
    }

    /// A cycle whose commits land but whose cursor updates don't: the
    /// process dies after committing, before recording how far it got.
    fn crash_after_commit(&mut self) {
        let mut tables = vec![ident()];
        if SECOND_TABLE.get() > 0 {
            tables.push(other_ident());
        }
        let cursors: Vec<_> = tables
            .iter()
            .flat_map(|t| ["default", "default#snapshot"].map(|g| (t.clone(), g)))
            .map(|(t, g)| {
                let cursor = block_on(self.coord.get_cursor(g, &t)).unwrap();
                (t, g, cursor)
            })
            .collect();
        self.materialize();
        for (t, g, cursor) in cursors {
            let now = block_on(self.coord.get_cursor(g, &t)).unwrap();
            if now != cursor {
                // No cursor reads as -1: from the log's start.
                block_on(self.coord.set_cursor(g, &t, cursor.unwrap_or(-1))).unwrap();
            }
        }
        self.restart_materializer();
    }

    /// Add `note` back — with `default`, if given — unless it's there.
    fn add_note(&mut self, default: Option<PgValue>) {
        if NOTE_PRESENT.get() {
            return;
        }
        // A column re-added in last place can't be told from one never
        // dropped unless pg2iceberg saw the drop, which pgoutput reports
        // only with the table's next change: make it first. (Without it —
        // no change between drop and re-add — the re-add is invisible.)
        write_after_schema_change(self);
        let col = schema()
            .columns
            .into_iter()
            .find(|c| c.name == "note")
            .unwrap();
        NOTE_DEFAULT_UNSTAGED.set(default.is_some());
        match default {
            Some(value) => self
                .db
                .alter_add_column_with_default(&ident(), col, value)
                .unwrap(),
            None => self.db.alter_add_column(&ident(), col).unwrap(),
        }
        NOTE_PRESENT.set(true);
    }

    fn crash_and_restart(&mut self) {
        // Drain + ack first so we model "graceful crash after a flush" — the
        // simpler case. Mid-flush crashes (orphan blobs from PUT-without-claim)
        // are a tracked follow-up.
        self.drive();
        self.flush_and_ack();
        self.crash_mid_stream();
    }

    /// Drop the pipeline + stream as-is and restart replication where
    /// the lifecycle does.
    fn crash_mid_stream(&mut self) {
        self.pipeline = new_pipeline(&self.coord, &self.blob_store, &self.namer, &self.db);
        self.restart_stream();
    }

    /// What the lifecycle does when the stream fails (`reopen_stream`).
    fn reconnect(&mut self) {
        self.pipeline.reset_session();
        self.restart_stream();
    }

    fn restart_stream(&mut self) {
        let start = block_on(replication_start_lsn(&*self.coord, None)).unwrap();
        self.stream = self.db.start_replication_at(SLOT, start).unwrap();
        #[cfg(feature = "integration")]
        {
            self.wire = Wire::for_case();
        }
    }

    fn run_step(&mut self, step: &Step) {
        match step {
            Step::Insert { id, qty } => {
                if !self.live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.insert(&ident(), row(*id, *qty));
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.live.insert(*id);
                    }
                }
            }
            Step::ToastUpdate { id, qty } => {
                if self.live.contains(id) && NOTE_PRESENT.get() {
                    let mut tx = self.db.begin_tx();
                    tx.update_with_unchanged(&ident(), row(*id, *qty), vec![note()]);
                    let _ = tx.commit(Timestamp(0));
                }
            }
            Step::ChangePk { from, to, toast } => {
                if self.live.contains(from) && !self.live.contains(to) {
                    let id = ColumnName("id".into());
                    let before = self
                        .db
                        .read_table(&ident())
                        .unwrap()
                        .into_iter()
                        .find(|r| stored(r.clone()).get(&id) == Some(&PgValue::Int4(*from)))
                        .expect("live row");
                    let mut after = before.clone();
                    after.insert(id, id_value(*to));
                    let unchanged = if *toast && NOTE_PRESENT.get() {
                        vec![note()]
                    } else {
                        Vec::new()
                    };
                    let mut tx = self.db.begin_tx();
                    tx.update_with_pk_change_unchanged(&ident(), before, after, unchanged);
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.live.remove(from);
                        self.live.insert(*to);
                    }
                }
            }
            Step::Truncate { reinsert } => {
                let mut tx = self.db.begin_tx();
                tx.truncate(&ident());
                if let Some((id, qty)) = reinsert {
                    tx.insert(&ident(), row(*id, *qty));
                }
                if tx.commit(Timestamp(0)).is_ok() {
                    self.live.clear();
                    self.live.extend(reinsert.map(|(id, _)| id));
                }
            }
            Step::Update { id, qty } => {
                if self.live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.update(&ident(), row(*id, *qty));
                    let _ = tx.commit(Timestamp(0));
                }
            }
            Step::Delete { id } => {
                if self.live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.delete(&ident(), pk_only(*id));
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.live.remove(id);
                    }
                }
            }
            Step::RollbackInsert { id, qty } => {
                let mut tx = self.db.begin_tx();
                tx.insert(&ident(), row(*id, *qty));
                tx.rollback();
            }
            Step::UnpublishedWrite { qty } => {
                self.noise_next_id += 1;
                let mut tx = self.db.begin_tx();
                tx.insert(&noise_ident(), row(self.noise_next_id, *qty));
                tx.commit(Timestamp(0)).unwrap();
            }
            Step::DriveFlush => {
                self.drive();
                self.flush_and_ack();
            }
            Step::MaterializerCycle => {
                let _ = self.materialize();
            }
            Step::CrashAndRestart => self.crash_and_restart(),
            Step::BigTx { inserts, qty } => {
                let mut tx = self.db.begin_tx();
                for id in &self.live {
                    tx.update(&ident(), row(*id, *qty));
                }
                let mut fresh = Vec::with_capacity(*inserts);
                for _ in 0..*inserts {
                    self.next_bulk_id += 1;
                    tx.insert(&ident(), row(self.next_bulk_id, *qty));
                    fresh.push(self.next_bulk_id);
                }
                // Touch every row again, now in a later chunk — including
                // the ones this transaction just inserted: the materializer
                // must hide their first version within the same atomic
                // commit.
                for id in self.live.iter().chain(&fresh) {
                    tx.update(&ident(), row(*id, *qty + 1));
                }
                tx.commit(Timestamp(0)).unwrap();
                self.live.extend(fresh);
            }
            Step::DrivePartial { n } => self.drive_partial(*n),
            Step::FlushTick => self.flush_and_ack(),
            Step::DriveFlushWithoutAck => {
                self.drive();
                block_on(self.pipeline.flush()).unwrap();
                self.crash_mid_stream();
            }
            Step::CrashMidStream => self.crash_mid_stream(),
            Step::Reconnect => self.reconnect(),
            Step::RestartMaterializer => self.restart_materializer(),
            Step::Compact => self.compact(),
            Step::BackfillChunk => self.backfill_chunk(),
            Step::OtherInsert { id, qty } => {
                if SECOND_TABLE.get() > 0 && !self.other_live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.insert(&other_pg_ident(), other_row(*id, *qty));
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.other_live.insert(*id);
                    }
                }
            }
            Step::OtherUpdate { id, qty } => {
                if self.other_live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    tx.update(&other_pg_ident(), other_row(*id, *qty));
                    let _ = tx.commit(Timestamp(0));
                }
            }
            Step::OtherDelete { id } => {
                if self.other_live.contains(id) {
                    let mut tx = self.db.begin_tx();
                    let pk = BTreeMap::from([(ColumnName("id".into()), PgValue::Int4(*id))]);
                    tx.delete(&other_pg_ident(), pk);
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.other_live.remove(id);
                    }
                }
            }
            Step::TruncateBoth => {
                if SECOND_TABLE.get() > 0 {
                    let mut tx = self.db.begin_tx();
                    tx.truncate_all(&[ident(), other_pg_ident()]);
                    if tx.commit(Timestamp(0)).is_ok() {
                        self.live.clear();
                        self.other_live.clear();
                    }
                }
            }
            Step::Invalidate => {
                self.db.invalidate_relation(&ident()).unwrap();
                if SECOND_TABLE.get() > 0 {
                    self.db.invalidate_relation(&other_pg_ident()).unwrap();
                }
            }
            Step::OtherWorkerCycle => {
                let _ = self.materialize_other();
            }
            Step::ClockTick => self
                .clock
                .advance(WORKER_TTL + std::time::Duration::from_secs(1)),
            Step::DropNote => {
                if NOTE_PRESENT.get() {
                    // pg2iceberg reads a column's default when it stages the
                    // column's add, and the drop clears it: dropped within
                    // pg2iceberg's lag, the column leaves older rows without
                    // it — a known gap. Model the drop coming later.
                    if NOTE_DEFAULT_UNSTAGED.replace(false) {
                        self.drive();
                        self.flush_and_ack();
                    }
                    self.db.alter_drop_column(&ident(), "note").unwrap();
                    NOTE_PRESENT.set(false);
                }
            }
            Step::AddNote => self.add_note(None),
            Step::AddNoteWithDefault => self.add_note(Some(PgValue::Text(DEFAULT_NOTE.into()))),
            Step::ExternalCompactPlan => self.external_compact_plan(),
            Step::ExternalCompactCommit => {
                if let Some(pass) = self.pending_external.take() {
                    // The job's own process, as `pg2iceberg compact` is.
                    let job = CachingCatalog::new(
                        self.audited.clone(),
                        self.coord.clone() as Arc<dyn Coordinator>,
                    );
                    match block_on(job.commit_compaction(pass)) {
                        Ok(_) => {}
                        Err(e) if e.to_string().contains("response lost") => {}
                        // Another pass rewrote its files first; the job
                        // plans again on its next run.
                        Err(pg2iceberg_iceberg::IcebergError::Conflict(_)) => {}
                        Err(e) => panic!("external compaction commit: {e}"),
                    }
                }
            }
            Step::ExternalCompactMidCommit => {
                if let Some(pass) = self.pending_external.take() {
                    *self.audited.land_before_next_commit.lock().unwrap() = Some(pass);
                }
            }
            Step::LoseCommitResponse => self
                .audited
                .lose_next_response
                .store(true, std::sync::atomic::Ordering::SeqCst),
            Step::CrashAfterCommit => self.crash_after_commit(),
            Step::Expire => {
                block_on(self.materializer.expire_cycle(0)).unwrap();
            }
            Step::CleanupOrphans => {
                block_on(self.materializer.cleanup_orphans_cycle(i64::MAX, 0)).unwrap();
            }
        }
    }
}

// ---------- invariant checks ----------

/// Invariants that must hold after *every* step, not just at quiescence.
fn check_step_invariants(h: &DstHarness) -> Result<(), String> {
    // 7. Bounded memory: no transaction, however large, makes the
    //    pipeline hold more than MAX_BUFFERED_ROWS change events.
    let buffered = h.pipeline.buffered_rows();
    if buffered > MAX_BUFFERED_ROWS {
        return Err(format!(
            "invariant 7 (bounded memory): pipeline buffers {buffered} rows > {MAX_BUFFERED_ROWS}"
        ));
    }

    let entries = block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .map_err(|e| format!("read_log failed: {e}"))?;

    // 8. Bounded staged objects, so the materializer never has to load
    //    one huge file either.
    if let Some(e) = entries
        .iter()
        .find(|e| e.record_count as usize > MAX_BUFFERED_ROWS)
    {
        return Err(format!(
            "invariant 8 (bounded staged object): {} holds {} rows > {MAX_BUFFERED_ROWS}",
            e.s3_path, e.record_count
        ));
    }

    // 9. No torn transactions: each transaction is either entirely in the
    //    claimed log or absent. Staged-but-unclaimed objects don't count —
    //    only claims are visible to the materializer.
    let mut staged = BTreeSet::new();
    for entry in &entries {
        let bytes = block_on(h.blob_store.get(&entry.s3_path))
            .map_err(|e| format!("blob_store.get({}): {e}", entry.s3_path))?;
        let chunk =
            decode_chunk(&bytes).map_err(|e| format!("decode_chunk({}): {e}", entry.s3_path))?;
        staged.extend(chunk.into_iter().map(|m| m.lsn));
    }
    let mut by_xid: BTreeMap<u32, Vec<_>> = BTreeMap::new();
    for c in
        h.db.dump_change_events(PUB)
            .map_err(|e| format!("dump_change_events: {e}"))?
            .into_iter()
            .filter(|c| c.table == ident())
    {
        by_xid.entry(c.xid.unwrap_or(0)).or_default().push(c.lsn);
    }
    for (xid, lsns) in &by_xid {
        let claimed = lsns.iter().filter(|l| staged.contains(*l)).count();
        if claimed != 0 && claimed != lsns.len() {
            return Err(format!(
                "invariant 9 (torn transaction): xid {xid} has {claimed} of {} events claimed",
                lsns.len()
            ));
        }
    }

    // 10. Atomic visibility per table — now, and at every commit made
    //     (not while a backfill has the table half loaded)
    //     since the last check (a cycle may commit several times).
    if !h.backfilling {
        block_on(atomic_visibility(&h.storage, &h.db))?;
    }
    if let Some(v) = h.audited.violations.lock().unwrap().first() {
        return Err(v.clone());
    }

    // 11. The materializer's FileIndex is what the catalog holds. (A
    //     distributed worker's is checked after each cycle it writes the
    //     table in: one that doesn't hold the table catches up on taking
    //     it over.)
    if !DISTRIBUTED.get() {
        file_index_matches_catalog(h, &h.materializer, false)?;
    }

    // 12. The catalog's history replays to the table readers see —
    //     before and after snapshot expiry. The FileIndex rebuild,
    //     compaction, orphan cleanup and `verify` all read it.
    //     Both sides are read the same way (by field id, deletes scoped),
    //     so this checks the history alone.
    let readers = block_on(h.storage.engine_rows(&ident()))
        .map_err(|e| format!("invariant 12: readers' view: {e}"))?;
    let history = block_on(async {
        let snapshots = h
            .audited
            .snapshots(&ident())
            .await
            .map_err(|e| e.to_string())?;
        let schema = h
            .audited
            .load_table(&ident())
            .await
            .map_err(|e| e.to_string())?
            .expect("table exists")
            .schema;
        let mut rows = engine_read(
            h.blob_store.as_ref(),
            &schema,
            &live_files_from_history(&snapshots),
        )
        .await?;
        sort_by_pk(&mut rows);
        Ok::<_, String>(rows)
    })
    .map_err(|e| format!("invariant 12: history: {e}"))?;
    if history != readers {
        return Err(format!(
            "invariant 12: Catalog::snapshots replays to {history:?}, readers see {readers:?}"
        ));
    }
    Ok(())
}

/// Invariant 11: the materializer's FileIndex equals a rebuild from the
/// catalog, and each file's live count equals the PKs pointing at it —
/// compaction picks dirty files by those counts, and a file's count
/// running high would make a dirty file look clean.
///
/// The index may lag behind a `compact` job's commit, which moves rows
/// between files but keeps their keys and partitions: it catches up
/// before it's next used. Not once `wrote` — the materializer just
/// committed to the table, and it takes in every commit before its own.
fn file_index_matches_catalog(
    h: &DstHarness,
    m: &Materializer<AuditedCatalog>,
    wrote: bool,
) -> Result<(), String> {
    let Some(index) = m.file_index(&ident()) else {
        return Ok(());
    };
    let at = m.file_index_snapshot(&ident());
    let current = h.current_snapshot();
    if wrote && at != current {
        return Err(format!(
            "invariant 11: after a commit, the FileIndex reflects snapshot {at:?}, the table is at {current:?}"
        ));
    }
    let mut pks_per_file: BTreeMap<&str, u64> = BTreeMap::new();
    for pk in index.all_pks() {
        let path = index.lookup(pk).expect("an indexed PK has a file");
        *pks_per_file.entry(path).or_default() += 1;
    }
    if index.live_rows_per_file() != pks_per_file {
        return Err(format!(
            "invariant 11: FileIndex live counts {:?} != PKs per file {pks_per_file:?}",
            index.live_rows_per_file()
        ));
    }
    let rebuilt = block_on(pg2iceberg_iceberg::rebuild_from_catalog(
        h.audited.as_ref(),
        h.blob_store.as_ref(),
        &ident(),
        &schema(),
        &[ColumnName("id".into())],
    ))
    .map_err(|e| format!("rebuild_from_catalog: {e}"))?;
    let drifted = if at != current {
        let keys = |fi: &pg2iceberg_iceberg::FileIndex| -> BTreeMap<String, String> {
            fi.all_pks()
                .map(|pk| {
                    (
                        pk.to_string(),
                        format!("{:?}", fi.partition_values_for_pk(pk)),
                    )
                })
                .collect()
        };
        keys(index) != keys(&rebuilt)
    } else {
        *index != rebuilt
    };
    if drifted {
        return Err(format!(
            "invariant 11: FileIndex drifted from the catalog:\n  materializer={index:?}\n  rebuilt={rebuilt:?}"
        ));
    }
    Ok(())
}

/// Invariant 10: Iceberg matches PG as of some transaction boundary.
/// Lagging behind is fine; a partly applied transaction is not.
async fn atomic_visibility(storage: &Storage, db: &SimPostgres) -> Result<(), String> {
    let mut iceberg = storage.engine_rows(&ident()).await?;
    sort_by_pk(&mut iceberg);
    let events: Vec<_> = db
        .dump_change_events(PUB)
        .map_err(|e| format!("dump_change_events: {e}"))?
        .into_iter()
        .filter(|c| c.table == ident())
        .collect();
    let pk = |r: &Row| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    };
    let mut state: BTreeMap<i32, Row> = BTreeMap::new();
    // Each boundary's rows, with the source's columns then.
    let mut boundaries: Vec<(Vec<Row>, Vec<String>)> = vec![(Vec::new(), Vec::new())];
    let mut i = 0;
    let mut last_lsn = None;
    while i < events.len() {
        let xid = events[i].xid;
        // A column dropped since the last transaction takes its values
        // with it: one re-added later starts out NULL. Iceberg can show
        // that state too, once it has applied the drop.
        let columns = db.columns_at(&ident(), events[i].lsn);
        let kept = match last_lsn {
            Some(from) => db.columns_kept(&ident(), from, events[i].lsn),
            None => columns.clone(),
        };
        let dropped = state
            .values()
            .any(|r| r.keys().any(|c| !kept.contains(&c.0)));
        if dropped {
            for row in state.values_mut() {
                row.retain(|c, _| kept.contains(&c.0));
            }
            boundaries.push((state.values().cloned().collect(), columns.clone()));
        }
        // A column added since has, in the rows already there, the value
        // Postgres gave them (its default), which no WAL carries.
        let added: Vec<ColumnName> = columns
            .iter()
            .filter(|c| !kept.contains(c))
            .map(|c| ColumnName(c.clone()))
            .collect();
        if !added.is_empty() && !state.is_empty() {
            let before: BTreeMap<i32, Row> = db
                .rows_at(&ident(), pg2iceberg_core::Lsn(events[i].lsn.0 - 1))
                .into_iter()
                .map(|r| {
                    let r = stored(r);
                    (pk(&r), r)
                })
                .collect();
            for (k, row) in state.iter_mut() {
                for c in &added {
                    if let Some(v) = before.get(k).and_then(|r| r.get(c)) {
                        row.insert(c.clone(), v.clone());
                    }
                }
            }
            boundaries.push((state.values().cloned().collect(), columns.clone()));
        }
        while i < events.len() && events[i].xid == xid {
            let e = &events[i];
            match e.op {
                Op::Insert | Op::Update => {
                    let mut after = stored(e.after.clone().expect("insert/update carries after"));
                    // A key-changing UPDATE moves the row.
                    let old_pk = e.before.as_ref().map(|b| pk(&stored(b.clone())));
                    let prev = match old_pk {
                        Some(k) if k != pk(&after) => state.remove(&k),
                        _ => state.get(&pk(&after)).cloned(),
                    };
                    // Unchanged (TOASTed) columns keep their stored values.
                    for col in &e.unchanged_cols {
                        if let Some(v) = prev.as_ref().and_then(|p| p.get(col)) {
                            after.insert(col.clone(), v.clone());
                        }
                    }
                    state.insert(pk(&after), after);
                }
                Op::Delete => {
                    let before = stored(e.before.clone().expect("delete carries before"));
                    state.remove(&pk(&before));
                }
                _ => state.clear(),
            }
            last_lsn = Some(e.lsn);
            i += 1;
        }
        boundaries.push((state.values().cloned().collect(), columns));
    }
    // Compare on the source's columns at each boundary: Iceberg keeps
    // dropped columns, and learns of a schema change only with the
    // table's next change (a column dropped then re-added still reads
    // as the dropped one until then).
    let matches = boundaries
        .into_iter()
        .any(|(rows, cols)| on_columns(&cols, iceberg.clone()) == on_columns(&cols, rows));
    if !matches {
        return Err(format!(
            "invariant 10 (atomic visibility): Iceberg state matches no transaction boundary: {iceberg:?}"
        ));
    }
    Ok(())
}

/// A catalog that holds a compaction commit back instead of applying it,
/// so another process's pass can commit after the table moved on.
struct HeldCompaction {
    inner: Arc<AuditedCatalog>,
    held: Mutex<Option<PreparedCompaction>>,
}

#[async_trait::async_trait]
impl Catalog for HeldCompaction {
    async fn ensure_namespace(&self, ns: &Namespace) -> pg2iceberg_iceberg::Result<()> {
        self.inner.ensure_namespace(ns).await
    }
    async fn load_table(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_iceberg::Result<Option<TableMetadata>> {
        self.inner.load_table(ident).await
    }
    async fn create_table(
        &self,
        schema: &TableSchema,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner.create_table(schema).await
    }
    async fn commit_snapshot(
        &self,
        prepared: PreparedCommit,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner.commit_snapshot(prepared).await
    }
    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
        log_range: Option<LogRange>,
        remove_properties: BTreeSet<String>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner
            .commit_snapshots(steps, log_range, remove_properties)
            .await
    }
    async fn commit_compaction(
        &self,
        prepared: PreparedCompaction,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        let ident = prepared.ident.clone();
        *self.held.lock().unwrap() = Some(prepared);
        Ok(self.inner.load_table(&ident).await?.expect("table exists"))
    }
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: BTreeMap<String, String>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner
            .evolve_schema(ident, changes, set_properties)
            .await
    }
    async fn snapshots(&self, ident: &TableIdent) -> pg2iceberg_iceberg::Result<Vec<Snapshot>> {
        self.inner.snapshots(ident).await
    }
}

/// The materializer's catalog in the DST: delegates to the in-memory
/// catalog and checks invariant 10 after every commit — the moments a
/// reader could observe the table — since one materializer cycle can
/// commit several times between two DST steps.
struct AuditedCatalog {
    inner: Arc<dyn Catalog>,
    storage: Storage,
    db: SimPostgres,
    violations: Mutex<Vec<String>>,
    /// When set, the next multi-step commit fails without committing.
    fail_next_commit: std::sync::atomic::AtomicBool,
    /// When set, the commit after the next schema change fails without
    /// committing — a crash between the two.
    fail_commit_after_schema_change: std::sync::atomic::AtomicBool,
    /// When set, commits aren't audited — for tests that count blob reads
    /// (an audit reads the whole table).
    audit_paused: std::sync::atomic::AtomicBool,
    /// When set, the next commit — data or compaction — applies and then
    /// reports failure, as a REST catalog does when the response is lost
    /// (a timeout, a 502/504): "commit state unknown".
    lose_next_response: std::sync::atomic::AtomicBool,
    /// Another process's compaction pass, committed first when the next
    /// commit — data or compaction — is.
    land_before_next_commit: Mutex<Option<PreparedCompaction>>,
    /// Where that process records its writes, as every pg2iceberg
    /// process does (see `CachingCatalog`).
    coord: Arc<dyn Coordinator>,
    /// Per table and cursor group, how far commits that landed applied
    /// the log (invariant 16).
    applied: Mutex<BTreeMap<(TableIdent, String), u64>>,
}

impl AuditedCatalog {
    /// `result` of a commit that applied; turned into an error if this
    /// commit's response is to be lost.
    fn respond<T>(&self, result: T) -> pg2iceberg_iceberg::Result<T> {
        if self
            .lose_next_response
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(pg2iceberg_iceberg::IcebergError::Other(
                "commit applied, response lost".into(),
            ));
        }
        Ok(result)
    }
}

impl AuditedCatalog {
    /// Commit [`Self::land_before_next_commit`]'s pass, if one is held.
    async fn land_other_pass(&self) -> pg2iceberg_iceberg::Result<()> {
        let pass = self.land_before_next_commit.lock().unwrap().take();
        if let Some(pass) = pass {
            let table = pass.ident.clone();
            let result = self.inner.commit_compaction(pass).await;
            self.coord.bump_table_epoch(&table).await.unwrap();
            match result {
                // The job's pass lost to a newer rewrite of its files: the
                // job fails, not this commit.
                Ok(_) | Err(pg2iceberg_iceberg::IcebergError::Conflict(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn audit(&self) {
        if self.audit_paused.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        if let Err(e) = atomic_visibility(&self.storage, &self.db).await {
            self.violations
                .lock()
                .unwrap()
                .push(format!("after a commit: {e}"));
        }
    }
}

#[async_trait::async_trait]
impl Catalog for AuditedCatalog {
    async fn ensure_namespace(&self, ns: &Namespace) -> pg2iceberg_iceberg::Result<()> {
        self.inner.ensure_namespace(ns).await
    }
    async fn load_table(
        &self,
        ident: &TableIdent,
    ) -> pg2iceberg_iceberg::Result<Option<TableMetadata>> {
        self.inner.load_table(ident).await
    }
    async fn create_table(
        &self,
        schema: &TableSchema,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.inner.create_table(schema).await
    }
    async fn commit_snapshot(
        &self,
        prepared: PreparedCommit,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        let meta = self.inner.commit_snapshot(prepared).await?;
        self.audit().await;
        let meta = self.respond(meta)?;
        Ok(meta)
    }
    async fn commit_snapshots(
        &self,
        steps: Vec<PreparedCommit>,
        log_range: Option<LogRange>,
        remove_properties: BTreeSet<String>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(pg2iceberg_iceberg::IcebergError::Other(
                "injected: commit_snapshots".into(),
            ));
        }
        self.land_other_pass().await?;
        let ident = steps.first().map(|s| s.ident.clone());
        let meta = self
            .inner
            .commit_snapshots(steps, log_range.clone(), remove_properties)
            .await?;
        // 16. Each log entry is applied at most once: a commit's range
        //     starts where the last one that landed ended, or later.
        if let (Some(ident), Some(range)) = (ident, log_range) {
            let mut applied = self.applied.lock().unwrap();
            let end = applied
                .entry((ident.clone(), range.group.clone()))
                .or_default();
            if range.start < *end {
                self.violations.lock().unwrap().push(format!(
                    "invariant 16 (log applied at most once): {ident} ({}) commits log [{}, {}), \
                     but commits already applied it up to {end}",
                    range.group, range.start, range.end
                ));
            }
            *end = (*end).max(range.end);
        }
        self.audit().await;
        let meta = self.respond(meta)?;
        Ok(meta)
    }
    async fn commit_compaction(
        &self,
        prepared: PreparedCompaction,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        self.land_other_pass().await?;
        let meta = self.inner.commit_compaction(prepared).await?;
        self.audit().await;
        let meta = self.respond(meta)?;
        Ok(meta)
    }
    async fn evolve_schema(
        &self,
        ident: &TableIdent,
        changes: Vec<SchemaChange>,
        set_properties: BTreeMap<String, String>,
    ) -> pg2iceberg_iceberg::Result<TableMetadata> {
        let meta = self
            .inner
            .evolve_schema(ident, changes, set_properties)
            .await?;
        if self
            .fail_commit_after_schema_change
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.fail_next_commit
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(meta)
    }
    async fn expire_snapshots(
        &self,
        ident: &TableIdent,
        retention_ms: i64,
    ) -> pg2iceberg_iceberg::Result<usize> {
        self.inner.expire_snapshots(ident, retention_ms).await
    }
    async fn snapshots(&self, ident: &TableIdent) -> pg2iceberg_iceberg::Result<Vec<Snapshot>> {
        self.inner.snapshots(ident).await
    }
}

/// pgoutput tells pg2iceberg about a schema change only with the table's
/// next change; until a write comes, Iceberg can't reflect it — a column
/// dropped and re-added still shows its old values. That write comes
/// eventually: make it now, writing a row back unchanged, so the end
/// state compares what pg2iceberg can know.
fn write_after_schema_change(h: &DstHarness) {
    if !h.db.relation_changed_since_last_change(&ident()) {
        return;
    }
    if let Some(row) = h.db.read_table(&ident()).unwrap().into_iter().next() {
        let mut tx = h.db.begin_tx();
        tx.update(&ident(), row);
        tx.commit(Timestamp(0)).unwrap();
    }
}

fn check_invariants(h: &mut DstHarness) -> Result<(), String> {
    while h.backfilling {
        h.backfill_chunk();
    }
    write_after_schema_change(h);
    // Reach quiescence: drain WAL, flush, ack, then materialize until idle.
    // Loop because a flush may produce events the materializer hasn't seen.
    h.drive();
    h.flush_and_ack();
    // Drain materializer; safety bound to catch infinite loops.
    // A cycle whose commit response was lost (`None`) did work: retry.
    for _ in 0..1000 {
        let a = h.materialize();
        let b = h.materialize_other();
        if a == Some(0) && b == Some(0) {
            break;
        }
    }

    // 10, 16. Every commit along the way.
    if let Some(v) = h.audited.violations.lock().unwrap().first() {
        return Err(v.clone());
    }

    // 6. No WAL retention at quiescence: the slot is acked up to the end
    //    of WAL, including WAL that only touched unpublished tables.
    //    Otherwise PG can't recycle it until the next published change.
    let slot =
        h.db.slot_state(SLOT)
            .map_err(|e| format!("slot_state failed: {e}"))?;
    let wal_end = h.db.current_lsn();
    if slot.confirmed_flush_lsn != wal_end {
        return Err(format!(
            "invariant 6 (WAL retention): slot confirmed_flush_lsn {:?} != WAL end {wal_end:?} at quiescence",
            slot.confirmed_flush_lsn
        ));
    }

    let entries = block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .map_err(|e| format!("read_log failed: {e}"))?;

    // 1. Every log_index s3_path resolves in blob_store.
    let blob_keys = block_on(h.storage.blob_keys())?;
    for entry in &entries {
        if !blob_keys.contains(object_key(&entry.s3_path)) {
            return Err(format!(
                "invariant 1 (blob completeness) violated: log_index references {} but blob_store doesn't have it",
                entry.s3_path
            ));
        }
    }

    // 2. Contiguous offsets per table.
    let mut prev_end = 0u64;
    for entry in &entries {
        if entry.start_offset != prev_end {
            return Err(format!(
                "invariant 2 (contiguous offsets) violated: prev_end={} but next start={}",
                prev_end, entry.start_offset
            ));
        }
        if entry.end_offset != entry.start_offset + entry.record_count {
            return Err(format!(
                "invariant 2 (offset arithmetic): start={}, end={}, record_count={}",
                entry.start_offset, entry.end_offset, entry.record_count
            ));
        }
        prev_end = entry.end_offset;
    }

    // 3. pipeline.flushed_lsn <= slot.confirmed_flush_lsn (after ack).
    let slot =
        h.db.slot_state(SLOT)
            .map_err(|e| format!("slot_state: {e}"))?;
    if h.pipeline.flushed_lsn() > slot.confirmed_flush_lsn {
        return Err(format!(
            "invariant 3 (LSN ordering): pipeline.flushed_lsn={} > slot.confirmed_flush_lsn={}",
            h.pipeline.flushed_lsn(),
            slot.confirmed_flush_lsn
        ));
    }

    // 4. Sorted staged events == sorted committed WAL events.
    //    (Read only blobs that are in coord — orphans from a hypothetical
    //    mid-flush crash would be ignored. We don't currently produce orphans
    //    in this harness, but this keeps the check robust to future expansion.)
    let mut staged_events = Vec::new();
    for entry in &entries {
        let bytes = block_on(h.blob_store.get(&entry.s3_path))
            .map_err(|e| format!("blob_store.get({}): {e}", entry.s3_path))?;
        let mut chunk =
            decode_chunk(&bytes).map_err(|e| format!("decode_chunk({}): {e}", entry.s3_path))?;
        staged_events.append(&mut chunk);
    }
    // Staging is at-least-once: a crash between a claim and the slot ack
    // replays the transaction, staging it again, and the fold absorbs the
    // repeat (invariant 5). So per transaction, staging holds its events,
    // possibly repeated.
    // 14. Staging keeps commit order. At-least-once staging may repeat
    //     the last transaction staged (a reconnect resends the one
    //     committing at the acked LSN), but never stages an older one
    //     after a newer one: the materializer applies the log in order,
    //     so that would publish the older transaction's rows over the
    //     newer ones. Snapshot chunks interleave with CDC by design.
    let mut last: Option<(u32, pg2iceberg_core::Lsn)> = None;
    for e in &staged_events {
        let Some(xid) = e
            .xid
            .filter(|x| *x < pg2iceberg_snapshot::SNAPSHOT_XID_BASE)
        else {
            continue;
        };
        if last.is_some_and(|(x, _)| x == xid) {
            continue;
        }
        let commit = h.db.commit_lsn(xid);
        if let Some((prev, prev_commit)) = last {
            if commit <= prev_commit {
                return Err(format!(
                    "invariant 14 (commit order): xid {xid} (commit {commit:?}) staged after \
                     xid {prev} (commit {prev_commit:?})"
                ));
            }
        }
        last = Some((xid, commit));
    }
    let mut staged: BTreeMap<u32, Vec<pg2iceberg_stream::MatEvent>> = BTreeMap::new();
    // Staged schema changes aren't WAL changes.
    for e in staged_events.into_iter().filter(|e| e.op != Op::Relation) {
        staged.entry(e.xid.unwrap_or(0)).or_default().push(e);
    }
    // What staging should hold for each WAL event: its row as sent, except
    // that the pipeline splits a key-changing UPDATE into a Delete of the
    // old key and an Update of the new one (taking unchanged TOAST values
    // from the old tuple when it has them), and a TRUNCATE carries no row.
    // On the wire every change carries its transaction's commit LSN.
    let id = ColumnName("id".into());
    let mut expected: BTreeMap<u32, Vec<(pg2iceberg_core::Lsn, Op, Row)>> = BTreeMap::new();
    for c in
        h.db.dump_change_events_as_sent(PUB)
            .map_err(|e| format!("dump_change_events: {e}"))?
            .into_iter()
            .filter(|c| c.table == ident())
    {
        let xid = c.xid.unwrap_or(0);
        if h.db.commit_lsn(xid) <= h.slot_start {
            // Before the slot: only the snapshot carries it.
            continue;
        }
        let lsn = if WIRE.get() {
            h.db.commit_lsn(xid)
        } else {
            c.lsn
        };
        let row = |r: &Option<Row>| {
            r.clone().ok_or_else(|| {
                format!("invariant 4: WAL event at lsn={} has no payload row", c.lsn)
            })
        };
        let tx = expected.entry(xid).or_default();
        match c.op {
            Op::Update
                if c.before.is_some()
                    && c.before.as_ref().map(|b| b.get(&id))
                        != c.after.as_ref().map(|a| a.get(&id)) =>
            {
                let before = row(&c.before)?;
                let mut after = row(&c.after)?;
                for col in &c.unchanged_cols {
                    match before.get(col) {
                        Some(v) if *v != PgValue::Null => {
                            after.insert(col.clone(), v.clone());
                        }
                        _ => {}
                    }
                }
                tx.push((lsn, Op::Delete, before));
                tx.push((lsn, Op::Update, after));
            }
            Op::Insert | Op::Update => tx.push((lsn, c.op, row(&c.after)?)),
            Op::Delete => tx.push((lsn, Op::Delete, row(&c.before)?)),
            Op::Truncate => tx.push((lsn, Op::Truncate, Row::new())),
            _ => return Err(format!("invariant 4: unexpected op in WAL: {:?}", c.op)),
        }
    }
    // The snapshot stages its chunks as synthetic transactions.
    staged.retain(|xid, _| expected.contains_key(xid) || !BACKFILL.get());
    if staged.keys().ne(expected.keys()) {
        return Err(format!(
            "invariant 4 (WAL == staged): staged transactions {:?}, WAL transactions {:?}",
            staged.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        ));
    }
    for (xid, want) in &expected {
        let got = &staged[xid];
        if got.len() % want.len() != 0 {
            return Err(format!(
                "invariant 4: xid {xid} staged {} events, not a multiple of its {}",
                got.len(),
                want.len()
            ));
        }
        for (m, (lsn, op, row)) in got.iter().zip(want.iter().cycle()) {
            if m.lsn != *lsn || m.op != *op || m.row != *row {
                return Err(format!(
                    "invariant 4: xid {xid} staged ({}, {:?}, {:?}), WAL ({lsn}, {op:?}, {row:?})",
                    m.lsn, m.op, m.row
                ));
            }
        }
    }

    // 5. Iceberg materialized state == PG ground truth.
    let mut iceberg_rows = block_on(h.storage.engine_rows(&ident()))
        .map_err(|e| format!("read_materialized_state: {e}"))?;
    sort_by_pk(&mut iceberg_rows);

    let mut pg_rows =
        h.db.read_table(&ident())
            .map(stored_rows)
            .map_err(|e| format!("read_table: {e}"))?;
    sort_by_pk(&mut pg_rows);

    let iceberg_rows = on_source_columns(&h.db, iceberg_rows);
    let pg_rows = on_source_columns(&h.db, pg_rows);
    if iceberg_rows != pg_rows {
        return Err(format!(
            "invariant 5 (PG == Iceberg) violated:\n  pg={pg_rows:?}\n  iceberg={iceberg_rows:?}"
        ));
    }

    // 5b. The second table: PG == Iceberg, apart from the first.
    if SECOND_TABLE.get() > 0 {
        let iceberg = block_on(h.storage.engine_rows(&other_ident()))
            .map_err(|e| format!("read {}: {e}", other_ident()))?;
        let mut pg =
            h.db.read_table(&other_pg_ident())
                .map_err(|e| e.to_string())?;
        sort_by_pk(&mut pg);
        let cols = [ColumnName("id".into()), note(), ColumnName("qty".into())];
        let keep = |rows: Vec<Row>| -> Vec<Row> {
            rows.into_iter()
                .map(|r| {
                    cols.iter()
                        .map(|c| (c.clone(), r.get(c).cloned().unwrap_or(PgValue::Null)))
                        .collect()
                })
                .collect()
        };
        let (iceberg, pg) = (keep(iceberg), keep(pg));
        if iceberg != pg {
            return Err(format!(
                "invariant 5 ({} → {}): pg={pg:?}\n  iceberg={iceberg:?}",
                other_pg_ident(),
                other_ident()
            ));
        }
    }

    // 13. `pg2iceberg verify`, run as the binary runs it — with the schema
    //     discovered from the source — finds the tables equal: they are
    //     (5). A difference it reports is a false alarm.
    let diff = block_on(pg2iceberg_validate::verify::verify_table(
        &h.db,
        h.audited.as_ref(),
        h.blob_store.as_ref(),
        &discovered_schema(&h.db),
        3,
    ))
    .map_err(|e| format!("invariant 13: verify: {e}"))?;
    if !diff.is_empty() {
        return Err(format!(
            "invariant 13 (verify finds PG == Iceberg): it reports {diff:?}"
        ));
    }

    // 15. The snapshot summary's totals count the table's files: engines
    //     plan with them, and some answer `count()` from them.
    block_on(h.storage.summary_counts_live_files(&ident()))
        .map_err(|e| format!("invariant 15: {e}"))?;

    Ok(())
}

fn sort_by_pk(rows: &mut [Row]) {
    rows.sort_by_key(|r| match r.get(&ColumnName("id".into())) {
        Some(PgValue::Int4(n)) => *n,
        _ => i32::MAX,
    });
}

/// Relaxed invariant set for snapshot-aware tests.
///
/// Invariant 4 (`staged events == WAL events sorted by LSN`) doesn't apply
/// once the snapshot phase is in play: snapshot stages all rows at
/// `snap_lsn`, while their WAL counterparts have varying earlier LSNs. The
/// LSN-sorted comparison would falsely flag this as a mismatch.
///
/// We keep invariants 1, 2, 3, and 5 — the headline correctness property
/// (PG == Iceberg at quiescence) still holds, which is what matters.
fn check_invariants_with_snapshot(h: &mut DstHarness) -> Result<(), String> {
    write_after_schema_change(h);
    h.drive();
    h.flush_and_ack();
    for _ in 0..16 {
        if h.materialize() == Some(0) {
            break;
        }
    }

    let entries = block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .map_err(|e| format!("read_log failed: {e}"))?;
    let blob_keys = block_on(h.storage.blob_keys())?;
    for entry in &entries {
        if !blob_keys.contains(object_key(&entry.s3_path)) {
            return Err(format!(
                "invariant 1: log_index references {} but blob_store doesn't have it",
                entry.s3_path
            ));
        }
    }

    let mut prev_end = 0u64;
    for entry in &entries {
        if entry.start_offset != prev_end {
            return Err(format!(
                "invariant 2: prev_end={} but next start={}",
                prev_end, entry.start_offset
            ));
        }
        prev_end = entry.end_offset;
    }

    let slot =
        h.db.slot_state(SLOT)
            .map_err(|e| format!("slot_state: {e}"))?;
    if h.pipeline.flushed_lsn() > slot.confirmed_flush_lsn {
        return Err(format!(
            "invariant 3: pipeline.flushed_lsn={} > slot.confirmed_flush_lsn={}",
            h.pipeline.flushed_lsn(),
            slot.confirmed_flush_lsn
        ));
    }

    // Invariant 5: PG == Iceberg at quiescence.
    let mut iceberg_rows = block_on(h.storage.engine_rows(&ident()))
        .map_err(|e| format!("read_materialized_state: {e}"))?;
    sort_by_pk(&mut iceberg_rows);
    let mut pg_rows =
        h.db.read_table(&ident())
            .map(stored_rows)
            .map_err(|e| format!("read_table: {e}"))?;
    sort_by_pk(&mut pg_rows);
    let iceberg_rows = on_source_columns(&h.db, iceberg_rows);
    let pg_rows = on_source_columns(&h.db, pg_rows);
    if iceberg_rows != pg_rows {
        return Err(format!(
            "invariant 5: pg={pg_rows:?}\n  iceberg={iceberg_rows:?}"
        ));
    }
    Ok(())
}

// ---------- proptest ----------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Random workloads (including rollbacks and pipeline crashes) preserve
    /// every checked invariant at quiescence, with an `int` or a
    /// `smallint` primary key.
    #[test]
    fn pipeline_preserves_invariants_under_random_workload(
        steps in workload(),
        smallint_pk in any::<bool>(),
        prod_backend in integration_only(),
        wire in integration_only(),
        partitioned in any::<bool>(),
        default_identity in any::<bool>(),
        backfill in any::<bool>(),
        distributed in any::<bool>(),
        second_table in 0u8..3,
    ) {
        SECOND_TABLE.set(second_table);
        DISTRIBUTED.set(distributed);
        BACKFILL.set(backfill);
        SMALLINT_PK.set(smallint_pk);
        PROD_BACKEND.set(prod_backend);
        WIRE.set(wire);
        PARTITIONED.set(partitioned);
        DEFAULT_IDENTITY.set(default_identity);
        let mut h = DstHarness::boot();
        for (i, step) in steps.iter().enumerate() {
            h.run_step(step);
            if let Err(e) = check_step_invariants(&h) {
                panic!("workload {:?}\nfailed after step {i}: {}", steps, e);
            }
        }
        if let Err(e) = check_invariants(&mut h) {
            // proptest will shrink and re-print this as needed.
            panic!("workload {:?}\nfailed: {}", steps, e);
        }
    }
}

// ---------- pinned regressions ----------
//
// As DST surfaces failing seeds we pin them here as deterministic tests so the
// regression doesn't reappear silently. None yet.

/// A materializer restart must never write over a file a committed
/// snapshot still references (the sim blob store refuses overwrites).
#[test]
fn materializer_restart_never_overwrites_committed_files() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::RestartMaterializer);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A `smallint` key reads back from Iceberg as `int`. After a restart
/// the FileIndex is rebuilt from data files, and a delete + re-insert of
/// the key that folds into one `Insert` must still find the old row, or
/// it survives next to the new one.
#[test]
fn smallint_pk_reinsert_after_restart_replaces_the_row() {
    SMALLINT_PK.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::RestartMaterializer);
    h.run_step(&Step::Delete { id: 1 });
    h.run_step(&Step::Insert { id: 1, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// Compaction feeds FileIndex keys read back from data files; for a
/// `smallint` key they must match the keys of incoming events.
#[test]
fn smallint_pk_file_index_survives_compaction() {
    SMALLINT_PK.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Compact);
    check_invariants(&mut h).unwrap();
}

/// `maintain` = expire, then delete unreferenced files. Files added by
/// expired snapshots are still part of the table; cleanup must keep them.
#[test]
fn orphan_cleanup_after_expiry_keeps_live_files() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Expire);
    h.run_step(&Step::CleanupOrphans);
    check_invariants(&mut h).unwrap();
}

/// Orphan cleanup must keep every file the table references. The
/// catalog holds full `s3://` URIs while object stores list keys.
#[test]
fn orphan_cleanup_keeps_live_files() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::CleanupOrphans);
    check_invariants(&mut h).unwrap();
}

/// A materializer restarted after expiry rebuilds its FileIndex; rows
/// from expired snapshots must be in it, or a delete + re-insert of one
/// folds into an `Insert` that leaves the old row live beside the new.
#[test]
fn restart_after_expiry_keeps_reinserted_rows_unique() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Expire);
    h.run_step(&Step::RestartMaterializer);
    h.run_step(&Step::Delete { id: 1 });
    h.run_step(&Step::Insert { id: 1, qty: 30 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A TRUNCATE wipes every row the table holds — including rows written
/// since the last materializer cycle, which share its fold step and
/// aren't in the FileIndex yet.
#[test]
fn truncate_removes_rows_not_yet_materialized() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Truncate { reinsert: None });
    check_invariants(&mut h).unwrap();
}

/// A key change with a TOASTed column unchanged, replayed after a crash
/// between its claim and the slot ack: the replayed copy's delete of the
/// old key comes after the first copy's moved the row. A delete has no
/// TOASTed value to resolve, so it must not look for one.
#[test]
fn replayed_key_change_with_toast_deletes_without_resolving() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 0 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ChangePk {
        from: 1,
        to: 5,
        toast: true,
    });
    // Its Begin, change and Commit, claimed before any keepalive: the
    // restart resends the transaction committing at the claimed LSN.
    h.run_step(&Step::DrivePartial { n: 3 });
    block_on(h.pipeline.flush()).unwrap();
    h.run_step(&Step::CrashMidStream);
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A row updated twice in one batch with its TOASTed column unchanged
/// both times keeps that column's value: the second update must not
/// take the first one's unchanged placeholder for a value.
#[test]
fn repeated_toast_updates_keep_the_unchanged_value() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 4, qty: 0 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ToastUpdate { id: 4, qty: 7 });
    h.run_step(&Step::ToastUpdate { id: 4, qty: 8 });
    check_invariants(&mut h).unwrap();
}

/// An UPDATE that moves a row to a new key with its TOASTed column
/// unchanged: the column's value lives under the old key.
#[test]
fn key_change_with_toast_resolves_from_the_old_key() {
    key_change_with_toast(false, false);
}

/// Under REPLICA IDENTITY DEFAULT the old tuple is key-only, so the value
/// comes from the old key's committed row.
#[test]
fn key_change_with_toast_under_default_identity_resolves_from_the_old_key() {
    key_change_with_toast(true, false);
}

/// The same through production's decoder, whose key-only old tuple has
/// NULL in every other column.
#[cfg(feature = "integration")]
#[test]
fn key_change_with_toast_under_default_identity_resolves_on_the_wire() {
    key_change_with_toast(true, true);
}

fn key_change_with_toast(default_identity: bool, wire: bool) {
    DEFAULT_IDENTITY.set(default_identity);
    WIRE.set(wire);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 5, qty: 0 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ChangePk {
        from: 5,
        to: 1,
        toast: true,
    });
    check_invariants(&mut h).unwrap();
}

/// A row moved twice and then updated in one batch, its TOASTed column
/// unchanged throughout: the value is under the first key.
#[test]
fn moves_within_a_batch_resolve_toast_from_the_first_key() {
    for default_identity in [false, true] {
        DEFAULT_IDENTITY.set(default_identity);
        let mut h = DstHarness::boot();
        h.run_step(&Step::Insert { id: 5, qty: 0 });
        h.run_step(&Step::DriveFlush);
        h.run_step(&Step::MaterializerCycle);
        for (from, to) in [(5, 1), (1, 2)] {
            h.run_step(&Step::ChangePk {
                from,
                to,
                toast: true,
            });
        }
        h.run_step(&Step::ToastUpdate { id: 2, qty: 3 });
        check_invariants(&mut h).unwrap();
    }
}

/// A compaction pass can rewrite inputs whose rows were all deleted
/// since, leaving no output file. Iceberg must still accept the commit
/// that removes them — the sim catalog does; production's must too.
#[cfg(feature = "integration")]
#[test]
fn compaction_with_no_surviving_rows_commits_on_iceberg() {
    PROD_BACKEND.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx { inserts: 1, qty: 0 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Compact);
    check_invariants(&mut h).unwrap();
}

/// The current snapshot's summary counts the table's files, after
/// updates, deletes and a compaction.
#[cfg(feature = "integration")]
#[test]
fn snapshot_summary_counts_the_live_files_on_iceberg() {
    PROD_BACKEND.set(true);
    let mut h = DstHarness::boot();
    for id in 1..=4 {
        h.run_step(&Step::Insert { id, qty: 10 });
    }
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Update { id: 1, qty: 11 });
    h.run_step(&Step::Delete { id: 2 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Compact);
    check_invariants(&mut h).unwrap();
}

/// An UPDATE that moves a row to another partition must delete it from
/// the old one: Iceberg scopes an equality delete to its partition.
#[test]
fn update_moving_a_row_between_partitions_leaves_no_copy() {
    PARTITIONED.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Update { id: 1, qty: 60 });
    check_invariants(&mut h).unwrap();
}

/// Under Postgres's default replica identity a DELETE carries only the
/// key: a partitioned table's delete must still reach the row's
/// partition.
#[test]
fn delete_under_default_replica_identity_reaches_the_rows_partition() {
    PARTITIONED.set(true);
    DEFAULT_IDENTITY.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Delete { id: 1 });
    check_invariants(&mut h).unwrap();
}

/// A commit can apply and still report failure (REST timeout, 502/504).
/// The materializer then retries the same transaction, and must not
/// apply it twice.
#[test]
fn retry_after_a_lost_commit_response_applies_once() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx { inserts: 3, qty: 0 });
    h.run_step(&Step::LoseCommitResponse);
    h.run_step(&Step::Insert { id: 1, qty: 0 });
    check_invariants(&mut h).unwrap();
}

/// A key change leaving a TOASTed value unchanged, whose commit applies
/// but whose response is lost. The retry must not fail resolving the
/// value from the old key — a key the applied commit deleted.
#[test]
fn retry_after_a_lost_response_resolves_a_key_changes_toast() {
    DEFAULT_IDENTITY.set(true);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 3, qty: 0 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::ChangePk {
            from: 3,
            to: 4,
            toast: true,
        },
        Step::LoseCommitResponse,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// A commit whose response is lost but which landed is no failure: the
/// cycle carries on as committed. (The lifecycle stops on a failed cycle.)
#[test]
fn lost_response_to_a_commit_that_landed_is_not_an_error() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 0 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::LoseCommitResponse);
    let cycle = block_on(h.materializer.cycle());
    assert!(cycle.is_ok(), "{cycle:?}");
    check_invariants(&mut h).unwrap();
}

/// As above, with the old key inserted again in the same unit. The retry
/// would resolve the moved row's value from the row now at the old key:
/// silently wrong.
#[test]
fn retry_after_a_lost_response_keeps_a_moved_rows_toast() {
    lost_response_after_a_moved_row(false);
}

#[cfg(feature = "integration")]
#[test]
fn retry_after_a_lost_response_keeps_a_moved_rows_toast_on_iceberg() {
    lost_response_after_a_moved_row(true);
}

fn lost_response_after_a_moved_row(prod: bool) {
    DEFAULT_IDENTITY.set(true);
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 3, qty: 0 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::ChangePk {
            from: 3,
            to: 4,
            toast: true,
        },
        Step::Insert { id: 3, qty: 7 },
        Step::LoseCommitResponse,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// A materializer crash between a commit landing and its cursor update:
/// the restarted materializer must not apply the commit again — here,
/// resolving the moved row's value from the row now at the old key.
#[test]
fn crash_after_a_commit_does_not_apply_it_again() {
    crash_after_a_commit(false);
}

#[cfg(feature = "integration")]
#[test]
fn crash_after_a_commit_does_not_apply_it_again_on_iceberg() {
    crash_after_a_commit(true);
}

fn crash_after_a_commit(prod: bool) {
    DEFAULT_IDENTITY.set(true);
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 3, qty: 0 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::ChangePk {
            from: 3,
            to: 4,
            toast: true,
        },
        Step::Insert { id: 3, qty: 7 },
        Step::DriveFlush,
        Step::CrashAfterCommit,
        // Past the commit: applied once the restarted materializer has
        // skipped it.
        Step::Update { id: 4, qty: 9 },
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// As above, with the commit's snapshot expired before the restart: a
/// compaction — the materializer's own, or a `compact` job's — went on
/// top, and `maintain` expired every snapshot but that one. The
/// compaction carries the log position forward.
#[test]
fn crash_after_a_commit_survives_its_snapshot_expiring() {
    expiry_after_a_crash_after_a_commit(false, false);
    expiry_after_a_crash_after_a_commit(false, true);
}

#[cfg(feature = "integration")]
#[test]
fn crash_after_a_commit_survives_its_snapshot_expiring_on_iceberg() {
    expiry_after_a_crash_after_a_commit(true, false);
    expiry_after_a_crash_after_a_commit(true, true);
}

fn expiry_after_a_crash_after_a_commit(prod: bool, external: bool) {
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    let compact: &[Step] = if external {
        &[Step::ExternalCompactPlan, Step::ExternalCompactCommit]
    } else {
        &[Step::Compact]
    };
    let steps = [
        &[
            Step::BigTx { inserts: 1, qty: 0 },
            Step::DriveFlush,
            Step::CrashAfterCommit,
        ][..],
        compact,
        &[Step::Expire],
    ]
    .concat();
    for step in steps {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("external={external}: after {step:?}: {e}");
        }
    }
    if let Err(e) = check_invariants(&mut h) {
        panic!("external={external}: {e}");
    }
}

/// As above, with a column dropped and re-added in what landed: applying
/// those schema changes again, on top of the schema they produced, reads
/// as more columns moving — `qty` among them — and renames them away.
#[test]
fn crash_after_a_commit_does_not_apply_its_schema_changes_again() {
    let mut h = DstHarness::boot();
    for step in [
        Step::BigTx { inserts: 1, qty: 0 },
        Step::DropNote,
        Step::AddNote,
        Step::BigTx { inserts: 1, qty: 0 },
        Step::DriveFlush,
        Step::CrashAfterCommit,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// As above, for a transaction committed in several steps at once: the
/// log range goes on the last of their snapshots.
#[cfg(feature = "integration")]
#[test]
fn crash_after_a_multi_step_commit_does_not_apply_it_again_on_iceberg() {
    PROD_BACKEND.set(true);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 1 },
        Step::BigTx { inserts: 6, qty: 2 },
        Step::DriveFlush,
        Step::CrashAfterCommit,
        Step::Update { id: 1, qty: 3 },
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// A materializer crash after a backfill's rows commit, before they're
/// marked applied: the restarted materializer must not apply them again
/// — and goes on to the changes after them.
#[test]
fn crash_after_a_backfill_commit_does_not_apply_it_again() {
    BACKFILL.set(true);
    let mut h = DstHarness::boot();
    while h.backfilling {
        h.run_step(&Step::BackfillChunk);
    }
    h.run_step(&Step::Insert { id: 5, qty: 50 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::CrashAfterCommit);
    check_invariants(&mut h).unwrap();
}

/// A key change with a TOASTed column unchanged, replayed after a crash
/// between its claim and the slot ack, with its first copy already in
/// Iceberg: the replayed copy can't resolve the value from the old key —
/// the first copy moved the row off it.
#[test]
fn replayed_key_change_with_toast_resolves_after_its_first_copy_landed() {
    DEFAULT_IDENTITY.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 0 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ChangePk {
        from: 1,
        to: 5,
        toast: true,
    });
    // Its Begin, change and Commit, claimed before any keepalive: the
    // restart resends the transaction committing at the claimed LSN.
    h.run_step(&Step::DrivePartial { n: 3 });
    block_on(h.pipeline.flush()).unwrap();
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::CrashMidStream);
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A pipeline crash partway through staging a transaction, then a replay
/// from the slot, must never make part of that transaction visible.
#[test]
fn pipeline_crash_mid_transaction_keeps_it_atomic() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 0 },
        Step::BigTx { inserts: 1, qty: 0 },
        // Begin, Relation, Insert, Commit, then into the big transaction.
        Step::DrivePartial { n: 7 },
        Step::DrivePartial { n: 3 },
        Step::CrashMidStream,
        Step::DriveFlush,
        Step::MaterializerCycle,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
}

/// The replication connection dropping partway through a large
/// transaction: the reopened stream sends it again from its Begin, and
/// none of the first copy — chunks already staged included — may be
/// claimed with it.
#[test]
fn reconnect_mid_transaction_keeps_it_atomic() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 0 },
        Step::BigTx { inserts: 6, qty: 0 },
        // Begin, Relation, Insert, Commit, then far enough into the big
        // transaction that a chunk of it is staged.
        Step::DrivePartial { n: 10 },
        Step::Reconnect,
        Step::DriveFlush,
        Step::MaterializerCycle,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// The connection drops after a re-added column's Relation arrived but
/// before it was staged. The reopened stream sends the Relation again;
/// were it taken for the one already seen, pg2iceberg would never learn
/// of the re-add, and row 3 would get back its dropped value.
#[test]
fn reconnect_keeps_a_relation_not_yet_staged() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::Insert { id: 3, qty: 30 },
        Step::DropNote,
        Step::Update { id: 1, qty: 20 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::AddNote,
        Step::Insert { id: 2, qty: 40 },
        // Begin, Relation, Insert, Commit: buffered, not yet staged.
        Step::DrivePartial { n: 4 },
        Step::Reconnect,
        Step::DriveFlush,
        Step::MaterializerCycle,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// A keepalive received before the connection dropped vouches for the
/// transactions buffered before it — which the reset discards. Kept, it
/// would let the first flush after the reconnect, of only part of their
/// replay, claim and ack past the rest: lost to the next crash.
#[test]
fn reconnect_forgets_keepalives_from_the_old_stream() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::DriveFlush,
        Step::Insert { id: 2, qty: 20 },
        Step::Insert { id: 3, qty: 30 },
        Step::UnpublishedWrite { qty: 0 },
        // Both transactions, then the keepalive past them.
        Step::DrivePartial { n: 7 },
        Step::Reconnect,
        // Row 2's transaction again, with the Relation a new stream sends.
        Step::DrivePartial { n: 4 },
        Step::FlushTick,
        Step::CrashMidStream,
        Step::DriveFlush,
        Step::MaterializerCycle,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// A compaction commit that applied but reported failure must not leave
/// the materializer's FileIndex pointing at the files it rewrote.
#[test]
fn file_index_stays_true_after_a_lost_compaction_response() {
    let mut h = DstHarness::boot();
    // One data file holding a live row (2) and a deleted one (1): the
    // pass rewrites it, moving row 2 to a new file.
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Update { id: 1, qty: 11 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::LoseCommitResponse);
    h.run_step(&Step::Compact);
    check_invariants(&mut h).unwrap();
}

/// A `compact` job plans a pass, the materializer then commits an update
/// to a row in the pass's input, and only then does the job commit: the
/// rewritten copy of the old row must not come back.
#[test]
fn external_compaction_racing_an_update_keeps_the_new_row() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    // Row 2 moves on: the first file now holds a dead row, so it's worth
    // rewriting.
    h.run_step(&Step::Update { id: 2, qty: 11 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ExternalCompactPlan);
    assert!(h.pending_external.is_some(), "the job planned a pass");
    h.run_step(&Step::Update { id: 1, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ExternalCompactCommit);
    check_invariants(&mut h).unwrap();
    // The rewritten copy is dead: the next pass must drop it, not keep it.
    h.run_step(&Step::Compact);
    check_invariants(&mut h).unwrap();
}

/// A `compact` job's commit lands while the materializer's is in flight,
/// and the materializer's goes on top: its FileIndex must take in both.
#[test]
fn a_commit_landing_on_a_compaction_keeps_the_index_true() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    // Row 2 moves on: the first file now holds a dead row, so it's worth
    // rewriting.
    h.run_step(&Step::Update { id: 2, qty: 11 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ExternalCompactPlan);
    h.run_step(&Step::ExternalCompactMidCommit);
    // Leaves row 1, which the pass rewrites, alone.
    h.run_step(&Step::Insert { id: 3, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// As above, with the materializer's own compaction pass going on top:
/// the two passes rewrite different partitions.
#[test]
fn a_compaction_landing_on_another_keeps_the_index_true() {
    PARTITIONED.set(true);
    let mut h = DstHarness::boot();
    let cycle = |h: &mut DstHarness| {
        h.run_step(&Step::DriveFlush);
        h.run_step(&Step::MaterializerCycle);
    };
    // The oldest file, in partition 50..100.
    h.run_step(&Step::Insert { id: 3, qty: 60 });
    cycle(&mut h);
    // A file in partition 0..50, made dirty: the job takes it.
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 10 });
    cycle(&mut h);
    h.run_step(&Step::Update { id: 2, qty: 20 });
    cycle(&mut h);
    h.run_step(&Step::ExternalCompactPlan);
    assert!(h.pending_external.is_some(), "the job planned a pass");
    // The oldest file made dirty: the materializer's pass takes it.
    h.run_step(&Step::Update { id: 3, qty: 70 });
    cycle(&mut h);
    h.run_step(&Step::ExternalCompactMidCommit);
    h.run_step(&Step::Compact);
    assert!(
        h.audited.land_before_next_commit.lock().unwrap().is_none(),
        "the job's pass landed"
    );
    check_invariants(&mut h).unwrap();
}
/// A `compact` job's pass lands while the materializer compacts the same
/// files: the materializer's pass loses, as a conflict, and plans again
/// on its next one — not a failure.
#[test]
fn compaction_losing_to_a_compact_job_is_no_failure() {
    let mut h = DstHarness::boot();
    for step in [
        Step::BigTx { inserts: 1, qty: 0 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::ExternalCompactPlan,
        Step::ExternalCompactMidCommit,
        Step::Compact,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// `external_compaction_racing_an_update_keeps_the_new_row` on
/// iceberg-rust: the rewritten copy keeps the sequence number its pass
/// read the table at.
#[cfg(feature = "integration")]
#[test]
fn external_compaction_racing_an_update_keeps_the_new_row_on_iceberg() {
    PROD_BACKEND.set(true);
    external_compaction_racing_an_update_keeps_the_new_row();
}

/// A `compact` job and the materializer rewrite the same file; the job
/// commits second. Its commit must fail rather than add a second copy of
/// the file's rows.
fn two_compactions_of_one_file(prod: bool) {
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Update { id: 2, qty: 11 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ExternalCompactPlan);
    assert!(h.pending_external.is_some(), "the job planned a pass");
    h.run_step(&Step::Compact);
    h.run_step(&Step::ExternalCompactCommit);
    check_invariants(&mut h).unwrap();
}

#[test]
fn two_compactions_of_one_file_keep_one_copy() {
    two_compactions_of_one_file(false);
}

#[cfg(feature = "integration")]
#[test]
fn two_compactions_of_one_file_keep_one_copy_on_iceberg() {
    two_compactions_of_one_file(true);
}

/// Dropping a column and adding one with the same name gives a new,
/// empty column: the dropped column's values must not come back.
#[test]
fn re_added_column_does_not_bring_back_dropped_values() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::DropNote);
    h.run_step(&Step::AddNote);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    check_invariants(&mut h).unwrap();
}

/// Rows staged before a column is dropped and re-added, but materialized
/// after: their values belong to the dropped column, not the new one.
/// (Fails today: schema changes apply when the Relation message arrives,
/// ahead of rows already staged, which are keyed by column name.)
#[test]
fn rows_staged_before_a_re_add_keep_their_values_out_of_the_new_column() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DropNote);
    h.run_step(&Step::AddNote);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    check_invariants(&mut h).unwrap();
}

/// The materializer fails between applying a schema change and committing
/// the rows after it. The retry re-reads the log from the last commit, so
/// that commit must cover every row staged before the change: re-read
/// under the changed schema, a dropped and re-added column would take
/// their values.
#[test]
fn a_failed_commit_after_a_schema_change_retries_cleanly() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    // Drop and re-add `note` with no write between (unlike `AddNote`):
    // seen by order, as it moves last, and staged in the same flush as
    // row 1.
    h.db.alter_drop_column(&ident(), "note").unwrap();
    let note = schema().columns.into_iter().find(|c| c.name == "note");
    h.db.alter_add_column(&ident(), note.unwrap()).unwrap();
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.audited
        .fail_commit_after_schema_change
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = block_on(h.materializer.cycle()).unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
    // Nor may the retry re-apply the older schema change on top of the
    // newer one: that renames columns that were never re-added.
    let schema = block_on(h.audited.load_table(&ident()))
        .unwrap()
        .unwrap()
        .schema;
    let columns: Vec<(String, i32)> = schema
        .columns
        .into_iter()
        .map(|c| (c.name, c.field_id))
        .collect();
    let want = [("id", 1), ("note__dropped_2", 2), ("qty", 3), ("note", 4)];
    assert_eq!(columns, want.map(|(n, i)| (n.to_string(), i)));
}

/// Compaction and TOAST resolution read older data files, which hold the
/// dropped column's values under the re-added column's name: they must
/// read columns by field id, or those values come back.
#[test]
fn reading_old_files_after_a_re_add_keeps_the_new_column_empty() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::DropNote);
    h.run_step(&Step::AddNote);
    h.run_step(&Step::Insert { id: 3, qty: 30 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::ToastUpdate { id: 1, qty: 11 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Compact);
    check_invariants(&mut h).unwrap();
}

/// After a column ahead of others is dropped, a restarted materializer
/// discovers the remaining columns at new positions; it must keep
/// writing each value under its column's Iceberg field id.
#[test]
fn restart_after_dropping_a_column_keeps_field_ids() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::DropNote);
    h.run_step(&Step::RestartMaterializer);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    check_invariants(&mut h).unwrap();
}

/// A backfill reads rows as of its snapshot; a change streamed (and
/// staged) before the backfill reaches that row is newer, and must win.
#[test]
fn backfill_does_not_overwrite_newer_changes() {
    BACKFILL.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Update { id: 1, qty: 99 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A row deleted while the backfill runs stays deleted: its snapshot copy
/// (from before the delete) mustn't bring it back.
#[test]
fn backfill_does_not_bring_back_rows_deleted_during_it() {
    BACKFILL.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Delete { id: 2 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
}

/// An update made while the backfill runs, leaving a TOASTed column
/// unchanged, resolves that column from the row's snapshot copy — which
/// has to be in Iceberg first.
#[test]
fn backfill_resolves_toast_updates_made_during_it() {
    BACKFILL.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::ToastUpdate { id: 1, qty: 99 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
}

/// A commit that fails while the backfill's rows are applied: the retry
/// picks up from the snapshot cursor, and the changes still come after.
#[test]
fn a_failed_commit_while_applying_a_backfill_retries_cleanly() {
    BACKFILL.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Update { id: 1, qty: 99 });
    h.run_step(&Step::Delete { id: 2 });
    h.run_step(&Step::DriveFlush);
    while h.backfilling {
        h.backfill_chunk();
    }
    h.audited
        .fail_next_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = block_on(h.materializer.cycle()).unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A backfill's rows are applied only once it has staged them all — even
/// if the table is ungated early — or the rows staged after would be
/// skipped as already applied.
#[test]
fn backfill_rows_wait_for_the_backfill_to_complete() {
    BACKFILL.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::BackfillChunk);
    h.materializer.mark_snapshot_complete(&ident());
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// A column dropped and re-added while it's the last one — order can't
/// show the re-add — across a materializer restart: the restarted
/// materializer must still know it was dropped, or row 3 gets its old
/// value back under the re-added column.
#[test]
fn re_adding_the_last_column_after_a_restart_keeps_old_values_out() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 1 },
        // Drop and re-add: `note` moves last.
        Step::DropNote,
        Step::AddNote,
        Step::Update { id: 1, qty: 2 },
        // Written before the next drop, and not after.
        Step::Insert { id: 3, qty: 5 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        // Again, now that it's last: only having seen the drop tells.
        Step::DropNote,
        Step::Update { id: 1, qty: 3 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::RestartMaterializer,
        Step::AddNote,
        Step::Insert { id: 2, qty: 4 },
        Step::DriveFlush,
        Step::MaterializerCycle,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// Worker b applies a column's drop and re-add; worker a, which didn't,
/// writes the table next. With the columns it had, a's value for the
/// re-added column would land in the dropped one's field.
#[test]
fn worker_writing_after_anothers_schema_change_uses_the_new_columns() {
    DISTRIBUTED.set(true);
    let mut h = DstHarness::boot();
    for step in [
        Step::DropNote,
        Step::AddNote,
        Step::Insert { id: 1, qty: 0 },
        Step::DriveFlush,
        Step::OtherWorkerCycle,
        Step::Invalidate,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// As above, with rows on both sides of the schema change.
#[test]
fn worker_writing_after_anothers_schema_change_keeps_old_rows_apart() {
    DISTRIBUTED.set(true);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 0 },
        Step::DropNote,
        Step::AddNote,
        Step::Insert { id: 2, qty: 0 },
        Step::DriveFlush,
        Step::OtherWorkerCycle,
        Step::Insert { id: 3, qty: 0 },
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// `pg2iceberg verify` after a column is dropped: data files hold values
/// by field id, and the source's schema numbers columns by position — so
/// read by the source's, `qty` would read the dropped `note`'s values (and
/// fail to decode them).
#[test]
fn verify_after_a_column_drop_reads_the_right_columns() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::DropNote,
        Step::Update { id: 1, qty: 20 },
    ] {
        h.run_step(&step);
    }
    check_invariants(&mut h).unwrap();
}

/// `pg2iceberg verify` on a table keyed by a `smallint`: Postgres reads
/// the key as one, Iceberg stores it as an `int` — the same key, or every
/// row is reported missing on both sides.
#[test]
fn verify_matches_rows_keyed_by_a_smallint() {
    SMALLINT_PK.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    check_invariants(&mut h).unwrap();
}

/// A column added with a default: rows already in the table read it —
/// Postgres stores the value once instead of writing it into them, so the
/// WAL carries nothing for rows 1 and 2 — and must read it in Iceberg too.
/// Row 4, deleted, shares a data file with row 2, and stays deleted.
#[test]
fn a_default_reaches_rows_that_predate_its_column() {
    default_reaches_older_rows(false);
}

#[cfg(feature = "integration")]
#[test]
fn a_default_reaches_rows_that_predate_its_column_on_iceberg() {
    default_reaches_older_rows(true);
}

fn default_reaches_older_rows(prod: bool) {
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::Insert { id: 2, qty: 20 },
        Step::Insert { id: 4, qty: 40 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::Delete { id: 4 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::DropNote,
        Step::AddNoteWithDefault,
        Step::Insert { id: 3, qty: 30 },
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// The commit after the column's schema change fails: the schema change
/// stays, the rows given the default don't. The retry must still give
/// them the default.
#[test]
fn a_failed_commit_after_adding_a_default_still_fills_it_in() {
    failed_commit_after_adding_a_default(false);
}

#[cfg(feature = "integration")]
#[test]
fn a_failed_commit_after_adding_a_default_still_fills_it_in_on_iceberg() {
    failed_commit_after_adding_a_default(true);
}

fn failed_commit_after_adding_a_default(prod: bool) {
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::DropNote,
        Step::AddNoteWithDefault,
        Step::Insert { id: 2, qty: 20 },
        Step::DriveFlush,
    ] {
        h.run_step(&step);
    }
    h.audited
        .fail_commit_after_schema_change
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = block_on(h.materializer.cycle()).unwrap_err();
    assert!(err.to_string().contains("injected"), "{err}");
    check_invariants(&mut h).unwrap();
}

/// A restarted stream sends the column's Relation again — and Postgres
/// still stores its default. Rows given a value of their own since must
/// keep it: the default is filled in once.
#[test]
fn a_default_is_filled_in_once() {
    default_filled_in_once(false);
}

#[cfg(feature = "integration")]
#[test]
fn a_default_is_filled_in_once_on_iceberg() {
    default_filled_in_once(true);
}

fn default_filled_in_once(prod: bool) {
    PROD_BACKEND.set(prod);
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::Insert { id: 2, qty: 20 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::DropNote,
        Step::AddNoteWithDefault,
        Step::Insert { id: 3, qty: 30 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::Update { id: 1, qty: 11 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::CrashMidStream,
        Step::Insert { id: 4, qty: 40 },
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// The column is dropped again before pg2iceberg reads its default:
/// Postgres has cleared it, so the rows that predate the column keep NULL
/// for it — a known gap, which pg2iceberg must report rather than pass
/// over.
#[test]
fn a_default_dropped_before_it_was_read_is_reported() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::Insert { id: 2, qty: 20 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::DropNote,
        Step::AddNoteWithDefault,
        Step::Insert { id: 3, qty: 30 },
    ] {
        h.run_step(&step);
    }
    // Not `Step::DropNote`, which lets pg2iceberg read the default first.
    h.db.alter_drop_column(&ident(), "note").unwrap();
    NOTE_PRESENT.set(false);
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    let mut labels = pg2iceberg_core::Labels::new();
    labels.insert("table".into(), ident().to_string());
    labels.insert("column".into(), "note".into());
    labels.insert("reason".into(), "column_gone".into());
    assert_eq!(
        metrics().counter_value(
            pg2iceberg_core::metrics::names::UNFILLED_COLUMN_DEFAULTS,
            &labels
        ),
        1
    );
}

/// A column added with a default and dropped again: rows that predate it
/// read the default until the drop, and keep it under the dropped
/// column's name. (Found by the random DST.)
#[test]
fn a_default_dropped_after_it_was_staged_stays_filled_in() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 0 },
        Step::DropNote,
        Step::AddNoteWithDefault,
        Step::Insert { id: 2, qty: 0 },
        Step::DropNote,
        Step::DriveFlushWithoutAck,
        Step::MaterializerCycle,
    ] {
        h.run_step(&step);
        if let Err(e) = check_step_invariants(&h) {
            panic!("after {step:?}: {e}");
        }
    }
    check_invariants(&mut h).unwrap();
}

/// The table is rewritten (`VACUUM FULL`) before pg2iceberg reads the new
/// column's default: Postgres no longer stores the value rows 1 and 2
/// read, so there's nothing to fill them in with — a known gap, which
/// pg2iceberg must report rather than pass over.
#[test]
fn a_default_postgres_no_longer_stores_is_reported() {
    let mut h = DstHarness::boot();
    for step in [
        Step::Insert { id: 1, qty: 10 },
        Step::Insert { id: 2, qty: 20 },
        Step::DriveFlush,
        Step::MaterializerCycle,
        Step::DropNote,
        Step::AddNoteWithDefault,
    ] {
        h.run_step(&step);
    }
    h.db.rewrite_table(&ident()).unwrap();
    h.run_step(&Step::Insert { id: 3, qty: 30 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    let mut labels = pg2iceberg_core::Labels::new();
    labels.insert("table".into(), ident().to_string());
    labels.insert("column".into(), "note".into());
    labels.insert("reason".into(), "not_stored".into());
    assert_eq!(
        metrics().counter_value(
            pg2iceberg_core::metrics::names::UNFILLED_COLUMN_DEFAULTS,
            &labels
        ),
        1
    );
}

/// When a table changes owner between distributed workers, the new owner
/// must know the rows the previous one wrote — or a delete + re-insert
/// leaves the old row, and TOAST updates fail.
#[test]
fn worker_taking_over_a_table_knows_its_rows() {
    DISTRIBUTED.set(true);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    // Worker "b" cycles first, alone: it owns the table and writes row 1.
    h.run_step(&Step::OtherWorkerCycle);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::ToastUpdate { id: 1, qty: 11 });
    h.run_step(&Step::DriveFlush);
    // Worker "a" joins and takes the table over.
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

/// With `sink.namespace` set, `sales.returns` materializes as
/// `public.returns`: its changes are staged under the Iceberg name.
/// (`public.orders` and `sales.orders` would collide there; config
/// loading refuses that.)
#[test]
fn table_mapped_into_the_sink_namespace_replicates() {
    SECOND_TABLE.set(2);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::OtherInsert { id: 1, qty: 99 });
    check_invariants(&mut h).unwrap();
}

/// Worker "b" loses its commit's response, as the harness drains to
/// quiescence: the drain retries it rather than stop with the rows staged.
#[test]
fn draining_retries_a_second_workers_lost_commit() {
    DISTRIBUTED.set(true);
    SECOND_TABLE.set(1);
    let mut h = DstHarness::boot();
    h.run_step(&Step::LoseCommitResponse);
    h.run_step(&Step::OtherInsert { id: 1, qty: 0 });
    h.run_step(&Step::OtherInsert { id: 2, qty: 0 });
    h.run_step(&Step::TruncateBoth);
    h.run_step(&Step::OtherWorkerCycle);
    check_invariants(&mut h).unwrap();
}

/// One TRUNCATE naming both tables empties both, in the sim's decoded
/// stream and, with `wire`, through production's decoder, which splits
/// pgoutput's single message per table.
fn truncate_both_tables(wire: bool) {
    SECOND_TABLE.set(1);
    WIRE.set(wire);
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::OtherInsert { id: 1, qty: 10 });
    // Materialized first: rows sharing a fold step with a TRUNCATE are
    // `truncate_removes_rows_not_yet_materialized`.
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::TruncateBoth);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    check_invariants(&mut h).unwrap();
}

#[test]
fn truncating_two_tables_in_one_statement_empties_both() {
    truncate_both_tables(false);
}

#[cfg(feature = "integration")]
#[test]
fn truncating_two_tables_in_one_statement_empties_both_on_the_wire() {
    truncate_both_tables(true);
}

/// pgoutput resends a table's Relation after anything that invalidates
/// its cache entry, unchanged; applying it again must change nothing.
#[test]
fn redundant_relation_messages_change_nothing() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::Invalidate);
    h.run_step(&Step::Update { id: 1, qty: 11 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::Invalidate);
    h.run_step(&Step::DropNote);
    h.run_step(&Step::Invalidate);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    check_invariants(&mut h).unwrap();
}

#[test]
fn happy_path_one_insert_one_flush() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
}

/// One transaction far bigger than `FLUSH_ROWS`: staged in bounded
/// chunks, never partly visible while it streams in (invariants 7-9),
/// and intact once it lands.
#[test]
fn large_transaction_is_staged_in_bounded_chunks() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 1,
    });
    for _ in 0..8 {
        h.run_step(&Step::DrivePartial { n: 3 });
        check_step_invariants(&h).unwrap();
        h.run_step(&Step::FlushTick);
        check_step_invariants(&h).unwrap();
    }
    check_invariants(&mut h).unwrap();
}

/// A transaction spanning many staged chunks, materialized in batches
/// far smaller than it: readers must still see all of it or none of it.
#[test]
fn large_transaction_becomes_visible_atomically() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 1 });
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 2,
    });
    h.run_step(&Step::DriveFlush);
    for _ in 0..100 {
        h.run_step(&Step::MaterializerCycle);
        check_step_invariants(&h).unwrap();
    }
    check_invariants(&mut h).unwrap();
}

/// A transaction that inserts rows, truncates, then inserts more, spread
/// over several materializer steps. The TRUNCATE becomes deletes for
/// every row the materializer knows about — which must include rows this
/// same, not-yet-committed unit wrote in earlier steps.
#[test]
fn truncate_inside_large_transaction_hides_its_earlier_inserts() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 1 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    let mut tx = h.db.begin_tx();
    for id in 100..106 {
        tx.insert(&ident(), row(id, 1));
    }
    tx.truncate(&ident());
    for id in 200..202 {
        tx.insert(&ident(), row(id, 2));
    }
    tx.commit(Timestamp(0)).unwrap();
    h.run_step(&Step::DriveFlush);
    for _ in 0..100 {
        h.run_step(&Step::MaterializerCycle);
        check_step_invariants(&h).unwrap();
    }
    let mut iceberg = block_on(h.storage.engine_rows(&ident())).unwrap();
    sort_by_pk(&mut iceberg);
    let mut pg = stored_rows(h.db.read_table(&ident()).unwrap());
    sort_by_pk(&mut pg);
    assert_eq!(
        on_source_columns(&h.db, iceberg),
        on_source_columns(&h.db, pg)
    );
}

/// The atomic commit of a multi-step transaction fails: nothing may
/// become visible, and the next cycle must land the whole transaction
/// intact — despite FileIndex having been updated for the lost steps.
#[test]
fn failed_multi_step_commit_leaves_nothing_and_retry_lands_it() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 1 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 2,
    });
    h.run_step(&Step::DriveFlush);
    h.audited
        .fail_next_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(block_on(h.materializer.cycle()).is_err());
    check_step_invariants(&h).unwrap();
    for _ in 0..100 {
        h.run_step(&Step::MaterializerCycle);
        check_step_invariants(&h).unwrap();
    }
    check_invariants(&mut h).unwrap();
}

/// A compaction pass that rewrites one dirty file must not re-read the
/// rest of the table: the materializer's FileIndex is updated from what
/// the pass rewrote, not rebuilt from catalog history.
#[test]
fn compaction_does_not_replay_the_table() {
    let mut h = DstHarness::boot();
    for id in 1..=20 {
        h.run_step(&Step::Insert { id, qty: 1 });
        h.run_step(&Step::DriveFlush);
        h.run_step(&Step::MaterializerCycle);
    }
    // Row 1's first file is now dirty: its only row is dead.
    h.run_step(&Step::Update { id: 1, qty: 2 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    let cfg = CompactionConfig {
        data_file_threshold: 1,
        delete_file_threshold: 1,
        target_size_bytes: 1024,
        max_input_bytes_per_pass: 1,
    };
    h.audited
        .audit_paused
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let before = h.storage.sim_blob().gets();
    let out = block_on(h.materializer.compact_table(&ident(), &cfg))
        .unwrap()
        .expect("the dirty file is rewritten");
    let reads = h.storage.sim_blob().gets() - before;
    h.audited
        .audit_paused
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(out.input_data_files, 1);
    assert!(reads <= 4, "one compaction pass read {reads} files");
    check_invariants(&mut h).unwrap();
}

/// A pass that carries live rows into a new file must point FileIndex at
/// it (the `Compact` step checks FileIndex against a catalog rebuild).
#[test]
fn compaction_moves_live_rows_in_file_index() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx {
        inserts: 10,
        qty: 1,
    });
    h.run_step(&Step::DriveFlush);
    for _ in 0..20 {
        h.run_step(&Step::MaterializerCycle);
    }
    // Kill one row of the first multi-row file; its other rows stay live.
    h.run_step(&Step::Update { id: 1001, qty: 9 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    for _ in 0..10 {
        h.run_step(&Step::Compact);
    }
    check_invariants(&mut h).unwrap();
}

/// A flush that runs mid-transaction — here on a keepalive received just
/// before the transaction began — must not claim the chunks already
/// staged for it.
#[test]
fn flush_mid_transaction_never_claims_its_chunks() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::UnpublishedWrite { qty: 0 });
    h.run_step(&Step::DrivePartial { n: 1 }); // just the caught-up keepalive
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 1,
    });
    h.run_step(&Step::DrivePartial { n: 10 }); // spills, stays open
    h.run_step(&Step::FlushTick);
    check_step_invariants(&h).unwrap();
    check_invariants(&mut h).unwrap();
}

/// Crash partway through a large transaction: chunks staged so far are
/// never claimed, the slot replays the whole transaction, and nothing is
/// lost or torn.
#[test]
fn crash_mid_large_transaction_replays_cleanly() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::BigTx {
        inserts: 20,
        qty: 1,
    });
    h.run_step(&Step::DrivePartial { n: 10 });
    check_step_invariants(&h).unwrap();
    h.run_step(&Step::CrashMidStream);
    h.run_step(&Step::BigTx { inserts: 5, qty: 2 });
    h.run_step(&Step::DriveFlush);
    check_step_invariants(&h).unwrap();
    check_invariants(&mut h).unwrap();
}

/// Writes only to tables outside the publication after the last
/// published change: pgoutput sends nothing for them, so the slot must
/// advance from the caught-up keepalive alone (invariant 6).
#[test]
fn unpublished_writes_do_not_pin_the_slot() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    for qty in 0..3 {
        h.run_step(&Step::UnpublishedWrite { qty });
    }
    check_invariants(&mut h).unwrap();
}

#[test]
fn crash_after_some_inserts_then_more_inserts() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::CrashAndRestart);
    h.run_step(&Step::Insert { id: 3, qty: 30 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
}

/// Row changes staged for the main table (schema events aside).
fn staged_changes(h: &DstHarness) -> usize {
    block_on(h.coord.read_log(&ident(), 0, 1_000_000))
        .unwrap()
        .iter()
        .flat_map(|e| decode_chunk(&block_on(h.blob_store.get(&e.s3_path)).unwrap()).unwrap())
        .filter(|e| e.op != Op::Relation)
        .count()
}

#[test]
fn rollback_does_not_appear_in_staged_or_coord() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::RollbackInsert { id: 99, qty: 1 });
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
    assert_eq!(
        staged_changes(&h),
        1,
        "only the committed insert should be staged"
    );
}

#[test]
fn update_then_delete_round_trips_to_staged() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::Update { id: 1, qty: 99 });
    h.run_step(&Step::Delete { id: 1 });
    h.run_step(&Step::DriveFlush);
    check_invariants(&mut h).unwrap();
    assert_eq!(staged_changes(&h), 3, "I + U + D");
}

#[test]
fn materializer_runs_between_writes_keeps_iceberg_in_sync() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::Update { id: 1, qty: 99 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

#[test]
fn materializer_idempotent_when_run_extra_times() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::MaterializerCycle);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}

// ---------- snapshot integration ----------

#[test]
fn snapshot_then_workload_keeps_all_invariants() {
    // Pre-seed 10 rows. Snapshot bootstraps Iceberg with them. Then run
    // mixed live-replication workload on top. All 5 invariants must hold.
    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = DstHarness::boot_with_seeds(&seeds);
    h.run_snapshot();
    h.materialize();

    // Now run live workload that mutates the snapshot rows.
    h.run_step(&Step::Update { id: 1, qty: 999 });
    h.run_step(&Step::Delete { id: 2 });
    h.run_step(&Step::Insert { id: 11, qty: 110 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants_with_snapshot(&mut h).unwrap();
}

#[test]
fn snapshot_resume_keeps_invariants() {
    // Partial snapshot (chunk_size=3, max_chunks=2 → 6 of 10 rows staged).
    // Pipeline + Snapshotter dropped; rebuild and resume to completion.
    // Invariants must hold across the resume boundary.
    let seeds: Vec<(i32, i32)> = (1..=10).map(|i| (i, i * 10)).collect();
    let mut h = DstHarness::boot_with_seeds(&seeds);
    {
        let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(3);
        block_on(s.run_chunks(&h.db, &[schema()], &mut h.pipeline, Some(2))).unwrap();
    }
    // Crash the pipeline (replay from slot's restart_lsn) so subsequent
    // snapshot run rebuilds state cleanly.
    h.crash_and_restart();

    // Resume the snapshot.
    {
        let s = Snapshotter::new(h.coord.clone() as Arc<dyn Coordinator>).with_chunk_size(3);
        let snap_lsn = block_on(s.run(&h.db, &[schema()], &mut h.pipeline)).unwrap();
        h.stream.send_standby(snap_lsn);
    }
    h.materialize();
    check_invariants_with_snapshot(&mut h).unwrap();
}

#[test]
fn pipeline_crash_then_materializer_catches_up() {
    let mut h = DstHarness::boot();
    h.run_step(&Step::Insert { id: 1, qty: 10 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::CrashAndRestart);
    h.run_step(&Step::Insert { id: 2, qty: 20 });
    h.run_step(&Step::DriveFlush);
    h.run_step(&Step::MaterializerCycle);
    check_invariants(&mut h).unwrap();
}
