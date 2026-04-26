# pg2iceberg Rust Port — Plan

**Source of truth:** the Go implementation at `../pg2iceberg/`. This document supersedes the previous draft, which described an "events-table in Iceberg" design that does not match the code. Where this plan and the Go code disagree, the Go code wins until we explicitly decide to diverge.

---

## 1. What pg2iceberg actually is

Two pipelines that share an Iceberg writer and a coordination schema in the source Postgres.

### 1a. Logical-replication mode (the recommended mode)

```
PG WAL ──► WALDecoder ──► events chan ──► tableSink (per-table txBuffer)
                                              │
                                              │  on flush:
                                              ▼
                                  RollingWriter → staged Parquet (S3)
                                              │
                                              ▼
                                  Coordinator.ClaimOffsets()  (PG txn)
                                    ├─ UPDATE _pg2iceberg.log_seq
                                    └─ INSERT _pg2iceberg.log_index
                                              │
                                              ▼
                                  src.SetFlushedLSN(lsn)
                                              │
                                              ▼
                            sendStandby() — confirmed_flush_lsn → PG

Materializer (separate cadence, may be a separate process):
  GetCursor() → ReadLog() → ReadMatEvents() → FoldEvents()
    → TableWriter.Prepare() (equality deletes + data files)
      → Catalog.CommitSnapshot()
        → AdvanceCursor()
```

**Durability boundary** (this is the invariant to encode in types):
> `flushedLSN` may only advance after both
>  (a) the staged Parquet object exists in object storage, and
>  (b) the matching `log_seq`/`log_index` rows are committed in the source PG.
>
> Iceberg catalog commit is **not** on the slot-advance path. It happens later, on the materializer's cadence.

This is fundamentally different from the previous draft and changes which surfaces DST has to attack.

### 1b. Query mode

Watermark polling per table; no WAL, no staging layer. Buffers rows by PK, writes directly to the materialized Iceberg table via the same `TableWriter` (equality deletes + data files). Snapshot phase populates the initial state. Reuses the coordination schema only for `checkpoints` (last watermark per table).

### 1c. Coordination schema (`_pg2iceberg` in source PG)

Owned by the coordinator package; every distributed-state question routes here instead of a consensus library.

| Table | Purpose |
|---|---|
| `log_seq` | Per-table monotonic offset counter (`table_name → next_offset`) |
| `log_index` | Manifest of staged Parquet files (`table_name, [start_offset, end_offset], s3_path, record_count, byte_size`) |
| `mat_cursor` | Materializer progress per consumer group (`group, table → last_offset, last_committed`) |
| `lock` / `consumer` | Heartbeat-TTL claims for distributed materializer workers |
| `checkpoints` | Mode + LSN + snapshot state + per-table query watermarks |
| `markers` | Fence rows for snapshot↔CDC handoff |

---

## 2. Goal and non-goals

**Goal:** a Rust port that is correctness-first, with deterministic simulation testing (DST) as the primary tool for proving correctness. The Go reference is the differential oracle.

**Non-goals (defer past first GA):**
- Beating Go on perf. Match it.
- PostGIS types (`geometry`, `geography`).
- PG14+ in-progress (streamed) transactions. Non-streaming first.
- Two-phase commit decoding.
- Multi-region / multi-catalog.
- Compaction parity with the Go bug at [project_compaction_partition_bug.md] (deferred there too).

**Pre-users.** Optimize for getting the architecture right.

---

## 3. Guiding principles

1. **Mirror, not CDC.** pg2iceberg is a managed-mirror product, not a streaming framework. PG and Iceberg are guaranteed to match 100%. Therefore: no `--start-lsn`, no `--skip-snapshot`, no `--reset-checkpoint` flag. If a user thinks they need one, the answer is "delete the Iceberg table and re-snapshot," not "rewind the slot." This shapes the CLI surface, the recovery design, and what `verify` means — the diff is only meaningful because there's no legitimate way the operator could have skipped data.
2. **IO seams from day one.** All IO, time, task spawning, and randomness go through traits owned by this codebase. Production impls wrap real runtimes; sim impls are in-process fakes driven by a seeded scheduler. Banned-call CI enforces this — see §7.
3. **The coordinator is the source of truth for durability.** `flushedLSN` advances only after the coord write commits in PG. Encode this in types: the function that calls `SetFlushedLSN` accepts a "coord-commit receipt" parameter, and that receipt can only be constructed by `Coordinator::claim_offsets`. The receipt's `flushable_lsn` is supplied by the *pipeline* (it knows what LSN it just staged), not by user input — keep the user-facing surface free of any "advance LSN" knob (see principle 1).
4. **Idempotent replay everywhere.** Recovery is "resume from last cursor / `restart_lsn`." `log_index` uses `(table_name, end_offset)` as PK; materializer fold is keyed by user PK; equality deletes are commutative under replay. Make this structurally true, not just observed.
5. **The sim is your model of Postgres.** It will be wrong. Differential testing against real PG is how you find out, and the bugs in your sim are wins.
6. **Verify is a product feature.** `pg2iceberg verify` diffs PG ground truth against Iceberg state. The Go code has only startup validation; the Rust port should ship a CLI verify subcommand from the start. Users will run it.

---

## 4. Workspace layout

Each crate's allowed deps are listed; CI enforces this.

```
pg2iceberg-core         — types only, zero IO deps
pg2iceberg-pg           — PgClient + ReplicationStream + WAL decoder traits + prod impl
pg2iceberg-coord        — Coordinator trait + PG impl (owns _pg2iceberg schema)
pg2iceberg-stream       — Staged Parquet writer/reader; CachedStream for combined mode
pg2iceberg-iceberg      — Catalog trait, TableWriter, schema evolution, vended creds, compaction
pg2iceberg-logical      — logical pipeline + materializer (depends on traits only)
pg2iceberg-query        — query-mode pipeline (depends on traits only)
pg2iceberg-snapshot     — initial snapshot (CTID-page chunked, parallel)
pg2iceberg-validate     — startup validation (the 8 checks from pipeline/validate.go)
pg2iceberg-sim          — sim impls: SimPostgres, sim Catalog, in-memory object store, fault injector
pg2iceberg-tests        — DST harness, property tests, differential tests
pg2iceberg              — binary: CLI, config, wiring; only place real impls meet
```

**Crate dependency rules:**
- `core` — no IO crates. No `tokio-postgres`, `reqwest`, `aws-sdk-*`, `iceberg::*`, `object_store`.
- `pg`, `coord`, `iceberg`, `stream` — depend on `core`; expose traits + prod impl.
- `logical`, `query`, `snapshot`, `validate` — depend on trait crates only, never on prod impls.
- `sim` — alternate impls of the same traits.
- `pg2iceberg` (bin) — only crate allowed to import every prod impl.

Layered this way, swapping `pg2iceberg-sim` for prod impls in tests is a wiring change, not a code change.

---

## 5. Key types and traits (sketch first)

Go file references in brackets are the behavior to mirror.

### 5a. Core types — no IO

```rust
// pg2iceberg-core/src/lsn.rs
#[derive(Copy, Clone, Eq, Ord, PartialEq, PartialOrd, Hash, Debug)]
pub struct Lsn(pub u64);

// pg2iceberg-core/src/event.rs  [postgres/event.go]
pub struct ChangeEvent {
    pub table: TableIdent,
    pub op: Op,
    pub lsn: Lsn,
    pub commit_ts: Timestamp,
    pub before: Option<Row>,
    pub after: Option<Row>,
    pub unchanged_cols: Vec<ColumnName>,
}

// pg2iceberg-core/src/staged.rs  [logical/events.go:20-22, iceberg/events_schema.go]
// FIXED Parquet schema for all staged files:
//   _op (i32), _lsn (u64), _ts (timestamptz), _unchanged_cols (list<string>),
//   _data (string, JSON of user row)
pub fn staged_event_schema() -> arrow::datatypes::SchemaRef;

pub struct MatEvent { /* parsed-once form for materializer */ }

// pg2iceberg-core/src/checkpoint.rs  [pipeline/checkpoint.go]
pub struct Checkpoint {
    pub mode: Mode,
    pub flushed_lsn: Lsn,
    pub snapshot_state: SnapshotState,
    pub query_watermarks: BTreeMap<TableIdent, String>,
    pub tracked_tables: Vec<TableIdent>,
}
```

### 5b. IO traits

```rust
// pg2iceberg-core/src/io.rs
#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn sleep(&self, d: Duration);
}

#[async_trait]
pub trait IdGen: Send + Sync {
    fn new_uuid(&self) -> Uuid;          // for marker rows, file names
    fn worker_id(&self) -> WorkerId;      // stable per process
}

// pg2iceberg-core re-exports object_store::ObjectStore — don't reinvent.

// pg2iceberg-pg
#[async_trait]
pub trait PgClient: Send + Sync {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<RowStream>;
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64>;
    async fn begin(&self) -> Result<Box<dyn PgTxn>>;
    async fn create_publication(&self, name: &str, tables: &[TableIdent]) -> Result<()>;
    async fn create_slot(&self, slot: &str) -> Result<Lsn>;
    async fn export_snapshot(&self) -> Result<SnapshotId>;     // for snapshot phase
    async fn start_replication(&self, slot: &str, start: Lsn) -> Result<Box<dyn ReplicationStream>>;
}

#[async_trait]
pub trait ReplicationStream: Send {
    async fn recv(&mut self) -> Result<DecodedMessage>;
    async fn send_standby(&mut self, flushed: Lsn, applied: Lsn) -> Result<()>;
}

// pg2iceberg-coord  [stream/coordinator_pg.go]
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Atomic: bump log_seq AND insert log_index in one PG txn.
    /// Returns a non-Clone receipt that proves the write committed.
    async fn claim_offsets(&self, claims: &[OffsetClaim]) -> Result<CoordCommitReceipt>;

    async fn read_log(&self, table: &TableIdent, after: u64, limit: usize) -> Result<Vec<LogEntry>>;
    async fn truncate_log(&self, table: &TableIdent, before: u64) -> Result<()>;

    async fn get_cursor(&self, group: &str, table: &TableIdent) -> Result<Option<u64>>;
    async fn advance_cursor(&self, group: &str, table: &TableIdent, to: u64) -> Result<()>;

    async fn register_consumer(&self, group: &str, worker: WorkerId, ttl: Duration) -> Result<()>;
    async fn active_consumers(&self, group: &str) -> Result<Vec<WorkerId>>;

    async fn load_checkpoint(&self) -> Result<Option<Checkpoint>>;
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<()>;
}

// The receipt enforces the durability invariant in the type system.
pub struct CoordCommitReceipt { pub(crate) flushed_lsn: Lsn }

// pg2iceberg-stream
#[async_trait]
pub trait StagedWriter: Send {
    async fn append(&mut self, evt: &ChangeEvent) -> Result<()>;
    async fn flush(&mut self) -> Result<StagedObject>;       // path + counts; ready to claim_offsets
}

#[async_trait]
pub trait StagedReader: Send + Sync {
    async fn read(&self, path: &ObjectPath) -> Result<Vec<MatEvent>>;
}

// pg2iceberg-iceberg
#[async_trait]
pub trait Catalog: Send + Sync {
    async fn ensure_namespace(&self, ns: &Namespace) -> Result<()>;
    async fn load_table(&self, ident: &TableIdent) -> Result<TableMetadata>;
    async fn create_table(&self, ident: &TableIdent, schema: &Schema, partition: &PartitionSpec) -> Result<TableMetadata>;
    async fn commit_snapshot(&self, ident: &TableIdent, prepared: PreparedCommit) -> Result<TableMetadata>;
    async fn evolve_schema(&self, ident: &TableIdent, changes: Vec<SchemaChange>) -> Result<TableMetadata>;
}
```

### 5c. The durability invariant in the type system

```rust
// only the Coordinator can mint a CoordCommitReceipt; SetFlushedLSN demands one.
impl LogicalSource {
    pub async fn set_flushed_lsn(&self, receipt: CoordCommitReceipt) { ... }
}
```

This makes "advance the slot before the coord write commits" a compile error, not a runtime concern. The whole point of the rewrite hinges on this — get it right in week 1.

---

## 6. Dependency choices

| Layer | Production | Simulation |
|---|---|---|
| Async runtime | `tokio` (or `madsim` if it intercepts cleanly) | `madsim` deterministic mode, **or** custom seeded executor if madsim falls short |
| Object storage | `object_store` (S3 backend) | `object_store::memory::InMemory` |
| PG queries | `tokio-postgres` | `SimPostgres` impl of `PgClient` |
| PG replication | `tokio-postgres`'s replication mode + manual `pgoutput` decoder, **or** `pgwire-replication` if its decoder is complete | `SimPostgres` emits `DecodedMessage` directly — no wire protocol in the sim |
| Iceberg | `iceberg` (iceberg-rust) | `SimCatalog` with in-memory metadata |
| HTTP (catalog auth, vended creds) | `reqwest` | madsim-intercepted, or sim catalog returns canned responses |
| Parquet/Arrow | `parquet`, `arrow` | same — pure compute |
| Property tests | `proptest` | — |
| Tracing | `tracing` | `tracing` test subscriber |
| Config | `serde`, `serde_yaml` | same |

**Audit before committing to a dep:** transitive thread spawning, real clock use, OS RNG, blocking IO. These escape determinism. The banned-calls CI check (§7) catches first-party violations; for transitive ones, build a small `cargo deny`-style allowlist as we go.

**Risk callouts:**
- **`madsim`**: untested with `tokio-postgres` replication mode. If it can't intercept the protocol, our sim path uses `SimPostgres` directly (it never speaks wire protocol anyway), and we use `madsim`'s mock-net only for the catalog/HTTP paths. Acceptable.
- **`iceberg-rust` gap audit (open question, see §10).**
- **TLS in tests**: prefer plaintext between testcontainers; native-tls/rustls are minor escapability hazards in a sim.

---

## 7. Banned practices, enforced in CI

`.github/workflows/ban-nondeterminism.yml` greps for the following outside allowlisted prod-impl crates (`pg2iceberg-pg`, `pg2iceberg-iceberg::s3`, `pg2iceberg`-bin):

- `SystemTime::`, `Instant::now`, `chrono::Utc::now`, `chrono::Local::now`
- `tokio::spawn`, `tokio::task::spawn_blocking`, `std::thread::spawn`
- `rand::thread_rng`, `Uuid::new_v4`, `getrandom`
- `HashMap` iteration where the result is observable (prefer `BTreeMap` / `IndexMap`)
- `rayon` parallel iterators on the pipeline hot path
- `#[tokio::test]` (all tests run under the DST harness or are pure-function property tests)

Replacements live in `pg2iceberg-core`: `Clock::now`, `IdGen::new_uuid`, `Spawner::spawn` (a thin trait around `tokio::spawn` whose sim impl uses the seeded scheduler).

---

## 8. Implementation order

Reordered from the previous draft to match what's actually risky in the Go architecture. **Bold** phases are correctness-critical; everything else is plumbing once those are right.

### Phase 0 — Skeleton (week 1)

- Workspace scaffold; all crates compile empty.
- `core` types and IO traits as in §5.
- CI: `cargo check`, `cargo clippy -D warnings`, `cargo test`, banned-calls grep.
- **Deliverable:** `cargo test` green, CI fails on a deliberately-introduced `SystemTime::now()`, no prod-impl deps yet.

### Phase 1 — **PG → Iceberg type mapping** (week 1–2)

This is where silent data loss hides. Do it with no IO.

- Mirror `postgres/schema.go:104-161` exactly. Include `serial`/`bigserial` aliasing.
- **Decimal edge cases:** `numeric(p≤38,s)` → `decimal(p,s)`; unconstrained `numeric` → `decimal(38,18)` with warning; `numeric(p>38)` → hard fail at startup, never silent truncation.
- **Timestamp:** microsecond precision preserved; `timestamp` (no tz) vs `timestamptz` distinct.
- **Time/timetz:** both → Iceberg `time`. Document that timetz timezone offset is dropped (matches Go).
- `json` and `jsonb` → `string`. Bytea → binary. UUID → uuid. Oid → int.
- Property tests: random PG value → Iceberg value → PG value, assert equality (or documented loss).

### Phase 2 — **Staged-event schema + StagedWriter/Reader** (week 2)

The fixed staged Parquet schema is the entire reason logical mode can keep slot-advance off the catalog path. Get it right early.

- Implement `staged_schema()` to match `iceberg/events_schema.go:30-42`. The exact contract:

  | Field ID | Name | Type | Null | Notes |
  |---|---|---|---|---|
  | 1 | `_op` | string | not null | `"I"`, `"U"`, `"D"` |
  | 2 | `_lsn` | int64 | not null | WAL position |
  | 3 | `_ts` | timestamptz (μs) | not null | PG commit time |
  | 4 | `_unchanged_cols` | string | nullable | comma-separated column names |
  | 5 | `_data` | string | not null | JSON-encoded user row |
  | 6 | `_xid` | int64 | nullable | PG transaction ID; null for snapshot rows |

- Field IDs are stored in Parquet column metadata under `PARQUET:field_id` so Iceberg readers (Go and Rust) resolve columns by ID, not name.
- The JSON inside `_data` is a separate compatibility surface from the Parquet schema. For now we use a serde-tagged Rust-internal form so round-trips are lossless. **Before differential testing (Phase 9)** switch to Go's "natural JSON" convention (numbers as JSON numbers, dates/timestamps/uuid as ISO strings, bytea as base64) so Rust- and Go-written staged files are mutually readable. Track as an explicit Phase 2.5 task.
- `RollingWriter`: thresholds on rows / bytes / time, named for offset range like Go does.
- `StagedReader`: parses staged Parquet → `Vec<MatEvent>` (pre-decoded, no JSON re-parse on materializer hot path).
- Property test: round-trip through staged Parquet preserves all `ChangeEvent` fields, including `unchanged_cols`.

### Phase 3 — **Coordinator (PG impl)** (week 2–3)

- DDL setup: mirror `stream/coordinator_pg.go:66-110`. Idempotent (`CREATE TABLE IF NOT EXISTS`).
- `claim_offsets`: single PG transaction that updates `log_seq` and inserts `log_index`. Returns the `CoordCommitReceipt` only on commit success.
- Cursor / consumer / lock methods.
- Integration test against real PG (testcontainers) for SQL correctness; **separate** from the DST harness.

#### Phase 3 status — Postgres-backed Coordinator wired

`pg2iceberg-coord/src/prod/PostgresCoordinator` implements every method
of the `Coordinator` trait against a real `tokio_postgres::Client`,
sharing `[crate::sql]` builders with the sim impl so wire SQL stays
consistent. 12 unit tests pass (SQL builders, table-key formatting,
duration→interval string conversion). What's wired:

- `connect()` opens a regular-mode (non-replication) `tokio_postgres`
  connection. The connection task is spawned on tokio and its
  `AbortHandle` is owned by the coordinator so drops are
  deterministic.
- `migrate()` runs `CREATE SCHEMA IF NOT EXISTS` plus the six
  `CREATE TABLE IF NOT EXISTS` statements. The `checkpoints` table
  was added to support `save_checkpoint` / `load_checkpoint` (single
  row, JSONB payload).
- `claim_offsets` runs the ensure-row → claim-sequence → insert-log-index
  sequence inside a single PG transaction. The `CoordCommitReceipt`
  is minted only after `tx.commit()` returns, encoding the durability
  invariant.
- All methods serialize through a `tokio::sync::Mutex<Client>` because
  `Client::transaction()` requires `&mut self`. In our use case all
  coord ops are sequential through the materializer cycle, so the
  serialization isn't a perf concern.

**Remaining items:**

1. **Testcontainers integration tests.** Spin up real PG, exercise
   the full coordinator surface (concurrent claim_offsets, expired
   consumers, lock contention, checkpoint round-trip). Same Docker
   setup as the rest of the prod path
   ([reference_integration_tests.md]).
2. **TLS.** Same `NoTls` placeholder as the other prod paths.
3. **Connection pool.** Single connection today; for high-throughput
   deployments we'll want a pool. Defer until we have profiling data.

### Phase 4 — **SimPostgres** (week 3–4)

The sim is where most DST mileage comes from. Build it before the prod logical pipeline.

- In-memory tables: `BTreeMap<TableOid, BTreeMap<PkValue, Row>>`.
- Append-only WAL: `Vec<WalRecord>`, each with assigned `Lsn`.
- Replication slots with `confirmed_flush_lsn` + `restart_lsn`. WAL recycling honors the slot.
- Transaction semantics: ops buffer, atomic commit producing `Begin` / events / `Commit` in WAL.
- DDL for `_pg2iceberg.*`: SimPostgres also hosts the coordinator schema (since they're the same DB).
- Implement the `_pg2iceberg.markers` fence-row pattern for snapshot↔CDC handoff.
- `ReplicationStream` impl: cursor over WAL from `restart_lsn`; updates `confirmed_flush_lsn` on standby messages.

### Phase 5 — **Logical pipeline** (week 4–5)

- WAL decoder: mirror `logical/decode.go:125-171`, including `'u'`-byte unchanged-toast handling.
- `tableSink` with per-tx buffer keyed by XID.
- Flush path: serialize via StagedWriter → `Coordinator::claim_offsets` → receive receipt → `set_flushed_lsn(receipt)` → next standby tick acks PG.
- Standby ticker: separate task, sends `confirmed_flush_lsn` periodically (Go: every `standby_interval`, default 10s).
- **Encode the durability invariant.** No code path advances `flushedLSN` without a `CoordCommitReceipt`. Verify by trying to write a violating implementation and confirm it's a compile error.

#### Phase 5 status — Postgres prod source wired

`pg2iceberg-pg/src/prod/` provides `PgClientImpl` + `ReplicationStreamImpl`
behind the `prod` feature, built on `tokio-postgres` +
`postgres-replication` (Supabase etl's fork — see workspace
`Cargo.toml`). 25 unit tests pass (typemap, value decode, LSN
parsing, helpers). What's wired:

- `PgClientImpl::connect(conn_str)` opens a logical-replication-mode
  client. All SQL goes through `simple_query`.
- `create_publication`, `create_slot` (returns consistent_point LSN),
  `slot_exists`, `slot_restart_lsn`, `export_snapshot`.
- `start_replication` opens `START_REPLICATION SLOT <s> LOGICAL <lsn>
  ("proto_version" '1', "publication_names" '<p>')` and wraps the
  resulting `LogicalReplicationStream`.
- `ReplicationStreamImpl` translates `LogicalReplicationMessage` →
  our `DecodedMessage`. Maintains a per-stream relation cache for
  decoding DML tuples; tracks current-txn `(final_lsn, xid,
  commit_ts)` so DML rows are tagged with the commit LSN.
- Text-mode tuple decoding via `prod/value_decode.rs`: bool, int2/4/8,
  float4/8, numeric (rust_decimal mantissa+scale), text, json/jsonb,
  bytea hex, date, time, timetz, timestamp, timestamptz, uuid.
- `prod/typemap.rs`: PG OID + type-modifier → our `PgType`. Tested
  against `postgres-types::Type` constants for all supported OIDs.

**Remaining items for the prod source path:**

1. **TLS.** `connect()` currently uses `NoTls`. Wire `tokio-postgres-rustls`
   (or `MakeTlsConnector`) for managed PG (Supabase, RDS, etc.).
2. **Testcontainers integration tests.** Spin up real PG, exercise the
   full path end-to-end. Needs Docker setup we already have a runbook
   for ([reference_integration_tests.md]).
3. **Stream-level translation tests** for the `LogicalReplicationMessage`
   → `DecodedMessage` mapping. Current tests cover decode helpers but
   not the routing logic; testcontainers covers it implicitly.
4. ~~Schema discovery query~~ — **DONE**.
   `pg2iceberg-pg/src/prod/discover.rs::discover_schema` queries
   `information_schema.columns` + `pg_index`/`pg_attribute` at startup,
   mirroring `postgres/schema.go::DiscoverSchema`. The binary calls it
   when a YAML table has no explicit `columns:` block. PK columns
   come from the table's PRIMARY KEY index by default; operators
   can override via `primary_key:` in YAML.

### Phase 6 — **DST harness** (week 5–6, can start in week 4)

- Workload generator (`proptest`): sequences of `Insert`/`Update`/`Delete`/`AddColumn`/`DropColumn`/`BeginTxn`/`CommitTxn`/`RollbackTxn`.
- Graceful crash + restart of the pipeline: drop in-memory state, rebuild from coord/slot durable state.
- Invariants 1–4 (durability subset; see §9), checked at quiescence.
- Flight recorder: on failure, dump seed + event trace.
- Pin failing seeds as regression tests.

After Phase 8 lands the materializer, fold it into the workload (interleaved `MaterializerCycle`) and add invariant 5 ("PG ground truth == Iceberg materialized state at quiescence").

### Phase 6.5 — **Fault injection layer** (deferred follow-on)

The Phase 6 harness only exercises *graceful* crashes (drop + rebuild after a clean drive/flush/ack). The actual fault paths — IO failure mid-flight — are uncovered. This phase fills that gap by wrapping the sim impls with a fault scheduler.

- `FaultyBlobStore` and `FaultyCoordinator` wrappers in `pg2iceberg-sim`. Each wraps the in-memory impl and consults an `Arc<Mutex<FaultSchedule>>` per call. When armed, the call returns a typed error matching the real-world failure mode (`StreamError::Io`, `CoordError::Pg`).
- `FaultSchedule` API: `fail_after_n_calls`, `fail_n_times`, `clear`. Tests can both pin specific scenarios and let the proptest generator randomize.
- New workload steps in DST: `FailNextBlobPut`, `FailNextCoordWrite`, `FailNextCatalogCommit`. Each arms a single failure; the next operation that touches that surface bounces with an error.
- New invariants under fault:
  - **6. Orphan blobs are harmless.** A blob put that succeeded but whose coord write failed is not referenced by `log_index`. Invariant 1 ("every coord row's blob exists") still holds. Invariant 4 (WAL ≡ staged) must be relaxed to "every WAL event ends up in the union of coord-referenced blobs once retries succeed."
  - **7. No stuck slot.** A coord write failure must leave `pipeline.flushed_lsn` unchanged (the receipt-gated invariant). The next successful flush must resume from the same point and produce the same LSN advance.
  - **8. No duplicate snapshot.** A catalog commit failure must leave `coord.cursor` unchanged. The next materializer cycle replays the same staged batch and produces an Iceberg snapshot whose post-MoR state is identical to what the failed commit would have produced.
- Mid-flush crash: pipeline crashes after blob `put` but before coord `claim_offsets`. Modelled by arming `FailNextCoordWrite` mid-flush + then crashing the pipeline harness. Recovery on restart should re-encode the same events into NEW blobs and claim those.
- Stall: a wrapper that delays a call (resolves after N "ticks" of `Clock::sleep`). Models slow standby ticker / slow materializer / slow catalog. Lets us verify nothing else is blocked.

Plan-§9 invariants 6–8 above are appended to §9 once this phase ships.

This is the highest-leverage P1 work after the materializer is operational — it turns the receipt-gated durability invariant from "structurally enforced" into "structurally enforced AND empirically proven under random IO failure."

### Phase 7 — **Iceberg `TableWriter`** (week 6–7)

- Equality-delete + data-file output (Go: `iceberg/tablewriter.go:130-265`).
- `FileIndex`: PK → file path mapping for TOAST resolution from prior data.
- Schema evolution (column add; column drop = soft drop nullable; type widening only).
- Vended-credentials per-table S3 client routing (Go: `iceberg/s3_vended_router.go`). Design as `ObjectStoreFactory: TableMetadata → Arc<dyn ObjectStore>` with a small TTL cache.
- Defer compaction.

### Phase 8 — **Materializer** (week 7–8)

- Cycle loop on `Clock::sleep` (default 10s).
- `FoldEvents`: dedupe by user PK; final state per PK with op + unchanged-col resolution against `FileIndex`.
- `TableWriter::Prepare` → `Catalog::commit_snapshot` → `Coordinator::advance_cursor`. Cursor advance only after catalog commit returns success.
- Distributed mode: `register_consumer` heartbeat, deterministic round-robin assignment from sorted `(workers, tables)` lists. No leader election.
- DST: stall, resume, crash mid-fold, verify idempotency.
- Combined-mode optimization: in-memory CachedStream for reads, falling back to S3 on miss (Go: `pipeline/checkpoint_cached.go` plus the in-memory cache around `Stream`).

### Phase 9 — **Differential testing** (week 8–9, runs nightly)

Same `proptest`-generated workload against:
1. SimPostgres + sim catalog + in-memory object store.
2. Real PG (testcontainers) + Iceberg REST + MinIO.
3. **Optional third arm:** the Go binary, same workload. If we can drive it with the same harness, we get a free oracle on questions about pgoutput / TOAST / numeric edge cases. The integration-test runbook at [reference_integration_tests.md] applies (Colima, `-p 1`, env vars).

After workload completes, run `pg2iceberg verify` on each. Discrepancies mean the sim is wrong, the port is wrong, or PG behaves differently than we modeled. All three are wins.

### Phase 10 — **Query mode** (week 9–10)

- Watermark poller (Go: `query/poller.go:84-137`): `SELECT * FROM t WHERE wm > $1 ORDER BY wm LIMIT N`.
- Per-table buffer with PK dedupe; flush via the same `TableWriter` as logical materializer.
- Snapshot phase via `pg_export_snapshot` (Phase 11).
- Reuse the DST harness — same sim PG.

### Phase 11 — Snapshot phase (week 10)

CTID-page chunked, parallel; marker-row fence to PG WAL stream so logical-mode handoff is exact (Go: `logical/logical.go:507-519`, `snapshot/`).

### Phase 12 — Validation + verify CLI (week 10–11)

- Mirror the 8 startup checks from `pipeline/validate.go:36-136`.
- `pg2iceberg verify --table t`: open `pg_export_snapshot`, read PG ground truth, read Iceberg at the snapshot ID committed at that LSN, report row-count + per-PK diffs + type anomalies. Must be production-ready, not a test helper.

### Phase 13 — Operational hardening (week 11–12)

- Online invariant metrics (continuous, not just DST):
  - `flushed_lsn ≤ max(end_offset_committed_in_log_index_for_corresponding_lsns)`
  - `mat_cursor[t] ≤ max(end_offset)` in `log_index[t]`
  - `flushed_lsn` monotonic per process restart (modulo known recovery rewind)
- Graceful shutdown: drain buffer → flush → coord write → final standby → exit.
- Crash test: real `SIGKILL` mid-flush, restart, `verify` shows no loss.

#### Phase 13 status — iceberg-rust prod catalog wired (forked)

`pg2iceberg-iceberg/src/prod/catalog.rs::IcebergRustCatalog<C>` wraps any
`iceberg::Catalog` (Memory, REST, Glue, SQL, S3Tables, HMS) behind our
`Catalog` trait. **All trait methods are wired end-to-end:** namespace +
table CRUD, append-only commits, equality-delete commits, schema
evolution, and snapshot reads. 25 prod tests pass against
`iceberg::memory::MemoryCatalog`. See
`crates/pg2iceberg-iceberg/src/prod/gap_audit.rs` for the full
method-by-method status.

The workspace pins `iceberg` to `polynya-dev/iceberg-rust` (branch
`polynya-patches`, based on upstream `v0.9.0`) via `[patch.crates-io]`.
The fork carries three minimal patches we'll cut PRs for upstream:

1. `TransactionAction` trait + `BoxedTransactionAction` flipped from
   `pub(crate)` to `pub`. Lets downstream crates author custom actions.
2. New `UpdateSchemaAction` (and `Transaction::update_schema()`
   convenience). Takes a target `Schema`, emits `AddSchema` +
   `SetCurrentSchema(-1)` with three guarding requirements
   (`UuidMatch`, `CurrentSchemaIdMatch`, `LastAssignedFieldIdMatch`).
3. `FastAppendAction.add_data_files()` routes by `content_type()` into
   data vs delete buckets; `SnapshotProducer` writes them to separate
   manifests (`build_v{2,3}_data` vs `build_v{2,3}_deletes`). The old
   "Only data content type is allowed" check is gone. Same approach
   RisingWave's fork uses.

Carry-forward into binary wiring: `IcebergRustCatalog::new(Arc<C>)`
accepts any concrete catalog, so swapping Memory → REST → Glue is a
config-only change at the binary layer.

Optional future optimization: RisingWave's `DeltaWriter` uses position
deletes for in-batch self-cancellation (insert+delete same PK in one
batch → cheaper position delete on the just-written row, instead of an
equality delete on prior data). Equality-delete-only is correct; this
is purely a read-perf knob. Revisit if profiling shows in-batch churn.

### Phase 14 — Compaction + maintenance (week 12+)

Ports `iceberg/compact.go`, `iceberg/maintain.go`. Note the existing partition bug (deferred, [project_compaction_partition_bug.md]) — do not re-introduce.

---

## Binary status — `pg2iceberg` CLI wired

`crates/pg2iceberg` now produces a real binary that assembles all four
prod surfaces. CLI surface (clap-based):

- `pg2iceberg connect-pg --config <toml>` — opens a replication-mode
  connection via `PgClientImpl`, reports slot existence + restart_lsn.
- `pg2iceberg connect-iceberg --config <toml>` — opens an Iceberg
  catalog (memory only today; REST/Glue/SQL/HMS are follow-ons),
  pre-creates configured namespaces.
- `pg2iceberg migrate-coord --config <toml>` — runs
  `PostgresCoordinator::migrate()` (idempotent).
- `pg2iceberg run --config <toml>` — assembles `Pipeline` (with
  `PostgresCoordinator`, `ObjectStoreBlobStore<InMemory>`, UUID-based
  blob namer) plus a `Materializer<IcebergRustCatalog<MemoryCatalog>>`,
  starts replication from the slot's confirmed_flush_lsn, drives the
  `Ticker` with default 10s flush/standby/materialize cadence, and
  exits cleanly on SIGINT/SIGTERM.

`src/realio.rs` provides the wall-clock `Clock`, `Uuid::new_v4()`-backed
`IdGen`, and `tokio::spawn`-backed `Spawner`. The ban-script's allowlist
extends to `crates/pg2iceberg/src/` so non-determinism is contained
to the binary's prod glue.

`src/config.rs` defines the TOML schema: `[pg]`, `[coord]`,
`[iceberg]`, `[blob]`, `[[table]]` with per-column type declarations.
4 unit tests cover type-name parsing, schema construction with PK
detection + auto field-id assignment, and TOML round-trip.

**Status — P0 production blockers now wired (config matches Go YAML):**

The binary's config is YAML and **mirrors the Go reference's shape exactly**
(`tables`, `source.{postgres, logical, query}`, `sink`, `state`).
Operators can drop their existing `pg2iceberg.yaml` in unchanged.
See `config.example.yaml` for the full surface.

- **TLS** — `pg2iceberg-pg/prod` and `pg2iceberg-coord/prod` both
  expose a `TlsMode { Disable, Webpki }` enum and a `connect_with`
  variant. Webpki uses `tokio-postgres-rustls` 0.13 with rustls's
  ring crypto provider against Mozilla's `webpki-roots` bundle.
  Driven from `source.postgres.sslmode` (`"disable"` →
  `TlsMode::Disable`; everything else → `TlsMode::Webpki`).
  mTLS / custom CA / `verify-ca` vs `verify-full` differentiation
  is a follow-on.
- **Iceberg REST catalog** — `iceberg-catalog-rest = "0.9"` (from
  the same `polynya-dev/iceberg-rust` fork via `[patch.crates-io]`).
  Driven from `sink.{catalog_uri, catalog_auth, catalog_token,
  catalog_client_id, catalog_client_secret, warehouse}` plus a
  free-form `sink.catalog_props` passthrough for vendor quirks.
  Covers Polaris, Tabular, Snowflake-managed-catalog, and the
  open-source Iceberg REST reference. Other catalog flavors
  (Glue/SQL/HMS/S3Tables) are follow-ons.
- **S3 object store** — `object_store` 0.11 with `aws` feature on.
  Driven from `sink.credential_mode` (`"static"` / `"iam"` /
  `"vended"`).
  - `"static"` builds an `AmazonS3` with `s3_endpoint` +
    `s3_access_key` + `s3_secret_key` + `s3_region`. Bucket and
    prefix are parsed from `s3://bucket/prefix/...` in
    `sink.warehouse`. Path-style addressing for non-AWS
    compatibility (MinIO, LocalStack, R2).
  - `"iam"` builds via `AmazonS3Builder::from_env()` so the
    standard AWS chain (env vars, instance profile, AWS SSO)
    applies.
  - `"vended"` errors with "not yet wired" — that's the Phase 7
    vended-credentials S3 router.

**Iceberg partition spec — done:**

- **Parsing + create-table:** YAML
  `tables[].iceberg.partition: ["day(col)", "bucket[16](id)", ...]`
  parses through `pg2iceberg_core::parse_partition_spec` (all six
  transforms: identity, year, month, day, hour, bucket[N], truncate[W]).
  `IcebergRustCatalog::create_table` translates to
  `iceberg::spec::UnboundPartitionSpec`. `load_table` round-trips
  the spec back through `from_iceberg_schema`.
- **Per-partition file routing.** `TableWriter::prepare` groups
  folded rows by their per-row partition tuple and emits one parquet
  chunk per `(partition_tuple, kind)` group. The materializer
  uploads each chunk to a unique blob path and constructs a
  `DataFile` carrying `partition_values: Vec<PartitionLiteral>`.
  The catalog translates the per-file values to an
  `iceberg::spec::Struct` at `commit_snapshot` time. Snapshots
  round-trip back through `iceberg_struct_to_partition_literals`.
- **Transform application.** `apply_transform` handles all six
  transforms over `PgValue`: identity, year, month, day, hour,
  bucket[N] (via murmur3_x86_32 per iceberg spec appendix B,
  `(hash & i32::MAX) % N`), and truncate[W] (integer floor toward
  -inf, first W code points for strings, first W bytes for binary,
  unscaled-i128 floor for decimals — iceberg's 38-digit precision cap
  fits in i128, so no big-int dep needed).
- **Delete partition resolution (three-tier, mirrors Go).**
  - Tier 1 — **row-direct**: read partition cols off the row.
    Always works for `Insert`/`Update`; works for `Delete` iff
    every partition source column is in the PK *or* the source
    has `REPLICA IDENTITY FULL`.
  - Tier 2 — **FileIndex lookup**: the materializer's `FileIndex`
    carries `path → partition_values` per data file (populated on
    each commit and on `rebuild_from_catalog`). For a `Delete`
    whose row lacks partition cols, look up the prior data file by
    PK and reuse its partition tuple. Equivalent to Go's
    `ExtractPartBucketKey` + `ParsePartitionPath`
    ([iceberg/tablewriter.go:230-244](../pg2iceberg/iceberg/tablewriter.go#L230-L244))
    but with structured values per file instead of hive-path
    mining, so foreign engines that write with non-hive paths
    still round-trip.
  - Tier 3 — **error**: PK isn't in the FileIndex (transient on a
    fresh process before `rebuild_from_catalog` finishes; an old
    cycle replaying after compaction). Surfaces as
    `WriterError::DeletePartitionUnresolved` rather than Go's
    silent drop, so the failure is visible in logs/metrics.

**Remaining items for the binary (now P0.5 / P1):**

1. **Glue / SQL / S3Tables / HMS catalog backends** — variants of
   `IcebergConfig`. Same dispatch pattern as Memory/REST.
2. **GCS + Azure object store backends** — feature-gate
   `object_store/gcp` and `object_store/azure`, add config variants.
3. **Vended-credentials S3 router** — Plan §Phase 7. Per-table S3
   client routing for Polaris/Tabular tables that vend creds at
   `loadTable` time. `IcebergRustCatalog` already exposes
   per-table config maps; needs a router on the blob-store side.
4. **Verify + validate subcommands** — `pg2iceberg-validate` and the
   verifier in `pg2iceberg-iceberg::verify` aren't exposed in the
   CLI yet.
5. **Invariant watcher task** — `pg2iceberg-validate::watcher`
   exists but the binary's `Handler::Watcher` arm is a no-op.
6. **Testcontainers integration** — boot real PG + Iceberg REST
   reference + MinIO; exercise `connect-pg` / `migrate-coord` / a
   short `run` end-to-end. Existing `reference_integration_tests.md`
   has the Colima setup.
7. **mTLS / custom CA / channel binding** — `TlsMode` is currently
   `Disable | Webpki`. Add `CustomCa { path }` and `Mtls { ... }`
   when a deployment needs them.

---

## 9. Invariants (the contract to prove)

DST and the production-runtime metric checker both assert these. Wording matters; phrase as predicates, not prose.

1. **Slot LSN never ahead of durable coord state.**
   `confirmed_flush_lsn ≤ max(log_index.end_offset_lsn_for(t)) for all t` at all times.
2. **Coord state never references missing staged objects.**
   For every `log_index` row, `s3_path` exists in object storage.
3. **Slot LSN monotonic across restarts** (modulo a documented recovery rewind to `restart_lsn`).
4. **No lost commits.** For every PG transaction with `commit_lsn ≤ confirmed_flush_lsn`, every row of that txn appears in some staged object referenced by `log_index`.
5. **No phantom commits.** Every event row in a staged object corresponds to a committed PG transaction.
6. **Idempotent replay.** Forcing reconnect at any LSN and re-consuming produces identical final coord state and identical materialized Iceberg state.
7. **Materializer cursor monotonic and ≤ committed offsets.** `mat_cursor[t] ≤ max(log_index.end_offset for t with seen_in_committed_iceberg_snapshot)`.
8. **Materialized table state = fold of staged events up to cursor.** For every table, replaying staged events from offset 0 to `mat_cursor[t]` yields the current Iceberg state.
9. **Type round-trip.** PG value → staged → materialized → readback equals original (where spec allows; precision loss is configured failure, not silent truncation).
10. **Schema evolution monotonicity.** Column add additive; column drop = soft-drop (nullable retained); precision-losing type changes refused.
11. **Eventual consistency at quiescence.** After workload ends and pipeline catches up, Iceberg materialized state per-table equals PG ground truth.
12. **Worker assignment determinism.** Given the same `(active_workers, tables)` set, every worker computes the same partition.

---

## 10. Open questions to resolve before deep coding

1. **`iceberg-rust` gap audit.** ✅ done — see
   `crates/pg2iceberg-iceberg/src/prod/gap_audit.rs`. Result: append-only
   commits + namespace/table CRUD + snapshot reads work end-to-end via
   `IcebergRustCatalog<C>`. Equality-delete commits are upstream-blocked
   in 0.9 (no public action accepts non-Data content; `TableCommit::builder`
   is `pub(crate)`); schema evolution wiring is deferred until DDL handling.
2. **Replication protocol crate.** `pgwire-replication` vs hand-rolled `pgoutput` decoder over `tokio-postgres`'s replication mode. Spike both with a 50-row publication to compare. Decision criterion: does the crate handle truncate, relation, message, type, origin? If gaps, hand-roll.
3. **`madsim` interception coverage.** Run a hello-world test that uses `tokio-postgres` and `reqwest` under `madsim`. If either escapes, decide: (a) use a custom seeded executor for those paths, (b) require all PG/HTTP go through trait boundaries (which we already do — sim bypasses the wire protocol entirely), (c) fork.
4. **TOAST handling on UPDATE.** Go preserves `unchanged_cols` and resolves at materialize time via FileIndex. Match exactly. Failure mode if FileIndex miss (e.g., compaction removed the prior file): re-read source row? Refuse the update? Document the chosen mode and DST-test it.
5. **Marker UUID / file-name UUID determinism.** Funnel through `IdGen` so DST is reproducible. Stable ordering of `file_name = base64(claim_id) + ".parquet"` rather than random suffixes is acceptable too.
6. **Cross-table txn atomicity.** A single PG txn that touches two tables produces two staged objects + two `log_index` rows in one PG txn (atomic w.r.t. the coordinator). Materialized into Iceberg, those become two per-table catalog commits — not atomic across tables. Document this as a known weakening, decide if a "txn fence" abstraction is needed for users with cross-table consistency requirements.
7. **Combined-mode in-memory cache.** Is the cache eviction policy correct under backpressure (materializer falling behind WAL)? Property: in combined mode, materializer must *prefer* cache but fall back to S3 reads without correctness loss.

---

## 11. What to delete from `pg2iceberg-rust/` first

`pg2iceberg-rust/` is currently a Go-source copy. The Rust workspace will replace it. Delete in order:

```
benchmark/        clickhouse/       cmd/            config/
docs/             example/          iceberg/        iceberg-rest/
logical/          pipeline/         postgres/       query/
scripts/          snapshot/         stream/         tests/
tracing/          utils/

Dockerfile        LICENSE           README.md       config.example.yaml
docker-compose.yml go.mod           go.sum          zensical.toml
```

Keep:
- `pg2iceberg-rust-port-plan.md` (this doc)
- `.git` (stay on the `rustify` branch)

Replace with: a Cargo workspace skeleton per §4. README will be re-written when there's actually a Rust thing to describe.

The Go reference at `../pg2iceberg/` stays untouched and is what the differential test harness in Phase 9 runs against.

---

## 12. First-session deliverables

End of first session:
1. `pg2iceberg-rust/` purged of Go sources; Cargo workspace skeleton in place.
2. `pg2iceberg-core` types and IO traits compile (per §5).
3. CI (`cargo check` + `clippy -D warnings` + `test` + banned-calls grep) green.
4. Phase 1 type-mapping module with property tests covering int / text / bool / timestamp / decimal happy-path. Numeric-precision-too-large rejection is a hard error, not a warning.
5. No prod IO deps yet — no `tokio-postgres`, no `reqwest`, no `iceberg`, no `aws-sdk-*`.

Success criterion: `cargo test` passes ~20 property tests; CI fails when a `SystemTime::now()` is added to `core`; trait surfaces compile without prod impls.
