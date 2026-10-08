//! Observability surface. Production records into a [`Registry`] the
//! binary serves in the Prometheus text format; the sim records into an
//! in-memory recorder that tests can introspect.
//!
//! Kept narrow on purpose — three primitive shapes (counter, gauge,
//! histogram) cover everything pg2iceberg needs to emit. Avoiding a wider
//! API keeps the prod impl small and the sim impl simple.
//!
//! Every metric pg2iceberg emits is declared once, in [`CATALOG`], with
//! its kind, labels and help text: [`names`] holds the names call sites
//! use, the registry takes each one's `# HELP` and `# TYPE` from it, and
//! debug builds refuse a `pg2iceberg_` metric it doesn't declare.

use crate::io::{Clock, Timestamp};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// Static metric labels. Sorted by key for stable serialization.
pub type Labels = BTreeMap<String, String>;

/// Internal recorder key: `(metric_name, sorted_(label_key, label_value)_pairs)`.
type MetricKey = (String, Vec<(String, String)>);

pub trait Metrics: Send + Sync {
    /// Monotonic counter; only ever increments. The sim accumulates
    /// per-(name, labels) totals.
    fn counter(&self, name: &str, labels: &Labels, delta: u64);

    /// Point-in-time value. Last write wins.
    fn gauge(&self, name: &str, labels: &Labels, value: f64);

    /// Distribution sample. Every histogram pg2iceberg emits is a
    /// duration in seconds ([`DURATION_BUCKETS`]). Sim stores raw
    /// observations so tests can read percentiles.
    fn histogram(&self, name: &str, labels: &Labels, value: f64);
}

/// `Labels` from `(key, value)` pairs.
pub fn labels<const N: usize>(pairs: [(&str, &str); N]) -> Labels {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Seconds from `start` to `end`; never negative (a wall clock can step
/// back).
pub fn seconds_between(start: Timestamp, end: Timestamp) -> f64 {
    end.0.saturating_sub(start.0).max(0) as f64 / 1_000_000.0
}

/// Set an enumeration gauge: `name{key=<state>}` to 1 for `current`, 0
/// for the other `states`, so exactly one series reads 1.
pub fn set_state(metrics: &dyn Metrics, name: &str, key: &str, states: &[&str], current: &str) {
    for state in states {
        let value = if *state == current { 1.0 } else { 0.0 };
        metrics.gauge(name, &labels([(key, state)]), value);
    }
}

/// What a pg2iceberg process is doing, as [`names::PHASE`] reports it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Connecting, validating, setting up the slot and tables.
    Starting,
    /// Copying the tables' existing rows.
    Snapshotting,
    /// Replicating, materializing, or both.
    Running,
    /// Draining before exit.
    Stopping,
}

impl Phase {
    pub const ALL: [Phase; 4] = [
        Phase::Starting,
        Phase::Snapshotting,
        Phase::Running,
        Phase::Stopping,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Snapshotting => "snapshotting",
            Phase::Running => "running",
            Phase::Stopping => "stopping",
        }
    }

    /// Record `self` as the process's phase.
    pub fn set(self, metrics: &dyn Metrics) {
        let states = Phase::ALL.map(Phase::as_str);
        set_state(metrics, names::PHASE, "phase", &states, self.as_str());
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// One metric pg2iceberg emits.
#[derive(Debug)]
pub struct MetricDef {
    pub name: &'static str,
    pub kind: MetricKind,
    /// Every series carries exactly these label keys.
    pub labels: &'static [&'static str],
    /// The `# HELP` line: one sentence.
    pub help: &'static str,
}

/// The declaration of `name`, if [`CATALOG`] has one.
pub fn definition(name: &str) -> Option<&'static MetricDef> {
    CATALOG.iter().find(|d| d.name == name)
}

/// Declares each metric once: a constant in [`names`] and an entry in
/// [`CATALOG`].
macro_rules! catalog {
    ($( $id:ident: $kind:ident $name:literal [$($label:literal),*] $help:literal; )*) => {
        /// Standard metric names emitted across the workspace. Centralized
        /// so renames don't drift between emitters and asserters.
        pub mod names {
            $( #[doc = $help] pub const $id: &str = $name; )*
        }

        /// Every metric pg2iceberg emits.
        pub const CATALOG: &[MetricDef] = &[
            $( MetricDef {
                name: $name,
                kind: MetricKind::$kind,
                labels: &[$($label),*],
                help: $help,
            }, )*
        ];
    };
}

catalog! {
    // ── Process ──────────────────────────────────────────────────────
    BUILD_INFO: Gauge "pg2iceberg_build_info" ["version", "revision"]
        "Always 1; the labels name the running build.";
    PHASE: Gauge "pg2iceberg_phase" ["phase"]
        "1 for what the process is doing (starting, snapshotting, running, stopping), 0 for the rest.";
    LAST_SUCCESS: Gauge "pg2iceberg_last_success_timestamp_seconds" ["stage"]
        "When a stage last completed (flush, ack, materialize, watch), in Unix seconds.";

    // ── Replication slot ─────────────────────────────────────────────
    REPLICATION_LAG: Gauge "pg2iceberg_replication_lag_bytes" []
        "WAL the source has written past the replication slot's confirmed position, in bytes.";
    SLOT_RETAINED_WAL: Gauge "pg2iceberg_slot_retained_wal_bytes" []
        "WAL the replication slot keeps the source from removing (its WAL position minus the slot's restart_lsn), in bytes.";
    SLOT_SAFE_WAL_SIZE: Gauge "pg2iceberg_slot_safe_wal_size_bytes" []
        "WAL the source can still write before the slot passes max_slot_wal_keep_size, in bytes; absent when unlimited.";
    SLOT_WAL_STATUS: Gauge "pg2iceberg_slot_wal_status" ["status"]
        "1 for the replication slot's wal_status (reserved, extended, unreserved, lost), 0 for the rest.";
    SLOT_ACK_LAG: Gauge "pg2iceberg_slot_ack_lag_bytes" []
        "WAL staged but not yet acked to the replication slot, in bytes: the next ack's to make.";
    REPLICATION_RECONNECTS_TOTAL: Counter "pg2iceberg_replication_reconnects_total" ["outcome"]
        "Attempts to reopen the replication stream after it dropped, by outcome (ok, error).";
    REPLICATION_BUFFERED_MESSAGES: Gauge "pg2iceberg_replication_buffered_messages" []
        "Decoded replication messages waiting for the main loop; near 1000 the main loop is the bottleneck.";

    // ── Staging ──────────────────────────────────────────────────────
    PIPELINE_CHANGES_TOTAL: Counter "pg2iceberg_pipeline_changes_total" ["table", "op"]
        "Row changes received from the replication stream, by operation (insert, update, delete, truncate).";
    PIPELINE_TRANSACTIONS_TOTAL: Counter "pg2iceberg_pipeline_transactions_total" []
        "Source transactions received from the replication stream.";
    PIPELINE_FLUSH_TOTAL: Counter "pg2iceberg_pipeline_flush_total" []
        "Flushes that staged changes (or advanced the flushed LSN) and recorded them in the coordinator.";
    PIPELINE_ROWS_STAGED_TOTAL: Counter "pg2iceberg_pipeline_rows_staged_total" ["table"]
        "Rows staged as Parquet in the object store, snapshot rows included.";
    PIPELINE_STAGED_BYTES_TOTAL: Counter "pg2iceberg_pipeline_staged_bytes_total" ["table"]
        "Bytes of staged Parquet uploaded.";
    PIPELINE_FLUSHED_LSN: Gauge "pg2iceberg_pipeline_flushed_lsn" []
        "Highest source LSN staged and recorded in the coordinator.";

    // ── Materializing ────────────────────────────────────────────────
    MATERIALIZER_CYCLE_TOTAL: Counter "pg2iceberg_materializer_cycle_total" ["table"]
        "Times the materializer looked for staged rows to commit to a table.";
    MATERIALIZER_ROWS_TOTAL: Counter "pg2iceberg_materializer_rows_total" ["table"]
        "Rows the materializer wrote to Iceberg, deletes included: each key once per step it changed in.";
    MATERIALIZER_COMMITS_TOTAL: Counter "pg2iceberg_materializer_commits_total" ["table"]
        "Iceberg commits of materialized rows.";
    MATERIALIZER_FILES_TOTAL: Counter "pg2iceberg_materializer_files_written_total" ["table", "kind"]
        "Files the materializer's commits added, by kind (data, delete).";
    MATERIALIZER_BYTES_TOTAL: Counter "pg2iceberg_materializer_bytes_written_total" ["table"]
        "Bytes of data and delete files the materializer's commits added.";
    MATERIALIZER_COMMIT_DURATION: Histogram "pg2iceberg_materializer_commit_duration_seconds" []
        "How long an Iceberg commit of materialized rows took.";
    MATERIALIZER_CYCLE_DURATION: Histogram "pg2iceberg_materializer_cycle_duration_seconds" []
        "How long a materializer cycle over every assigned table took.";
    MATERIALIZER_CYCLE_FAILURES_TOTAL: Counter "pg2iceberg_materializer_cycle_failures_total" []
        "Materializer cycles that failed.";
    MATERIALIZER_BACKLOG_ROWS: Gauge "pg2iceberg_materializer_backlog_rows" ["table"]
        "Rows staged but not yet materialized.";
    MATERIALIZER_SOURCE_TIMESTAMP: Gauge "pg2iceberg_materializer_source_commit_timestamp_seconds" ["table"]
        "Source commit time of the newest change materialized, in Unix seconds; with a backlog, time() minus it is how stale the table is.";
    UNFILLED_COLUMN_DEFAULTS: Counter "pg2iceberg_unfilled_column_defaults_total" ["table", "column", "reason"]
        "Columns added with a default Postgres no longer had for the rows already in the table, which read NULL in Iceberg, by reason (not_stored, column_gone).";
    DISTRIBUTED_WORKERS: Gauge "pg2iceberg_distributed_workers" []
        "Materializer workers in this worker's consumer group.";
    DISTRIBUTED_ASSIGNED_TABLES: Gauge "pg2iceberg_distributed_assigned_tables" []
        "Tables assigned to this materializer worker.";

    // ── Compaction ───────────────────────────────────────────────────
    COMPACTION_RUNS_TOTAL: Counter "pg2iceberg_compaction_runs_total" ["table", "outcome"]
        "Compactions of a table that rewrote files, or failed (outcome rewritten, failed).";
    COMPACTION_FILES_REWRITTEN_TOTAL: Counter "pg2iceberg_compaction_files_rewritten_total" ["table"]
        "Data and delete files compaction replaced.";
    COMPACTION_DURATION: Histogram "pg2iceberg_compaction_duration_seconds" []
        "How long a compaction of one table took.";

    // ── Invariants ───────────────────────────────────────────────────
    INVARIANT_VIOLATIONS_TOTAL: Counter "pg2iceberg_invariant_violations_total" ["invariant"]
        "Runtime invariant violations the watcher observed.";

    // ── Requests ─────────────────────────────────────────────────────
    CATALOG_REQUEST_DURATION: Histogram "pg2iceberg_catalog_request_duration_seconds" ["op"]
        "How long an Iceberg catalog request took, by operation.";
    CATALOG_REQUEST_ERRORS_TOTAL: Counter "pg2iceberg_catalog_request_errors_total" ["op", "kind"]
        "Iceberg catalog requests that failed, by operation and kind (conflict, not_found, other).";
    BLOB_REQUEST_DURATION: Histogram "pg2iceberg_blob_request_duration_seconds" ["op"]
        "How long an object store request took, by operation.";
    BLOB_REQUEST_ERRORS_TOTAL: Counter "pg2iceberg_blob_request_errors_total" ["op"]
        "Object store requests that failed, by operation.";
    BLOB_BYTES_TOTAL: Counter "pg2iceberg_blob_bytes_total" ["op"]
        "Bytes uploaded (put) to and downloaded (get) from the object store.";
    COORD_REQUEST_DURATION: Histogram "pg2iceberg_coord_request_duration_seconds" ["op"]
        "How long a coordinator request took, by operation.";
    COORD_REQUEST_ERRORS_TOTAL: Counter "pg2iceberg_coord_request_errors_total" ["op"]
        "Coordinator requests that failed, by operation.";
}

/// In debug builds, refuse a `pg2iceberg_` metric [`CATALOG`] doesn't
/// declare as `kind` with exactly `labels`' keys.
fn debug_check(name: &str, kind: MetricKind, labels: &Labels) {
    if !cfg!(debug_assertions) || !name.starts_with("pg2iceberg_") {
        return;
    }
    let def = definition(name).unwrap_or_else(|| panic!("metric {name} isn't in CATALOG"));
    assert_eq!(def.kind, kind, "metric {name} is declared a {:?}", def.kind);
    let keys: Vec<&str> = labels.keys().map(String::as_str).collect();
    let mut declared = def.labels.to_vec();
    declared.sort_unstable();
    assert_eq!(keys, declared, "metric {name}'s labels");
}

/// Upper bounds, in seconds, of every duration histogram's buckets.
pub const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// The production recorder: keeps each series' current value for
/// [`Registry::render`] to expose, and when anything was last recorded —
/// the liveness signal ([`Registry::idle_for`]).
///
/// pg2iceberg records only on finishing work: a request to the catalog,
/// object store or coordinator, a handler tick, a commit. Nothing records
/// on a timer of its own. So a registry nothing has been recorded to for
/// a while is a process that's stuck, not idle: the watcher alone records
/// every 30 seconds while the main loop turns.
pub struct Registry {
    clock: Arc<dyn Clock>,
    families: Mutex<BTreeMap<String, Family>>,
    /// [`Clock::now`] micros at the last record, or at creation.
    last_record: AtomicI64,
}

struct Family {
    kind: MetricKind,
    series: BTreeMap<Labels, Series>,
}

enum Series {
    Counter(u64),
    Gauge(f64),
    Histogram(Histogram),
}

struct Histogram {
    /// Observations per bucket of [`DURATION_BUCKETS`] — not cumulative;
    /// one past the last bound only counts toward `count`.
    buckets: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Registry {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let now = clock.now().0;
        Self {
            clock,
            families: Mutex::new(BTreeMap::new()),
            last_record: AtomicI64::new(now),
        }
    }

    /// How long since anything was recorded (or since creation).
    pub fn idle_for(&self) -> Duration {
        let micros = self
            .clock
            .now()
            .0
            .saturating_sub(self.last_record.load(Ordering::SeqCst))
            .max(0);
        Duration::from_micros(micros as u64)
    }

    /// The phase last recorded ([`Phase::set`]).
    pub fn phase(&self) -> Option<Phase> {
        Phase::ALL
            .into_iter()
            .find(|p| self.gauge_value(names::PHASE, &labels([("phase", p.as_str())])) == Some(1.0))
    }

    pub fn counter_value(&self, name: &str, labels: &Labels) -> u64 {
        match self.lock().get(name).and_then(|f| f.series.get(labels)) {
            Some(Series::Counter(v)) => *v,
            _ => 0,
        }
    }

    pub fn gauge_value(&self, name: &str, labels: &Labels) -> Option<f64> {
        match self.lock().get(name).and_then(|f| f.series.get(labels)) {
            Some(Series::Gauge(v)) => Some(*v),
            _ => None,
        }
    }

    /// Observations of a histogram series.
    pub fn histogram_count(&self, name: &str, labels: &Labels) -> u64 {
        match self.lock().get(name).and_then(|f| f.series.get(labels)) {
            Some(Series::Histogram(h)) => h.count,
            _ => 0,
        }
    }

    /// Every series of `name`, with its labels.
    pub fn series(&self, name: &str) -> Vec<Labels> {
        self.lock()
            .get(name)
            .map(|f| f.series.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The names recorded so far.
    pub fn names(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// Every series in the Prometheus text exposition format (0.0.4).
    pub fn render(&self) -> String {
        let families = self.lock();
        let mut out = String::new();
        for (name, family) in families.iter() {
            let help = definition(name).map_or("", |d| d.help);
            let _ = writeln!(out, "# HELP {name} {}", escape_help(help));
            let _ = writeln!(out, "# TYPE {name} {}", family.kind.as_str());
            for (labels, series) in &family.series {
                match series {
                    Series::Counter(v) => sample(&mut out, name, labels, None, &v.to_string()),
                    Series::Gauge(v) => sample(&mut out, name, labels, None, &format_value(*v)),
                    Series::Histogram(h) => {
                        let bucket = format!("{name}_bucket");
                        let mut cumulative = 0;
                        for (le, n) in DURATION_BUCKETS.iter().zip(&h.buckets) {
                            cumulative += n;
                            let le = format_value(*le);
                            sample(
                                &mut out,
                                &bucket,
                                labels,
                                Some(&le),
                                &cumulative.to_string(),
                            );
                        }
                        sample(
                            &mut out,
                            &bucket,
                            labels,
                            Some("+Inf"),
                            &h.count.to_string(),
                        );
                        let sum = format_value(h.sum);
                        sample(&mut out, &format!("{name}_sum"), labels, None, &sum);
                        let count = h.count.to_string();
                        sample(&mut out, &format!("{name}_count"), labels, None, &count);
                    }
                }
            }
        }
        out
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, Family>> {
        // A panic mid-record leaves at worst one stale series.
        self.families.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn record(
        &self,
        name: &str,
        labels: &Labels,
        kind: MetricKind,
        apply: impl FnOnce(&mut Series),
    ) {
        debug_check(name, kind, labels);
        {
            let mut families = self.lock();
            if !families.contains_key(name) {
                let family = Family {
                    kind,
                    series: BTreeMap::new(),
                };
                families.insert(name.to_string(), family);
            }
            let family = families.get_mut(name).expect("inserted above");
            // A name recorded as two kinds keeps the first.
            if family.kind == kind {
                if !family.series.contains_key(labels) {
                    let series = match kind {
                        MetricKind::Counter => Series::Counter(0),
                        MetricKind::Gauge => Series::Gauge(0.0),
                        MetricKind::Histogram => Series::Histogram(Histogram {
                            buckets: vec![0; DURATION_BUCKETS.len()],
                            sum: 0.0,
                            count: 0,
                        }),
                    };
                    family.series.insert(labels.clone(), series);
                }
                apply(family.series.get_mut(labels).expect("inserted above"));
            }
        }
        self.last_record.store(self.clock.now().0, Ordering::SeqCst);
    }
}

impl Metrics for Registry {
    fn counter(&self, name: &str, labels: &Labels, delta: u64) {
        self.record(name, labels, MetricKind::Counter, |s| {
            if let Series::Counter(v) = s {
                *v = v.saturating_add(delta);
            }
        });
    }

    fn gauge(&self, name: &str, labels: &Labels, value: f64) {
        self.record(name, labels, MetricKind::Gauge, |s| {
            if let Series::Gauge(v) = s {
                *v = value;
            }
        });
    }

    fn histogram(&self, name: &str, labels: &Labels, value: f64) {
        self.record(name, labels, MetricKind::Histogram, |s| {
            if let Series::Histogram(h) = s {
                if let Some(i) = DURATION_BUCKETS.iter().position(|le| value <= *le) {
                    h.buckets[i] += 1;
                }
                h.sum += value;
                h.count += 1;
            }
        });
    }
}

/// One sample line: `name{labels,le="…"} value`.
fn sample(out: &mut String, name: &str, labels: &Labels, le: Option<&str>, value: &str) {
    out.push_str(name);
    let mut pairs = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .chain(le.map(|le| ("le", le)))
        .peekable();
    if pairs.peek().is_some() {
        out.push('{');
        for (i, (k, v)) in pairs.enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{k}=\"{}\"", escape_label(v));
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(value);
    out.push('\n');
}

/// A sample value as Prometheus spells it.
pub fn format_value(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "+Inf".into()
    } else if v == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        format!("{v}")
    }
}

fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_help(v: &str) -> String {
    v.replace('\\', "\\\\").replace('\n', "\\n")
}

/// `Metrics` impl that records into a shared `Arc<Mutex<…>>` so tests can
/// read what was emitted. NOT for production — concurrent writers
/// contend on the mutex, and histograms keep every observation.
#[derive(Default, Clone)]
pub struct InMemoryMetrics {
    inner: Arc<Mutex<RecorderState>>,
}

#[derive(Default)]
struct RecorderState {
    counters: BTreeMap<MetricKey, u64>,
    gauges: BTreeMap<MetricKey, f64>,
    histograms: BTreeMap<MetricKey, Vec<f64>>,
}

fn key(name: &str, labels: &Labels) -> MetricKey {
    (
        name.to_string(),
        labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    )
}

impl InMemoryMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read a counter total. Returns 0 if never emitted.
    pub fn counter_value(&self, name: &str, labels: &Labels) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .counters
            .get(&key(name, labels))
            .copied()
            .unwrap_or(0)
    }

    pub fn gauge_value(&self, name: &str, labels: &Labels) -> Option<f64> {
        self.inner
            .lock()
            .unwrap()
            .gauges
            .get(&key(name, labels))
            .copied()
    }

    pub fn histogram_observations(&self, name: &str, labels: &Labels) -> Vec<f64> {
        self.inner
            .lock()
            .unwrap()
            .histograms
            .get(&key(name, labels))
            .cloned()
            .unwrap_or_default()
    }

    /// Snapshot of every counter. Useful for `dbg!()`-style inspection in
    /// failing tests.
    pub fn dump_counters(&self) -> BTreeMap<MetricKey, u64> {
        self.inner.lock().unwrap().counters.clone()
    }
}

impl Metrics for InMemoryMetrics {
    fn counter(&self, name: &str, labels: &Labels, delta: u64) {
        debug_check(name, MetricKind::Counter, labels);
        *self
            .inner
            .lock()
            .unwrap()
            .counters
            .entry(key(name, labels))
            .or_insert(0) += delta;
    }

    fn gauge(&self, name: &str, labels: &Labels, value: f64) {
        debug_check(name, MetricKind::Gauge, labels);
        self.inner
            .lock()
            .unwrap()
            .gauges
            .insert(key(name, labels), value);
    }

    fn histogram(&self, name: &str, labels: &Labels, value: f64) {
        debug_check(name, MetricKind::Histogram, labels);
        self.inner
            .lock()
            .unwrap()
            .histograms
            .entry(key(name, labels))
            .or_default()
            .push(value);
    }
}

/// `Metrics` impl that drops everything. Useful as a default when callers
/// don't care about observability (e.g., one-off CLI invocations).
#[derive(Default)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn counter(&self, _: &str, _: &Labels, _: u64) {}
    fn gauge(&self, _: &str, _: &Labels, _: f64) {}
    fn histogram(&self, _: &str, _: &Labels, _: f64) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    fn no_labels() -> Labels {
        BTreeMap::new()
    }

    /// A clock tests move by hand.
    #[derive(Default)]
    struct ManualClock(AtomicI64);

    impl ManualClock {
        fn advance(&self, d: Duration) {
            self.0.fetch_add(d.as_micros() as i64, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Clock for ManualClock {
        fn now(&self) -> Timestamp {
            Timestamp(self.0.load(Ordering::SeqCst))
        }
        async fn sleep(&self, _: Duration) {}
    }

    fn registry() -> (Registry, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::default());
        (Registry::new(clock.clone()), clock)
    }

    #[test]
    fn counter_accumulates() {
        let m = InMemoryMetrics::new();
        let l = no_labels();
        m.counter("c", &l, 1);
        m.counter("c", &l, 2);
        m.counter("c", &l, 3);
        assert_eq!(m.counter_value("c", &l), 6);
    }

    #[test]
    fn counter_labels_are_distinct() {
        let m = InMemoryMetrics::new();
        let mut l1 = Labels::new();
        l1.insert("k".into(), "v1".into());
        let mut l2 = Labels::new();
        l2.insert("k".into(), "v2".into());

        m.counter("c", &l1, 1);
        m.counter("c", &l2, 5);
        assert_eq!(m.counter_value("c", &l1), 1);
        assert_eq!(m.counter_value("c", &l2), 5);
    }

    #[test]
    fn gauge_last_write_wins() {
        let m = InMemoryMetrics::new();
        let l = no_labels();
        m.gauge("g", &l, 1.0);
        m.gauge("g", &l, 5.0);
        m.gauge("g", &l, 2.0);
        assert_eq!(m.gauge_value("g", &l), Some(2.0));
    }

    #[test]
    fn histogram_records_all_observations() {
        let m = InMemoryMetrics::new();
        let l = no_labels();
        m.histogram("h", &l, 1.0);
        m.histogram("h", &l, 2.0);
        m.histogram("h", &l, 3.0);
        assert_eq!(m.histogram_observations("h", &l), vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn missing_metric_returns_default() {
        let m = InMemoryMetrics::new();
        let l = no_labels();
        assert_eq!(m.counter_value("missing", &l), 0);
        assert_eq!(m.gauge_value("missing", &l), None);
        assert!(m.histogram_observations("missing", &l).is_empty());
    }

    #[test]
    fn noop_does_nothing() {
        let m = NoopMetrics;
        let l = no_labels();
        m.counter("c", &l, 100);
        // Can't assert "did nothing" without recording — just confirm it
        // compiles and doesn't panic.
        m.gauge("g", &l, 1.0);
        m.histogram("h", &l, 1.0);
    }

    #[test]
    fn catalog_names_are_unique_and_well_formed() {
        let mut seen = std::collections::BTreeSet::new();
        for def in CATALOG {
            assert!(seen.insert(def.name), "{} declared twice", def.name);
            assert!(def.name.starts_with("pg2iceberg_"), "{}", def.name);
            assert!(
                def.name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{}",
                def.name
            );
            match def.kind {
                MetricKind::Counter => assert!(def.name.ends_with("_total"), "{}", def.name),
                MetricKind::Histogram => assert!(def.name.ends_with("_seconds"), "{}", def.name),
                MetricKind::Gauge => assert!(!def.name.ends_with("_total"), "{}", def.name),
            }
            assert!(
                !def.help.is_empty() && !def.help.contains('\n'),
                "{}",
                def.name
            );
            assert!(!def.labels.contains(&"le"), "{}", def.name);
        }
    }

    /// Every metric is documented for operators.
    #[test]
    fn every_metric_is_documented() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/usage/observability.md"
        );
        let doc = std::fs::read_to_string(path).expect("read the observability docs");
        let missing: Vec<&str> = CATALOG
            .iter()
            .map(|d| d.name)
            .filter(|name| !doc.contains(&format!("`{name}`")))
            .collect();
        assert!(missing.is_empty(), "undocumented in {path}: {missing:?}");
    }

    #[test]
    fn registry_renders_the_text_format() {
        let (r, _) = registry();
        let table = labels([("table", "public.orders")]);
        r.counter(names::PIPELINE_ROWS_STAGED_TOTAL, &table, 3);
        r.counter(names::PIPELINE_ROWS_STAGED_TOTAL, &table, 4);
        r.gauge(names::REPLICATION_LAG, &no_labels(), 1024.0);
        r.histogram(names::MATERIALIZER_COMMIT_DURATION, &no_labels(), 0.3);
        r.histogram(names::MATERIALIZER_COMMIT_DURATION, &no_labels(), 400.0);
        let text = r.render();
        assert!(
            text.contains(
                "# HELP pg2iceberg_pipeline_rows_staged_total Rows staged as Parquet in the object store, snapshot rows included.\n\
                 # TYPE pg2iceberg_pipeline_rows_staged_total counter\n\
                 pg2iceberg_pipeline_rows_staged_total{table=\"public.orders\"} 7\n"
            ),
            "{text}"
        );
        assert!(
            text.contains("pg2iceberg_replication_lag_bytes 1024\n"),
            "{text}"
        );
        for line in [
            "# TYPE pg2iceberg_materializer_commit_duration_seconds histogram",
            "pg2iceberg_materializer_commit_duration_seconds_bucket{le=\"0.25\"} 0",
            "pg2iceberg_materializer_commit_duration_seconds_bucket{le=\"0.5\"} 1",
            "pg2iceberg_materializer_commit_duration_seconds_bucket{le=\"300\"} 1",
            "pg2iceberg_materializer_commit_duration_seconds_bucket{le=\"+Inf\"} 2",
            "pg2iceberg_materializer_commit_duration_seconds_sum 400.3",
            "pg2iceberg_materializer_commit_duration_seconds_count 2",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "{line} missing from\n{text}"
            );
        }
    }

    #[test]
    fn registry_escapes_label_values() {
        let (r, _) = registry();
        let odd = labels([("table", "a\"b\\c\nd")]);
        r.gauge(names::MATERIALIZER_BACKLOG_ROWS, &odd, 1.0);
        assert!(
            r.render()
                .contains(r#"pg2iceberg_materializer_backlog_rows{table="a\"b\\c\nd"} 1"#),
            "{}",
            r.render()
        );
    }

    #[test]
    fn registry_formats_special_values() {
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_value(2.0), "2");
        assert_eq!(format_value(0.005), "0.005");
        assert_eq!(format_value(1_700_000_000.5), "1700000000.5");
    }

    #[test]
    fn registry_is_idle_until_something_is_recorded() {
        let (r, clock) = registry();
        clock.advance(Duration::from_secs(30));
        assert_eq!(r.idle_for(), Duration::from_secs(30));
        r.counter(names::PIPELINE_FLUSH_TOTAL, &no_labels(), 1);
        assert_eq!(r.idle_for(), Duration::ZERO);
        clock.advance(Duration::from_secs(5));
        assert_eq!(r.idle_for(), Duration::from_secs(5));
    }

    #[test]
    fn phase_reads_back_the_last_one_set() {
        let (r, _) = registry();
        assert_eq!(r.phase(), None);
        Phase::Starting.set(&r);
        assert_eq!(r.phase(), Some(Phase::Starting));
        Phase::Running.set(&r);
        assert_eq!(r.phase(), Some(Phase::Running));
        let text = r.render();
        assert!(
            text.contains("pg2iceberg_phase{phase=\"running\"} 1\n"),
            "{text}"
        );
        assert!(
            text.contains("pg2iceberg_phase{phase=\"starting\"} 0\n"),
            "{text}"
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "isn't in CATALOG")]
    fn an_undeclared_metric_is_refused() {
        let (r, _) = registry();
        r.counter("pg2iceberg_made_up_total", &no_labels(), 1);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "labels")]
    fn a_metric_with_other_labels_is_refused() {
        let m = InMemoryMetrics::new();
        m.counter(names::PIPELINE_FLUSH_TOTAL, &labels([("table", "t")]), 1);
    }
}
