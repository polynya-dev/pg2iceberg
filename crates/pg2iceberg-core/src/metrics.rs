//! Observability surface. Production wraps Prometheus / OpenTelemetry; the
//! sim records into an in-memory recorder that tests can introspect.
//!
//! Kept narrow on purpose — three primitive shapes (counter, gauge,
//! histogram) cover everything pg2iceberg needs to emit. Avoiding a wider
//! API keeps the prod impl small and the sim impl simple.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Static metric labels. Sorted by key for stable serialization.
pub type Labels = BTreeMap<String, String>;

/// Internal recorder key: `(metric_name, sorted_(label_key, label_value)_pairs)`.
type MetricKey = (String, Vec<(String, String)>);

pub trait Metrics: Send + Sync {
    /// Monotonic counter; only ever increments. Production: Prometheus
    /// `CounterVec`. The sim accumulates per-(name, labels) totals.
    fn counter(&self, name: &str, labels: &Labels, delta: u64);

    /// Point-in-time value. Last write wins. Production: `GaugeVec`.
    fn gauge(&self, name: &str, labels: &Labels, value: f64);

    /// Distribution sample. Production: `HistogramVec`. Sim stores raw
    /// observations so tests can read percentiles.
    fn histogram(&self, name: &str, labels: &Labels, value: f64);
}

/// `Metrics` impl that records into a shared `Arc<Mutex<…>>` so tests can
/// read what was emitted. NOT for production — concurrent writers
/// contend on the mutex.
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
        *self
            .inner
            .lock()
            .unwrap()
            .counters
            .entry(key(name, labels))
            .or_insert(0) += delta;
    }

    fn gauge(&self, name: &str, labels: &Labels, value: f64) {
        self.inner
            .lock()
            .unwrap()
            .gauges
            .insert(key(name, labels), value);
    }

    fn histogram(&self, name: &str, labels: &Labels, value: f64) {
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

/// Standard metric names emitted across the workspace. Centralized so
/// renames don't drift between emitters and asserters.
pub mod names {
    /// Counter: total flush invocations on the logical pipeline. Labels: `{table}`.
    pub const PIPELINE_FLUSH_TOTAL: &str = "pg2iceberg_pipeline_flush_total";
    /// Counter: total rows staged across all flushes. Labels: `{table}`.
    pub const PIPELINE_ROWS_STAGED_TOTAL: &str = "pg2iceberg_pipeline_rows_staged_total";
    /// Gauge: pipeline.flushed_lsn. Labels: `{}`.
    pub const PIPELINE_FLUSHED_LSN: &str = "pg2iceberg_pipeline_flushed_lsn";

    /// Counter: total materializer cycle invocations. Labels: `{table}`.
    pub const MATERIALIZER_CYCLE_TOTAL: &str = "pg2iceberg_materializer_cycle_total";
    /// Counter: total rows materialized into Iceberg. Labels: `{table}`.
    pub const MATERIALIZER_ROWS_TOTAL: &str = "pg2iceberg_materializer_rows_total";

    /// Counter: invariant-watcher violation count. Labels: `{invariant}`.
    pub const INVARIANT_VIOLATIONS_TOTAL: &str = "pg2iceberg_invariant_violations_total";
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_labels() -> Labels {
        BTreeMap::new()
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
}
