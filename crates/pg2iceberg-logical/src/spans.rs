//! Spans for pg2iceberg's units of work — a flush, a materializer cycle,
//! a snapshot chunk — which an OpenTelemetry exporter sends as traces.
//!
//! Every span is at INFO, named for what it does (`pipeline.flush`,
//! `materializer.commit`), and declares `otel.status_description`, which
//! marks it failed as OpenTelemetry reads it ([`record_outcome`]).
//! [`work_span!`](crate::work_span) declares it.
//!
//! A unit of work long enough to hold thousands of others — a snapshot
//! that runs for hours, say — is no span of its own: its parts are traces
//! of their own (`parent: None`), so no trace grows without bound.

use std::fmt::Display;
use tracing::Span;

/// An INFO span named `$name` with the given fields, plus the status
/// field [`record_outcome`] sets. Starts with `parent: None` for a span
/// that begins a trace of its own wherever it's entered.
#[macro_export]
macro_rules! work_span {
    (parent: None, $name:literal $(, $($fields:tt)*)?) => {
        ::tracing::info_span!(
            parent: None,
            $name,
            otel.status_description = ::tracing::field::Empty,
            $($($fields)*)?
        )
    };
    ($name:literal $(, $($fields:tt)*)?) => {
        ::tracing::info_span!(
            $name,
            otel.status_description = ::tracing::field::Empty,
            $($($fields)*)?
        )
    };
}

/// Mark `span` failed if `out` is an error.
pub fn record_outcome<T, E: Display>(span: &Span, out: &Result<T, E>) {
    if let Err(e) = out {
        record_error(span, e);
    }
}

/// Mark `span` failed with `error`. (A status description is an error's.)
pub fn record_error(span: &Span, error: &dyn Display) {
    span.record("otel.status_description", tracing::field::display(error));
}
