//! Where pg2iceberg's logs and spans go.
//!
//! - **Logs**: stdout, as plain text or — `PG2ICEBERG_LOG_FORMAT=json` —
//!   one JSON object a line, filtered by `RUST_LOG` (default
//!   `info,pg2iceberg=debug`).
//! - **Spans**: exported as OpenTelemetry traces over OTLP/HTTP (protobuf)
//!   when `OTEL_EXPORTER_OTLP_ENDPOINT` or
//!   `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` says where to. The exporter and
//!   SDK read the other standard variables themselves: headers, timeout
//!   and compression (`OTEL_EXPORTER_OTLP_*`), sampling
//!   (`OTEL_TRACES_SAMPLER`, `_ARG`), `OTEL_SERVICE_NAME` and
//!   `OTEL_RESOURCE_ATTRIBUTES`. `OTEL_SDK_DISABLED=true` or
//!   `OTEL_TRACES_EXPORTER=none` turns the export off.
//!
//! The spans are pg2iceberg's units of work — see
//! `pg2iceberg_logical::spans`.

use crate::config::Env;
use anyhow::{Context, Result};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer};

/// Logs, unless `RUST_LOG` says otherwise.
const DEFAULT_LOG_FILTER: &str = "info,pg2iceberg=debug";

/// What's exported: pg2iceberg's spans, and other crates' warnings within
/// them. Not the exporter's own: they'd be exported by the exporter.
const TRACE_FILTER: &str = "warn,pg2iceberg=info,opentelemetry=off";

/// How [`init`] sets up logs and traces, as the environment says.
#[derive(Debug, PartialEq, Eq)]
pub struct Settings {
    /// Logs as JSON lines, not text.
    pub json: bool,
    /// Export spans over OTLP.
    pub export_traces: bool,
}

impl Settings {
    pub fn from_env(env: Env) -> Result<Self> {
        let json = match env("PG2ICEBERG_LOG_FORMAT").as_deref() {
            None | Some("text") => false,
            Some("json") => true,
            Some(other) => {
                anyhow::bail!("PG2ICEBERG_LOG_FORMAT={other:?}: expected `text` or `json`")
            }
        };
        let disabled = env("OTEL_SDK_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true"))
            || env("OTEL_TRACES_EXPORTER").is_some_and(|v| v.eq_ignore_ascii_case("none"));
        let endpoint = env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .or_else(|| env("OTEL_EXPORTER_OTLP_ENDPOINT"))
            .is_some();
        let export_traces = endpoint && !disabled;
        if export_traces {
            let protocol = env("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
                .or_else(|| env("OTEL_EXPORTER_OTLP_PROTOCOL"));
            if let Some(p) = protocol.filter(|p| p != "http/protobuf") {
                anyhow::bail!(
                    "OTLP protocol {p:?} isn't supported: pg2iceberg exports over \
                     http/protobuf — point OTEL_EXPORTER_OTLP_ENDPOINT at the collector's \
                     HTTP port (4318) and unset OTEL_EXPORTER_OTLP_PROTOCOL"
                );
            }
        }
        Ok(Self {
            json,
            export_traces,
        })
    }
}

/// The installed subscriber. Dropping it exports the spans not yet sent.
pub struct Subscriber {
    provider: Option<SdkTracerProvider>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                eprintln!("exporting the last spans failed: {e}");
            }
        }
    }
}

/// Install the process's subscriber, as `env` says ([`Settings`]).
pub fn init(env: Env) -> Result<Subscriber> {
    let settings = Settings::from_env(env)?;
    let provider = if settings.export_traces {
        Some(tracer_provider(env, None)?)
    } else {
        None
    };
    let log_filter = env("RUST_LOG")
        .and_then(|f| EnvFilter::try_new(f).ok())
        .unwrap_or_else(|| EnvFilter::new(DEFAULT_LOG_FILTER));
    let logs = if settings.json {
        tracing_subscriber::fmt::layer()
            .json()
            .flatten_event(true)
            .with_filter(log_filter)
            .boxed()
    } else {
        // Plain text: log collectors (Cloudflare's, CloudWatch, `docker
        // logs` piped to a file) show color codes as raw escapes.
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_filter(log_filter)
            .boxed()
    };
    let traces = provider.as_ref().map(trace_layer);
    tracing_subscriber::registry()
        .with(logs)
        .with(traces)
        .try_init()
        .context("install the tracing subscriber")?;
    if provider.is_some() {
        tracing::info!("exporting spans over OTLP");
    }
    Ok(Subscriber { provider })
}

/// Exports spans over OTLP/HTTP — to `endpoint`, else where the
/// environment says — in batches, from a thread of its own.
pub fn tracer_provider(env: Env, endpoint: Option<&str>) -> Result<SdkTracerProvider> {
    let mut exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary);
    if let Some(endpoint) = endpoint {
        exporter = exporter.with_endpoint(endpoint);
    }
    let exporter = exporter.build().context("build the OTLP span exporter")?;
    let mut resource = Resource::builder()
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .with_attribute(KeyValue::new(
            "pg2iceberg.revision",
            crate::telemetry::REVISION,
        ));
    let names_service = env("OTEL_SERVICE_NAME").is_some()
        || env("OTEL_RESOURCE_ATTRIBUTES").is_some_and(|a| a.contains("service.name="));
    if !names_service {
        resource = resource.with_service_name("pg2iceberg");
    }
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.build())
        .build())
}

/// The layer exporting the spans [`TRACE_FILTER`] lets through to
/// `provider`.
pub fn trace_layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + Send + Sync
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("pg2iceberg"))
        .with_filter(EnvFilter::new(TRACE_FILTER))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn settings(pairs: &[(&str, &str)]) -> Result<Settings> {
        Settings::from_env(&vars(pairs))
    }

    #[test]
    fn spans_are_exported_only_where_the_environment_says() {
        let off = Settings {
            json: false,
            export_traces: false,
        };
        assert_eq!(settings(&[]).unwrap(), off);
        let on = Settings {
            json: false,
            export_traces: true,
        };
        for endpoint in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        ] {
            assert_eq!(settings(&[(endpoint, "http://c:4318")]).unwrap(), on);
        }
        for (name, value) in [
            ("OTEL_SDK_DISABLED", "true"),
            ("OTEL_TRACES_EXPORTER", "none"),
        ] {
            let disabled = settings(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318"),
                (name, value),
            ]);
            assert_eq!(disabled.unwrap(), off, "{name}={value}");
        }
    }

    #[test]
    fn only_otlp_over_http_protobuf_is_exported() {
        let endpoint = ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318");
        assert!(settings(&[endpoint, ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")]).is_ok());
        for protocol in ["grpc", "http/json"] {
            let err = settings(&[endpoint, ("OTEL_EXPORTER_OTLP_PROTOCOL", protocol)])
                .unwrap_err()
                .to_string();
            assert!(err.contains("4318"), "{err}");
        }
        // No export, no protocol to check.
        assert!(settings(&[("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")]).is_ok());
    }

    /// A collector: every OTLP/HTTP trace export it receives.
    fn collector(rt: &tokio::runtime::Runtime) -> (String, std::sync::mpsc::Receiver<Vec<u8>>) {
        use http_body_util::{BodyExt as _, Full};
        use hyper::body::Bytes;
        let (tx, rx) = std::sync::mpsc::channel();
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let addr = listener.local_addr().unwrap();
        rt.spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(
                        move |req: hyper::Request<hyper::body::Incoming>| {
                            let tx = tx.clone();
                            async move {
                                assert_eq!(req.uri().path(), "/v1/traces");
                                let body = req.into_body().collect().await?.to_bytes();
                                tx.send(body.to_vec()).unwrap();
                                Ok::<_, hyper::Error>(hyper::Response::new(Full::new(Bytes::new())))
                            }
                        },
                    );
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        });
        (format!("http://{addr}/v1/traces"), rx)
    }

    /// Spans reach a collector as OpenTelemetry has them: named,
    /// nested, failed with their error, and only pg2iceberg's.
    #[test]
    fn spans_are_exported_over_otlp() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use prost::Message as _;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (endpoint, exports) = collector(&rt);
        let provider = tracer_provider(&vars(&[]), Some(&endpoint)).unwrap();
        let subscriber = tracing_subscriber::registry().with(trace_layer(&provider));
        tracing::subscriber::with_default(subscriber, || {
            let cycle = pg2iceberg_logical::work_span!("materializer.cycle", rows = 3);
            let _in_cycle = cycle.enter();
            let request = tracing::info_span!(
                "request",
                otel.name = "catalog.commit_snapshots",
                otel.kind = "client",
                store = "catalog",
                op = "commit_snapshots",
                otel.status_description = tracing::field::Empty,
            );
            pg2iceberg_logical::spans::record_error(&request, &"conflict: table changed");
            drop(request);
            // Not pg2iceberg's: not exported.
            let _theirs = tracing::info_span!(target: "hyper", "their.span").entered();
        });
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();

        let mut spans = Vec::new();
        let mut services = Vec::new();
        for body in exports.try_iter() {
            let export = ExportTraceServiceRequest::decode(body.as_slice()).unwrap();
            for resource in export.resource_spans {
                let attrs = resource.resource.unwrap_or_default().attributes;
                services.extend(attrs.into_iter().filter(|a| a.key == "service.name"));
                spans.extend(resource.scope_spans.into_iter().flat_map(|s| s.spans));
            }
        }
        let names: Vec<&str> = spans.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names.len(), 2, "{names:?}");
        let span = |name: &str| spans.iter().find(|s| s.name == name).unwrap();
        let (cycle, request) = (span("materializer.cycle"), span("catalog.commit_snapshots"));
        assert_eq!(request.parent_span_id, cycle.span_id);
        assert_eq!(request.trace_id, cycle.trace_id);
        assert!(cycle.parent_span_id.is_empty());
        let status = request.status.clone().unwrap();
        // STATUS_CODE_ERROR, SPAN_KIND_CLIENT
        assert_eq!(
            (status.code, status.message.as_str()),
            (2, "conflict: table changed")
        );
        assert_eq!(request.kind, 3);
        let attr = |s: &opentelemetry_proto::tonic::trace::v1::Span, key: &str| {
            s.attributes
                .iter()
                .find(|a| a.key == key)
                .and_then(|a| a.value.clone()?.value)
        };
        assert_eq!(
            attr(request, "op"),
            Some(Value::StringValue("commit_snapshots".into()))
        );
        assert_eq!(attr(cycle, "rows"), Some(Value::IntValue(3)));
        assert_eq!(
            services.first().and_then(|s| s.value.clone()?.value),
            Some(Value::StringValue("pg2iceberg".into()))
        );
    }

    #[test]
    fn logs_are_text_or_json() {
        assert!(!settings(&[("PG2ICEBERG_LOG_FORMAT", "text")]).unwrap().json);
        assert!(settings(&[("PG2ICEBERG_LOG_FORMAT", "json")]).unwrap().json);
        assert!(settings(&[("PG2ICEBERG_LOG_FORMAT", "yaml")]).is_err());
    }
}
