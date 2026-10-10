//! The long-running subcommands' HTTP endpoint: `/metrics` in the
//! Prometheus text format, `/healthz` for liveness and `/readyz` for
//! readiness.
//!
//! - **`/metrics`**: every series in the [`Registry`], then the standard
//!   `process_*` series.
//! - **`/healthz`**: 503 once nothing has completed — no request to the
//!   catalog, object store or coordinator, no handler tick — for the
//!   liveness timeout. pg2iceberg records only on finishing work, so that
//!   is a stuck process (a deadlock, a request that never returns), not
//!   an idle one; restarting it is the fix. A slow one passes.
//! - **`/readyz`**: 200 while snapshotting or running; 503 while starting
//!   up and draining.

use crate::config::{Config, MetricsAddr};
use crate::realio::MonotonicClock;
use anyhow::{Context, Result};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use pg2iceberg_core::metrics::{labels, names};
use pg2iceberg_core::{Metrics, Phase, Registry};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// The process's metrics, and whether it's alive and ready.
pub struct Telemetry {
    registry: Arc<Registry>,
    liveness_timeout: Duration,
}

impl Telemetry {
    /// A registry for this process — its build and [`Phase::Starting`]
    /// recorded — whose liveness check allows `liveness_timeout`.
    pub fn new(liveness_timeout: Duration) -> Arc<Self> {
        let registry = Arc::new(Registry::new(Arc::new(MonotonicClock::new())));
        let build = labels([
            ("version", env!("CARGO_PKG_VERSION")),
            ("revision", REVISION),
        ]);
        registry.gauge(names::BUILD_INFO, &build, 1.0);
        Phase::Starting.set(registry.as_ref());
        Arc::new(Self {
            registry,
            liveness_timeout,
        })
    }

    /// [`Self::new`], serving its endpoint where `cfg` says. An address
    /// `metrics_addr` names must be free; the default one may be taken,
    /// by a second pg2iceberg on the host say, and then nothing is
    /// served.
    pub async fn start(cfg: &Config, liveness_timeout: Duration) -> Result<Arc<Self>> {
        let telemetry = Self::new(liveness_timeout);
        let (addr, configured) = match cfg.metrics_addr()? {
            MetricsAddr::Off => return Ok(telemetry),
            MetricsAddr::Default(addr) => (addr, false),
            MetricsAddr::Configured(addr) => (addr, true),
        };
        match TcpListener::bind(&addr).await {
            Ok(listener) => {
                tracing::info!(
                    addr = %listener.local_addr().map_or(addr, |a| a.to_string()),
                    "serving /metrics, /healthz and /readyz"
                );
                tokio::spawn(serve(listener, telemetry.clone()));
            }
            Err(e) if !configured => tracing::warn!(
                %addr,
                error = %e,
                "not serving /metrics, /healthz or /readyz: set metrics_addr \
                 (PG2ICEBERG_METRICS_ADDR) to a free address, or `off`"
            ),
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("serve /metrics on metrics_addr {addr} (PG2ICEBERG_METRICS_ADDR)")
                })
            }
        }
        Ok(telemetry)
    }

    /// Where the process records its metrics.
    pub fn metrics(&self) -> Arc<dyn Metrics> {
        self.registry.clone()
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The response to `method path`.
    pub fn respond(&self, method: &Method, path: &str) -> Response<Full<Bytes>> {
        if method != Method::GET && method != Method::HEAD {
            return text(StatusCode::METHOD_NOT_ALLOWED, "GET or HEAD only\n".into());
        }
        match path {
            "/metrics" => {
                let mut body = self.registry.render();
                process::render(&mut body);
                let mut response = text(StatusCode::OK, body);
                response.headers_mut().insert(
                    hyper::header::CONTENT_TYPE,
                    hyper::header::HeaderValue::from_static(
                        "text/plain; version=0.0.4; charset=utf-8",
                    ),
                );
                response
            }
            "/healthz" => {
                let idle = self.registry.idle_for();
                if idle <= self.liveness_timeout {
                    text(StatusCode::OK, "ok\n".into())
                } else {
                    let message = format!(
                        "stuck: nothing completed for {}s (liveness_timeout {}s)\n",
                        idle.as_secs(),
                        self.liveness_timeout.as_secs()
                    );
                    text(StatusCode::SERVICE_UNAVAILABLE, message)
                }
            }
            "/readyz" => match self.registry.phase() {
                Some(phase @ (Phase::Snapshotting | Phase::Running)) => {
                    text(StatusCode::OK, format!("ready: {}\n", phase.as_str()))
                }
                phase => {
                    let phase = phase.map_or("unknown", Phase::as_str);
                    text(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("not ready: {phase}\n"),
                    )
                }
            },
            _ => text(
                StatusCode::NOT_FOUND,
                "not found: try /metrics, /healthz or /readyz\n".into(),
            ),
        }
    }
}

/// The commit the binary was built from, when the build said
/// (`PG2ICEBERG_COMMIT_SHA`, as the Dockerfile sets it).
pub(crate) const REVISION: &str = match option_env!("PG2ICEBERG_COMMIT_SHA") {
    Some(sha) if !sha.is_empty() => sha,
    _ => "unknown",
};

fn text(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Answer HTTP/1.1 on `listener` until the process exits.
async fn serve(listener: TcpListener, telemetry: Arc<Telemetry>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                // Out of file descriptors, say: back off rather than spin.
                tracing::warn!(error = %e, "metrics endpoint: accept failed");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let telemetry = telemetry.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                let response = telemetry.respond(request.method(), request.uri().path());
                async move { Ok::<_, std::convert::Infallible>(response) }
            });
            let served = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), service)
                .await;
            if let Err(e) = served {
                tracing::debug!(error = %e, "metrics endpoint: connection failed");
            }
        });
    }
}

/// The standard `process_*` series, from `/proc` on Linux. Elsewhere,
/// only the start time.
mod process {
    use std::fmt::Write as _;
    use std::sync::OnceLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// When the process started, as near as pg2iceberg can tell: when
    /// it was first asked.
    fn start_time() -> f64 {
        static START: OnceLock<f64> = OnceLock::new();
        *START.get_or_init(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64())
        })
    }

    fn series(out: &mut String, name: &str, kind: &str, help: &str, value: f64) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        let _ = writeln!(
            out,
            "{name} {}",
            pg2iceberg_core::metrics::format_value(value)
        );
    }

    pub fn render(out: &mut String) {
        series(
            out,
            "process_start_time_seconds",
            "gauge",
            "Start time of the process since unix epoch in seconds.",
            start_time(),
        );
        #[cfg(target_os = "linux")]
        linux(out);
    }

    #[cfg(target_os = "linux")]
    fn linux(out: &mut String) {
        if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
            if let Some(s) = parse_stat(&stat) {
                series(
                    out,
                    "process_cpu_seconds_total",
                    "counter",
                    "Total user and system CPU time spent in seconds.",
                    s.cpu_seconds,
                );
                series(
                    out,
                    "process_virtual_memory_bytes",
                    "gauge",
                    "Virtual memory size in bytes.",
                    s.virtual_bytes,
                );
                series(
                    out,
                    "process_threads",
                    "gauge",
                    "Number of OS threads in the process.",
                    s.threads,
                );
            }
        }
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            if let Some(kb) = status_kb(&status, "VmRSS:") {
                series(
                    out,
                    "process_resident_memory_bytes",
                    "gauge",
                    "Resident memory size in bytes.",
                    kb * 1024.0,
                );
            }
        }
        if let Ok(fds) = std::fs::read_dir("/proc/self/fd") {
            series(
                out,
                "process_open_fds",
                "gauge",
                "Number of open file descriptors.",
                fds.count() as f64,
            );
        }
        if let Ok(limits) = std::fs::read_to_string("/proc/self/limits") {
            if let Some(max) = max_open_files(&limits) {
                series(
                    out,
                    "process_max_fds",
                    "gauge",
                    "Maximum number of open file descriptors.",
                    max,
                );
            }
        }
    }

    #[cfg(any(target_os = "linux", test))]
    pub struct Stat {
        pub cpu_seconds: f64,
        pub virtual_bytes: f64,
        pub threads: f64,
    }

    #[cfg(any(target_os = "linux", test))]
    /// The fields of `/proc/self/stat` the series need. The command name
    /// is in parentheses and may hold spaces or parentheses itself, so
    /// fields count from after the last `)`.
    pub fn parse_stat(stat: &str) -> Option<Stat> {
        let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
        // proc(5) numbers fields from 1, `state` (after the name) being 3.
        let field = |n: usize| fields.get(n - 3)?.parse::<f64>().ok();
        // utime and stime count clock ticks: USER_HZ, 100 on Linux.
        Some(Stat {
            cpu_seconds: (field(14)? + field(15)?) / 100.0,
            threads: field(20)?,
            virtual_bytes: field(23)?,
        })
    }

    #[cfg(any(target_os = "linux", test))]
    /// `key`'s value in `/proc/self/status`, in kB.
    pub fn status_kb(status: &str, key: &str) -> Option<f64> {
        status
            .lines()
            .find_map(|l| l.strip_prefix(key))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }

    #[cfg(any(target_os = "linux", test))]
    /// The soft limit on open files in `/proc/self/limits`.
    pub fn max_open_files(limits: &str) -> Option<f64> {
        limits
            .lines()
            .find_map(|l| l.strip_prefix("Max open files"))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn stat_fields_count_from_after_the_command_name() {
            let stat = "4242 (pg2 (iceberg) x) S 1 4242 4242 0 -1 4194560 2196 0 0 0 \
                        350 120 0 0 20 0 9 0 51539 1325760512 4562 18446744073709551615";
            let s = parse_stat(stat).unwrap();
            assert_eq!(s.cpu_seconds, 4.7);
            assert_eq!(s.threads, 9.0);
            assert_eq!(s.virtual_bytes, 1_325_760_512.0);
        }

        #[test]
        fn status_and_limits_are_read() {
            let status = "Name:\tpg2iceberg\nVmRSS:\t   51200 kB\nThreads:\t9\n";
            assert_eq!(status_kb(status, "VmRSS:"), Some(51200.0));
            let limits = "Limit                     Soft Limit           Hard Limit           Units\n\
                          Max open files            1048576              1048576              files\n";
            assert_eq!(max_open_files(limits), Some(1_048_576.0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    fn body(response: Response<Full<Bytes>>) -> (StatusCode, String) {
        let status = response.status();
        let bytes = futures_body(response);
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn futures_body(response: Response<Full<Bytes>>) -> Bytes {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(response.into_body().collect())
            .unwrap()
            .to_bytes()
    }

    #[test]
    fn metrics_expose_the_registry_and_the_process() {
        let telemetry = Telemetry::new(Duration::from_secs(300));
        let table = labels([("table", "public.orders")]);
        telemetry
            .metrics()
            .counter(names::PIPELINE_ROWS_STAGED_TOTAL, &table, 3);
        let response = telemetry.respond(&Method::GET, "/metrics");
        assert_eq!(
            response.headers()[hyper::header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let (status, text) = body(response);
        assert_eq!(status, StatusCode::OK);
        assert!(
            text.contains("pg2iceberg_pipeline_rows_staged_total{table=\"public.orders\"} 3\n"),
            "{text}"
        );
        assert!(text.contains("pg2iceberg_build_info{revision=\""), "{text}");
        assert!(text.contains("\nprocess_start_time_seconds "), "{text}");
    }

    #[test]
    fn ready_while_snapshotting_or_running() {
        let telemetry = Telemetry::new(Duration::from_secs(300));
        let ready = |t: &Telemetry| body(t.respond(&Method::GET, "/readyz"));
        assert_eq!(
            ready(&telemetry),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "not ready: starting\n".into()
            )
        );
        Phase::Snapshotting.set(telemetry.registry());
        assert_eq!(
            ready(&telemetry),
            (StatusCode::OK, "ready: snapshotting\n".into())
        );
        Phase::Running.set(telemetry.registry());
        assert_eq!(
            ready(&telemetry),
            (StatusCode::OK, "ready: running\n".into())
        );
        Phase::Stopping.set(telemetry.registry());
        assert_eq!(ready(&telemetry).0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn unhealthy_once_nothing_completes_for_the_liveness_timeout() {
        let telemetry = Telemetry::new(Duration::ZERO);
        std::thread::sleep(Duration::from_millis(5));
        let (status, text) = body(telemetry.respond(&Method::GET, "/healthz"));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(text.starts_with("stuck: nothing completed for"), "{text}");

        let telemetry = Telemetry::new(Duration::from_secs(300));
        assert_eq!(
            body(telemetry.respond(&Method::GET, "/healthz")),
            (StatusCode::OK, "ok\n".into())
        );
    }

    #[test]
    fn other_paths_and_methods_are_refused() {
        let telemetry = Telemetry::new(Duration::from_secs(300));
        assert_eq!(
            telemetry.respond(&Method::GET, "/").status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            telemetry.respond(&Method::POST, "/metrics").status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }

    /// The endpoint answers over HTTP where `metrics_addr` says.
    #[tokio::test]
    async fn serves_over_http() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let cfg = Config {
            metrics_addr: "127.0.0.1:0".into(),
            ..Config::default()
        };
        // Port 0 can't name the bound port: bind first, then serve it.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let telemetry = Telemetry::new(cfg.liveness_timeout().unwrap());
        tokio::spawn(serve(listener, telemetry));

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(response.ends_with("\r\n\r\nok\n"), "{response}");
    }

    /// An address `metrics_addr` names must be free; the default may be
    /// taken.
    #[tokio::test]
    async fn a_taken_address_fails_the_start_only_when_configured() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap().to_string();
        let cfg = Config {
            metrics_addr: addr,
            ..Config::default()
        };
        let err = Telemetry::start(&cfg, Duration::from_secs(1))
            .await
            .err()
            .expect("the address is taken");
        assert!(format!("{err:#}").contains("metrics_addr"), "{err:#}");
        let off = Config {
            metrics_addr: "off".into(),
            ..Config::default()
        };
        Telemetry::start(&off, Duration::from_secs(1))
            .await
            .unwrap();
    }
}
