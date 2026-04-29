# syntax=docker/dockerfile:1.7
# Rust port of the pg2iceberg binary. Multi-stage build:
#
# 1. `build` — pulls the workspace, fetches deps (including the
#    polynya-dev/iceberg-rust git fork the workspace pins), and
#    builds the `pg2iceberg` binary with `--features prod` so the
#    REST catalog + S3 + replication-mode tokio-postgres are wired
#    in. Layer caching: deps fetch happens before source copy so
#    repeated source-only changes reuse the deps layer.
#
# 2. `runtime` — debian-slim with `ca-certificates` for TLS roots
#    (rustls uses webpki-roots, but the catalog REST client and
#    S3 traffic still need a system CA bundle for non-AWS
#    endpoints). Statically-linked-ish binary copied in; no Rust
#    toolchain in the final image.
#
# Build args:
#   COMMIT_SHA   — stamped into binary metadata via build script (TODO).
#   RUST_VERSION — override the toolchain (default matches
#                  `rust-toolchain.toml`'s "stable").
#   FEATURES     — extra cargo features (default: "prod"). Pass
#                  `FEATURES=""` for a sim-only build (no S3, no PG
#                  prod path).
#
# Build:
#   docker build -t pg2iceberg-rust:dev .
#
# Run:
#   docker run --rm -v $(pwd)/config.yaml:/etc/pg2iceberg/config.yaml \
#     pg2iceberg-rust:dev run --config /etc/pg2iceberg/config.yaml

ARG RUST_VERSION=1.85

FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-bookworm AS build
WORKDIR /src

# System deps the build needs:
# - `git` for the iceberg-rust fork (cargo fetches via git+https)
# - `pkg-config` + `libssl-dev` are NOT needed because we use rustls
#   throughout (tokio-postgres-rustls, reqwest+rustls). Including them
#   would silently switch object_store to native-tls if a feature flip
#   ever changes default-features.
# - `protobuf-compiler` not needed (no .proto in the build).
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Pre-fetch deps. Copying just the manifests + workspace structure
# means Docker can cache the deps layer until any Cargo.* changes.
# `cargo fetch` populates the registry + git deps without compiling.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
RUN cargo fetch --locked

ARG FEATURES="prod"
ARG COMMIT_SHA=""
ENV PG2ICEBERG_COMMIT_SHA=${COMMIT_SHA}

# Release build of just the binary crate. `--locked` to refuse to
# update Cargo.lock — reproducible builds. `--frozen` would also
# refuse network, but `cargo fetch` above already populated the
# offline cache, so it'd technically work; we keep `--locked` only
# to allow lockfile re-resolution if a transitive crate yanks
# (rare, but cleaner failure mode).
RUN cargo build --release --locked --bin pg2iceberg \
    $(if [ -n "$FEATURES" ]; then echo "--features $FEATURES"; fi) \
 && strip target/release/pg2iceberg

# ── runtime ──────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# `ca-certificates` for TLS roots; `tini` as PID 1 so SIGINT/SIGTERM
# reach the binary (the lifecycle has explicit handlers and needs
# the signal). Both tiny.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 65532 --no-create-home --shell /usr/sbin/nologin pg2iceberg

COPY --from=build /src/target/release/pg2iceberg /usr/local/bin/pg2iceberg

USER pg2iceberg

# tini-as-PID-1 gives correct signal forwarding without a shell wrapper.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/pg2iceberg"]
# No default subcommand — operators must pass one
# (`run`, `snapshot`, `cleanup`, `compact`, `maintain`, etc.).
