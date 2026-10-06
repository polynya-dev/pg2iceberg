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
#   docker build --platform linux/amd64 -t pg2iceberg-rust:dev .
#
# The build stage runs on the build host's architecture and
# cross-compiles to the target's (linux/amd64 or linux/arm64), so an
# amd64 image builds natively fast on an arm64 host (Apple Silicon)
# and vice versa.
#
# Run:
#   docker run --rm -v $(pwd)/config.yaml:/etc/pg2iceberg/config.yaml \
#     pg2iceberg-rust:dev run --config /etc/pg2iceberg/config.yaml

ARG RUST_VERSION=1.85

FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-bookworm AS build
ARG BUILDARCH
ARG TARGETARCH
WORKDIR /src

# System deps the build needs:
# - `git` for the iceberg-rust fork (cargo fetches via git+https)
# - `pkg-config` + `libssl-dev` are NOT needed because we use rustls
#   throughout (tokio-postgres-rustls, reqwest+rustls). Including them
#   would silently switch object_store to native-tls if a feature flip
#   ever changes default-features.
# - `protobuf-compiler` not needed (no .proto in the build).
# - a cross C toolchain when the target architecture isn't the build
#   host's: `ring` and the compression crates build C code, and the
#   final link needs the target's libc.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      case "$TARGETARCH" in \
        amd64) apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross ;; \
        arm64) apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu libc6-dev-arm64-cross ;; \
        *) echo "unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
      esac; \
    fi \
 && rm -rf /var/lib/apt/lists/*

# Pre-fetch deps. Copying just the manifests + workspace structure
# means Docker can cache the deps layer until any Cargo.* changes.
# `cargo fetch` populates the registry + git deps without compiling.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
RUN cargo fetch --locked

# The target's Rust triple and, cross-compiling, its C compiler and
# linker. After the toolchain file, so the target is added to the
# toolchain it selects.
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-gnu > /target ;; \
      arm64) echo aarch64-unknown-linux-gnu > /target ;; \
      *) echo "unsupported target architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac \
 && rustup target add "$(cat /target)"

ARG FEATURES="prod"
ARG COMMIT_SHA=""
ENV PG2ICEBERG_COMMIT_SHA=${COMMIT_SHA}

# Release build of just the binary crate. `--locked` to refuse to
# update Cargo.lock — reproducible builds. `--frozen` would also
# refuse network, but `cargo fetch` above already populated the
# offline cache, so it'd technically work; we keep `--locked` only
# to allow lockfile re-resolution if a transitive crate yanks
# (rare, but cleaner failure mode).
RUN TARGET="$(cat /target)" \
 && if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      GCC="${TARGET%%-unknown-*}-linux-gnu-gcc"; \
      export "CC_$(echo "$TARGET" | tr - _)=$GCC"; \
      export "CARGO_TARGET_$(echo "$TARGET" | tr a-z- A-Z_)_LINKER=$GCC"; \
    fi \
 && CARGO_PROFILE_RELEASE_STRIP=symbols cargo build --release --locked \
    --target "$TARGET" --bin pg2iceberg \
    $(if [ -n "$FEATURES" ]; then echo "--features $FEATURES"; fi) \
 && cp "target/$TARGET/release/pg2iceberg" /pg2iceberg

# ── runtime ──────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# `ca-certificates` for TLS roots; `tini` as PID 1 so SIGINT/SIGTERM
# reach the binary (the lifecycle has explicit handlers and needs
# the signal). Both tiny.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 65532 --no-create-home --shell /usr/sbin/nologin pg2iceberg

COPY --from=build /pg2iceberg /usr/local/bin/pg2iceberg

USER pg2iceberg

# tini-as-PID-1 gives correct signal forwarding without a shell wrapper.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/pg2iceberg"]
# No default subcommand — operators must pass one
# (`run`, `snapshot`, `cleanup`, `compact`, `maintain`, etc.).
