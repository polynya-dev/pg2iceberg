# syntax=docker/dockerfile:1.7
# The release binary with line tables kept (for perf), built incrementally:
# cargo's registry and target dir live in BuildKit cache mounts.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
RUN --mount=type=cache,id=pg2iceberg-tpcc-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=pg2iceberg-tpcc-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=pg2iceberg-tpcc-target,target=/src/target \
    CARGO_PROFILE_RELEASE_DEBUG=line-tables-only \
    cargo build --release --locked --bin pg2iceberg --features prod \
 && cp target/release/pg2iceberg /pg2iceberg

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini linux-perf procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /pg2iceberg /usr/local/bin/pg2iceberg
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/pg2iceberg"]
