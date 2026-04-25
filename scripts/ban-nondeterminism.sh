#!/usr/bin/env bash
# Fail if any non-deterministic call shows up outside an allowlisted crate.
#
# Allowlist: production-impl crates that legitimately need real clocks, real
# task spawning, real RNG, or real network. Trait-only crates and pipeline
# crates must funnel through `pg2iceberg-core::io::{Clock, IdGen, Spawner}`.
#
# Run from the workspace root.

set -euo pipefail

cd "$(dirname "$0")/.."

# Patterns that escape determinism. Each entry is grep -E.
PATTERNS=(
  'SystemTime::'
  'Instant::now'
  'chrono::Utc::now'
  'chrono::Local::now'
  'tokio::spawn'
  'tokio::task::spawn_blocking'
  '#\[tokio::test'
  'std::thread::spawn'
  'rand::thread_rng'
  'rand::random'
  'Uuid::new_v4'
  'getrandom::'
)

# Allowlisted paths (regex, matched against the file path). Keep the list
# narrow — production-impl modules only.
ALLOW='^crates/pg2iceberg-pg/src/prod/|^crates/pg2iceberg-iceberg/src/prod/|^crates/pg2iceberg-coord/src/prod/|^crates/pg2iceberg-stream/src/prod/|^crates/pg2iceberg/src/main\.rs$|^scripts/|^crates/[^/]+/tests/|/target/'

fail=0

for pat in "${PATTERNS[@]}"; do
  # Find offending Rust files. Exclude target/, the allowlist, and any line
  # whose code portion starts with `//` (doc/line comment). This keeps API
  # references in docs from tripping the check. The trailing grep keeps lines
  # whose content (everything after `path:line:`) does NOT start with `//` once
  # leading whitespace is stripped.
  hits=$(grep -RInE --include='*.rs' "$pat" crates/ 2>/dev/null \
    | grep -vE "$ALLOW" \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*//' \
    || true)
  if [[ -n "$hits" ]]; then
    echo "ban-nondeterminism: pattern '$pat' found in non-allowlisted code:"
    echo "$hits"
    echo
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  echo "ban-nondeterminism: FAIL"
  echo "If a hit is legitimate (e.g. a production-impl module), add it under crates/<crate>/src/prod/ and adjust ALLOW in scripts/ban-nondeterminism.sh."
  exit 1
fi

echo "ban-nondeterminism: OK"
