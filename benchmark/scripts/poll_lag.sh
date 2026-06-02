#!/usr/bin/env bash
# Sample the shim's /admin/lag endpoint on an interval and append a CSV
# row each time. Runs locally (alongside k6). Captures replication slot
# lag, flush→materialize pending events, and per-table row counts —
# including the post-traffic drain window so report.sh can compute
# catch-up time.
#
# Usage: poll_lag.sh <alb_url> <duration_seconds> [out_csv] [interval_seconds]
set -euo pipefail

TARGET="${1:?usage: poll_lag.sh <alb_url> <duration_seconds> [out_csv] [interval]}"
DURATION="${2:?missing duration in seconds}"
OUT="${3:-out/lag.csv}"
INTERVAL="${4:-5}"

mkdir -p "$(dirname "$OUT")"
echo "ts,slot_present,slot_lag_bytes,wal_status,pending_events,riders,drivers,rides,payments,ratings" > "$OUT"

end=$(( $(date +%s) + DURATION ))
while [ "$(date +%s)" -lt "$end" ]; do
  j="$(curl -fsS --max-time 10 "$TARGET/admin/lag" 2>/dev/null || echo '{}')"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  row="$(printf '%s' "$j" | jq -r --arg ts "$ts" '
    [ $ts,
      (.slot.present // false),
      (.slot.lag_bytes // ""),
      (.slot.wal_status // ""),
      (.pending_events // ""),
      (.rows.riders // ""),
      (.rows.drivers // ""),
      (.rows.rides // ""),
      (.rows.payments // ""),
      (.rows.ratings // "")
    ] | @csv' 2>/dev/null || echo "")"
  if [ -n "$row" ]; then
    echo "$row" >> "$OUT"
    echo "[$ts] lag_bytes=$(printf '%s' "$j" | jq -r '.slot.lag_bytes // "?"') pending=$(printf '%s' "$j" | jq -r '.pending_events // "?"')"
  fi
  sleep "$INTERVAL"
done

echo "lag samples written to $OUT"
