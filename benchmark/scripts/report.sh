#!/usr/bin/env bash
# Merge the local k6 summary (out/summary.json) and the lag samples
# (out/lag.csv) into a human-readable out/report.md: write throughput +
# latency, replication-lag profile, and the headline post-traffic
# catch-up time.
#
# Usage: report.sh [out_dir]
set -euo pipefail

OUT="${1:-out}"
SUMMARY="$OUT/summary.json"
LAG="$OUT/lag.csv"
REPORT="$OUT/report.md"

[ -f "$SUMMARY" ] || { echo "missing $SUMMARY (run the bench first)"; exit 1; }

m() { jq -r "$1 // \"n/a\"" "$SUMMARY"; }

total_reqs="$(m '.metrics.http_reqs.values.count')"
rps="$(m '.metrics.http_reqs.values.rate')"
fail_rate="$(m '.metrics.http_req_failed.values.rate')"
p50="$(m '.metrics.http_req_duration.values["p(50)"]')"
p95="$(m '.metrics.http_req_duration.values["p(95)"]')"
p99="$(m '.metrics.http_req_duration.values["p(99)"]')"

{
  echo "# pg2iceberg benchmark report"
  echo
  echo "_Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)_"
  echo
  echo "## Write traffic (k6 → shim → RDS)"
  echo
  echo "| Metric | Value |"
  echo "|---|---|"
  printf "| Total requests | %s |\n" "$total_reqs"
  printf "| Throughput (req/s) | %.1f |\n" "$rps" 2>/dev/null || printf "| Throughput (req/s) | %s |\n" "$rps"
  printf "| Error rate | %s |\n" "$fail_rate"
  printf "| Latency p50 / p95 / p99 (ms) | %s / %s / %s |\n" "$p50" "$p95" "$p99"
  echo
  echo "### Per-action counts"
  echo
  echo "| Action | Count |"
  echo "|---|---|"
  for a in act_new_rider act_new_driver act_request_ride act_complete_ride act_cancel_ride; do
    c="$(jq -r ".metrics.\"$a\".values.count // 0" "$SUMMARY")"
    printf "| %s | %s |\n" "${a#act_}" "$c"
  done
  echo
} > "$REPORT"

if [ -f "$LAG" ]; then
  # Headline metrics from the lag CSV: peak slot lag, peak pending
  # events, and catch-up time (seconds from the last non-trivial-lag
  # sample to the final near-zero sample).
  awk -F',' '
    NR==1 { next }
    {
      gsub(/"/,"",$3); gsub(/"/,"",$5);
      lb=$3+0; pe=$5+0;
      if (lb>maxlb) maxlb=lb;
      if (pe>maxpe) maxpe=pe;
      n++; ts[n]=$1; lag[n]=lb; pend[n]=pe;
    }
    END {
      # catch-up: find last sample where pending>0 or lag>1MB, measure to end.
      last_busy=0;
      for (i=1;i<=n;i++) if (pend[i]>0 || lag[i]>1048576) last_busy=i;
      catchup="n/a";
      if (last_busy>0 && last_busy<n) {
        cmd="date -u -j -f %Y-%m-%dT%H:%M:%SZ " ts[n] " +%s 2>/dev/null || date -u -d " ts[n] " +%s";
        cmd | getline t_end; close(cmd);
        cmd2="date -u -j -f %Y-%m-%dT%H:%M:%SZ " ts[last_busy] " +%s 2>/dev/null || date -u -d " ts[last_busy] " +%s";
        cmd2 | getline t_busy; close(cmd2);
        catchup=(t_end-t_busy) "s";
      } else if (last_busy==0) { catchup="0s (never lagged)"; }
      printf "## Replication lag\n\n";
      printf "| Metric | Value |\n|---|---|\n";
      printf "| Peak slot lag (bytes) | %d |\n", maxlb;
      printf "| Peak pending events (flush→materialize) | %d |\n", maxpe;
      printf "| Post-traffic catch-up to ~0 lag | %s |\n", catchup;
      printf "| Lag samples | %d |\n\n", n;
    }
  ' "$LAG" >> "$REPORT"

  {
    echo "### Lag over time (sampled)"
    echo
    echo '```'
    echo "ts                    slot_lag_bytes  pending_events"
    awk -F',' 'NR>1 { gsub(/"/,"",$1); gsub(/"/,"",$3); gsub(/"/,"",$5); printf "%s  %14s  %14s\n", $1, $3, $5 }' "$LAG"
    echo '```'
    echo
  } >> "$REPORT"
else
  echo "_(no lag.csv found — lag profile unavailable)_" >> "$REPORT"
fi

echo "Report written to $REPORT"
echo
cat "$REPORT"
