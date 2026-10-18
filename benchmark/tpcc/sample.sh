#!/usr/bin/env bash
# Prints one CSV line every INTERVAL seconds (default 5) until killed:
#
#   t            seconds since start
#   wal_mb       WAL written since start (MB)
#   slot_lag_mb  WAL the slot hasn't confirmed (current LSN - confirmed_flush)
#   unstaged_mb  WAL not yet staged (current LSN - pipeline_flushed_lsn metric)
#   pending      staged change events not yet materialized (log_seq - mat_cursor)
#   changes      pg2iceberg_pipeline_changes_total
#   mat_rows     pg2iceberg_materializer_rows_total
#   fresh_s      now - oldest per-table materialized source commit time
#   buf          pg2iceberg_replication_buffered_messages
#   xacts        Postgres commits (pg_stat_database.xact_commit, db tpcc)
#   src_changes  rows Postgres inserted, updated and deleted in the TPROC-C tables
#   p2i_cpu      pg2iceberg container CPU% (100 = one core)
#   p2i_mem      pg2iceberg container memory
#   pg_cpu       postgres container CPU%
set -euo pipefail
cd "$(dirname "$0")"
INTERVAL="${INTERVAL:-5}"
METRICS="${METRICS:-http://localhost:9391/metrics}"

psql() { docker compose exec -T postgres psql -U postgres -d tpcc -AtX -F, -c "$1"; }

start_lsn=$(psql "select pg_current_wal_lsn() - '0/0'")
t0=$(date +%s)
echo "t,wal_mb,slot_lag_mb,unstaged_mb,pending,changes,mat_rows,fresh_s,buf,xacts,src_changes,p2i_cpu,p2i_mem,pg_cpu"
while true; do
  pgrow=$(psql "select pg_current_wal_lsn() - '0/0',
      coalesce((select pg_current_wal_lsn() - confirmed_flush_lsn from pg_replication_slots where slot_name = 'pg2iceberg_slot'), 0),
      (select xact_commit from pg_stat_database where datname = 'tpcc'),
      (select sum(n_tup_ins + n_tup_upd + n_tup_del) from pg_stat_user_tables where schemaname = 'public')" 2>/dev/null || echo "0,0,0,0")
  IFS=, read -r lsn slot_lag xacts src_changes <<<"$pgrow"
  # Log entries are [start, end): log_seq.next_offset is the end claimed so
  # far, mat_cursor.last_offset the end materialized (-1: none).
  pending=$(psql "select coalesce(sum(s.next_offset - greatest(c.last_offset, 0)), 0)
      from _pg2iceberg.log_seq s left join _pg2iceberg.mat_cursor c
      on c.table_name = s.table_name and c.group_name = 'default'" 2>/dev/null || echo "")
  m=$(curl -sf "$METRICS" 2>/dev/null || true)
  metric() { awk -v n="$1" '$1 == n || index($1, n "{") == 1 { s += $2 } END { printf "%.0f", s }' <<<"$m"; }
  changes=$(metric pg2iceberg_pipeline_changes_total)
  mat_rows=$(metric pg2iceberg_materializer_rows_total)
  buf=$(metric pg2iceberg_replication_buffered_messages)
  flushed=$(metric pg2iceberg_pipeline_flushed_lsn)
  now=$(date +%s)
  fresh=$(awk -v now="$now" '/^pg2iceberg_materializer_source_commit_timestamp_seconds\{/ { if (min == "" || $2 < min) min = $2 } END { if (min == "") print ""; else printf "%.0f", now - min }' <<<"$m")
  unstaged=""
  if [[ -n "$flushed" && "$flushed" != 0 ]]; then unstaged=$(( (lsn - flushed) / 1048576 )); fi
  stats=$(docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' pg2iceberg-tpcc-pg2iceberg-1 pg2iceberg-tpcc-postgres-1 2>/dev/null || true)
  p2i_cpu=$(awk -F, '/pg2iceberg-1/ { sub("%", "", $2); print $2 }' <<<"$stats")
  p2i_mem=$(awk -F, '/pg2iceberg-1/ { split($3, a, " / "); print a[1] }' <<<"$stats")
  pg_cpu=$(awk -F, '/postgres-1/ { sub("%", "", $2); print $2 }' <<<"$stats")
  echo "$(( now - t0 )),$(( (lsn - start_lsn) / 1048576 )),$(( slot_lag / 1048576 )),${unstaged},${pending},${changes},${mat_rows},${fresh},${buf},${xacts},${src_changes},${p2i_cpu},${p2i_mem},${pg_cpu}"
  sleep "$INTERVAL"
done
