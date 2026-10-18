"""Summarizes a sample.sh CSV: rates over the run, and lag at its end.

usage: python3 summarize.py run.csv [from_s] [to_s]
"""
import csv
import sys


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


rows = list(csv.DictReader(open(sys.argv[1])))
lo = float(sys.argv[2]) if len(sys.argv) > 2 else -1
hi = float(sys.argv[3]) if len(sys.argv) > 3 else float("inf")
rows = [r for r in rows if lo <= float(r["t"]) <= hi]
first, last = rows[0], rows[-1]
dt = float(last["t"]) - float(first["t"])


def rate(col):
    a, b = num(first[col]), num(last[col])
    return (b - a) / dt if a is not None and b is not None and dt else None


def series(col):
    return [v for v in (num(r[col]) for r in rows) if v is not None]


print(f"window {first['t']}s..{last['t']}s ({dt:.0f}s, {len(rows)} samples)")
print(f"  PG commits/s          {rate('xacts'):.0f}")
print(f"  WAL MB/s              {rate('wal_mb'):.2f}")
print(f"  source changes/s      {rate('src_changes'):.0f}")
print(f"  changes received/s    {rate('changes'):.0f}")
print(f"  rows materialized/s   {rate('mat_rows'):.0f}")
for col in ["slot_lag_mb", "unstaged_mb", "pending", "fresh_s", "buf", "p2i_cpu", "pg_cpu"]:
    s = series(col)
    if s:
        print(f"  {col:<20}  start {s[0]:>10.0f}  end {s[-1]:>10.0f}  max {max(s):>10.0f}  mean {sum(s) / len(s):>10.1f}")
