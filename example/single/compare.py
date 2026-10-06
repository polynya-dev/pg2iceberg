"""Compare Postgres with ClickHouse reading the Iceberg tables pg2iceberg
wrote: every row, every column rendered the same way on both sides, and
ClickHouse's `count()`. Exits non-zero on any difference."""

import os
import subprocess
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
TABLES = ["riders", "drivers", "rides", "payments", "ratings"]


def pg(sql):
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "rideshare",
         "-At", "-F", "\t", "-c", sql],
        cwd=HERE, capture_output=True, text=True, check=True)
    return out.stdout.splitlines()


def ch(sql):
    req = urllib.request.Request("http://localhost:8123", data=(sql + " FORMAT TSVRaw").encode())
    return urllib.request.urlopen(req).read().decode().splitlines()


def render(name, ty):
    """The same text for a value on both sides."""
    if name == "notes":  # TOAST-sized: compare digests
        return f"coalesce(md5({name}), 'NULL')", f"ifNull(lower(hex(MD5({name}))), 'NULL')"
    if ty.startswith("timestamp"):
        return (f"coalesce((extract(epoch FROM {name}) * 1000000)::bigint::text, 'NULL')",
                f"ifNull(toString(toUnixTimestamp64Micro({name})), 'NULL')")
    if ty == "numeric":
        return f"coalesce(trim_scale({name})::text, 'NULL')", f"ifNull(toString({name}), 'NULL')"
    return f"coalesce({name}::text, 'NULL')", f"ifNull(toString({name}), 'NULL')"


ok = True
for table in TABLES:
    cols = [tuple(r.split("\t")) for r in pg(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = 'public' AND table_name = '{table}' ORDER BY ordinal_position")]
    pg_exprs, ch_exprs = zip(*(render(n, t) for n, t in cols))
    iceberg = f"rideshare.`rideshare.{table}`"
    pg_rows = pg(f"SELECT {', '.join(pg_exprs)} FROM {table}")
    ch_rows = ch(f"SELECT {', '.join(ch_exprs)} FROM {iceberg}")
    ch_count = int(ch(f"SELECT count() FROM {iceberg}")[0])
    only_pg, only_ch = set(pg_rows) - set(ch_rows), set(ch_rows) - set(pg_rows)
    dupes = len(ch_rows) - len(set(ch_rows))
    good = not (only_pg or only_ch or dupes) and ch_count == len(pg_rows)
    ok &= good
    print(f"{table:9} pg={len(pg_rows):6} iceberg={len(ch_rows):6} count()={ch_count:6} "
          f"only_pg={len(only_pg)} only_iceberg={len(only_ch)} duplicates={dupes}  "
          f"{'OK' if good else 'DIFF'}")
    for r in sorted(only_pg)[:3]:
        print("   pg      :", r[:200])
    for r in sorted(only_ch)[:3]:
        print("   iceberg :", r[:200])
print("ALL MATCH" if ok else "MISMATCHES FOUND")
sys.exit(0 if ok else 1)
