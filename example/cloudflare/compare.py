"""Compare Postgres with DuckDB reading the Iceberg tables pg2iceberg wrote
to the R2 bucket's catalog: every row, every column rendered the same way
on both sides. Exits non-zero on any difference.

Reads POSTGRES_URL, ICEBERG_CATALOG_URL, ICEBERG_WAREHOUSE,
ICEBERG_CATALOG_TOKEN and ICEBERG_NAMESPACE from the environment."""

import os
import sys

import duckdb
import psycopg2

TABLES = ["riders", "drivers", "rides", "payments", "ratings"]
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "rideshare")


def render(name, ty):
    """The same text for a value on both sides: (Postgres, DuckDB)."""
    if name == "notes":  # TOAST-sized: compare digests
        return f"coalesce(md5({name}), 'NULL')", f"coalesce(md5({name}), 'NULL')"
    if ty.startswith("timestamp"):
        return (f"coalesce((extract(epoch FROM {name}) * 1000000)::bigint::text, 'NULL')",
                f"coalesce(epoch_us({name})::varchar, 'NULL')")
    if ty == "numeric":
        return (f"coalesce(trim_scale({name})::text, 'NULL')",
                f"coalesce(CASE WHEN contains({name}::varchar, '.') "
                f"THEN rtrim(rtrim({name}::varchar, '0'), '.') ELSE {name}::varchar END, 'NULL')")
    return f"coalesce({name}::text, 'NULL')", f"coalesce({name}::varchar, 'NULL')"


def lake():
    db = duckdb.connect()
    db.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs")
    db.execute("CREATE SECRET (TYPE ICEBERG, TOKEN $token)",
               {"token": os.environ["ICEBERG_CATALOG_TOKEN"]})
    warehouse = os.environ["ICEBERG_WAREHOUSE"].replace("'", "''")
    endpoint = os.environ["ICEBERG_CATALOG_URL"].replace("'", "''")
    db.execute(f"ATTACH '{warehouse}' AS lake (TYPE ICEBERG, ENDPOINT '{endpoint}')")
    return db


def main():
    conn = psycopg2.connect(os.environ["POSTGRES_URL"])
    db = lake()
    ok = True
    for table in TABLES:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position",
                        (table,))
            cols = cur.fetchall()
            pg_exprs, duck_exprs = zip(*(render(n, t) for n, t in cols))
            cur.execute(f"SELECT {' || chr(9) || '.join(pg_exprs)} FROM {table}")
            pg_rows = [r[0] for r in cur.fetchall()]
        duck_rows = [r[0] for r in db.execute(
            f"SELECT {' || chr(9) || '.join(duck_exprs)} FROM lake.{NAMESPACE}.{table}").fetchall()]
        only_pg, only_duck = set(pg_rows) - set(duck_rows), set(duck_rows) - set(pg_rows)
        dupes = len(duck_rows) - len(set(duck_rows))
        good = not (only_pg or only_duck or dupes)
        ok &= good
        print(f"{table:9} pg={len(pg_rows):6} iceberg={len(duck_rows):6} "
              f"only_pg={len(only_pg)} only_iceberg={len(only_duck)} duplicates={dupes}  "
              f"{'OK' if good else 'DIFF'}")
        for r in sorted(only_pg)[:3]:
            print("   pg      :", r[:200])
        for r in sorted(only_duck)[:3]:
            print("   iceberg :", r[:200])
    print("ALL MATCH" if ok else "MISMATCHES FOUND")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
