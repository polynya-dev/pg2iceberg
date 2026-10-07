"""How long a change takes to reach the Iceberg tables: update one row in
Postgres, then poll DuckDB until it reads the new value. Prints seconds.

Reads POSTGRES_URL, ICEBERG_CATALOG_URL, ICEBERG_WAREHOUSE,
ICEBERG_CATALOG_TOKEN and ICEBERG_NAMESPACE from the environment."""

import os
import sys
import time

import psycopg2

from compare import NAMESPACE, lake


def main(timeout=300):
    conn = psycopg2.connect(os.environ["POSTGRES_URL"])
    conn.autocommit = True
    db = lake()
    with conn.cursor() as cur:
        cur.execute("UPDATE riders SET last_ride_at = clock_timestamp() "
                    "WHERE id = (SELECT min(id) FROM riders) "
                    "RETURNING id, (extract(epoch FROM last_ride_at) * 1000000)::bigint")
        rider, stamp = cur.fetchone()
    start = time.time()
    while time.time() - start < timeout:
        seen = db.execute(
            f"SELECT epoch_us(last_ride_at) FROM lake.{NAMESPACE}.riders WHERE id = ?", [rider]
        ).fetchone()
        if seen and seen[0] == stamp:
            print(f"a change reached Iceberg in {time.time() - start:.1f}s")
            return 0
        time.sleep(1)
    print(f"a change hadn't reached Iceberg after {timeout}s")
    return 1


def bulk(n, timeout=600):
    """Insert `n` riders in one statement; time until DuckDB counts them."""
    conn = psycopg2.connect(os.environ["POSTGRES_URL"])
    conn.autocommit = True
    tag = f"bulk{int(time.time())}"
    with conn.cursor() as cur:
        cur.execute("INSERT INTO riders (email, first_name, city) "
                    "SELECT %s || '-' || g || '@example.com', 'Bulk', 'Austin' "
                    "FROM generate_series(1, %s) g", (tag, n))
    start = time.time()
    db = lake()
    while time.time() - start < timeout:
        (seen,) = db.execute(
            f"SELECT count(*) FROM lake.{NAMESPACE}.riders WHERE email LIKE ?", [f"{tag}-%"]
        ).fetchone()
        if seen == n:
            print(f"{n} rows reached Iceberg in {time.time() - start:.1f}s")
            return 0
        time.sleep(1)
    print(f"{seen} of {n} rows had reached Iceberg after {timeout}s")
    return 1


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "bulk":
        sys.exit(bulk(int(sys.argv[2])))
    sys.exit(main())
