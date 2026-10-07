"""Sustained load: RATE riders a second for DURATION seconds — and, from a
minute in, the batch from a minute before deleted — while sampling the
replication slot. Then wait for pg2iceberg to drain and judge whether lag
and retained WAL stayed flat.

Reads POSTGRES_URL from the environment (and RATE, DURATION, SAMPLE)."""

import os
import threading
import time

import psycopg2

RATE = int(os.environ.get("RATE", "1000"))
DURATION = int(os.environ.get("DURATION", "300"))
SAMPLE = int(os.environ.get("SAMPLE", "5"))
URL = os.environ["POSTGRES_URL"]

SLOT_SQL = """
SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint,
       wal_status, active
FROM pg_replication_slots WHERE slot_name = 'pg2iceberg_slot'"""


def mb(n):
    return f"{n / 1e6:7.2f} MB"


def load(stats, stop):
    conn = psycopg2.connect(URL)
    conn.autocommit = True
    cur = conn.cursor()
    tag = f"load{int(time.time())}"
    start = time.time()
    for second in range(DURATION):
        if stop.is_set():
            break
        with conn:
            cur.execute(
                "INSERT INTO riders (email, first_name, city) "
                "SELECT %s || '-' || %s || '-' || g || '@example.com', 'Load', 'Austin' "
                "FROM generate_series(1, %s) g",
                (tag, second, RATE),
            )
            if second >= 60:
                cur.execute("DELETE FROM riders WHERE email LIKE %s",
                            (f"{tag}-{second - 60}-%",))
        stats["inserted"] += RATE
        stats["deleted"] += RATE if second >= 60 else 0
        behind = start + second + 1 - time.time()
        if behind > 0:
            time.sleep(behind)
        else:
            stats["late"] += 1
    stats["load_seconds"] = time.time() - start
    stop.set()


def main():
    conn = psycopg2.connect(URL)
    conn.autocommit = True
    cur = conn.cursor()
    # Earlier runs' rows out, their space reused, and pg2iceberg caught up.
    cur.execute("DELETE FROM riders r WHERE first_name IN ('Load', 'Bulk') "
                "AND NOT EXISTS (SELECT 1 FROM rides WHERE rider_id = r.id)")
    print(f"deleted {cur.rowcount} rows from earlier runs")
    cur.execute("VACUUM riders")
    settle0 = time.time()
    while time.time() - settle0 < 600:
        cur.execute(SLOT_SQL)
        if cur.fetchone()[0] < 1 << 20:
            break
        time.sleep(2)
    print(f"caught up after {time.time() - settle0:.0f}s")
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    print(f"database size before: {cur.fetchone()[0]}; {RATE} rows/s for {DURATION}s")
    stats = {"inserted": 0, "deleted": 0, "late": 0}
    stop = threading.Event()
    loader = threading.Thread(target=load, args=(stats, stop))
    t0 = time.time()
    loader.start()
    samples = []
    print(f"{'t':>5}  {'inserted':>9}  {'slot lag':>10}  {'retained WAL':>12}  status")
    while not stop.is_set():
        cur.execute(SLOT_SQL)
        lag, retained, status, active = cur.fetchone()
        t = time.time() - t0
        samples.append((t, lag, retained))
        print(f"{t:5.0f}  {stats['inserted']:9d}  {mb(lag)}  {mb(retained):>12}  {status}{'' if active else ' (inactive)'}",
              flush=True)
        stop.wait(SAMPLE)
    loader.join()
    print(f"load: {stats['inserted']} inserted, {stats['deleted']} deleted in "
          f"{stats['load_seconds']:.0f}s ({stats['late']} seconds fell behind schedule)")

    # Drain.
    drain0 = time.time()
    while True:
        cur.execute(SLOT_SQL)
        lag, retained, status, _ = cur.fetchone()
        if lag < 1 << 20 or time.time() - drain0 > 600:
            break
        time.sleep(2)
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    print(f"database size after: {cur.fetchone()[0]}")
    print(f"drained to {mb(lag)} slot lag in {time.time() - drain0:.0f}s after the load stopped "
          f"(retained WAL {mb(retained)})")

    # Flat or growing? Compare the load's second half with its first.
    steady = [s for s in samples if 60 <= s[0] <= DURATION]
    if len(steady) >= 4:
        half = len(steady) // 2
        avg = lambda xs, i: sum(x[i] for x in xs) / len(xs)
        print(f"slot lag:     max {mb(max(s[1] for s in samples))}, mean {mb(avg(steady[:half], 1))} "
              f"(first half) -> {mb(avg(steady[half:], 1))} (second half)")
        print(f"retained WAL: max {mb(max(s[2] for s in samples))}, mean {mb(avg(steady[:half], 2))} "
              f"(first half) -> {mb(avg(steady[half:], 2))} (second half)")


if __name__ == "__main__":
    main()
