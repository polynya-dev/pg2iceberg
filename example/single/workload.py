"""Smoke-test workload for pg2iceberg: simulate.py's rideshare actions
(multi-table transactions) plus deletes, churn within a transaction,
rollbacks, key changes, TOAST updates, partition moves, large
transactions, TRUNCATE and schema changes. Two random workers run
concurrently with a timed-events process for DURATION seconds."""

import collections
import multiprocessing as mp
import os
import random
import time

import psycopg2

import simulate

DURATION = float(os.environ.get("DURATION", "300"))
URL = os.environ["DATABASE_URL"]

# A ~12KB mostly incompressible value: stored out of line (TOAST), so an
# UPDATE that leaves it alone sends pgoutput's "unchanged TOAST" marker.
BIG_NOTE = "(SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 400))"


def random_id(cur, table):
    cur.execute(f"SELECT id FROM {table} ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def delete_rating(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ratings WHERE id = (SELECT id FROM ratings ORDER BY random() LIMIT 1)")
    conn.commit()


def delete_cancelled_ride(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM rides WHERE id = (SELECT r.id FROM rides r WHERE r.status = 'cancelled' "
            "AND NOT EXISTS (SELECT 1 FROM payments p WHERE p.ride_id = r.id) "
            "AND NOT EXISTS (SELECT 1 FROM ratings g WHERE g.ride_id = r.id) ORDER BY random() LIMIT 1)"
        )
    conn.commit()


def churn(conn):
    """Insert, update and delete one rating, and insert + update another,
    in one transaction."""
    with conn.cursor() as cur:
        ride = random_id(cur, "rides")
        if ride is None:
            conn.rollback()
            return
        cur.execute("INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) "
                    "VALUES (%s, true, 3, 'churn', now()) RETURNING id", (ride,))
        gone = cur.fetchone()[0]
        cur.execute("UPDATE ratings SET score = 4 WHERE id = %s", (gone,))
        cur.execute("DELETE FROM ratings WHERE id = %s", (gone,))
        cur.execute("INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) "
                    "VALUES (%s, false, 2, 'churn', now()) RETURNING id", (ride,))
        kept = cur.fetchone()[0]
        cur.execute("UPDATE ratings SET score = 5, comment = 'churn kept' WHERE id = %s", (kept,))
    conn.commit()


def rolled_back(conn):
    with conn.cursor() as cur:
        ride = random_id(cur, "rides")
        if ride is not None:
            cur.execute("INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) "
                        "VALUES (%s, true, 1, 'rolled back', now())", (ride,))
        cur.execute("UPDATE riders SET city = 'Nowhere' WHERE id = (SELECT id FROM riders ORDER BY random() LIMIT 1)")
    conn.rollback()


def change_key(conn):
    with conn.cursor() as cur:
        cur.execute("UPDATE ratings SET id = nextval('ratings_id_seq') "
                    "WHERE id = (SELECT id FROM ratings ORDER BY random() LIMIT 1)")
    conn.commit()


def toast_write(conn):
    with conn.cursor() as cur:
        cur.execute(f"UPDATE drivers SET notes = {BIG_NOTE} "
                    "WHERE id = (SELECT id FROM drivers ORDER BY random() LIMIT 1)")
    conn.commit()


def toast_untouched(conn):
    with conn.cursor() as cur:
        cur.execute("UPDATE drivers SET rating = round((4 + random())::numeric, 2) "
                    "WHERE id = (SELECT id FROM drivers WHERE notes IS NOT NULL ORDER BY random() LIMIT 1)")
    conn.commit()


def move_partition(conn):
    with conn.cursor() as cur:
        cur.execute("UPDATE drivers SET status = (ARRAY['active','inactive','suspended'])[1 + floor(random() * 3)::int] "
                    "WHERE id = (SELECT id FROM drivers ORDER BY random() LIMIT 1)")
    conn.commit()


def set_tier(conn):
    # Fails while the column is dropped; counted as an error.
    with conn.cursor() as cur:
        cur.execute("UPDATE riders SET tier = (ARRAY['basic','plus','gold'])[1 + floor(random() * 3)::int] "
                    "WHERE id = (SELECT id FROM riders ORDER BY random() LIMIT 1)")
    conn.commit()


def simulated(conn):
    simulate.pick_action()(conn)


MIX = [
    (simulated, 40), (delete_rating, 8), (delete_cancelled_ride, 4), (churn, 8),
    (rolled_back, 5), (change_key, 5), (toast_write, 6), (toast_untouched, 10),
    (move_partition, 6), (set_tier, 3),
]


def worker(seed, deadline, out):
    random.seed(seed)
    conn = psycopg2.connect(URL)
    counts = collections.Counter()
    fns, weights = zip(*MIX)
    while time.time() < deadline:
        fn = random.choices(fns, weights)[0]
        try:
            fn(conn)
            counts[fn.__name__] += 1
        except Exception as e:  # noqa: BLE001 — a failed action is just skipped
            conn.rollback()
            counts[f"error:{fn.__name__}:{type(e).__name__}"] += 1
        time.sleep(random.uniform(0.005, 0.03))
    out.put(dict(counts))


EVENTS = [
    (20, "bulk insert 5000 ratings",
     "INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) "
     "SELECT (SELECT id FROM rides ORDER BY random() LIMIT 1), true, 5, 'bulk', now() FROM generate_series(1, 5000)"),
    (45, "add riders.tier", "ALTER TABLE riders ADD COLUMN tier text DEFAULT 'basic'"),
    (70, "bulk delete half the bulk ratings", "DELETE FROM ratings WHERE comment = 'bulk' AND id % 2 = 0"),
    (100, "truncate ratings", "TRUNCATE ratings"),
    (130, "drop riders.tier", "ALTER TABLE riders DROP COLUMN tier"),
    (160, "re-add riders.tier", "ALTER TABLE riders ADD COLUMN tier text"),
    (190, "bulk update every ride", "UPDATE rides SET fare_cents = coalesce(fare_cents, 0) + 1"),
    (220, "bulk insert 3000 ratings",
     "INSERT INTO ratings (ride_id, from_rider, score, comment, created_at) "
     "SELECT (SELECT id FROM rides ORDER BY random() LIMIT 1), false, 4, 'bulk2', now() FROM generate_series(1, 3000)"),
    (250, "flip every driver's status (partition moves)",
     "UPDATE drivers SET status = CASE status WHEN 'active' THEN 'inactive' ELSE 'active' END"),
]


def events(start):
    conn = psycopg2.connect(URL)
    for at, name, sql in EVENTS:
        if at >= DURATION:
            break
        time.sleep(max(0, start + at - time.time()))
        t = time.time()
        with conn.cursor() as cur:
            cur.execute(sql)
            n = cur.rowcount
        conn.commit()
        print(f"[t={at:>3}s] {name}: {n} rows ({time.time() - t:.2f}s)", flush=True)


def main():
    start = time.time()
    deadline = start + DURATION
    out = mp.Queue()
    procs = [mp.Process(target=worker, args=(i, deadline, out)) for i in range(2)]
    procs.append(mp.Process(target=events, args=(start,)))
    for p in procs:
        p.start()
    total = collections.Counter()
    for _ in range(2):
        total.update(out.get())
    for p in procs:
        p.join()
    print("--- workload done ---")
    for k, v in sorted(total.items()):
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
