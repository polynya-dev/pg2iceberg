"""Tiny write workload for the blue/green example.

Continuously inserts transfers and updates account balances on blue. All
writes flow through logical replication to green, and both sides are
captured by their respective pg2iceberg instances.
"""
import argparse
import os
import random
import time

import psycopg


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=1.0,
                        help="Seconds between write operations.")
    args = parser.parse_args()

    dsn = os.environ["DATABASE_URL"]
    print(f"[simulate] connecting to {dsn}", flush=True)
    conn = psycopg.connect(dsn, autocommit=False)

    # Cache the account ids once.
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM accounts ORDER BY id")
        account_ids = [r[0] for r in cur.fetchall()]
    if len(account_ids) < 2:
        raise RuntimeError("need at least 2 seed accounts")
    print(f"[simulate] {len(account_ids)} accounts", flush=True)

    tick = 0
    # Insert-only workload: each transaction appends a single transfer row.
    # No UPDATE on accounts so the demo stays within what append-only
    # Iceberg readers (e.g. PyIceberg) can read at historical snapshots —
    # UPDATE/DELETE workloads produce equality deletes, which the verifier
    # in this example (PyIceberg-based) does not yet handle.
    while True:
        tick += 1
        a, b = random.sample(account_ids, 2)
        amount = random.randint(1, 50)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transfers (from_acct, to_acct, amount, status) "
                "VALUES (%s, %s, %s, 'completed') RETURNING id",
                (a, b, amount),
            )
        conn.commit()
        if tick % 10 == 0:
            print(f"[simulate] tick={tick} last_transfer={a}->{b} amount={amount}",
                  flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
