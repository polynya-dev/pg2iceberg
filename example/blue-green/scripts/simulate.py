"""Mixed INSERT / UPDATE / DELETE workload for the blue/green example.

All writes go to blue and flow to green via logical replication. Both sides
are captured by their respective pg2iceberg instances and land in distinct
Iceberg namespaces. The verifier (iceberg-diff) handles equality deletes,
so we can exercise the full CDC event mix here.
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

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM accounts ORDER BY id")
        account_ids = [r[0] for r in cur.fetchall()]
    if len(account_ids) < 2:
        raise RuntimeError("need at least 2 seed accounts")
    print(f"[simulate] {len(account_ids)} accounts", flush=True)

    tick = 0
    while True:
        tick += 1
        a, b = random.sample(account_ids, 2)
        amount = random.randint(1, 50)

        with conn.cursor() as cur:
            # Insert a transfer + update both accounts' balances atomically —
            # produces one INSERT event on transfers and two UPDATE events on
            # accounts per tick.
            cur.execute(
                "INSERT INTO transfers (from_acct, to_acct, amount, status) "
                "VALUES (%s, %s, %s, 'completed') RETURNING id",
                (a, b, amount),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = now() "
                "WHERE id = %s",
                (amount, a),
            )
            cur.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = now() "
                "WHERE id = %s",
                (amount, b),
            )

            # Occasionally DELETE an older completed transfer so the pipeline
            # exercises equality deletes on transfers too.
            if tick % 20 == 0:
                cur.execute(
                    "DELETE FROM transfers "
                    "WHERE id = (SELECT id FROM transfers "
                    "            WHERE status = 'completed' "
                    "            ORDER BY id ASC LIMIT 1) "
                    "RETURNING id"
                )

        conn.commit()
        if tick % 10 == 0:
            print(f"[simulate] tick={tick} last_transfer={a}->{b} amount={amount}",
                  flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
