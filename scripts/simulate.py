#!/usr/bin/env python3
"""
Simulates writes to PostgreSQL for testing pg2iceberg.
Creates an orders table and continuously inserts, updates, and deletes rows.
"""

import os
import random
import time
import psycopg2
from datetime import datetime

DSN = os.getenv("PG_DSN", "host=localhost port=5432 dbname=testdb user=postgres password=postgres")

STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

DDL = """
CREATE TABLE IF NOT EXISTS public.orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER     NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure we get full before-images on UPDATE/DELETE for logical replication
ALTER TABLE public.orders REPLICA IDENTITY FULL;
"""

def setup(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    print("[setup] table and publication ready")


def insert_order(conn):
    with conn.cursor() as cur:
        customer_id = random.randint(1, 1000)
        amount = round(random.uniform(10.0, 500.0), 2)
        cur.execute(
            "INSERT INTO public.orders (customer_id, amount, status) VALUES (%s, %s, %s) RETURNING id",
            (customer_id, amount, "pending"),
        )
        row_id = cur.fetchone()[0]
    conn.commit()
    print(f"[insert] order id={row_id} customer={customer_id} amount={amount}")
    return row_id


def update_order(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        row_id = row[0]
        new_status = random.choice(STATUSES)
        new_amount = round(random.uniform(10.0, 500.0), 2)
        cur.execute(
            "UPDATE public.orders SET status = %s, amount = %s, updated_at = now() WHERE id = %s",
            (new_status, new_amount, row_id),
        )
    conn.commit()
    print(f"[update] order id={row_id} status={new_status} amount={new_amount}")


def delete_order(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM public.orders ORDER BY random() LIMIT 1")
        row = cur.fetchone()
        if not row:
            return
        row_id = row[0]
        cur.execute("DELETE FROM public.orders WHERE id = %s", (row_id,))
    conn.commit()
    print(f"[delete] order id={row_id}")


def main():
    conn = psycopg2.connect(DSN)
    setup(conn)

    # Seed some initial rows
    print("[seed] inserting 10 initial orders...")
    for _ in range(10):
        insert_order(conn)

    print("[loop] starting continuous writes (Ctrl+C to stop)")
    try:
        while True:
            r = random.random()
            if r < 0.5:
                insert_order(conn)
            elif r < 0.8:
                update_order(conn)
            else:
                delete_order(conn)
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\n[done] stopped")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
