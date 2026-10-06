# Single-node example and smoke test

A rideshare app's Postgres → pg2iceberg → Iceberg REST catalog + MinIO, with
ClickHouse to query the Iceberg tables and a mixed workload to drive
Postgres.

Bring it up and watch rows arrive:

```sh
cd example/single
docker compose up -d --wait
docker compose --profile workload up -d workload   # DURATION=300 seconds of writes
```

Then query `http://localhost:8123/play`, e.g.
``SELECT * FROM rideshare.`rideshare.rides` ``.

Or run it as a smoke test, which ends with a row-by-row comparison of
Postgres and ClickHouse and exits non-zero if they differ (logs in `out/`):

```sh
./run.sh                 # DURATION=300 by default
docker compose --profile workload down -v
```

## What runs

Two concurrent writers ([`workload.py`](workload.py)) mix:

- `simulate.py`'s rideshare actions: multi-table transactions of inserts and
  updates;
- deletes, and transactions that insert, update and delete the same rows;
- rolled-back transactions and primary-key changes;
- TOAST: ~12 KB `drivers.notes` values, and updates that leave them alone.

`drivers` has Postgres's default replica identity (a delete carries only the
key) and is partitioned by `status`, so a status change moves a row between
partitions ([`setup.sql`](setup.sql), [`config.yaml`](config.yaml)).

On a timeline, the workload also runs a 5000-row insert, a bulk delete, a
`TRUNCATE`, adding / dropping / re-adding a column, a bulk update and a status
flip of every driver; and [`run.sh`](run.sh) runs `pg2iceberg compact` twice,
SIGKILLs and restarts pg2iceberg, and runs `pg2iceberg maintain` with a 1m
snapshot retention.

## What's checked

- [`compare.py`](compare.py): every row and column of each table, Postgres
  against ClickHouse (an independent Iceberg reader), plus ClickHouse's
  `count()`.
- `pg2iceberg verify`, in `out/verify.log`.
- The replication slot catches up once the workload stops (to within a few
  hundred bytes: the coordinator's own state lives in this Postgres).

On Colima, bind mounts only work under `$HOME`: run from a checkout there.
