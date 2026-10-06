# Smoke test

Real Postgres → pg2iceberg → Iceberg REST catalog + MinIO, under a mixed
workload, then a row-by-row comparison of Postgres with ClickHouse reading
the Iceberg tables. Uses [`example/single`](../single)'s rideshare schema,
seed and service configs.

```sh
cd example/smoke
./run.sh                 # DURATION=300 by default (seconds of workload)
docker compose --profile workload down -v
```

It exits non-zero if Postgres and Iceberg differ. Logs land in `out/`.

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
