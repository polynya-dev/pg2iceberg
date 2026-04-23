# Blue/green upgrade walkthrough

End-to-end demo of pg2iceberg's snapshot-alignment feature. Two PostgreSQL
clusters (blue = current, green = future) are kept in sync via PG logical
replication. One pg2iceberg instance streams each side into its own Iceberg
namespace. An operator can drop a *marker* on blue at any point, and a
verifier compares the aligned Iceberg snapshots on each side to confirm the
replication is byte-correct — before cutting downstream consumers from blue
to green.

```
                 writes
┌────────────┐ ─────────▶ ┌─────────────┐
│ simulator  │             │ blue-postgres│──logical rep──▶ ┌──────────────┐
└────────────┘             └─────────────┘                   │ green-postgres│
                                │                            └──────────────┘
                                │                                   │
                        pg2iceberg-blue                    pg2iceberg-green
                                │                                   │
                                ▼                                   ▼
                       iceberg://app_blue              iceberg://app_green
                       _pg2iceberg_blue.markers        _pg2iceberg_green.markers
                                ▲                                   ▲
                                └────────── verify.py ──────────────┘
```

## Quick start

```sh
docker compose up -d
# wait a bit for seed replication + first materializer cycles
sleep 30
./scripts/insert-marker.sh
# copy the UUID printed above, then:
./scripts/verify.sh <uuid>
```

Expected output:

```
marker UUID: <uuid>
blue-side rows : 2  tables=['public.accounts', 'public.transfers']
green-side rows: 2 tables=['public.accounts', 'public.transfers']
  [OK ] public.accounts: blue@<sid_b> total-records=10  vs  green@<sid_g> total-records=10
  [OK ] public.transfers: blue@<sid_b> total-records=N   vs  green@<sid_g> total-records=N

aligned — snapshot total-records match on every tracked table.
```

Note the snapshot ids are different between blue and green: each cluster
has its own Iceberg snapshot id space. The marker UUID is the join key; the
verifier uses it to look up the matching `(table, snapshot_id)` pair on each
side.

## What is actually happening

1. **Shared schema.** `schema.sql` creates `accounts`, `transfers`, and
   `_pg2iceberg.markers` on both clusters. PG logical
   replication does not ship DDL so both sides need it applied identically.

2. **Blue gets seeded.** `seed.sql` inserts ten rows into `accounts` on blue
   only. Green will receive them via the subscription.

3. **Replication bootstrap.** `scripts/bootstrap-replication.sh` (run once,
   in its own container) creates a publication `bluegreen` on blue covering
   `accounts`, `transfers`, and `_pg2iceberg.markers`, and
   subscribes green to it. It waits until green is fully synced.

4. **Dual pg2iceberg.** Two pg2iceberg containers come up, each pointed at
   its respective PG. Each has its own publication, replication slot, data
   namespace, and meta namespace — no shared state in the Iceberg catalog.

5. **Traffic.** `scripts/simulate.py` hammers blue with transfers + balance
   updates. Both pg2iceberg instances stream their side into Iceberg.

6. **Mark + verify.** Inserting into `_pg2iceberg.markers` on
   blue triggers:
   - pg2iceberg-blue observes the marker in blue's WAL, flushes the
     in-flight transaction, records `(marker_uuid, table, snapshot_id)` in
     `_pg2iceberg_blue.markers`.
   - The marker row replicates to green, where pg2iceberg-green does the
     same into `_pg2iceberg_green.markers`.
   - `verify.py` joins the two markers tables on `marker_uuid` and reads
     each tracked table at the paired snapshot ids. A row-by-row diff
     proves the two Iceberg outputs represent the same logical data.

## Ports

| Service       | Host port | Purpose                       |
|---------------|-----------|-------------------------------|
| blue-postgres | 5610      | `psql -h localhost -p 5610`   |
| green-postgres| 5611      | `psql -h localhost -p 5611`   |
| minio         | 9610/9611 | S3 API / console              |
| iceberg-rest  | 8181      | Iceberg REST catalog          |

## Tearing down

```sh
docker compose down -v
```

## Known limitations (honest list)

- **Insert-only workload.** The simulator only INSERTs into `transfers`.
  If we UPDATE or DELETE, pg2iceberg produces equality deletes (Iceberg
  merge-on-read) and PyIceberg — what the verifier uses to read snapshot
  summaries — cannot fully resolve rows. The `total-records` summary also
  over-counts under MoR because equality deletes are applied at read time,
  not at write time. To extend the demo to UPDATE/DELETE-heavy workloads,
  point a merge-on-read-capable reader (Trino, Spark, ClickHouse, or
  once it's ready the repo's `iceberg-diff` tool) at the same
  `(table, snapshot_id)` pairs the verifier prints.
- **Coarse verification.** The verifier compares Iceberg snapshot
  `total-records` summaries — this catches gross divergence (dropped rows,
  stuck pipelines) but is not a row-by-row byte comparison. A full diff is
  the next stage (stage 5 in the `iceberg-diff` decision tree).
- **Multi-minute backlogs** on green widen the window between
  `insert-marker.sh` succeeding and `verify.sh` succeeding — the marker
  row has to replicate to green via the subscription, then pg2iceberg-green
  has to observe and record it. For this demo the window is typically
  under 5 seconds.
- **No downstream cutover automation.** pg2iceberg's job is to produce the
  signal (aligned markers) and the data (two Iceberg namespaces). Who reads
  from which catalog, and how they get pointed at green, is intentionally
  outside this example's scope.
