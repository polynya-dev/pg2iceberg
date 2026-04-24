# Blue/green upgrade walkthrough

End-to-end demo of pg2iceberg's snapshot-alignment feature. Two PostgreSQL
clusters (blue = current, green = future) are kept in sync via PG logical
replication. One pg2iceberg instance streams each side into its own Iceberg
namespace. An operator drops a *marker* on blue at any point, and [`iceberg-diff`](https://github.com/polynya-dev/iceberg-diff)
runs per tracked table to confirm that the two Iceberg outputs represent the
same logical data *at that point in blue's WAL*, byte-level. Only after the
diff comes back EQUAL is it safe to cut downstream consumers from blue to
green.

```
                 writes (INSERT / UPDATE / DELETE)
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
                                └──────── iceberg-diff per table ───┘
```

## Quick start

```sh
docker compose up -d
# wait for seed replication + first materializer cycles
sleep 30
./scripts/insert-marker.sh
# copy the UUID printed above, then:
./scripts/verify.sh <uuid>
```

Expected output:

```
marker: <uuid>

  pair: public.accounts	<sid_blue>	<sid_green>
  pair: public.transfers	<sid_blue>	<sid_green>

diff public.accounts         blue@... vs green@... ... EQUAL
diff public.transfers        blue@... vs green@... ... EQUAL

summary: 2 equal, 0 unequal
```

`iceberg-diff` short-circuits cheapest-first: identity check, then schema
compare, then snapshot `total-records`, then file-set fingerprint, then
finally a parallel row-hash bucket scan. EQUAL at any earlier stage ends
the diff.

## What is actually happening

1. **Shared schema.** `schema.sql` creates `accounts`, `transfers`, and
   `_pg2iceberg.markers` on both clusters. PG logical replication does not
   ship DDL so both sides need it applied identically.

2. **Blue gets seeded.** `seed.sql` inserts ten rows into `accounts` on
   blue only. Green receives them via the subscription.

3. **Replication bootstrap.** `scripts/bootstrap-replication.sh` (run once,
   in its own container) creates a publication `bluegreen` on blue covering
   `accounts`, `transfers`, and `_pg2iceberg.markers`, and subscribes green
   to it. It waits until green is fully synced.

4. **Dual pg2iceberg.** Two pg2iceberg containers come up, each pointed at
   its respective PG. Each has its own publication, replication slot, data
   namespace (`app_blue` / `app_green`), and meta namespace
   (`_pg2iceberg_blue` / `_pg2iceberg_green`) — no shared state in the
   Iceberg catalog.

5. **Traffic.** `scripts/simulate.py` hits blue with INSERTs (new
   transfers), UPDATEs (balance changes on both accounts in every
   transfer), and periodic DELETEs of old transfers. Both pg2iceberg
   instances stream their side into Iceberg; UPDATE/DELETE produces
   merge-on-read equality deletes, which `iceberg-diff` reads natively.

6. **Mark + verify.** Inserting into `_pg2iceberg.markers` on blue
   triggers:
   - pg2iceberg-blue observes the marker in blue's WAL, flushes the
     transaction containing it, materializes any staged events, and
     appends `(marker_uuid, table, snapshot_id)` rows to
     `_pg2iceberg_blue.markers`.
   - The marker row replicates to green, where pg2iceberg-green does
     the same into `_pg2iceberg_green.markers`.
   - `verify.sh` joins the two markers tables by `marker_uuid`, then
     runs `iceberg-diff` per paired `(table, blue_sid, green_sid)`.

## Ports

| Service        | Host port  | Purpose                       |
|----------------|-----------|-------------------------------|
| blue-postgres  | 5610      | `psql -h localhost -p 5610`   |
| green-postgres | 5611      | `psql -h localhost -p 5611`   |
| minio          | 9610 / 9611 | S3 API / console            |
| iceberg-rest   | 8181      | Iceberg REST catalog          |

## Tearing down

```sh
docker compose down -v
```

## Preconditions the feature relies on

Good to know if you fork this for a real deployment:

1. **Schema parity.** PG logical replication doesn't ship DDL. Any
   `ALTER TABLE` on blue must also run on green. The example's
   `schema.sql` is applied to both clusters at `initdb` time.
2. **Tracked-table parity.** Both pg2iceberg configs must list the same
   tables, and all of those tables must be in the bluegreen publication.
   A table tracked by only one pg2iceberg will diverge the moment any row
   is touched.
3. **One-way writes pre-cutover.** No direct writes to green. pg2iceberg-
   green would happily ingest them, producing data that never existed on
   blue. The verifier would flag it but it's still a foot-gun.
4. **`_pg2iceberg.checkpoints` must NOT be in the bluegreen publication.**
   The example's bootstrap excludes it. A `FOR ALL TABLES` publication
   would break green's state.

## Notes

- There is no downstream cutover automation. pg2iceberg's job is to
  produce the signal (aligned markers) and the data (two Iceberg
  namespaces). Who reads from which catalog, and how they get pointed at
  green, is intentionally outside this example's scope.
