# pg2iceberg on Cloudflare

Postgres → pg2iceberg in a [Cloudflare container](https://developers.cloudflare.com/containers/) →
Iceberg tables in an R2 bucket's catalog ([Basin Catalog](https://developers.cloudflare.com/basin-catalog/),
formerly R2 Data Catalog) → Basin SQL or DuckDB.

The source is an ephemeral [Neon](https://neon.new/) Postgres with logical
replication, driven by the rideshare workload from [`example/single`](../single).
pg2iceberg needs only environment variables here: the catalog vends storage
credentials, so there are no S3 keys to manage.

```sh
cd example/cloudflare
BUCKET=my-bucket ./run.sh                     # DURATION=120 seconds of workload
BUCKET=my-bucket ./run.sh sql "SELECT count(*) FROM rideshare.riders"
BUCKET=my-bucket ./duckdb.sh                  # or query with DuckDB
./run.sh down                                 # delete the Worker and its container
```

`run.sh` creates the Neon database, loads the schema, deploys pg2iceberg, runs
the workload, compares every row of every table in Postgres with what DuckDB
reads from the catalog, and times how long one change takes to get there.

## Before you start

- A Cloudflare account on the Workers Paid plan (Containers need it), with
  `npx wrangler login` done.
- An R2 bucket with its catalog enabled, near the database. neon.new's
  databases are in AWS us-east-2 (Ohio), so Eastern North America:

  ```sh
  npx wrangler r2 bucket create my-bucket --location enam
  npx wrangler r2 bucket catalog enable my-bucket
  ```

- An R2 API token with **Admin Read & Write** (R2 → Manage API tokens; it can
  be limited to the bucket) — and **Basin SQL Read** for `./run.sh sql` — in
  `.dev.vars` (git-ignored):

  ```
  ICEBERG_CATALOG_TOKEN=<token>
  ```

- Node 20+ and Python 3; DuckDB 1.4+ for `duckdb.sh`.

## What runs

- **Postgres**: `npx neon-new --logical-replication` creates a database that
  needs no account and expires in 72 hours unless you claim it (its claim URL
  is in `.env.neon`). pg2iceberg connects to its direct host, not the pooler:
  replication can't go through PgBouncer.
- **pg2iceberg**: one container ([`src/index.ts`](src/index.ts)) running
  pg2iceberg's published image, `docker.io/polynyadev/pg2iceberg:latest`,
  in Eastern North America with the database and the bucket (see
  `constraints` in [`wrangler.jsonc`](wrangler.jsonc)). It runs `pg2iceberg
  run` with this configuration, from Worker secrets and vars:

  | Variable | Value |
  |---|---|
  | `POSTGRES_URL` | the Neon database (secret) |
  | `ICEBERG_CATALOG_URL` | the bucket's catalog URI |
  | `ICEBERG_CATALOG_TOKEN` | the R2 API token (secret) |
  | `ICEBERG_WAREHOUSE` | the catalog's warehouse name, `<account id>_<bucket>` |
  | `ICEBERG_NAMESPACE` | `rideshare` |

  It replicates every table with a primary key — here `riders`, `drivers`,
  `rides`, `payments` and `ratings` — and writes each under its location in
  the catalog with the credentials the catalog vends for it (see
  [the R2 catalog's page](../../docs/catalogs/r2.md)).

  The container serves nothing; a cron trigger starts it every minute if it
  isn't running, so it comes back after Cloudflare stops it (a host restart,
  a deploy). pg2iceberg keeps its state in Postgres and resumes where it left
  off. Its output is in the dashboard (Workers & Pages → pg2iceberg →
  Containers), and `curl https://pg2iceberg.<subdomain>.workers.dev` reports
  the container's state.
- **The workload**: [`example/single/workload.py`](../single/workload.py) —
  multi-table transactions, deletes, rollbacks, key changes, TOAST updates,
  bulk inserts, a `TRUNCATE` and schema changes.
- **Querying**: `./run.sh sql` runs [Basin SQL](https://developers.cloudflare.com/r2/sql/),
  Cloudflare's own engine for the catalog. DuckDB attaches the catalog with
  the token; [`compare.py`](compare.py) uses it to compare every row with
  Postgres. Both apply pg2iceberg's equality deletes.

## Measuring

With the deployment running:

```sh
BUCKET=my-bucket ./run.sh measure    # one change, then ROWS=10000 in one transaction
BUCKET=my-bucket ./run.sh loadtest   # RATE=1000 rows/s for DURATION=300s
./run.sh settings                    # the database's replication settings
```

- **`measure`** ([`freshness.py`](freshness.py)) times a change from its
  commit in Postgres until DuckDB reads it. pg2iceberg stages changes every
  `flush_interval` and commits them to Iceberg every `materializer_interval`
  (10s each by default), so one change takes 20–30s; 100,000 rows in one
  transaction take about as long.
- **`loadtest`** ([`loadtest.py`](loadtest.py)) inserts RATE riders a second
  — and, from a minute in, deletes the batch from a minute before — while
  sampling the replication slot: its lag behind the end of WAL, and the WAL
  it retains. Then it waits for pg2iceberg to drain and compares every row.
  At 1,000 inserts and 1,000 deletes a second, slot lag stays a sawtooth
  under about 12 MB and retained WAL under about 30 MB, both flat over the
  run, draining within seconds of the load stopping.

## Notes

- **neon.new is being retired** in favour of Claimable Neon
  (`npx neon@latest claim create`), which doesn't yet enable logical
  replication before you claim the database. To use your own Postgres, set
  `wal_level = logical`, and put its URL in `.env.neon` as
  `DATABASE_URL_DIRECT=...`; move the bucket and the container's region
  near it.
- **Neon's `wal_sender_timeout` is 10s** (Postgres's default is 60s): see
  `./run.sh settings`. pg2iceberg keeps the replication connection alive
  while it's busy committing.
- **Your own build**: set `"image": "../../Dockerfile"` in `wrangler.jsonc`;
  wrangler then builds it with Docker, for linux/amd64.
- **Costs**: the container runs until `./run.sh down`, billed by the 10 ms on
  a `basic` instance (¼ vCPU, 1 GiB — pg2iceberg peaked under 100 MB on this
  workload), plus R2 storage, catalog operations and Basin SQL queries.
- **Postgres egress**: Cloudflare containers have no fixed outbound IP, so
  the database must accept connections from anywhere (over TLS, as Neon's do).
