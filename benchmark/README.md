# pg2iceberg AWS benchmark

A reproducible benchmark anyone can run in their own AWS account. It
provisions an RDS Postgres with fake rideshare data, runs **pg2iceberg on
ECS Fargate** replicating into an **AWS Glue Iceberg REST catalog** (data
files in a plain S3 bucket), drives write traffic with **k6 running
locally** against a public HTTP shim in the VPC for **X minutes**, then
reports throughput, replication lag, and correctness — and tears
everything down with one command.

```
 laptop                              AWS (one region, default ap-southeast-1)
┌─────────┐  HTTP   ┌──────────────────────── VPC ────────────────────────┐
│ k6      │────────►│ public ALB ─► shim (Fargate) ─► RDS Postgres         │
│ scripts │         │                                  │ logical repl      │
└─────────┘◄── tofu │ pg2iceberg (Fargate) ◄───────────┘                   │
   outputs          │      │ sigv4 (Glue) + task-role S3 writes            │
                    │      ▼                                               │
                    │  Glue catalog  +  S3 warehouse bucket (Iceberg)      │
                    └──────────────────────────────────────────────────────┘
```

Only the shim's ALB is public; RDS and pg2iceberg stay private.

## How the catalog works

pg2iceberg talks to **AWS Glue's Iceberg REST endpoint**
(`https://glue.<region>.amazonaws.com/iceberg`), which requires every
request to be **SigV4-signed** (`catalog_auth: sigv4`). Glue does **not**
vend per-table S3 credentials, so `credential_mode: iam`: the Fargate
**task role** writes Iceberg data + metadata files straight to a plain S3
warehouse bucket. No access keys, no catalog container to run.

> Why not S3 Tables? S3 Tables' managed storage only hands out data-file
> credentials through **Lake Formation** credential vending; without it,
> signed requests succeed but `LoadTable` returns no S3 credentials.
> Glue + a plain S3 bucket avoids that dependency. Both need the SigV4
> signing the REST client gained on the `polynya-patches` fork.

## Prerequisites

Install locally and configure AWS credentials (`aws configure`) — Glue is
available in every commercial region:

- [OpenTofu](https://opentofu.org) (`tofu`)
- Docker (images build `linux/arm64` for Graviton Fargate — native on
  Apple Silicon, emulated elsewhere)
- AWS CLI v2, authenticated
- [k6](https://k6.io/docs/get-started/installation/)
- `jq`, `curl`, `bc`, GNU `make`

Your AWS principal needs permission to create VPC, RDS, ECS, ECR, ELB,
IAM roles, Glue databases/tables, S3, and CloudWatch resources.

## Quickstart

```bash
cd benchmark
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# edit terraform.tfvars: set allowed_cidr to "<your-ip>/32" (the shim has no auth)

make up                       # provision + build/push images (~15 min, RDS dominates)
make seed                     # schema + fake data, then start pg2iceberg
make logs                     # (optional) watch until snapshot completes / streaming
make bench DURATION=15m RATE=200 VUS=100
make results                  # render report.md + run correctness verify
make down                     # destroy EVERYTHING
```

`make results` prints and writes [`loadtest/out/report.md`](loadtest/out/):
write throughput + latency, the replication-lag profile, the headline
**post-traffic catch-up time**, and the `verify` pass/fail.

## Knobs

| Variable | Default | Where | Meaning |
|---|---|---|---|
| `DURATION` | `15m` | `make bench` | steady-state traffic window (the "X minutes") |
| `RATE` | `200` | `make bench` | target requests/sec (k6 arrival rate) |
| `VUS` | `100` | `make bench` | pre-allocated k6 virtual users |
| `DRAIN` | `300` | `make bench` | seconds to keep sampling lag after traffic stops |
| `RIDERS`/`DRIVERS`/`RIDES` | `50000`/`5000`/`200000` | `make seed` | initial seed volume (sizes the snapshot) |
| `region` | `ap-southeast-1` | `terraform/variables.tf` | any region with Glue |
| `db_instance_class` | `db.m6g.large` | `terraform/variables.tf` | RDS size (biggest cost lever) |
| `shim_desired_count` | `2` | `terraform/variables.tf` | scale up to push more load |
| `allowed_cidr` | `0.0.0.0/0` | `terraform/terraform.tfvars` | **set to `<your-ip>/32`** — the shim has no auth |
| `flush_interval`/`flush_rows` | `10s`/`1000` | `terraform/variables.tf` | pg2iceberg flush cadence |

Set Terraform vars in `terraform/terraform.tfvars` (gitignored) or via
`-var` flags.

## What's measured (and how)

pg2iceberg `main` does **not** expose a Prometheus endpoint, so the
benchmark derives its numbers from Postgres state + k6, not from scraping
the binary:

1. **Write throughput / latency** — the local k6 summary
   (`loadtest/out/summary.json`): total requests, req/s, error rate,
   p50/p95/p99, and per-action counts.
2. **Replication lag over time** — `scripts/poll_lag.sh` hits the shim's
   `/admin/lag` every 5 s and records:
   - **slot lag bytes**: `pg_wal_lsn_diff(pg_current_wal_lsn(),
     confirmed_flush_lsn)` from `pg_replication_slots` (how far behind the
     flusher is).
   - **pending events**: `log_seq.next_offset − mat_cursor.last_offset`
     summed across tables from the `_pg2iceberg` coordinator schema (the
     flush→materialize backlog).
   - per-table row counts.
   It keeps sampling through the `DRAIN` window so the report can compute
   how long pg2iceberg takes to catch up to ~0 lag after traffic stops.
3. **Correctness** — `scripts/verify.sh` runs `pg2iceberg verify` as a
   one-shot Fargate task (row-by-row Postgres vs Iceberg diff).

## Cost

Approximate, `ap-southeast-1`, defaults, for a full
create→bench→destroy cycle (~1–1.25 h of uptime — RDS create + seed +
30-min bench + teardown):

| Component | ~ per cycle |
|---|---|
| RDS `db.m6g.large` + 100 GB gp3 | ~$0.30 |
| Fargate (1× pg2iceberg + 2× shim) | ~$0.17 |
| ALB | ~$0.03 |
| NAT gateway + data | ~$0.10 |
| S3 (warehouse) + Glue + logs | ~$0.05 |
| **Total** | **~$0.60–1.00** |

The 30-minute window in isolation is ~$0.25–0.35. Biggest levers:
`db_instance_class`, `shim_desired_count`, and **not leaving it running**
(`make down`). Prices are approximate — check current AWS pricing.

## Teardown

```bash
make down
```

This deletes the Glue tables pg2iceberg created (so the Glue database can
be destroyed), then `tofu destroy -auto-approve`. RDS uses
`skip_final_snapshot` and the S3/ECR repos use force-delete, so destroy is
clean. **Everything is removed — don't keep anything you care about
here.**

## Troubleshooting

- **shim tasks won't start after `make up`** — they can't pull until
  images are pushed; `make up` runs `images` + `deploy` for you. If you
  ran apply manually, run `make images && make deploy`.
- **pg2iceberg crash-loops before `make seed`** — expected: it needs the
  tables to exist. It stays at desired 0 until `make seed` scales it.
- **`make seed` hangs on health** — the shim image may not be pushed, or
  `allowed_cidr` blocks you. Check `make logs` and the shim service in ECS.
- **`403 MissingAuthenticationToken` from the catalog** — the Glue
  endpoint requires SigV4; the config sets `catalog_auth: sigv4` and the
  task role must allow `glue:*Table*`/`glue:*Database*`. See
  `terraform/iam.tf`.
- **Image build is slow** — the pg2iceberg base compiles the Rust binary
  with `--features prod`; first build is several minutes.
```
