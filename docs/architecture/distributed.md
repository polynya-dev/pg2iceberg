---
icon: lucide/network
---

# Distributed Mode

For databases with large tables or high write throughput, pg2iceberg can scale the materializer horizontally — one process runs the WAL writer, and N processes run materializer workers that share the work across tables.

See [Run Modes](../usage/modes.md) for the flags to enable this.

## Architecture

```
┌──────────────────────┐
│  pg2iceberg          │     ┌─────────────────────┐
│  (--stream-only)     │────►│  materializer        │
│                      │     │  worker-1            │
│  WAL writer only     │     │  (tables: orders,    │
│  No materializer     │     │           products)  │
└──────────────────────┘     ├─────────────────────┤
         │                   │  materializer        │
      S3 + _pg2iceberg ──────│  worker-2            │
                             │  (tables: users,     │
                             │           payments)  │
                             └─────────────────────┘
```

All workers share the same leaderless log (S3 + `_pg2iceberg` schema). There is no central coordinator process — workers coordinate entirely through PostgreSQL.

## Table assignment

At the start of each materializer cycle, every worker independently computes the same deterministic assignment without any locking:

1. Register a heartbeat in the `consumer` table (TTL: 30 seconds)
2. Query `consumer` for all active workers, sorted alphabetically by worker ID
3. Sort table names alphabetically
4. Assign tables via round-robin: `table[i] → workers[i % len(workers)]`

Because both lists are sorted the same way on every worker, all workers reach identical conclusions about who owns which table — no coordination message needed.

**Example** with 2 workers and 4 tables:

| Table | Worker |
|-------|--------|
| `orders` | `worker-1` |
| `payments` | `worker-2` |
| `products` | `worker-1` |
| `users` | `worker-2` |

## Rebalancing

When a worker joins or leaves, the `consumer` table changes. Every worker detects this on the next cycle and recomputes the assignment. Tables rebalance automatically — no manual intervention needed.

A worker leaving (crash or graceful shutdown) is detected within one heartbeat TTL (30 seconds). Its tables are picked up by the remaining workers on the next cycle.

## Cursor isolation

Each worker tracks its own progress via `mat_cursor`, keyed by `(group_name, worker_id, table_name)`. Workers never read or write each other's cursors — their progress is fully independent.

## Commit isolation

Each worker commits its assigned tables independently. Because pg2iceberg uses Iceberg's `CommitTransaction` with an optimistic lock per table (`assert-ref-snapshot-id`), two workers accidentally processing the same table would produce a 409 conflict on commit. The losing worker's cursor is not advanced, so it retries safely on the next cycle. In practice this only happens transiently during rebalancing.
