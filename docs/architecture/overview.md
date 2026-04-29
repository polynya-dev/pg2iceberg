---
icon: lucide/cpu
---

# Overview

pg2iceberg has two operating modes:

**Logical replication mode** (recommended) decouples WAL capture from Iceberg writes. The flusher captures row changes from PostgreSQL and stages them as Parquet files in S3. The materializer runs on a separate interval, reads the staged files, and merges them into Iceberg using merge-on-read semantics. This split means the replication slot LSN can be advanced quickly — the hot write path never touches the Iceberg catalog.

**Query mode** polls PostgreSQL with watermark-based SELECT queries and writes directly to Iceberg. Simpler, but cannot detect hard deletes and has no transaction semantics.

```
                    ┌─────────────────────────────────────────────────────┐
                    │                  pg2iceberg                         │
                    │                                                     │
PostgreSQL  ──WAL──►│  Flusher  ──staged Parquet──►  Materializer  ──►  │──► Iceberg
                    │              (S3 + PG coord)                        │
                    └─────────────────────────────────────────────────────┘
```

The two components communicate through a **staged log** — append-only, offset-indexed Parquet files in S3 with a lightweight index in PostgreSQL (`_pg2iceberg.log_index`). Same offset-claim primitive (`Coordinator::claim_offsets`) is the durability gate for both single-process and distributed deployments.

In single-process mode (`pg2iceberg run`), the WAL writer and materializer share the same process and coord — staged files still upload to S3 but the materializer reads them via the coord cursor as in distributed mode. (The Go reference's "combined mode in-memory cache" optimization is not implemented in the Rust port; profiling didn't justify it.)

## End-to-end flow

``` mermaid
sequenceDiagram
    participant App as Application
    participant PG as PostgreSQL
    participant F as Flusher
    participant S3s as S3 (staging)
    participant Coord as _pg2iceberg
    participant Mat as Materializer
    participant Cat as Iceberg Catalog
    participant S3i as S3 (Iceberg)

    App->>PG: INSERT INTO orders ...
    PG-->>F: BEGIN
    PG-->>F: INSERT (row data)
    PG-->>F: INSERT (row data)
    PG-->>F: COMMIT

    Note over F: Serialize buffered events to Parquet
    F->>S3s: Upload staged.parquet
    F->>Coord: ClaimOffsets (log_seq++, insert log_index)
    F->>Coord: set_flushed_lsn (durable record for tamper detection)
    F->>PG: Standby status (WALFlushPosition = staged LSN)

    Note over PG: WAL before staged LSN can now be recycled

    Note over Mat: Materializer interval fires
    Mat->>Coord: GetCursor(group, table) → offset 42
    Mat->>Coord: Read log_index where offset > 42
    Coord-->>Mat: LogEntry (s3=staged.parquet, offsets 43–1042)
    Mat->>S3s: Download staged.parquet
    Note over Mat: Fold: dedup by PK

    Mat->>S3i: Upload data files (Parquet)
    Mat->>S3i: Upload manifests
    Mat->>Cat: CommitTransaction
    Cat-->>Mat: OK (snapshot committed)
    Mat->>Coord: SetCursor(offset 1042)
```

!!! note "Distributed mode"
    The same flow describes both single-process and distributed deployments. In distributed mode (`pg2iceberg stream-only` + N `pg2iceberg materializer-only`), each materializer worker takes a deterministic round-robin slice of the table list — see [distributed.md](distributed.md) for the assignment rules and rebalancing semantics.
