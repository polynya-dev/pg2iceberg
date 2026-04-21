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

The two components communicate through a **leaderless log** — an append-only, offset-indexed log stored as Parquet files in S3 with a lightweight index in PostgreSQL.

In combined (single-process) mode, the materializer reads staged events directly from an in-memory cache, avoiding S3 round-trips entirely on the hot path.

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
    F->>Coord: UpdateCheckpoint (confirmed flush LSN)
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

!!! note "Combined mode"
    In combined (single-process) mode the `S3s: Download staged.parquet` step is skipped — the materializer reads events from an in-memory cache populated by the flusher during `ClaimOffsets`.
