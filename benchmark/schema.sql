-- Benchmark table: mixed column types to exercise all code paths.
CREATE TABLE bench_events (
    id          BIGSERIAL PRIMARY KEY,
    seq         BIGINT NOT NULL,
    writer_id   UUID NOT NULL,
    op_type     TEXT NOT NULL,
    payload     JSONB NOT NULL,
    large_text  TEXT,
    value       INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE bench_events ALTER COLUMN large_text SET STORAGE EXTERNAL;

CREATE INDEX idx_bench_events_seq ON bench_events (seq);

-- Publication for pg2iceberg logical replication.
CREATE PUBLICATION pg2iceberg_bench FOR TABLE bench_events;

-- Operations log: bench-writer records every operation for verification.
-- bench-verify replays this to compute expected state.
CREATE TABLE bench_operations (
    id          BIGSERIAL PRIMARY KEY,
    op          TEXT NOT NULL,       -- 'insert', 'update', 'delete'
    seq         BIGINT NOT NULL,     -- target row seq
    value       INTEGER,             -- new value (for insert/update)
    writer_id   UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
