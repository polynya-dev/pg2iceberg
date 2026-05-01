//! SQL statements used by the production `Coordinator` impl.
//!
//! These are kept here so they're testable as strings (snapshot tests, future
//! sqllogictest, etc.) and shareable between the prod impl (Phase 3 PG) and
//! any tooling that needs to introspect the wire SQL.
//!
//! All statements take the schema as input rather than embedding it. Schema
//! names are pre-sanitized via [`crate::schema::CoordSchema`].

use crate::schema::CoordSchema;

/// `CREATE SCHEMA IF NOT EXISTS …`. Idempotent.
pub fn create_schema(schema: &CoordSchema) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {}", schema)
}

/// `DROP SCHEMA IF EXISTS … CASCADE`. Used by the `cleanup`
/// subcommand to tear down all coordinator state — every table the
/// migration created lives under this schema, so a CASCADE drop
/// removes the whole footprint atomically.
pub fn drop_schema(schema: &CoordSchema) -> String {
    format!("DROP SCHEMA IF EXISTS {} CASCADE", schema)
}

/// DDL for the coord tables. Each statement is idempotent
/// (`CREATE TABLE IF NOT EXISTS`).
///
/// Mirrors `stream/coordinator_pg.go:73-104` for the WAL/cursor
/// tables, and replaces Go's single-row `_pg2iceberg.checkpoints`
/// blob with four narrower per-concern tables:
///
/// - `pipeline_meta` — single-row cluster fingerprint
/// - `tables` — per-table snapshot status
/// - `snapshot_progress` — per-table mid-snapshot resume cursor
/// - `query_watermarks` — per-table watermark for query mode
///
/// Each is an idempotent UPSERT in its hot path; no OCC, no
/// SHA-256 sealing, no schema versioning. Multi-writer becomes
/// natural (different writers touch different rows).
pub fn migrate(schema: &CoordSchema) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name  TEXT PRIMARY KEY,
                next_offset BIGINT NOT NULL DEFAULT 0
            )",
            schema.qualify("log_seq")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name    TEXT   NOT NULL,
                end_offset    BIGINT NOT NULL,
                start_offset  BIGINT NOT NULL,
                s3_path       TEXT   NOT NULL,
                record_count  INT    NOT NULL,
                byte_size     BIGINT NOT NULL,
                flushable_lsn BIGINT NOT NULL DEFAULT 0,
                created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_name, end_offset)
            )",
            schema.qualify("log_index")
        ),
        // Idempotent column add for older deployments that
        // pre-date the `flushable_lsn` column. Required for
        // blue-green markers: the materializer's marker-eligibility
        // check joins on this column.
        format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS flushable_lsn BIGINT NOT NULL DEFAULT 0",
            schema.qualify("log_index")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                group_name     TEXT NOT NULL DEFAULT 'default',
                table_name     TEXT NOT NULL,
                last_offset    BIGINT NOT NULL DEFAULT -1,
                last_committed TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (group_name, table_name)
            )",
            schema.qualify("mat_cursor")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name TEXT PRIMARY KEY,
                worker_id  TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )",
            schema.qualify("lock")
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                group_name TEXT NOT NULL,
                worker_id  TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (group_name, worker_id)
            )",
            schema.qualify("consumer")
        ),
        // Pipeline-level singleton: cluster fingerprint, last update
        // timestamp. Always exactly one row (`id = 1`). The CHECK
        // constraint makes a stray INSERT impossible.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id                INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                system_identifier BIGINT NOT NULL DEFAULT 0,
                updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            schema.qualify("pipeline_meta")
        ),
        // Single-row record of the highest LSN we've ever told the
        // replication slot to flush past. Updated *before* every
        // `send_standby`, so on a crash between the two writes the
        // slot lags behind our record (slot replays, fold absorbs
        // duplicates) instead of leading it (which would be either
        // our own ack we missed recording, or external tampering).
        // Read at startup and compared to `slot.confirmed_flush_lsn`
        // to catch `pg_replication_slot_advance` / drop+recreate /
        // a stray `pg_recvlogical` debug session that advanced the
        // slot while we were down.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id         INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                lsn        BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            schema.qualify("flushed_lsn")
        ),
        // Per-table snapshot status. `pg_oid` drives the
        // TableIdentityChanged invariant (DROP+recreate detection);
        // `snapshot_complete` gates the snapshot phase on restart;
        // `snapshot_lsn` records the LSN at completion (sanity).
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name        TEXT PRIMARY KEY,
                pg_oid            BIGINT NOT NULL DEFAULT 0,
                snapshot_complete BOOLEAN NOT NULL DEFAULT false,
                snapshot_lsn      BIGINT NOT NULL DEFAULT 0,
                completed_at      TIMESTAMPTZ
            )",
            schema.qualify("tables")
        ),
        // Per-table mid-snapshot resume cursor. `last_pk_key` is the
        // canonical-PK JSON of the last successfully staged row.
        // Snapshotter resumes from the next PK after restart; rows
        // are deleted once the table's snapshot completes.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name  TEXT PRIMARY KEY,
                last_pk_key TEXT NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            schema.qualify("snapshot_progress")
        ),
        // Per-table watermark for query mode. JSONB so any PgValue
        // (timestamp, bigint, uuid, text) round-trips without a
        // wire-format change.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name TEXT PRIMARY KEY,
                watermark  JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            schema.qualify("query_watermarks")
        ),
        // Blue-green marker bookkeeping. Populated by
        // `claim_offsets` when the pipeline observed a
        // `_pg2iceberg.markers` INSERT in the source PG's WAL,
        // drained by the materializer post-cycle.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                uuid        TEXT   PRIMARY KEY,
                commit_lsn  BIGINT NOT NULL,
                inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
            schema.qualify("pending_markers")
        ),
        // Per-(uuid, table) emission record so the materializer can
        // dedup meta-marker writes across crash + replay.
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                uuid             TEXT NOT NULL,
                table_namespace  TEXT NOT NULL,
                table_name       TEXT NOT NULL,
                emitted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (uuid, table_namespace, table_name)
            )",
            schema.qualify("marker_emissions")
        ),
    ]
}

/// `INSERT INTO log_seq … ON CONFLICT DO NOTHING`. Idempotent ensure-row.
pub fn ensure_log_seq_row(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, next_offset) VALUES ($1, 0) \
         ON CONFLICT DO NOTHING",
        schema.qualify("log_seq")
    )
}

/// `UPDATE log_seq SET next_offset = next_offset + $2 WHERE table_name = $1
/// RETURNING next_offset`. Atomic per-row claim.
pub fn claim_log_seq(schema: &CoordSchema) -> String {
    format!(
        "UPDATE {} SET next_offset = next_offset + $2 \
         WHERE table_name = $1 RETURNING next_offset",
        schema.qualify("log_seq")
    )
}

/// Insert into `log_index`. Seven positional args:
/// `(table_name, end_offset, start_offset, s3_path, record_count,
/// byte_size, flushable_lsn)`.
pub fn insert_log_index(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, end_offset, start_offset, s3_path, \
         record_count, byte_size, flushable_lsn) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        schema.qualify("log_index")
    )
}

pub fn read_log(schema: &CoordSchema) -> String {
    format!(
        "SELECT table_name, start_offset, end_offset, s3_path, \
         record_count, byte_size, flushable_lsn \
         FROM {} WHERE table_name = $1 AND end_offset > $2 ORDER BY end_offset ASC LIMIT $3",
        schema.qualify("log_index")
    )
}

/// Idempotent INSERT into `pending_markers`. Two positional args:
/// `(uuid, commit_lsn)`. ON CONFLICT DO NOTHING so re-flushing a
/// marker (e.g. on crash + replay) doesn't conflict.
pub fn insert_pending_marker(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (uuid, commit_lsn) VALUES ($1, $2) \
         ON CONFLICT DO NOTHING",
        schema.qualify("pending_markers")
    )
}

/// Read pending markers eligible for emission for `(table_namespace,
/// table_name, cursor)`. Eligibility per
/// `Coordinator::pending_markers_for_table`: every log_index entry
/// for the table with `flushable_lsn <= marker.commit_lsn` must have
/// `end_offset <= cursor`. Excludes markers already in
/// `marker_emissions` for this `(uuid, table)`.
///
/// Three positional args: `(table_namespace, table_name, cursor)`.
/// Implementation note: the table identity in `log_index` /
/// `marker_emissions` uses the `qualified()` form (e.g.
/// `public.accounts`); the qualified name is built from
/// `(table_namespace, table_name)` so both halves can vary.
pub fn pending_markers_eligible(schema: &CoordSchema) -> String {
    format!(
        "SELECT pm.uuid, pm.commit_lsn \
         FROM {pm} pm \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {me} me \
             WHERE me.uuid = pm.uuid \
               AND me.table_namespace = $1 \
               AND me.table_name = $2 \
         ) \
         AND NOT EXISTS ( \
             SELECT 1 FROM {li} li \
             WHERE li.table_name = $1 || '.' || $2 \
               AND li.flushable_lsn <= pm.commit_lsn \
               AND li.end_offset > $3 \
         ) \
         ORDER BY pm.commit_lsn ASC, pm.uuid ASC",
        pm = schema.qualify("pending_markers"),
        me = schema.qualify("marker_emissions"),
        li = schema.qualify("log_index"),
    )
}

/// Idempotent INSERT into `marker_emissions`. Three positional args:
/// `(uuid, table_namespace, table_name)`.
pub fn insert_marker_emission(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (uuid, table_namespace, table_name) VALUES ($1, $2, $3) \
         ON CONFLICT DO NOTHING",
        schema.qualify("marker_emissions")
    )
}

pub fn truncate_log(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND end_offset <= $2 RETURNING s3_path",
        schema.qualify("log_index")
    )
}

pub fn ensure_cursor(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (group_name, table_name, last_offset) VALUES ($1, $2, -1) \
         ON CONFLICT DO NOTHING",
        schema.qualify("mat_cursor")
    )
}

pub fn get_cursor(schema: &CoordSchema) -> String {
    format!(
        "SELECT last_offset FROM {} WHERE group_name = $1 AND table_name = $2",
        schema.qualify("mat_cursor")
    )
}

pub fn set_cursor(schema: &CoordSchema) -> String {
    format!(
        "UPDATE {} SET last_offset = $3, last_committed = now() \
         WHERE group_name = $1 AND table_name = $2",
        schema.qualify("mat_cursor")
    )
}

pub fn register_consumer(schema: &CoordSchema) -> String {
    // `$3::text::interval` forces PG to infer `$3` as text (via the
    // inner cast) rather than interval. tokio-postgres can serialize
    // `String` for a text parameter; serializing for an interval
    // parameter has no built-in impl and panics at runtime. Mirror
    // the same idiom in `try_lock` / `renew_lock`.
    format!(
        "INSERT INTO {} (group_name, worker_id, expires_at) \
         VALUES ($1, $2, now() + $3::text::interval) \
         ON CONFLICT (group_name, worker_id) DO UPDATE \
         SET expires_at = now() + $3::text::interval",
        schema.qualify("consumer")
    )
}

pub fn unregister_consumer(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE group_name = $1 AND worker_id = $2",
        schema.qualify("consumer")
    )
}

pub fn expire_consumers(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE expires_at < now()",
        schema.qualify("consumer")
    )
}

pub fn active_consumers(schema: &CoordSchema) -> String {
    format!(
        "SELECT worker_id FROM {} WHERE group_name = $1 ORDER BY worker_id",
        schema.qualify("consumer")
    )
}

pub fn expire_locks(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND expires_at < now()",
        schema.qualify("lock")
    )
}

pub fn try_lock(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, worker_id, expires_at) \
         VALUES ($1, $2, now() + $3::text::interval) ON CONFLICT DO NOTHING",
        schema.qualify("lock")
    )
}

pub fn renew_lock(schema: &CoordSchema) -> String {
    format!(
        "UPDATE {} SET expires_at = now() + $3::text::interval \
         WHERE table_name = $1 AND worker_id = $2",
        schema.qualify("lock")
    )
}

pub fn release_lock(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1 AND worker_id = $2",
        schema.qualify("lock")
    )
}

// ── Pipeline meta ─────────────────────────────────────────────────────

/// Read the singleton pipeline-meta row. Returns 0 or 1 row; the
/// new-deployment case (no row) is reported via the row count, not
/// a NULL system_identifier (which would conflate "fresh" with
/// "stamped to zero").
pub fn select_pipeline_meta(schema: &CoordSchema) -> String {
    format!(
        "SELECT system_identifier FROM {} WHERE id = 1",
        schema.qualify("pipeline_meta")
    )
}

/// Idempotent UPSERT for the singleton pipeline-meta row.
/// One positional arg: `system_identifier`.
pub fn upsert_pipeline_meta(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (id, system_identifier, updated_at) \
         VALUES (1, $1, now()) \
         ON CONFLICT (id) DO UPDATE \
         SET system_identifier = $1, updated_at = now()",
        schema.qualify("pipeline_meta")
    )
}

// ── Last-acked LSN (single-row tamper detection) ──────────────────────

pub fn select_flushed_lsn(schema: &CoordSchema) -> String {
    format!(
        "SELECT lsn FROM {} WHERE id = 1",
        schema.qualify("flushed_lsn")
    )
}

pub fn upsert_flushed_lsn(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (id, lsn, updated_at) \
         VALUES (1, $1, now()) \
         ON CONFLICT (id) DO UPDATE \
         SET lsn = $1, updated_at = now()",
        schema.qualify("flushed_lsn")
    )
}

// ── Per-table snapshot status ─────────────────────────────────────────

pub fn select_table_state(schema: &CoordSchema) -> String {
    // Cast to double precision: EXTRACT(EPOCH FROM ...) returns numeric on
    // PG 13+, which doesn't decode into f64 via FromSql; the explicit cast
    // gives us a stable f64 column regardless of PG version.
    format!(
        "SELECT pg_oid, snapshot_complete, snapshot_lsn, \
         (EXTRACT(EPOCH FROM completed_at) * 1000000)::double precision \
         FROM {} WHERE table_name = $1",
        schema.qualify("tables")
    )
}

/// UPSERT a table's `snapshot_complete = true` with `pg_oid` and
/// `snapshot_lsn`. Three positional args: `(table_name, pg_oid,
/// snapshot_lsn)`. Idempotent — repeated marks of the same table
/// just refresh `completed_at`.
pub fn mark_table_complete(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, pg_oid, snapshot_complete, snapshot_lsn, completed_at) \
         VALUES ($1, $2, true, $3, now()) \
         ON CONFLICT (table_name) DO UPDATE \
         SET pg_oid = $2, \
             snapshot_complete = true, \
             snapshot_lsn = $3, \
             completed_at = now()",
        schema.qualify("tables")
    )
}

// ── Per-table mid-snapshot resume ─────────────────────────────────────

pub fn select_snapshot_progress(schema: &CoordSchema) -> String {
    format!(
        "SELECT last_pk_key FROM {} WHERE table_name = $1",
        schema.qualify("snapshot_progress")
    )
}

pub fn upsert_snapshot_progress(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, last_pk_key, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (table_name) DO UPDATE \
         SET last_pk_key = $2, updated_at = now()",
        schema.qualify("snapshot_progress")
    )
}

pub fn delete_snapshot_progress(schema: &CoordSchema) -> String {
    format!(
        "DELETE FROM {} WHERE table_name = $1",
        schema.qualify("snapshot_progress")
    )
}

// ── Per-table query-mode watermark ────────────────────────────────────

pub fn select_query_watermark(schema: &CoordSchema) -> String {
    format!(
        "SELECT watermark FROM {} WHERE table_name = $1",
        schema.qualify("query_watermarks")
    )
}

pub fn upsert_query_watermark(schema: &CoordSchema) -> String {
    format!(
        "INSERT INTO {} (table_name, watermark, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (table_name) DO UPDATE \
         SET watermark = $2, updated_at = now()",
        schema.qualify("query_watermarks")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_returns_idempotent_statements() {
        let s = CoordSchema::default_name();
        let stmts = migrate(&s);
        // 6 base CREATE TABLEs (log_seq, log_index, mat_cursor, lock,
        // consumer, pending_markers, marker_emissions)
        // + 1 ALTER TABLE (log_index.flushable_lsn)
        // + 5 new per-concern tables (pipeline_meta, flushed_lsn,
        //   tables, snapshot_progress, query_watermarks)
        // = 13. Each statement is idempotent (CREATE TABLE IF NOT
        // EXISTS or ADD COLUMN IF NOT EXISTS).
        assert_eq!(stmts.len(), 13);
        for stmt in &stmts {
            assert!(
                stmt.contains("IF NOT EXISTS"),
                "stmt is not idempotent: {stmt}"
            );
            assert!(stmt.contains("_pg2iceberg."));
        }
        // Check that the five new per-concern tables are present.
        for name in [
            "pipeline_meta",
            "flushed_lsn",
            "tables",
            "snapshot_progress",
            "query_watermarks",
        ] {
            assert!(
                stmts
                    .iter()
                    .any(|s| s.contains(&format!("_pg2iceberg.{name}"))),
                "missing migrate for {name}"
            );
        }
        // Marker tables present.
        assert!(stmts.iter().any(|s| s.contains("pending_markers")));
        assert!(stmts.iter().any(|s| s.contains("marker_emissions")));
        // log_index has flushable_lsn either in the CREATE TABLE
        // body or via the idempotent ALTER TABLE.
        assert!(stmts.iter().any(|s| s.contains("flushable_lsn")));
        // Old `checkpoints` table is gone — its concerns moved to
        // the four new tables.
        assert!(
            !stmts.iter().any(|s| s.contains("_pg2iceberg.checkpoints")),
            "checkpoints table should no longer be migrated"
        );
    }

    #[test]
    fn pipeline_meta_singleton_has_check_constraint() {
        let s = CoordSchema::default_name();
        let stmts = migrate(&s);
        let pm = stmts
            .iter()
            .find(|s| s.contains("pipeline_meta"))
            .expect("pipeline_meta migrate");
        assert!(
            pm.contains("CHECK (id = 1)"),
            "pipeline_meta must enforce singleton via CHECK"
        );
    }

    #[test]
    fn upsert_sql_uses_on_conflict() {
        let s = CoordSchema::default_name();
        for q in [
            upsert_pipeline_meta(&s),
            mark_table_complete(&s),
            upsert_snapshot_progress(&s),
            upsert_query_watermark(&s),
        ] {
            assert!(
                q.contains("ON CONFLICT"),
                "upsert SQL must use ON CONFLICT: {q}"
            );
        }
    }

    #[test]
    fn schema_name_propagates_into_qualified_identifiers() {
        let s = CoordSchema::sanitize("Tenant_42");
        let q = read_log(&s);
        assert!(q.contains("tenant_42.log_index"));
    }

    #[test]
    fn drop_schema_uses_cascade_and_if_exists() {
        let s = CoordSchema::default_name();
        let q = drop_schema(&s);
        assert!(q.contains("DROP SCHEMA"));
        assert!(q.contains("IF EXISTS"));
        assert!(q.contains("CASCADE"));
        assert!(q.contains("_pg2iceberg"));
    }
}
