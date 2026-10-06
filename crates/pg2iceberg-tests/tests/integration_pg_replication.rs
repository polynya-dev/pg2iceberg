//! Phase B integration: `PgClientImpl` + `ReplicationStreamImpl`
//! against a real Postgres container with `wal_level=logical`.
//!
//! Covers the parts of the replication path that the sim can't model
//! faithfully: pgoutput protocol framing, slot create/inspect SQL,
//! publication SQL with proper identifier quoting, and the
//! `LogicalReplicationMessage` → `DecodedMessage` translation in
//! `ReplicationStreamImpl`.
//!
//! Gated behind `--features integration`. Run command in the
//! integration_coord file's header doc applies here too.

#![cfg(feature = "integration")]

use std::time::Duration;

use pg2iceberg_core::{ColumnName, Namespace, Op, PgValue, TableIdent};
use pg2iceberg_pg::{
    prod::{PgClientImpl, TlsMode},
    DecodedMessage, PgClient,
};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::sync::OnceCell;

/// Single PG container shared across the test binary, started with
/// `wal_level=logical` + replication slots/wal_senders bumped to fit
/// each per-test slot. Tests isolate via UUID-suffixed slot,
/// publication, and table names.
static PG: OnceCell<SharedPg> = OnceCell::const_new();

struct SharedPg {
    _container: ContainerAsync<Postgres>,
    dsn: String,
}

async fn shared_pg() -> &'static SharedPg {
    PG.get_or_init(|| async {
        // Override the default Postgres cmd to enable logical replication.
        // `-c fsync=off` is the upstream default we preserve.
        // We pin `16-alpine` (rather than testcontainers-modules' default
        // `11-alpine`) so the PG 13+ slot-health columns
        // (`wal_status`, `safe_wal_size`, `conflicting`) are actually
        // populated when the integration test calls `slot_health`. PG 11
        // doesn't have them, and the prod query gracefully returns NULL
        // there — but exercising real values needs a current PG.
        let container = Postgres::default()
            .with_tag("16-alpine")
            .with_cmd([
                "-c",
                "fsync=off",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=16",
                "-c",
                "max_wal_senders=16",
            ])
            .start()
            .await
            .expect("start postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let dsn =
            format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
        SharedPg {
            _container: container,
            dsn,
        }
    })
    .await
}

/// Open a regular-mode connection (for DDL + inserts). The replication
/// client uses logical-replication mode which can't run CREATE TABLE.
async fn regular_client(dsn: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("regular connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

/// Unique short UUID suffix for naming test fixtures (slots,
/// publications, tables) so multiple tests can share a single
/// container without colliding.
fn uniq() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

#[tokio::test]
async fn slot_lifecycle_create_inspect_drop() {
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    // Need a publication before creating a slot? No — slots and
    // publications are independent until START_REPLICATION binds them.
    // We can create a slot directly. Use a fresh table for this test
    // so the publication has at least one table when we create it.
    let table = format!("t_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create table");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    let slot = format!("s_{}", uniq());
    assert!(
        !client.slot_exists(&slot).await.unwrap(),
        "slot must not exist before create"
    );
    let cp = client.create_slot(&slot).await.expect("create_slot");
    assert!(client.slot_exists(&slot).await.unwrap());
    assert!(cp.0 > 0, "consistent_point LSN should be > 0");

    let restart = client
        .slot_restart_lsn(&slot)
        .await
        .expect("slot_restart_lsn");
    assert!(restart.is_some(), "restart_lsn populated after create");

    // PG initializes confirmed_flush_lsn for a fresh logical slot to
    // its consistent_point, not zero. So the slot returns Some(>0)
    // immediately. (Some(Lsn(0)) is what the impl maps a SQL-NULL to,
    // which would only happen if PG ever returned NULL — empirically
    // it doesn't for a freshly-created slot.)
    let confirmed = client
        .slot_confirmed_flush_lsn(&slot)
        .await
        .expect("confirmed_flush")
        .expect("slot exists");
    assert!(confirmed.0 > 0, "confirmed_flush should be populated");
    assert_eq!(confirmed, cp, "confirmed_flush starts at consistent_point");

    // Drop slot via the regular client (replication-mode SQL is too
    // restricted for pg_drop_replication_slot()).
    regular
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot.as_str()])
        .await
        .expect("drop slot");
}

#[tokio::test]
async fn table_oid_changes_after_drop_recreate() {
    // Validates the prod `table_oid()` query path and the assumption
    // it depends on: PG's `pg_class.oid` increments on `DROP TABLE` +
    // recreate. Without this, our `TableIdentityChanged` startup
    // invariant has nothing to match on.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("ident_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    let oid_before = client
        .table_oid("public", &table)
        .await
        .expect("table_oid")
        .expect("table exists");
    assert!(oid_before > 0, "real-PG oid is positive");

    regular
        .batch_execute(&format!(
            "DROP TABLE {table}; CREATE TABLE {table} (id INT PRIMARY KEY)"
        ))
        .await
        .expect("drop+recreate");

    let oid_after = client
        .table_oid("public", &table)
        .await
        .expect("table_oid")
        .expect("table exists");
    assert_ne!(
        oid_before, oid_after,
        "PG must assign a fresh oid on recreate; that's what \
         drives the TableIdentityChanged startup invariant"
    );

    // Probing a non-existent table returns None.
    let none = client
        .table_oid("public", "definitely_not_a_real_table")
        .await
        .expect("table_oid on missing");
    assert!(none.is_none());
}

#[tokio::test]
async fn publication_tables_reflects_alter_publication() {
    // Validates the prod `publication_tables()` query and the
    // `ALTER PUBLICATION` round-trip the
    // `TableMissingFromPublication` invariant relies on.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("pub_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    let pubname = format!("p_{}", uniq());
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    client
        .create_publication(&pubname, std::slice::from_ref(&ident))
        .await
        .expect("create_publication");

    let members = client
        .publication_tables(&pubname)
        .await
        .expect("publication_tables");
    assert!(
        members.contains(&ident),
        "table must be in pub: {members:?}"
    );

    // Drop the table from the publication.
    regular
        .batch_execute(&format!("ALTER PUBLICATION {pubname} DROP TABLE {table}"))
        .await
        .expect("alter pub");

    let members = client
        .publication_tables(&pubname)
        .await
        .expect("publication_tables");
    assert!(
        !members.contains(&ident),
        "table must be gone from pub: {members:?}"
    );

    // Empty/non-existent publication returns empty list, not error.
    let none = client
        .publication_tables("definitely_not_a_real_publication")
        .await
        .expect("publication_tables on missing");
    assert!(none.is_empty());
}

#[tokio::test]
async fn slot_health_query_works_against_real_pg() {
    // Validates the `to_jsonb` indirection trick in the prod slot_health
    // query — confirms PG accepts the SQL and returns the expected
    // shape on a healthy slot. The watcher's wal_status invariant
    // depends on this query path.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("t_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create table");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    // Probing a non-existent slot returns None (not an error).
    let slot = format!("s_{}", uniq());
    let none = client
        .slot_health(&slot)
        .await
        .expect("slot_health on missing slot");
    assert!(none.is_none(), "missing slot should return None");

    // Create the slot and probe again.
    let cp = client.create_slot(&slot).await.expect("create_slot");
    let h = client
        .slot_health(&slot)
        .await
        .expect("slot_health on existing slot")
        .expect("slot exists");

    assert!(h.exists);
    assert_eq!(
        h.confirmed_flush_lsn, cp,
        "confirmed_flush starts at consistent_point"
    );
    assert!(h.restart_lsn.0 > 0, "restart_lsn populated after create");

    // PG 13+ should report `wal_status` (most likely `reserved` for a
    // fresh slot under normal config). PG 12 returns None — this test
    // assumes the test container is PG 13+, which testcontainers'
    // default Postgres image satisfies.
    assert_eq!(
        h.wal_status,
        Some(pg2iceberg_pg::WalStatus::Reserved),
        "fresh slot should be Reserved under default settings"
    );
    // PG 16+ has the `conflicting` column and reports false on a
    // healthy non-physical slot.
    assert!(!h.conflicting);

    // Cleanup.
    regular
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot.as_str()])
        .await
        .expect("drop slot");
}

#[tokio::test]
async fn create_publication_quotes_identifiers_correctly() {
    // Mostly an SQL-correctness probe: identifier quoting happens
    // via `quote_ident`. The interesting case is mixed-case +
    // dotted namespace names, which only manifest against real PG.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("MixedCase_{}", uniq());
    regular
        .batch_execute(&format!(
            "CREATE TABLE \"{table}\" (id INT PRIMARY KEY, note TEXT)"
        ))
        .await
        .expect("create mixed-case table");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    let pubname = format!("p_{}", uniq());
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    client
        .create_publication(&pubname, &[ident])
        .await
        .expect("create_publication");

    // Verify it landed in pg_publication.
    let row = regular
        .query_one(
            "SELECT pubname FROM pg_publication WHERE pubname = $1",
            &[&pubname.as_str()],
        )
        .await
        .expect("pub exists");
    let got: &str = row.get(0);
    assert_eq!(got, pubname);
}

#[tokio::test]
async fn start_replication_streams_insert_events() {
    // The headline test for Phase B. Set up a publication + slot, do
    // a few INSERTs, and consume the resulting events from the
    // replication stream. Verify the message ordering matches
    // pgoutput protocol contract (Relation → Begin → Change → Commit)
    // and the rows decode through the value_decode path.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("accounts_{}", uniq());
    let pubname = format!("p_{}", uniq());
    let slot = format!("s_{}", uniq());

    regular
        .batch_execute(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, balance INT NOT NULL)"
        ))
        .await
        .expect("create table");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    client
        .create_publication(&pubname, std::slice::from_ref(&ident))
        .await
        .expect("publication");
    let cp = client.create_slot(&slot).await.expect("slot");

    // Inserts after slot creation; the slot's consistent_point is the
    // start LSN we hand to start_replication, and these inserts sit
    // strictly after that point, so they must appear in the stream.
    for i in 1..=3 {
        regular
            .execute(
                &format!("INSERT INTO {table} (id, balance) VALUES ($1, $2)"),
                &[&i, &(i * 10)],
            )
            .await
            .expect("insert");
    }

    let mut stream = client
        .start_replication(&slot, cp, &pubname)
        .await
        .expect("start_replication");

    // Drain up to N messages or 10 seconds — whichever first — and
    // assert we see Begin/Change/Commit for our 3 inserts.
    let mut inserts_seen = 0usize;
    let mut commits_seen = 0usize;
    let mut relation_seen = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);

    while inserts_seen < 3 || commits_seen < 1 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "deadline: inserts_seen={inserts_seen}, commits_seen={commits_seen}, \
                 relation_seen={relation_seen}"
            );
        }
        let msg = match tokio::time::timeout(remaining, stream.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => panic!("stream error: {e:?}"),
            Err(_) => {
                panic!("recv deadline: inserts_seen={inserts_seen}, commits_seen={commits_seen}")
            }
        };
        match msg {
            DecodedMessage::Relation { ident: t, .. } if t.name == table => {
                relation_seen = true;
            }
            DecodedMessage::Change(ev) if ev.op == Op::Insert && ev.table.name == table => {
                inserts_seen += 1;
                let after = ev.after.expect("Insert must carry after-row");
                assert!(after.contains_key(&pg2iceberg_core::ColumnName("id".into())));
                assert!(after.contains_key(&pg2iceberg_core::ColumnName("balance".into())));
            }
            DecodedMessage::Commit { .. } => commits_seen += 1,
            _ => {}
        }
    }

    assert!(relation_seen, "Relation message must precede first Change");
    assert!(
        inserts_seen >= 3,
        "expected ≥3 Insert events, got {inserts_seen}"
    );
    assert!(
        commits_seen >= 1,
        "expected ≥1 Commit event, got {commits_seen}"
    );
}

/// Columns of types the decoder has no mapping for — a domain, an enum,
/// an array, an interval — replicate, typed as discovery types them: the
/// domain as its base type, the rest as text.
#[tokio::test]
async fn start_replication_types_columns_as_discovery_does() {
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;
    let u = uniq();
    let table = format!("typed_{u}");
    let pubname = format!("p_{u}");
    let slot = format!("s_{u}");
    regular
        .batch_execute(&format!(
            "CREATE DOMAIN posint_{u} AS int4 CHECK (VALUE > 0); \
             CREATE TYPE color_{u} AS ENUM ('red', 'green'); \
             CREATE TABLE {table} (id int4 PRIMARY KEY, d posint_{u}, e color_{u}, \
                                   a int4[], i interval)"
        ))
        .await
        .expect("create table");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    let discovered = client
        .discover_schema("public", &table)
        .await
        .expect("discover");
    client
        .create_publication(&pubname, std::slice::from_ref(&ident))
        .await
        .expect("publication");
    let cp = client.create_slot(&slot).await.expect("slot");
    regular
        .batch_execute(&format!(
            "INSERT INTO {table} VALUES (1, 5, 'red', '{{1,2}}', '1 day 2 hours')"
        ))
        .await
        .expect("insert");

    let mut stream = client
        .start_replication(&slot, cp, &pubname)
        .await
        .expect("start_replication");
    let mut relation = None;
    let after = loop {
        let msg = tokio::time::timeout(Duration::from_secs(15), stream.recv())
            .await
            .expect("recv deadline")
            .expect("stream error");
        match msg {
            DecodedMessage::Relation {
                ident: t, columns, ..
            } if t.name == table => {
                relation = Some(columns);
            }
            DecodedMessage::Change(ev) if ev.op == Op::Insert && ev.table.name == table => {
                break ev.after.expect("Insert carries after-row");
            }
            _ => {}
        }
    };

    let relation = relation.expect("Relation precedes the first change");
    for c in &discovered.columns {
        let decoded = relation.iter().find(|r| r.name == c.name).expect("column");
        assert_eq!(
            decoded.ty, c.ty,
            "column {}: decoded vs discovered type",
            c.name
        );
    }
    let value = |c: &str| after[&ColumnName(c.into())].clone();
    assert_eq!(value("d"), PgValue::Int4(5));
    assert_eq!(value("e"), PgValue::Text("red".into()));
    assert_eq!(value("a"), PgValue::Text("{1,2}".into()));
    assert_eq!(value("i"), PgValue::Text("1 day 02:00:00".into()));
}

#[tokio::test]
async fn keepalive_wal_end_covers_writes_to_unpublished_tables() {
    // The slot-advance fix leans on two pgoutput behaviours: a
    // transaction that only touches tables outside the publication is
    // skipped entirely (no Begin/Commit), and the walsender's keepalive
    // `wal_end` still moves past it. Without the keepalive, nothing would
    // tell us we may ack that WAL and the slot would pin it.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let published = format!("pub_t_{}", uniq());
    let noise = format!("noise_t_{}", uniq());
    let pubname = format!("p_{}", uniq());
    let slot = format!("s_{}", uniq());
    regular
        .batch_execute(&format!(
            "CREATE TABLE {published} (id INT PRIMARY KEY); \
             CREATE TABLE {noise} (id INT PRIMARY KEY, payload TEXT)"
        ))
        .await
        .expect("create tables");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: published.clone(),
    };
    client
        .create_publication(&pubname, std::slice::from_ref(&ident))
        .await
        .expect("publication");
    let cp = client.create_slot(&slot).await.expect("slot");
    let mut stream = client
        .start_replication(&slot, cp, &pubname)
        .await
        .expect("start_replication");

    // Only unpublished writes, each its own transaction.
    let current_lsn = || async {
        let row = regular
            .query_one("SELECT (pg_current_wal_insert_lsn() - '0/0')::bigint", &[])
            .await
            .expect("current lsn");
        pg2iceberg_core::Lsn(row.get::<_, i64>(0) as u64)
    };
    let mut before_last = current_lsn().await;
    for i in 0..50 {
        before_last = current_lsn().await;
        regular
            .execute(
                &format!("INSERT INTO {noise} (id, payload) VALUES ($1, repeat('x', 200))"),
                &[&i],
            )
            .await
            .expect("noise insert");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let msg = match tokio::time::timeout(remaining, stream.recv()).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => panic!("stream error: {e:?}"),
            Err(_) => panic!("no keepalive past {before_last:?} within 30s"),
        };
        match msg {
            DecodedMessage::Keepalive { wal_end, .. } if wal_end > before_last => break,
            DecodedMessage::Keepalive { .. } => {}
            other => panic!("unpublished-only transactions must not be streamed, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn discover_schema_against_real_pg() {
    // Validates `PgClientImpl::discover_schema` against
    // information_schema + pg_index for a real table — covers PK
    // detection, NOT NULL inference, and the OID-to-PgType mapping
    // for the common types.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("users_{}", uniq());
    regular
        .batch_execute(&format!(
            "CREATE TABLE {table} (
                id BIGINT PRIMARY KEY,
                email TEXT NOT NULL,
                bio TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"
        ))
        .await
        .expect("create table");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("connect");
    let schema = client
        .discover_schema("public", &table)
        .await
        .expect("discover");

    let cols: Vec<_> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["id", "email", "bio", "created_at"]);

    let id = schema.columns.iter().find(|c| c.name == "id").unwrap();
    assert!(id.is_primary_key);
    assert!(!id.nullable);

    let bio = schema.columns.iter().find(|c| c.name == "bio").unwrap();
    assert!(!bio.is_primary_key);
    assert!(bio.nullable);
}

#[tokio::test]
async fn drop_slot_and_publication_against_real_pg() {
    // End-to-end: create slot + publication, then drop both via
    // the new `PgClient::drop_slot` / `drop_publication` methods.
    // Verifies the SQL is valid and the operations are idempotent.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("cleanup_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create");

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("repl connect");

    let slot = format!("s_{}", uniq());
    let pubname = format!("p_{}", uniq());
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    client
        .create_publication(&pubname, &[ident])
        .await
        .expect("create_publication");
    client.create_slot(&slot).await.expect("create slot");
    assert!(client.slot_exists(&slot).await.unwrap());

    // First drop succeeds.
    client.drop_slot(&slot).await.expect("drop_slot");
    assert!(!client.slot_exists(&slot).await.unwrap());

    // Idempotent — second drop is a no-op (slot is gone).
    client.drop_slot(&slot).await.expect("idempotent drop_slot");

    // Drop publication, then idempotent re-drop.
    client
        .drop_publication(&pubname)
        .await
        .expect("drop_publication");
    client
        .drop_publication(&pubname)
        .await
        .expect("idempotent drop_publication");
    client
        .drop_publication("never_existed_pub")
        .await
        .expect("drop missing publication");
}

#[tokio::test]
async fn drop_slot_rejects_active_slot() {
    // While a START_REPLICATION is in flight, the slot is `active =
    // true`. Cleanup must refuse to drop it — silently dropping
    // would yank WAL out from under a live consumer.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("active_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create");

    // Open one client to create + START_REPLICATION the slot, and a
    // *separate* client to attempt the drop. PG enforces "can't drop
    // an active slot" between sessions.
    let writer = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("writer connect");
    let pubname = format!("p_{}", uniq());
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };
    writer
        .create_publication(&pubname, &[ident])
        .await
        .expect("create_publication");
    let slot = format!("s_{}", uniq());
    writer.create_slot(&slot).await.expect("create slot");

    // Hold the replication stream so the slot is `active`.
    let _stream = writer
        .start_replication(&slot, pg2iceberg_core::Lsn::ZERO, &pubname)
        .await
        .expect("start_replication");
    // Tiny pause so PG marks the slot active before the drop attempt.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let dropper = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("dropper connect");
    let err = dropper
        .drop_slot(&slot)
        .await
        .expect_err("active slot must reject drop");
    let msg = format!("{err}");
    assert!(
        msg.contains("active") || msg.contains("still active"),
        "error must call out activeness; got: {msg}"
    );

    // Drop the stream (release the slot), then the cleanup succeeds.
    drop(_stream);
    // Need to also drop the writer client because it holds the
    // replication-mode connection that's pinning the slot active.
    drop(writer);
    tokio::time::sleep(Duration::from_millis(200)).await;
    dropper.drop_slot(&slot).await.expect("drop after release");

    dropper
        .drop_publication(&pubname)
        .await
        .expect("drop publication");
}

#[tokio::test]
async fn client_stays_usable_while_it_streams() {
    // `start_replication` streams on a connection of its own, so the
    // client's own stays free for queries — the lifecycle's slot-health
    // watcher runs on it. On one connection a query would queue behind
    // the endless COPY BOTH and hang, stalling the main loop and CDC.
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("sh_{}", uniq());
    let pubname = format!("shp_{}", uniq());
    let slot = format!("shs_{}", uniq());

    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create table");

    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };

    let client = PgClientImpl::connect_with(&pg.dsn, TlsMode::Disable)
        .await
        .expect("connect");
    client
        .create_publication(&pubname, std::slice::from_ref(&ident))
        .await
        .expect("publication");
    let cp = client.create_slot(&slot).await.expect("slot");
    let _stream = client
        .start_replication(&slot, cp, &pubname)
        .await
        .expect("start_replication");

    let health = tokio::time::timeout(Duration::from_secs(10), client.slot_health(&slot))
        .await
        .expect("slot_health on the streaming client must not hang")
        .expect("slot_health query ok")
        .expect("the slot exists");
    assert!(health.exists);
}

/// Terminate every backend connected with `application_name = app`,
/// waiting for each to exit, as a Postgres restart or failover would
/// (or an operator's `pg_terminate_backend`). Returns how many.
async fn terminate_backends(regular: &tokio_postgres::Client, app: &str) -> i64 {
    let row = regular
        .query_one(
            "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid, 10000)) \
             FROM pg_stat_activity WHERE application_name = $1",
            &[&app],
        )
        .await
        .expect("terminate backends");
    row.get(0)
}

/// Postgres dropping the connections — the stream's and the client's
/// own — ends the stream with an error. `start_replication` on the same
/// client then streams again, from where the slot was acked, and the
/// client's queries reconnect too.
#[tokio::test]
async fn start_replication_resumes_after_the_connections_are_dropped() {
    let pg = shared_pg().await;
    let regular = regular_client(&pg.dsn).await;

    let table = format!("rc_{}", uniq());
    let pubname = format!("rcp_{}", uniq());
    let slot = format!("rcs_{}", uniq());
    let app = format!("rca_{}", uniq());
    regular
        .batch_execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY)"))
        .await
        .expect("create table");
    let ident = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: table.clone(),
    };

    let client = PgClientImpl::connect_with(
        &format!("{} application_name={app}", pg.dsn),
        TlsMode::Disable,
    )
    .await
    .expect("connect");
    client
        .create_publication(&pubname, std::slice::from_ref(&ident))
        .await
        .expect("publication");
    let cp = client.create_slot(&slot).await.expect("slot");
    let mut stream = client
        .start_replication(&slot, cp, &pubname)
        .await
        .expect("start_replication");

    let insert = |id: i32| format!("INSERT INTO {table} (id) VALUES ({id})");
    /// The ids inserted up to and including row `last`'s, and the LSN of
    /// its transaction's commit.
    async fn inserts_until(
        stream: &mut Box<dyn pg2iceberg_pg::ReplicationStream>,
        table: &str,
        last: i32,
    ) -> (Vec<i32>, pg2iceberg_core::Lsn) {
        let mut ids = Vec::new();
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(15), stream.recv())
                .await
                .expect("recv deadline")
                .expect("stream error");
            match msg {
                DecodedMessage::Change(ev) if ev.op == Op::Insert && ev.table.name == table => {
                    match ev.after.unwrap().get(&ColumnName("id".into())) {
                        Some(PgValue::Int4(id)) => ids.push(*id),
                        other => panic!("unexpected id {other:?}"),
                    }
                }
                DecodedMessage::Commit { commit_lsn, .. } if ids.last() == Some(&last) => {
                    return (ids, commit_lsn)
                }
                _ => {}
            }
        }
    }
    regular.batch_execute(&insert(1)).await.expect("insert 1");
    let (ids, acked) = inserts_until(&mut stream, &table, 1).await;
    assert_eq!(ids, [1]);
    stream.send_standby(acked, acked).await.expect("ack");

    assert_eq!(
        terminate_backends(&regular, &app).await,
        2,
        "the stream's and the client's"
    );
    loop {
        match tokio::time::timeout(Duration::from_secs(15), stream.recv()).await {
            Ok(Ok(_)) => continue,
            Ok(Err(_)) => break,
            Err(_) => panic!("the stream outlived its connection"),
        }
    }
    drop(stream);

    regular.batch_execute(&insert(2)).await.expect("insert 2");
    let mut stream = client
        .start_replication(&slot, acked, &pubname)
        .await
        .expect("start_replication again");
    // Postgres sends the transaction committed at the start position
    // again (see `replication_start_lsn`), then what followed.
    let (ids, _) = inserts_until(&mut stream, &table, 2).await;
    assert!(ids == [2] || ids == [1, 2], "{ids:?}");

    // The client's own connection went too: the first query may fail
    // with it, the next runs on a new one.
    if client.slot_exists(&slot).await.is_err() {
        assert!(client.slot_exists(&slot).await.expect("reconnected"));
    }
}
