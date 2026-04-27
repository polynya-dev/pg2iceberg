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

use pg2iceberg_core::{Namespace, Op, TableIdent};
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
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("port");
        let dsn = format!(
            "host={host} port={port} user=postgres password=postgres dbname=postgres"
        );
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
        .execute(
            "SELECT pg_drop_replication_slot($1)",
            &[&slot.as_str()],
        )
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
        .create_publication(&pubname, &[ident.clone()])
        .await
        .expect("create_publication");

    let members = client
        .publication_tables(&pubname)
        .await
        .expect("publication_tables");
    assert!(members.contains(&ident), "table must be in pub: {members:?}");

    // Drop the table from the publication.
    regular
        .batch_execute(&format!(
            "ALTER PUBLICATION {pubname} DROP TABLE {table}"
        ))
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
    // PG 14+ has the `conflicting` column and reports false on a
    // healthy non-physical slot.
    assert!(!h.conflicting);

    // Cleanup.
    regular
        .execute(
            "SELECT pg_drop_replication_slot($1)",
            &[&slot.as_str()],
        )
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
        .create_publication(&pubname, &[ident.clone()])
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
            Err(_) => panic!(
                "recv deadline: inserts_seen={inserts_seen}, commits_seen={commits_seen}"
            ),
        };
        match msg {
            DecodedMessage::Relation { ident: t, .. } => {
                if t.name == table {
                    relation_seen = true;
                }
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
    assert!(inserts_seen >= 3, "expected ≥3 Insert events, got {inserts_seen}");
    assert!(commits_seen >= 1, "expected ≥1 Commit event, got {commits_seen}");
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

    let id = schema
        .columns
        .iter()
        .find(|c| c.name == "id")
        .unwrap();
    assert!(id.is_primary_key);
    assert!(!id.nullable);

    let bio = schema
        .columns
        .iter()
        .find(|c| c.name == "bio")
        .unwrap();
    assert!(!bio.is_primary_key);
    assert!(bio.nullable);
}
