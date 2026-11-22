//! Failover against real PostgreSQL 17: a primary, and a physical standby
//! that syncs slots, which the primary's `synchronized_standby_slots`
//! names. Checks what pg2iceberg's failover handling rests on, and the
//! sim models (`SimPostgres::attach_standby`):
//!
//! - its failover slot is synced to the standby, and survives promotion;
//! - a failover slot's walsender sends nothing — no change, no keepalive
//!   position — the standby hasn't flushed (pg2iceberg's acks, commits
//!   and deletes of staged files wait on that);
//! - promotion starts a new timeline, which the replication client and
//!   the coordinator see.
//!
//! Gated behind `--features integration`.

#![cfg(feature = "integration")]

use std::future::Future;
use std::time::Duration;

use pg2iceberg_coord::prod::{
    connect_with as coord_connect, PostgresCoordinator, TlsMode as CoordTls,
};
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_coord::{CoordError, Coordinator};
use pg2iceberg_core::{Lsn, Namespace, Op, PgValue, TableIdent};
use pg2iceberg_pg::{
    prod::{PgClientImpl, TlsMode},
    DecodedMessage, PgClient, ReplicationStream,
};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const SLOT: &str = "p2i_failover";
const PUBLICATION: &str = "p2i_failover_pub";
const STANDBY_SLOT: &str = "standby_slot";

fn uniq() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

async fn regular_client(dsn: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("regular connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn text(client: &tokio_postgres::Client, query: &str) -> Option<String> {
    client
        .query_opt(query, &[])
        .await
        .expect(query)
        .and_then(|row| row.get::<_, Option<String>>(0))
}

fn lsn(text: &str) -> Lsn {
    let (hi, lo) = text.split_once('/').expect("an LSN");
    Lsn(u64::from_str_radix(hi, 16).unwrap() << 32 | u64::from_str_radix(lo, 16).unwrap())
}

/// Poll `check` every 200ms until it holds, for up to `secs`.
async fn eventually<F: Future<Output = bool>>(secs: u64, what: &str, mut check: impl FnMut() -> F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A primary and its standby, on a network of their own.
struct Pair {
    primary: ContainerAsync<Postgres>,
    _standby: ContainerAsync<GenericImage>,
    primary_dsn: String,
    standby_dsn: String,
    /// Both, for whichever takes writes: a connection that follows a
    /// failover.
    writable_dsn: String,
}

async fn pair() -> Pair {
    let network = format!("p2i-failover-{}", uniq());
    let primary_name = format!("p2i-primary-{}", uniq());
    let primary = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd([
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=16",
            "-c",
            "max_wal_senders=16",
        ])
        .with_network(&network)
        .with_container_name(&primary_name)
        .start()
        .await
        .expect("start the primary");
    let host = primary.get_host().await.expect("host");
    let primary_port = primary.get_host_port_ipv4(5432).await.expect("port");
    let primary_dsn =
        format!("host={host} port={primary_port} user=postgres password=postgres dbname=postgres");
    // The standby connects for replication; its physical slot is the one
    // `synchronized_standby_slots` names.
    primary
        .exec(ExecCommand::new([
            "sh",
            "-c",
            "echo 'host replication all all scram-sha-256' >> \"$PGDATA/pg_hba.conf\"",
        ]))
        .await
        .expect("allow replication");
    let admin = regular_client(&primary_dsn).await;
    admin
        .batch_execute("SELECT pg_reload_conf()")
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            "SELECT pg_create_physical_replication_slot('{STANDBY_SLOT}')"
        ))
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            "ALTER SYSTEM SET synchronized_standby_slots = '{STANDBY_SLOT}'"
        ))
        .await
        .unwrap();
    admin
        .batch_execute("SELECT pg_reload_conf()")
        .await
        .unwrap();

    let script = format!(
        r#"set -e
export PGPASSWORD=postgres
until pg_basebackup -h {primary_name} -U postgres -D /tmp/standby -R -S {STANDBY_SLOT} -X stream; do
  rm -rf /tmp/standby; sleep 1
done
echo "primary_conninfo = 'host={primary_name} user=postgres password=postgres dbname=postgres'" >> /tmp/standby/postgresql.auto.conf
chown -R postgres:postgres /tmp/standby
chmod 700 /tmp/standby
exec "$(command -v gosu || command -v su-exec)" postgres postgres -D /tmp/standby -c sync_replication_slots=on -c hot_standby_feedback=on -c wal_level=logical -c max_replication_slots=16 -c max_wal_senders=16"#
    );
    let standby = GenericImage::new("postgres", "17-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept read-only connections",
        ))
        .with_entrypoint("sh")
        .with_cmd(["-c", script.as_str()])
        .with_network(&network)
        .start()
        .await
        .expect("start the standby");
    let standby_port = standby.get_host_port_ipv4(5432).await.expect("port");
    let standby_dsn =
        format!("host={host} port={standby_port} user=postgres password=postgres dbname=postgres");
    let writable_dsn = format!(
        "host={host},{host} port={primary_port},{standby_port} user=postgres \
         password=postgres dbname=postgres target_session_attrs=read-write"
    );
    Pair {
        primary,
        _standby: standby,
        primary_dsn,
        standby_dsn,
        writable_dsn,
    }
}

/// The furthest position `msg` says the stream has sent.
fn position(msg: &DecodedMessage) -> Option<Lsn> {
    match msg {
        DecodedMessage::Commit { commit_lsn, .. } => Some(*commit_lsn),
        DecodedMessage::Keepalive { wal_end, .. } => Some(*wal_end),
        _ => None,
    }
}

/// Read `stream` for `secs`, acking `acked` as it goes; the messages.
async fn read_for(
    stream: &mut Box<dyn ReplicationStream>,
    secs: u64,
    acked: Lsn,
) -> Vec<DecodedMessage> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if let Ok(msg) = tokio::time::timeout(Duration::from_millis(500), stream.recv()).await {
            seen.push(msg.expect("the stream reads"));
        }
        stream.send_standby(acked, acked).await.expect("ack");
    }
    seen
}

async fn slot_failover(pg: &PgClientImpl) -> Option<bool> {
    pg.slot_health(SLOT)
        .await
        .unwrap()
        .expect("the slot")
        .failover
}

/// The id an insert of `msg` carries.
fn inserted_id(msg: &DecodedMessage) -> Option<i32> {
    match msg {
        DecodedMessage::Change(c) if c.op == Op::Insert => match c
            .after
            .as_ref()?
            .get(&pg2iceberg_core::ColumnName("id".into()))
        {
            Some(PgValue::Int4(id)) => Some(*id),
            _ => None,
        },
        _ => None,
    }
}

#[tokio::test]
async fn a_failover_slot_follows_the_standby_and_waits_for_it() {
    let pair = pair().await;
    let admin = regular_client(&pair.primary_dsn).await;
    admin
        .batch_execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let table = TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: "t".into(),
    };

    // A failover slot, which can be turned off and on again.
    let pg = PgClientImpl::connect_with(&pair.primary_dsn, TlsMode::Disable)
        .await
        .expect("replication connect");
    assert!(pg2iceberg_pg::failover_slots_supported(
        pg.server_version_num().await.unwrap()
    ));
    pg.create_publication(PUBLICATION, &[table]).await.unwrap();
    pg.create_slot(SLOT, true)
        .await
        .expect("create a failover slot");
    assert_eq!(slot_failover(&pg).await, Some(true));
    pg.set_slot_failover(SLOT, false).await.unwrap();
    assert_eq!(slot_failover(&pg).await, Some(false));
    pg.set_slot_failover(SLOT, true).await.unwrap();
    assert_eq!(slot_failover(&pg).await, Some(true));
    assert_eq!(pg.timeline().await.unwrap(), 1);

    // A coordinator on whichever takes writes: the primary, for now.
    let coord = PostgresCoordinator::new(
        coord_connect(&pair.writable_dsn, CoordTls::Disable)
            .await
            .expect("coordinator connect"),
        CoordSchema::sanitize("_p2i_failover"),
    );
    coord.migrate().await.expect("migrate");
    coord.set_flushed_lsn(Lsn(1)).await.expect("a write");

    // Streaming and acking, the standby syncs the slot.
    let mut stream = pg
        .start_replication(SLOT, Lsn::ZERO, PUBLICATION)
        .await
        .expect("start replication");
    admin
        .batch_execute("INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    let mut acked = Lsn::ZERO;
    let standby = regular_client(&pair.standby_dsn).await;
    let synced = format!(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
         WHERE slot_name = '{SLOT}' AND synced AND NOT temporary AND failover"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        for msg in read_for(&mut stream, 1, acked).await {
            acked = acked.max(position(&msg).unwrap_or(acked));
        }
        if text(&standby, &synced)
            .await
            .is_some_and(|c| lsn(&c) > Lsn::ZERO)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the standby never synced the slot"
        );
    }

    // With the standby stopped, the stream sends nothing the standby
    // hasn't flushed: not the insert, and no position past it.
    pair._standby.pause().await.expect("pause the standby");
    admin
        .batch_execute("INSERT INTO t VALUES (2)")
        .await
        .unwrap();
    let flushed = lsn(&text(&admin, "SELECT pg_current_wal_flush_lsn()::text")
        .await
        .unwrap());
    let while_paused = read_for(&mut stream, 5, acked).await;
    assert!(
        while_paused.iter().all(|m| inserted_id(m) != Some(2)),
        "the insert arrived before the standby had it"
    );
    for msg in &while_paused {
        if let Some(at) = position(msg) {
            assert!(
                at < flushed,
                "the stream sent position {at} past the standby, before {flushed}: {msg:?}"
            );
        }
    }

    // Back up, the standby gets it, and so does the stream.
    pair._standby.unpause().await.expect("unpause the standby");
    let mut got_insert = false;
    let mut furthest = Lsn::ZERO;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !(got_insert && furthest >= flushed) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stream never caught up once the standby did"
        );
        for msg in read_for(&mut stream, 1, acked).await {
            got_insert |= inserted_id(&msg) == Some(2);
            if let Some(at) = position(&msg) {
                furthest = furthest.max(at);
                acked = acked.max(at);
            }
        }
    }

    // The standby's copy catches up with the ack.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while text(&standby, &synced)
        .await
        .is_none_or(|c| lsn(&c) < flushed)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the synced slot never caught up"
        );
        read_for(&mut stream, 1, acked).await;
    }
    drop(stream);

    // Fail over: the primary dies, the standby is promoted.
    pair.primary.stop().await.expect("stop the primary");
    standby
        .batch_execute("SELECT pg_promote()")
        .await
        .expect("promote");
    eventually(60, "the standby takes writes", || {
        let standby = &standby;
        async move {
            text(standby, "SELECT pg_is_in_recovery()::text")
                .await
                .as_deref()
                == Some("false")
        }
    })
    .await;

    // Its `synchronized_standby_slots`, copied from the primary, names a
    // slot it doesn't have, which would hold its failover slots' streams
    // back for good: a new primary names its own standbys, or none.
    assert_eq!(
        text(&standby, "SHOW synchronized_standby_slots")
            .await
            .as_deref(),
        Some(STANDBY_SLOT)
    );
    standby
        .batch_execute("ALTER SYSTEM SET synchronized_standby_slots = ''")
        .await
        .unwrap();
    standby
        .batch_execute("SELECT pg_reload_conf()")
        .await
        .unwrap();

    // The slot is there, a failover slot still, on a new timeline, and
    // replication goes on from it.
    let promoted = PgClientImpl::connect_with(&pair.standby_dsn, TlsMode::Disable)
        .await
        .expect("connect to the promoted standby");
    assert_eq!(promoted.timeline().await.unwrap(), 2);
    let slot = promoted
        .slot_health(SLOT)
        .await
        .unwrap()
        .expect("the slot survived the failover");
    assert_eq!(slot.failover, Some(true));
    assert!(slot.confirmed_flush_lsn >= flushed);
    let mut stream = promoted
        .start_replication(SLOT, slot.confirmed_flush_lsn, PUBLICATION)
        .await
        .expect("replicate from the promoted standby");
    standby
        .batch_execute("INSERT INTO t VALUES (3)")
        .await
        .unwrap();
    let mut got_three = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !got_three {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no change from the promoted standby"
        );
        got_three = read_for(&mut stream, 1, slot.confirmed_flush_lsn)
            .await
            .iter()
            .any(|m| inserted_id(m) == Some(3));
    }

    // The coordinator's connection comes back on the new primary, on
    // another timeline: it refuses to go on.
    let mut refused = None;
    for _ in 0..20 {
        match coord.flushed_lsn().await {
            Err(CoordError::FailedOver { from, to }) => {
                refused = Some((from, to));
                break;
            }
            Ok(lsn) => panic!("the coordinator went on after the failover: {lsn}"),
            // The connection to the dead primary failing first.
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    assert_eq!(refused, Some((1, 2)));
}
