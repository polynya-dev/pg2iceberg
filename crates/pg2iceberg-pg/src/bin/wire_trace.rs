//! `wire_trace`: minimal CopyBoth reader for diagnosing replication-stream
//! delivery vs `pg_recvlogical`.
//!
//! Talks tokio-postgres directly (the same fork pg2iceberg pins),
//! issues `START_REPLICATION` raw, and dumps every CopyData frame's
//! type byte + length to stderr — plus, for `w` (XLogData) frames, the
//! embedded pgoutput message type. Optionally writes the pgoutput
//! payload to stdout in `pg_recvlogical -f -`-compatible format so we
//! can binary-diff the two against the same DML stream.
//!
//! Usage:
//!   wire_trace <slot> <publication> [--raw]
//!
//! Env: PG_DSN — defaults to localhost test creds.

use bytes::Bytes;
use futures_util::StreamExt;
use postgres_replication::protocol::{
    LogicalReplicationMessage as PgMsg, ReplicationMessage as PgEnv,
};
use postgres_replication::LogicalReplicationStream;
use std::env;
use std::io::Write;
use std::time::SystemTime;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{NoTls, SimpleQueryMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: wire_trace <slot> <publication> [--raw]");
        std::process::exit(2);
    }
    let slot = &args[1];
    let publication = &args[2];
    let raw_stdout = args.iter().any(|a| a == "--raw");
    let wrapped = args.iter().any(|a| a == "--wrapped");
    // --spawned mirrors prod ReplicationStreamImpl: a dedicated reader task
    // owns the stream and pushes decoded events into an mpsc; the "main"
    // (this fn) reads from the mpsc. If THIS reproduces the bug, the
    // spawn+channel pattern is the culprit.
    let spawned = args.iter().any(|a| a == "--spawned");
    // --hold-snapshot opens a second tokio-postgres connection in a normal
    // (non-replication) session, runs `pg_current_wal_lsn` then
    // `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`, and holds it for
    // the rest of the run. This mirrors what `PgSnapshotSource::open` does
    // in our prod path. If holding a long-lived snapshot tx on a separate
    // connection triggers walsender to stop sending DML on the replication
    // stream, this flag should reproduce the bug.
    let hold_snapshot = args.iter().any(|a| a == "--hold-snapshot");
    // --start-lsn=N/M makes us issue START_REPLICATION at a non-zero LSN
    // (matches our prod path that uses snap_lsn). Default 0/0 = use slot's
    // confirmed_flush_lsn.
    let start_lsn_arg = args
        .iter()
        .find_map(|a| a.strip_prefix("--start-lsn="))
        .unwrap_or("0/0")
        .to_string();
    // --initial-ack=N/M sends a standby_status_update right after
    // start_replication, mirroring our prod's `stream.send_standby(snap_lsn,
    // snap_lsn)` immediately after START_REPLICATION.
    let initial_ack_arg = args
        .iter()
        .find_map(|a| a.strip_prefix("--initial-ack="))
        .map(|s| s.to_string());

    let dsn = env::var("PG_DSN").unwrap_or_else(|_| {
        "host=localhost port=5434 user=postgres password=postgres dbname=testdb \
         replication=database"
            .to_string()
    });

    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Optional second connection holding a snapshot transaction open —
    // mirrors PgSnapshotSource::open in prod.
    let _snap_conn_guard: Option<tokio_postgres::Client> = if hold_snapshot {
        // Strip `replication=database` so the second session is a plain
        // SQL session (the snapshot path uses the coord connect, which is
        // non-replication mode).
        let plain_dsn = dsn
            .replace("replication=database", "")
            .replace("replication = database", "")
            .trim()
            .to_string();
        let (snap_client, snap_conn) = tokio_postgres::connect(&plain_dsn, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = snap_conn.await {
                eprintln!("snapshot connection error: {e}");
            }
        });
        let lsn_text = snap_client
            .simple_query("SELECT pg_current_wal_lsn()::text")
            .await?;
        eprintln!("[wire_trace] snapshot pg_current_wal_lsn = {lsn_text:?}");
        snap_client
            .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        eprintln!("[wire_trace] snapshot tx opened (REPEATABLE READ, read-only)");
        Some(snap_client)
    } else {
        None
    };

    // Confirm slot exists. Helps diagnose typo'd slot names.
    let rows = client
        .simple_query(&format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .await?;
    let mut found = false;
    for r in rows {
        if let SimpleQueryMessage::Row(_) = r {
            found = true;
        }
    }
    if !found {
        eprintln!("slot {slot} does not exist");
        std::process::exit(3);
    }
    eprintln!(
        "[wire_trace] slot={slot} publication={publication} raw={raw_stdout} wrapped={wrapped} \
         spawned={spawned} start_lsn={start_lsn_arg} initial_ack={initial_ack_arg:?}"
    );

    let q = format!(
        r#"START_REPLICATION SLOT "{slot}" LOGICAL {start_lsn_arg} ("proto_version" '1', "publication_names" '"{publication}"')"#,
    );
    let copy_stream = client.copy_both_simple::<Bytes>(&q).await?;

    let t0 = SystemTime::now();
    let mut frame_no: u64 = 0;
    let mut keepalives: u64 = 0;
    let mut wal_data: u64 = 0;
    let mut bytes_seen: u64 = 0;

    let mut stdout = std::io::stdout().lock();

    if spawned {
        // Mirror prod ReplicationStreamImpl: spawn a reader task that owns
        // the wrapped stream + pushes decoded events into an mpsc, and run
        // a select! between mpsc.recv() and a periodic ack tick on the main
        // task. Acks go through a oneshot in a cmd channel.
        let stream = LogicalReplicationStream::new(copy_stream);
        let (events_tx, mut events_rx) =
            tokio::sync::mpsc::channel::<PgEnv<PgMsg>>(1000);
        let (cmd_tx, mut cmd_rx) =
            tokio::sync::mpsc::channel::<(PgLsn, tokio::sync::oneshot::Sender<()>)>(8);
        let initial_ack = initial_ack_arg.clone();

        let reader = tokio::spawn(async move {
            let mut stream = Box::pin(stream);
            if let Some(lsn_text) = initial_ack.as_deref() {
                if let Ok(lsn) = lsn_text.parse::<PgLsn>() {
                    eprintln!("[reader] initial standby_status_update lsn={lsn_text}");
                    let _ = stream
                        .as_mut()
                        .standby_status_update(lsn, lsn, lsn, 0, 0)
                        .await;
                }
            }
            loop {
                let branch = {
                    let mut stream_ref = stream.as_mut();
                    tokio::select! {
                        biased;
                        next = stream_ref.next() => Ok(next),
                        cmd = cmd_rx.recv() => Err(cmd),
                    }
                };
                match branch {
                    Ok(Some(Ok(env))) => {
                        if events_tx.send(env).await.is_err() {
                            return;
                        }
                    }
                    Ok(Some(Err(e))) => {
                        eprintln!("[reader] err {e}");
                        return;
                    }
                    Ok(None) => {
                        eprintln!("[reader] eof");
                        return;
                    }
                    Err(None) => {
                        eprintln!("[reader] cmd_rx closed");
                        return;
                    }
                    Err(Some((lsn, done))) => {
                        eprintln!("[reader] standby_status_update lsn={lsn:?}");
                        let _ = stream
                            .as_mut()
                            .standby_status_update(lsn, lsn, lsn, 0, 0)
                            .await;
                        let _ = done.send(());
                    }
                }
            }
        });

        // Periodic ack matching prod's 10s Standby tick.
        let mut ack_ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ack_ticker.tick().await; // skip the immediate fire
        loop {
            tokio::select! {
                Some(env) = events_rx.recv() => {
                    frame_no += 1;
                    let dt = t0.elapsed().unwrap_or_default();
                    match env {
                        PgEnv::PrimaryKeepAlive(ka) => {
                            keepalives += 1;
                            if keepalives <= 3 || keepalives % 1000 == 0 || ka.reply() != 0 {
                                eprintln!(
                                    "[{:.3}] frame#{frame_no} k[{keepalives}] wal_end={:#x} reply={}",
                                    dt.as_secs_f64(),
                                    ka.wal_end(),
                                    ka.reply()
                                );
                            }
                        }
                        PgEnv::XLogData(xl) => {
                            wal_data += 1;
                            let pg_tag = match xl.data() {
                                PgMsg::Begin(_) => 'B',
                                PgMsg::Commit(_) => 'C',
                                PgMsg::Insert(_) => 'I',
                                PgMsg::Update(_) => 'U',
                                PgMsg::Delete(_) => 'D',
                                PgMsg::Relation(_) => 'R',
                                PgMsg::Truncate(_) => 'T',
                                _ => '?',
                            };
                            eprintln!(
                                "[{:.3}] frame#{frame_no} w wal_start={:#x} pg_tag={pg_tag}",
                                dt.as_secs_f64(),
                                xl.wal_start(),
                            );
                        }
                        _ => {}
                    }
                }
                _ = ack_ticker.tick() => {
                    if let Some(lsn_text) = initial_ack_arg.as_deref() {
                        if let Ok(lsn) = lsn_text.parse::<PgLsn>() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            if cmd_tx.send((lsn, done_tx)).await.is_ok() {
                                let _ = done_rx.await;
                            }
                        }
                    }
                }
                else => break,
            }
        }
        let _ = reader.await;
        eprintln!(
            "[wire_trace] EOF spawned: frames={frame_no} wal_data={wal_data} keepalives={keepalives}"
        );
        return Ok(());
    }

    if wrapped {
        // Same wrapper our prod ReplicationStreamImpl uses. If the bug
        // surfaces here but not in the raw-CopyBoth path, the parser
        // is dropping or mis-framing messages.
        let stream = LogicalReplicationStream::new(copy_stream);
        let mut stream = Box::pin(stream);

        if let Some(lsn_text) = initial_ack_arg.as_deref() {
            let lsn: PgLsn = lsn_text
                .parse()
                .map_err(|e| format!("parse --initial-ack {lsn_text}: {e:?}"))?;
            eprintln!("[wire_trace] sending initial standby_status_update lsn={lsn_text}");
            stream
                .as_mut()
                .standby_status_update(lsn, lsn, lsn, 0, 0)
                .await?;
        }
        while let Some(item) = stream.next().await {
            let env = item?;
            let dt = t0.elapsed().unwrap_or_default();
            frame_no += 1;
            match env {
                PgEnv::PrimaryKeepAlive(ka) => {
                    keepalives += 1;
                    if keepalives <= 3 || keepalives % 1000 == 0 || ka.reply() != 0 {
                        eprintln!(
                            "[{:.3}] frame#{frame_no} k[{keepalives}] wal_end={:#x} reply={}",
                            dt.as_secs_f64(),
                            ka.wal_end(),
                            ka.reply()
                        );
                    }
                }
                PgEnv::XLogData(xl) => {
                    wal_data += 1;
                    let pg_tag = match xl.data() {
                        PgMsg::Begin(_) => 'B',
                        PgMsg::Commit(_) => 'C',
                        PgMsg::Insert(_) => 'I',
                        PgMsg::Update(_) => 'U',
                        PgMsg::Delete(_) => 'D',
                        PgMsg::Relation(_) => 'R',
                        PgMsg::Truncate(_) => 'T',
                        PgMsg::Origin(_) => 'O',
                        PgMsg::Type(_) => 'Y',
                        PgMsg::Message(_) => 'M',
                        _ => '?',
                    };
                    eprintln!(
                        "[{:.3}] frame#{frame_no} w wal_start={:#x} wal_end={:#x} pg_tag={pg_tag}",
                        dt.as_secs_f64(),
                        xl.wal_start(),
                        xl.wal_end(),
                    );
                }
                _ => eprintln!(
                    "[{:.3}] frame#{frame_no} OTHER ReplicationMessage variant",
                    dt.as_secs_f64()
                ),
            }
        }
        eprintln!(
            "[wire_trace] EOF wrapped: frames={frame_no} wal_data={wal_data} keepalives={keepalives}"
        );
        return Ok(());
    }

    let mut stream = Box::pin(copy_stream);

    while let Some(item) = stream.next().await {
        let buf = item?;
        let dt = t0.elapsed().unwrap_or_default();
        frame_no += 1;
        bytes_seen += buf.len() as u64;
        if buf.is_empty() {
            eprintln!("[{:.3}] frame#{frame_no} EMPTY", dt.as_secs_f64());
            continue;
        }
        let kind = buf[0];
        match kind {
            // 'w' = XLogData. Layout: 1 byte tag + 8 wal_start + 8 wal_end + 8 ts + payload.
            b'w' => {
                wal_data += 1;
                if buf.len() < 25 {
                    eprintln!(
                        "[{:.3}] frame#{frame_no} w SHORT len={}",
                        dt.as_secs_f64(),
                        buf.len()
                    );
                    continue;
                }
                let wal_start = u64::from_be_bytes(buf[1..9].try_into().unwrap());
                let wal_end = u64::from_be_bytes(buf[9..17].try_into().unwrap());
                let payload = &buf[25..];
                let pg_tag = if payload.is_empty() {
                    '?'
                } else {
                    payload[0] as char
                };
                eprintln!(
                    "[{:.3}] frame#{frame_no} w wal_start={wal_start:#x} wal_end={wal_end:#x} \
                     payload_len={} pg_tag={pg_tag}",
                    dt.as_secs_f64(),
                    payload.len(),
                );
                if raw_stdout {
                    // Match pg_recvlogical -f - byte-for-byte: it strips
                    // the CopyData 'w' header + adds a newline after each
                    // pgoutput record? Actually it writes the pgoutput
                    // payload as-is. Mirror that.
                    stdout.write_all(payload)?;
                    stdout.flush()?;
                }
            }
            // 'k' = PrimaryKeepAlive. Layout: 1 byte tag + 8 wal_end + 8 ts + 1 reply.
            b'k' => {
                keepalives += 1;
                if buf.len() < 18 {
                    eprintln!(
                        "[{:.3}] frame#{frame_no} k SHORT len={}",
                        dt.as_secs_f64(),
                        buf.len()
                    );
                    continue;
                }
                let wal_end = u64::from_be_bytes(buf[1..9].try_into().unwrap());
                let reply = buf[17];
                if keepalives <= 3 || keepalives % 1000 == 0 || reply != 0 {
                    eprintln!(
                        "[{:.3}] frame#{frame_no} k[{keepalives}] wal_end={wal_end:#x} reply={reply}",
                        dt.as_secs_f64(),
                    );
                }
            }
            other => {
                eprintln!(
                    "[{:.3}] frame#{frame_no} UNKNOWN tag={:#x} len={}",
                    dt.as_secs_f64(),
                    other,
                    buf.len()
                );
            }
        }
    }

    eprintln!(
        "[wire_trace] EOF: frames={frame_no} wal_data={wal_data} keepalives={keepalives} bytes={bytes_seen}"
    );
    Ok(())
}
