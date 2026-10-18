//! How the pipeline stages a flush against an object store whose requests
//! take time, in tokio's paused time: what a flush's uploads cost, and
//! what the caller waits for.

use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_coord::schema::CoordSchema;
use pg2iceberg_core::{
    ChangeEvent, ColumnName, Lsn, Namespace, Op, PgValue, TableIdent, Timestamp,
};
use pg2iceberg_logical::pipeline::CounterBlobNamer;
use pg2iceberg_logical::Pipeline;
use pg2iceberg_pg::DecodedMessage;
use pg2iceberg_sim::blob::MemoryBlobStore;
use pg2iceberg_sim::clock::TestClock;
use pg2iceberg_sim::coord::MemoryCoordinator;
use pg2iceberg_stream::{BlobInfo, BlobStore};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const PUT: Duration = Duration::from_secs(1);

/// `MemoryBlobStore` whose every put takes [`PUT`] of (virtual) time.
struct SlowPuts(MemoryBlobStore);

#[async_trait]
impl BlobStore for SlowPuts {
    async fn put(&self, path: &str, bytes: Bytes) -> pg2iceberg_stream::Result<()> {
        tokio::time::sleep(PUT).await;
        self.0.put(path, bytes).await
    }
    async fn get(&self, path: &str) -> pg2iceberg_stream::Result<Bytes> {
        self.0.get(path).await
    }
    async fn list(&self, prefix: &str) -> pg2iceberg_stream::Result<Vec<BlobInfo>> {
        self.0.list(prefix).await
    }
    async fn delete(&self, path: &str) -> pg2iceberg_stream::Result<()> {
        self.0.delete(path).await
    }
}

fn pipeline(flush_rows: usize) -> Pipeline<MemoryCoordinator> {
    let clock: Arc<dyn pg2iceberg_core::Clock> = Arc::new(TestClock::at(0));
    let coord = Arc::new(MemoryCoordinator::new(CoordSchema::default_name(), clock));
    let blob = Arc::new(SlowPuts(MemoryBlobStore::new()));
    let namer = Arc::new(CounterBlobNamer::new("s3://stage"));
    Pipeline::new(coord, blob, namer, flush_rows)
}

fn table(n: usize) -> TableIdent {
    TableIdent {
        namespace: Namespace(vec!["public".into()]),
        name: format!("t{n}"),
    }
}

/// A transaction inserting one row into each of `tables`, committing at
/// `commit`.
async fn transaction(p: &mut Pipeline<MemoryCoordinator>, xid: u32, commit: u64, tables: usize) {
    p.process(DecodedMessage::Begin {
        xid,
        final_lsn: Lsn(commit),
    })
    .await
    .unwrap();
    for t in 0..tables {
        let mut row = pg2iceberg_core::Row::new();
        row.insert(ColumnName("id".into()), PgValue::Int4(xid as i32));
        p.process(DecodedMessage::Change(ChangeEvent {
            table: table(t),
            op: Op::Insert,
            lsn: Lsn(commit - 1),
            commit_ts: Timestamp(0),
            xid: Some(xid),
            before: None,
            after: Some(row),
            unchanged_cols: vec![],
        }))
        .await
        .unwrap();
    }
    p.process(DecodedMessage::Commit {
        xid,
        commit_lsn: Lsn(commit),
    })
    .await
    .unwrap();
}

/// A flush stages one chunk per table it has changes for; they upload
/// together, not one after another.
#[tokio::test(start_paused = true)]
async fn a_flush_uploads_its_tables_chunks_together() {
    let mut p = pipeline(1000);
    transaction(&mut p, 1, 100, 4).await;
    let start = Instant::now();
    p.flush().await.unwrap();
    let took = start.elapsed();
    assert_eq!(p.flushed_lsn(), Lsn(100));
    assert!(took < 2 * PUT, "four tables' chunks took {took:?} to stage");
}

/// With flushes in the background, a flush a commit triggers stages while
/// the caller goes on processing; the slot may be acked past it only once
/// it lands. The next flush due waits for it: one claim at a time, in
/// order.
#[tokio::test(start_paused = true)]
async fn processing_goes_on_while_a_flush_stages() {
    // Two committed rows make a flush due (one transaction's would spill
    // instead, a large transaction's path).
    let mut p = pipeline(2);
    p.flush_in_background(Arc::new(|flush| {
        tokio::spawn(flush);
    }));
    let start = Instant::now();

    transaction(&mut p, 1, 100, 1).await;
    transaction(&mut p, 2, 200, 1).await;
    assert_eq!(start.elapsed(), Duration::ZERO, "waited for the upload");
    assert_eq!(p.flushed_lsn(), Lsn::ZERO, "acked before the claim");

    // The next waits for the first to land, then starts its own.
    transaction(&mut p, 3, 300, 1).await;
    transaction(&mut p, 4, 400, 1).await;
    assert_eq!(start.elapsed(), PUT);
    assert_eq!(p.flushed_lsn(), Lsn(200));

    p.wait_flushed().await.unwrap();
    assert_eq!(start.elapsed(), 2 * PUT);
    assert_eq!(p.flushed_lsn(), Lsn(400));
}
