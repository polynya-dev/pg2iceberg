//! Reading a table's staged log ahead of applying it.
//!
//! A staged chunk is a round trip to the object store; read one at a time,
//! a cycle mostly waits. [`StagedLog`] lists the log and reads its chunks
//! ahead of the one being applied — in the cycle's own task, no spawning:
//! its requests make progress whenever the cycle awaits it, either for the
//! next chunk ([`StagedLog::next`]) or alongside its own work
//! ([`StagedLog::alongside`]).
//!
//! The cycle's every await while a `StagedLog` is live has to be one of
//! those two. A request read ahead and then left unpolled can hold what
//! the cycle's own requests wait for — `PostgresCoordinator`'s one
//! connection, `VendedBlobStoreRouter`'s credential lock mid-refresh —
//! and the cycle would wait forever.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, StreamExt};
use pg2iceberg_coord::{CoordError, Coordinator, LogEntry};
use pg2iceberg_core::TableIdent;
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::VecDeque;
use std::future::{poll_fn, Future};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Log entries listed per `read_log` call.
pub(crate) const LOG_PAGE_ENTRIES: usize = 64;
/// Most staged chunks a cycle reads ahead of the one it's applying, and
/// most data files a step reads at once to resolve TOAST: read one at a
/// time, a cycle waits a round trip per file.
pub const READ_AHEAD: usize = 8;

type Read = (LogEntry, Result<Bytes, StreamError>);

/// A table's staged log past an offset: its entries in order, each with
/// its chunk, read up to [`READ_AHEAD`] entries ahead. Dropping it, as a
/// cycle that stops early does, drops the reads ahead.
pub(crate) struct StagedLog {
    coord: Arc<dyn Coordinator>,
    blob_store: Arc<dyn BlobStore>,
    ident: TableIdent,
    /// Where the next page of the log starts; `None` past its end.
    after: Option<u64>,
    /// The page being listed.
    listing: Option<BoxFuture<'static, Result<Vec<LogEntry>, CoordError>>>,
    /// Listed, not yet being read.
    listed: VecDeque<LogEntry>,
    reading: FuturesOrdered<BoxFuture<'static, Read>>,
    /// Read, not yet taken: with `reading`, at most `READ_AHEAD`.
    read: VecDeque<Read>,
    /// Listing failed: the error, once the entries before it are taken.
    failed: Option<CoordError>,
}

impl StagedLog {
    pub(crate) fn new(
        coord: Arc<dyn Coordinator>,
        blob_store: Arc<dyn BlobStore>,
        ident: TableIdent,
        after: u64,
    ) -> Self {
        Self {
            coord,
            blob_store,
            ident,
            after: Some(after),
            listing: None,
            listed: VecDeque::new(),
            reading: FuturesOrdered::new(),
            read: VecDeque::new(),
            failed: None,
        }
    }

    /// The next entry and its chunk; `None` at the end of the log.
    pub(crate) async fn next(&mut self) -> crate::materializer::Result<Option<(LogEntry, Bytes)>> {
        poll_fn(|cx| {
            self.drive(cx);
            if let Some((entry, bytes)) = self.read.pop_front() {
                // Start reading the one after, in its place.
                self.drive(cx);
                return Poll::Ready(Ok(Some((entry, bytes?))));
            }
            if !self.reading.is_empty() || self.listing.is_some() {
                return Poll::Pending;
            }
            match self.failed.take() {
                Some(e) => Poll::Ready(Err(e.into())),
                None => Poll::Ready(Ok(None)),
            }
        })
        .await
    }

    /// `work`, while the reads ahead go on.
    pub(crate) async fn alongside<F: Future>(&mut self, work: F) -> F::Output {
        let mut work = pin!(work);
        poll_fn(|cx| {
            if let Poll::Ready(out) = work.as_mut().poll(cx) {
                return Poll::Ready(out);
            }
            self.drive(cx);
            Poll::Pending
        })
        .await
    }

    /// Move the listing and reading along as far as they go now.
    fn drive(&mut self, cx: &mut Context<'_>) {
        loop {
            // List the next page before running out of entries to read.
            if self.listing.is_none() && self.listed.len() < READ_AHEAD {
                if let Some(after) = self.after {
                    let (coord, ident) = (self.coord.clone(), self.ident.clone());
                    self.listing = Some(Box::pin(async move {
                        coord.read_log(&ident, after, LOG_PAGE_ENTRIES).await
                    }));
                }
            }
            let mut listed = false;
            if let Some(listing) = &mut self.listing {
                if let Poll::Ready(page) = listing.as_mut().poll(cx) {
                    self.listing = None;
                    listed = true;
                    match page {
                        Ok(entries) => {
                            self.after = entries.last().map(|e| e.end_offset);
                            self.listed.extend(entries);
                        }
                        Err(e) => {
                            self.after = None;
                            self.failed = Some(e);
                        }
                    }
                }
            }
            while self.reading.len() + self.read.len() < READ_AHEAD {
                let Some(entry) = self.listed.pop_front() else {
                    break;
                };
                let blob_store = self.blob_store.clone();
                self.reading.push_back(Box::pin(async move {
                    let bytes = blob_store.get(&entry.s3_path).await;
                    (entry, bytes)
                }));
            }
            while let Poll::Ready(Some(read)) = self.reading.poll_next_unpin(cx) {
                self.read.push_back(read);
            }
            if !listed {
                return;
            }
        }
    }
}
