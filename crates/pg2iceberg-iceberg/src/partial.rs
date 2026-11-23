//! Reading a data file in part: the rows a lookup needs, of the columns it
//! needs.
//!
//! A key the materializer's index evicted is found by reading the data
//! files that may hold it ([`crate::FileIndex::candidates`]), and a row's
//! stored row id by reading its file. Read whole, that costs the file's
//! size for a few rows. Most of a file is columns these lookups don't
//! need, and rows that can't be the ones looked for: its footer says where
//! each column's pages are and — in its page index, or per row group
//! without one — each page's smallest and largest value. So a
//! [`PartialFile`] reads the footer, then of the columns a lookup needs
//! only the pages that may hold its rows.

use crate::pk::PkKey;
use crate::reader::{conform, leaves, value_at};
use crate::writer::{arrow_schema_for, WriterError};
use arrow_array::{Array, Int64Array, RecordBatch};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::page_index::index::Index;
use parquet::file::statistics::Statistics;
use pg2iceberg_core::{ColumnName, ColumnSchema, IcebergType, PgValue};
use pg2iceberg_stream::{BlobStore, StreamError};
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

/// Bytes read off a file's end to open it: most footers, with their page
/// index, take one read.
const FOOTER_PREFETCH: usize = 64 * 1024;
/// Byte ranges at most this far apart are read as one.
const COALESCE_GAP: usize = 1024 * 1024;
/// Most byte ranges a read has in flight.
const CONCURRENT_RANGES: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error(transparent)]
    Blob(#[from] StreamError),
    #[error(transparent)]
    Decode(#[from] WriterError),
}

pub type Result<T> = std::result::Result<T, ReadError>;

/// A read's error: its object store's, or the file's.
fn read_error(e: ParquetError) -> ReadError {
    match e {
        ParquetError::External(e) => match e.downcast::<StreamError>() {
            Ok(e) => ReadError::Blob(*e),
            Err(e) => ReadError::Decode(WriterError::Encode(format!("parquet: {e}"))),
        },
        e => ReadError::Decode(WriterError::Encode(format!("parquet: {e}"))),
    }
}

/// One row a lookup found: its key, its position in the file, and the
/// row id it stores, if any.
pub type Found = (PkKey, u64, Option<i64>);

/// A data file opened for reading in part: its footer read, the rest read
/// as lookups need it.
pub struct PartialFile {
    reader: Source,
    metadata: ArrowReaderMetadata,
}

impl PartialFile {
    /// Open `path`, of `size` bytes, in `store`: read its footer and page
    /// index.
    pub async fn open(store: Arc<dyn BlobStore>, path: &str, size: u64) -> Result<Self> {
        let mut reader = Source::Store {
            store,
            path: path.into(),
        };
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .with_prefetch_hint(Some(FOOTER_PREFETCH))
            .load_and_finish(&mut reader, size as usize)
            .await
            .map_err(read_error)?;
        Self::with_metadata(reader, metadata)
    }

    /// A file already read whole, `bytes`.
    pub fn whole(bytes: Bytes) -> Result<Self> {
        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&bytes)
            .map_err(read_error)?;
        Self::with_metadata(Source::Bytes(bytes), metadata)
    }

    fn with_metadata(reader: Source, metadata: ParquetMetaData) -> Result<Self> {
        let options = ArrowReaderOptions::new().with_page_index(true);
        let metadata =
            ArrowReaderMetadata::try_new(Arc::new(metadata), options).map_err(read_error)?;
        Ok(Self { reader, metadata })
    }

    fn rows(&self) -> u64 {
        self.metadata
            .metadata()
            .row_groups()
            .iter()
            .map(|g| g.num_rows() as u64)
            .sum()
    }

    /// The rows holding any of `keys`, of key columns `pk_cols`, in
    /// order: each one's key, position, and the row id it stores.
    pub async fn find(
        &self,
        pk_cols: &[ColumnSchema],
        keys: &HashSet<&PkKey>,
        batch_rows: usize,
    ) -> Result<Vec<Found>> {
        let mut found = Vec::new();
        if keys.is_empty() {
            return Ok(found);
        }
        let rows = self.rows_holding(pk_cols, keys);
        let mut cols = pk_cols.to_vec();
        cols.push(crate::row_id_column());
        self.read(&cols, rows, batch_rows, |at, batch| {
            let ids = row_ids_of(batch, pk_cols.len());
            let mut values = Vec::with_capacity(pk_cols.len());
            for (i, &at) in at.iter().enumerate() {
                values.clear();
                for (c, col) in pk_cols.iter().enumerate() {
                    values.push(value_at(col, batch.column(c).as_ref(), i)?);
                }
                let key = PkKey::from_values(&values);
                if keys.contains(&key) {
                    found.push((key, at, ids.and_then(|ids| stored(ids, i))));
                }
            }
            Ok(())
        })
        .await?;
        Ok(found)
    }

    /// The row ids stored at `positions`, ascending: each one's, if any.
    pub async fn row_ids(&self, positions: &[u64], batch_rows: usize) -> Result<Vec<Option<i64>>> {
        let cols = [crate::row_id_column()];
        let schema = self.metadata.metadata().file_metadata().schema_descr();
        // A file storing none (all its rows' ids inherited) has no column
        // of them.
        if leaves(schema, &cols).is_empty() {
            return Ok(vec![None; positions.len()]);
        }
        let mut rows: Vec<Range<u64>> = Vec::new();
        for &at in positions {
            debug_assert!(rows.last().is_none_or(|r| r.end <= at), "ascending");
            push_range(&mut rows, at..at + 1);
        }
        let mut ids = Vec::with_capacity(positions.len());
        self.read(&cols, rows, batch_rows, |at, batch| {
            let column = row_ids_of(batch, 0);
            for i in 0..at.len() {
                ids.push(column.and_then(|c| stored(c, i)));
            }
            Ok(())
        })
        .await?;
        // A position past the file's end stores none.
        ids.resize(positions.len(), None);
        Ok(ids)
    }

    /// Read `cols` of the file's `rows` — sorted, apart — `batch_rows` at
    /// a time: `f` takes each batch, in `cols`' schema, with its rows'
    /// positions.
    async fn read(
        &self,
        cols: &[ColumnSchema],
        rows: Vec<Range<u64>>,
        batch_rows: usize,
        mut f: impl FnMut(&[u64], &RecordBatch) -> std::result::Result<(), WriterError>,
    ) -> Result<()> {
        let total = self.rows();
        let rows: Vec<Range<u64>> = rows
            .into_iter()
            .map(|r| r.start.min(total)..r.end.min(total))
            .filter(|r| !r.is_empty())
            .collect();
        if rows.is_empty() {
            return Ok(());
        }
        let parquet_schema = self.metadata.metadata().file_metadata().schema_descr();
        let projection = ProjectionMask::leaves(parquet_schema, leaves(parquet_schema, cols));
        let mut batches = ParquetRecordBatchStreamBuilder::new_with_metadata(
            self.reader.clone(),
            self.metadata.clone(),
        )
        .with_projection(projection)
        .with_row_selection(selection(&rows, total))
        .with_batch_size(batch_rows.max(1))
        .build()
        .map_err(read_error)?;
        let schema = Arc::new(arrow_schema_for(cols));
        let mut positions = rows.into_iter().flatten();
        let mut at = Vec::new();
        while let Some(batch) = batches.next().await {
            let batch = conform(&batch.map_err(read_error)?, cols, &schema)?;
            at.clear();
            at.extend(positions.by_ref().take(batch.num_rows()));
            if at.len() != batch.num_rows() {
                return Err(WriterError::Encode("read more rows than selected".into()).into());
            }
            f(&at, &batch)?;
        }
        Ok(())
    }

    /// The rows that may hold one of `keys`, sorted, apart: those of the
    /// pages — or row groups, a file without a page index — whose
    /// smallest and largest value of each key column bound that column's
    /// value in some key. A column of a type this doesn't compare doesn't
    /// narrow them, nor do keys it can't decode.
    fn rows_holding(&self, pk_cols: &[ColumnSchema], keys: &HashSet<&PkKey>) -> Vec<Range<u64>> {
        let metadata = self.metadata.metadata();
        let parquet_schema = metadata.file_metadata().schema_descr();
        let rows: Option<Vec<_>> = keys.iter().map(|k| k.to_row(pk_cols)).collect();
        // Each key column the pages can narrow: its leaf, and its values
        // sought, sorted.
        let columns: Vec<(usize, Sought)> = match &rows {
            None => Vec::new(),
            Some(rows) => pk_cols
                .iter()
                .filter_map(|col| {
                    let leaf = match leaves(parquet_schema, std::slice::from_ref(col))[..] {
                        [leaf] => leaf,
                        _ => return None,
                    };
                    let name = ColumnName(col.name.clone());
                    let values = rows.iter().map(|r| r.get(&name));
                    Some((leaf, Sought::of(col.ty, values)?))
                })
                .collect(),
        };
        let mut out = Vec::new();
        let mut first = 0;
        for (g, group) in metadata.row_groups().iter().enumerate() {
            let n = group.num_rows() as u64;
            let mut held: Vec<Range<u64>> = std::iter::once(0..n).collect();
            for (leaf, sought) in &columns {
                let pages: Vec<Range<u64>> = spans(metadata, g, *leaf, n)
                    .into_iter()
                    .filter(|(_, bounds)| bounds.as_ref().is_none_or(|b| sought.any_within(b)))
                    .map(|(rows, _)| rows)
                    .collect();
                held = intersect(&held, &pages);
                if held.is_empty() {
                    break;
                }
            }
            for r in held {
                push_range(&mut out, r.start + first..r.end + first);
            }
            first += n;
        }
        out
    }
}

/// The values of a key column sought, as its pages' statistics compare
/// them: sorted, without duplicates.
#[derive(Debug)]
enum Sought {
    Ints(Vec<i64>),
    Bytes(Vec<Vec<u8>>),
}

impl Sought {
    /// Of a column of type `ty`, `values`: `None` if they can't be
    /// compared with its statistics.
    fn of<'a>(ty: IcebergType, values: impl Iterator<Item = Option<&'a PgValue>>) -> Option<Self> {
        let mut sought = match ty {
            IcebergType::Int
            | IcebergType::Long
            | IcebergType::Date
            | IcebergType::Timestamp
            | IcebergType::TimestampTz => Sought::Ints(
                values
                    .map(|v| match v? {
                        PgValue::Int2(n) => Some(i64::from(*n)),
                        PgValue::Int4(n) => Some(i64::from(*n)),
                        PgValue::Int8(n) => Some(*n),
                        PgValue::Date(d) => Some(i64::from(d.0)),
                        PgValue::Timestamp(t) | PgValue::TimestampTz(t) => Some(t.0),
                        _ => None,
                    })
                    .collect::<Option<_>>()?,
            ),
            IcebergType::String | IcebergType::Uuid => Sought::Bytes(
                values
                    .map(|v| match v? {
                        PgValue::Text(s) => Some(s.as_bytes().to_vec()),
                        PgValue::Uuid(u) => Some(u.to_vec()),
                        _ => None,
                    })
                    .collect::<Option<_>>()?,
            ),
            _ => return None,
        };
        match &mut sought {
            Sought::Ints(v) => {
                v.sort_unstable();
                v.dedup();
            }
            Sought::Bytes(v) => {
                v.sort_unstable();
                v.dedup();
            }
        }
        Some(sought)
    }

    /// Whether a value sought is within `bounds`. One of another kind
    /// may be.
    fn any_within(&self, bounds: &Bounds) -> bool {
        match (self, bounds) {
            (Sought::Ints(v), Bounds::Ints(lo, hi)) => {
                let i = v.partition_point(|x| x < lo);
                v.get(i).is_some_and(|x| x <= hi)
            }
            (Sought::Bytes(v), Bounds::Bytes(lo, hi)) => {
                let i = v.partition_point(|x| x.as_slice() < lo.as_slice());
                v.get(i).is_some_and(|x| x.as_slice() <= hi.as_slice())
            }
            _ => true,
        }
    }
}

/// The smallest and largest value of a column in some rows. Integers
/// compare signed, bytes unsigned — as Parquet orders the types `Sought`
/// takes. A truncated string's bounds still bound it.
#[derive(Clone, Debug)]
enum Bounds {
    Ints(i64, i64),
    Bytes(Vec<u8>, Vec<u8>),
}

/// Row group `group`'s rows (`n` of them) by its pages of column `leaf`:
/// each page's, and its values' bounds, if the page index has them. A row
/// group without one is a page, of its statistics' bounds.
fn spans(
    metadata: &ParquetMetaData,
    group: usize,
    leaf: usize,
    n: u64,
) -> Vec<(Range<u64>, Option<Bounds>)> {
    let index = metadata
        .column_index()
        .and_then(|i| i.get(group)?.get(leaf));
    let offsets = metadata
        .offset_index()
        .and_then(|o| o.get(group)?.get(leaf));
    let pages = match (index, offsets) {
        (Some(index), Some(offsets)) => page_bounds(index)
            .filter(|b| b.len() == offsets.page_locations().len())
            .map(|b| (offsets.page_locations(), b)),
        _ => None,
    };
    let Some((locations, bounds)) = pages else {
        let stats = metadata.row_group(group).column(leaf).statistics();
        return vec![(0..n, stats.and_then(group_bounds))];
    };
    locations
        .iter()
        .zip(bounds)
        .enumerate()
        .map(|(p, (page, bounds))| {
            let start = page.first_row_index as u64;
            let end = locations
                .get(p + 1)
                .map_or(n, |next| next.first_row_index as u64);
            (start..end, bounds)
        })
        .collect()
}

/// Each page's bounds in a column index: `None` for a column of a type
/// `Bounds` doesn't take.
fn page_bounds(index: &Index) -> Option<Vec<Option<Bounds>>> {
    Some(match index {
        Index::INT32(i) => i
            .indexes
            .iter()
            .map(|p| Some(Bounds::Ints((*p.min()?).into(), (*p.max()?).into())))
            .collect(),
        Index::INT64(i) => i
            .indexes
            .iter()
            .map(|p| Some(Bounds::Ints(*p.min()?, *p.max()?)))
            .collect(),
        Index::BYTE_ARRAY(i) => i
            .indexes
            .iter()
            .map(|p| {
                Some(Bounds::Bytes(
                    p.min()?.data().to_vec(),
                    p.max()?.data().to_vec(),
                ))
            })
            .collect(),
        Index::FIXED_LEN_BYTE_ARRAY(i) => i
            .indexes
            .iter()
            .map(|p| {
                Some(Bounds::Bytes(
                    p.min()?.data().to_vec(),
                    p.max()?.data().to_vec(),
                ))
            })
            .collect(),
        _ => return None,
    })
}

/// A row group's bounds of a column, by its statistics.
fn group_bounds(stats: &Statistics) -> Option<Bounds> {
    Some(match stats {
        Statistics::Int32(s) => Bounds::Ints((*s.min_opt()?).into(), (*s.max_opt()?).into()),
        Statistics::Int64(s) => Bounds::Ints(*s.min_opt()?, *s.max_opt()?),
        Statistics::ByteArray(s) => {
            Bounds::Bytes(s.min_opt()?.data().to_vec(), s.max_opt()?.data().to_vec())
        }
        Statistics::FixedLenByteArray(s) => {
            Bounds::Bytes(s.min_opt()?.data().to_vec(), s.max_opt()?.data().to_vec())
        }
        _ => return None,
    })
}

/// The rows both `a` and `b` hold — each sorted, apart.
fn intersect(a: &[Range<u64>], b: &[Range<u64>]) -> Vec<Range<u64>> {
    let (mut i, mut j) = (0, 0);
    let mut out = Vec::new();
    while i < a.len() && j < b.len() {
        let start = a[i].start.max(b[j].start);
        let end = a[i].end.min(b[j].end);
        if start < end {
            out.push(start..end);
        }
        if a[i].end < b[j].end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// Add `r`, after every range in `ranges`, joining the last if it ends
/// where `r` starts.
fn push_range(ranges: &mut Vec<Range<u64>>, r: Range<u64>) {
    match ranges.last_mut() {
        Some(last) if last.end == r.start => last.end = r.end,
        _ => ranges.push(r),
    }
}

/// The selection of a file's `rows` — sorted, apart — of its `total`.
fn selection(rows: &[Range<u64>], total: u64) -> RowSelection {
    let mut selectors = Vec::with_capacity(rows.len() * 2 + 1);
    let mut at = 0;
    for r in rows {
        if r.start > at {
            selectors.push(RowSelector::skip((r.start - at) as usize));
        }
        selectors.push(RowSelector::select((r.end - r.start) as usize));
        at = r.end;
    }
    if total > at {
        selectors.push(RowSelector::skip((total - at) as usize));
    }
    RowSelection::from(selectors)
}

/// The batch's column of stored row ids, at `column`.
fn row_ids_of(batch: &RecordBatch, column: usize) -> Option<&Int64Array> {
    batch.column(column).as_any().downcast_ref::<Int64Array>()
}

fn stored(ids: &Int64Array, i: usize) -> Option<i64> {
    (!ids.is_null(i)).then(|| ids.value(i))
}

/// Where a partial file's bytes come from: its object store, a range at a
/// time, or its bytes read whole.
#[derive(Clone)]
enum Source {
    Store {
        store: Arc<dyn BlobStore>,
        path: Arc<str>,
    },
    Bytes(Bytes),
}

impl Source {
    async fn range(&self, range: Range<usize>) -> parquet::errors::Result<Bytes> {
        match self {
            Source::Store { store, path } => store
                .get_range(path, range.start as u64..range.end as u64)
                .await
                .map_err(|e| ParquetError::External(Box::new(e))),
            Source::Bytes(bytes) => {
                if range.start > range.end || range.end > bytes.len() {
                    return Err(ParquetError::EOF(format!(
                        "{range:?} of {} bytes",
                        bytes.len()
                    )));
                }
                Ok(bytes.slice(range))
            }
        }
    }
}

impl AsyncFileReader for Source {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let this = &*self;
        this.range(range).boxed()
    }

    /// Ranges close together are read as one, and several at once.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let this = &*self;
        async move {
            let merged = coalesce(&ranges);
            let read: Vec<Bytes> = futures::stream::iter(merged.iter().cloned())
                .map(|r| this.range(r))
                .buffered(CONCURRENT_RANGES)
                .try_collect()
                .await?;
            Ok(ranges
                .iter()
                .map(|r| {
                    // The merged range holding it.
                    let m = merged.partition_point(|m| m.start <= r.start) - 1;
                    let start = merged[m].start;
                    read[m].slice(r.start - start..r.end - start)
                })
                .collect())
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async {
            Err(ParquetError::General(
                "a partial file's footer is read as it opens".into(),
            ))
        }
        .boxed()
    }
}

/// `ranges` sorted, those at most [`COALESCE_GAP`] apart merged.
fn coalesce(ranges: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(sorted.len());
    for r in sorted {
        match merged.last_mut() {
            Some(last) if r.start <= last.end.saturating_add(COALESCE_GAP) => {
                last.end = last.end.max(r.end);
            }
            _ => merged.push(r),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, FixedSizeBinaryArray, Int32Array, StringArray};
    use async_trait::async_trait;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use pg2iceberg_stream::BlobInfo;
    use pollster::block_on;
    use std::sync::Mutex;

    /// Two key columns of the kinds pages narrow by, and a value column.
    #[derive(Clone, Copy, Debug)]
    enum Kind {
        IntString,
        LongUuid,
    }

    impl Kind {
        fn cols(self) -> Vec<ColumnSchema> {
            let (a, b) = match self {
                Kind::IntString => (IcebergType::Int, IcebergType::String),
                Kind::LongUuid => (IcebergType::Long, IcebergType::Uuid),
            };
            let col =
                |name: &str, field_id: i32, ty: IcebergType, is_primary_key: bool| ColumnSchema {
                    name: name.into(),
                    field_id,
                    ty,
                    nullable: !is_primary_key,
                    is_primary_key,
                };
            vec![
                col("a", 1, a, true),
                col("b", 2, b, true),
                col("v", 3, IcebergType::Long, false),
            ]
        }

        /// Row `k`'s key values: ascending in `k`, several `k`s a key.
        fn values(self, k: u64) -> [PgValue; 2] {
            let k = k as i64;
            match self {
                Kind::IntString => [
                    PgValue::Int4((k / 3 - 5) as i32),
                    PgValue::Text(format!("n{}", k % 3)),
                ],
                Kind::LongUuid => {
                    let mut u = [0u8; 16];
                    u[0] = 0xF0 | (k % 2) as u8;
                    u[15] = k as u8;
                    [
                        PgValue::Int8((k / 2 - 20) * 1_000_000_007),
                        PgValue::Uuid(u),
                    ]
                }
            }
        }

        fn key(self, k: u64) -> PkKey {
            PkKey::from_values(&self.values(k))
        }

        fn arrays(self, ks: &[u64]) -> Vec<ArrayRef> {
            let values: Vec<[PgValue; 2]> = ks.iter().map(|&k| self.values(k)).collect();
            let (a, b): (ArrayRef, ArrayRef) = match self {
                Kind::IntString => (
                    Arc::new(Int32Array::from_iter_values(values.iter().map(
                        |v| match v[0] {
                            PgValue::Int4(n) => n,
                            _ => unreachable!(),
                        },
                    ))),
                    Arc::new(StringArray::from_iter_values(values.iter().map(
                        |v| match &v[1] {
                            PgValue::Text(s) => s.clone(),
                            _ => unreachable!(),
                        },
                    ))),
                ),
                Kind::LongUuid => (
                    Arc::new(Int64Array::from_iter_values(values.iter().map(
                        |v| match v[0] {
                            PgValue::Int8(n) => n,
                            _ => unreachable!(),
                        },
                    ))),
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(values.iter().map(|v| match v[1] {
                            PgValue::Uuid(u) => u,
                            _ => unreachable!(),
                        }))
                        .unwrap(),
                    ),
                ),
            };
            let v = Arc::new(Int64Array::from_iter_values(ks.iter().map(|&k| k as i64)));
            vec![a, b, v]
        }
    }

    /// How a test file is laid out.
    #[derive(Clone, Debug)]
    struct Layout {
        kind: Kind,
        /// Each row's `k`, in order.
        ks: Vec<u64>,
        /// The row id each stores: with a column of them, if any.
        row_ids: Option<Vec<Option<i64>>>,
        page_rows: usize,
        group_rows: usize,
        statistics: EnabledStatistics,
        dictionary: bool,
    }

    impl Layout {
        fn sorted(kind: Kind, rows: u64, page_rows: usize, group_rows: usize) -> Self {
            Self {
                kind,
                ks: (0..rows).collect(),
                row_ids: None,
                page_rows,
                group_rows,
                statistics: EnabledStatistics::Page,
                dictionary: false,
            }
        }

        fn write(&self) -> Bytes {
            let mut cols = self.kind.cols();
            let mut arrays = self.kind.arrays(&self.ks);
            if let Some(ids) = &self.row_ids {
                cols.push(crate::row_id_column());
                arrays.push(Arc::new(Int64Array::from(ids.clone())));
            }
            let schema = Arc::new(arrow_schema_for(&cols));
            let props = WriterProperties::builder()
                .set_max_row_group_size(self.group_rows)
                .set_data_page_row_count_limit(self.page_rows)
                .set_write_batch_size(self.page_rows)
                .set_statistics_enabled(self.statistics)
                .set_dictionary_enabled(self.dictionary)
                .build();
            let mut out = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut out, schema.clone(), Some(props)).unwrap();
            writer
                .write(&RecordBatch::try_new(schema, arrays).unwrap())
                .unwrap();
            writer.close().unwrap();
            Bytes::from(out)
        }

        /// What a scan of the whole file finds of the `k`s' keys.
        fn scan(&self, sought: &[u64]) -> Vec<Found> {
            let keys: HashSet<PkKey> = sought.iter().map(|&k| self.kind.key(k)).collect();
            self.ks
                .iter()
                .enumerate()
                .map(|(at, &k)| (self.kind.key(k), at as u64))
                .filter(|(key, _)| keys.contains(key))
                .map(|(key, at)| {
                    let id = self.row_ids.as_ref().and_then(|ids| ids[at as usize]);
                    (key, at, id)
                })
                .collect()
        }
    }

    /// An object store of one file, `f`, counting what's read of it.
    struct OneFile {
        bytes: Bytes,
        gets: Mutex<u64>,
        ranged: Mutex<u64>,
    }

    impl OneFile {
        fn new(bytes: Bytes) -> Arc<Self> {
            Arc::new(Self {
                bytes,
                gets: Mutex::new(0),
                ranged: Mutex::new(0),
            })
        }
    }

    #[async_trait]
    impl BlobStore for OneFile {
        async fn put(&self, _: &str, _: Bytes) -> pg2iceberg_stream::Result<()> {
            unimplemented!()
        }
        async fn get(&self, _: &str) -> pg2iceberg_stream::Result<Bytes> {
            *self.gets.lock().unwrap() += 1;
            Ok(self.bytes.clone())
        }
        async fn get_range(&self, _: &str, r: Range<u64>) -> pg2iceberg_stream::Result<Bytes> {
            *self.ranged.lock().unwrap() += r.end - r.start;
            Ok(self.bytes.slice(r.start as usize..r.end as usize))
        }
        async fn list(&self, _: &str) -> pg2iceberg_stream::Result<Vec<BlobInfo>> {
            unimplemented!()
        }
        async fn delete(&self, _: &str) -> pg2iceberg_stream::Result<()> {
            unimplemented!()
        }
    }

    fn find(file: &PartialFile, kind: Kind, sought: &[u64], batch_rows: usize) -> Vec<Found> {
        let keys: Vec<PkKey> = sought.iter().map(|&k| kind.key(k)).collect();
        let keys: HashSet<&PkKey> = keys.iter().collect();
        block_on(file.find(&kind.cols()[..2], &keys, batch_rows)).unwrap()
    }

    /// A deterministic stream of numbers.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n.max(1)
        }
    }

    #[test]
    fn a_sorted_files_pages_narrow_a_lookup_to_the_keys_page() {
        let layout = Layout::sorted(Kind::IntString, 1000, 10, 300);
        let file = PartialFile::whole(layout.write()).unwrap();
        let cols = Kind::IntString.cols();
        let key = Kind::IntString.key(642);
        // The page of rows 640..650 — of the second row group, rows
        // 300..600 — holds it; a key column's page of another can't.
        let rows = file.rows_holding(&cols[..2], &HashSet::from([&key]));
        assert_eq!(rows, vec![640..650]);
        // Of keys in pages apart: theirs.
        let other = Kind::IntString.key(5);
        let rows = file.rows_holding(&cols[..2], &HashSet::from([&key, &other]));
        assert_eq!(rows, vec![0..10, 640..650]);
        // A key the file can't hold reads nothing.
        let absent = PkKey::from_values(&[PgValue::Int4(10_000), PgValue::Text("n0".into())]);
        assert!(file
            .rows_holding(&cols[..2], &HashSet::from([&absent]))
            .is_empty());
    }

    #[test]
    fn without_a_page_index_a_lookup_narrows_to_row_groups() {
        let mut layout = Layout::sorted(Kind::LongUuid, 1000, 10, 300);
        layout.statistics = EnabledStatistics::Chunk;
        let file = PartialFile::whole(layout.write()).unwrap();
        let key = Kind::LongUuid.key(642);
        let rows = file.rows_holding(&Kind::LongUuid.cols()[..2], &HashSet::from([&key]));
        assert_eq!(rows, vec![600..900]);
    }

    #[test]
    fn a_lookup_finds_what_a_whole_scan_does() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for case in 0..300 {
            let kind = [Kind::IntString, Kind::LongUuid][case % 2];
            let rows = rng.below(400);
            let mut ks: Vec<u64> = (0..rows).map(|_| rng.below(120)).collect();
            if rng.below(2) == 0 {
                ks.sort_unstable();
            }
            let row_ids = (rng.below(2) == 0).then(|| {
                ks.iter()
                    .map(|&k| (rng.below(3) > 0).then_some(k as i64 * 11))
                    .collect()
            });
            let layout = Layout {
                kind,
                ks,
                row_ids,
                page_rows: 1 + rng.below(12) as usize,
                group_rows: 1 + rng.below(80) as usize,
                statistics: [
                    EnabledStatistics::Page,
                    EnabledStatistics::Chunk,
                    EnabledStatistics::None,
                ][rng.below(3) as usize],
                dictionary: rng.below(2) == 0,
            };
            let sought: Vec<u64> = (0..rng.below(8)).map(|_| rng.below(130)).collect();
            let bytes = layout.write();
            let expected = layout.scan(&sought);
            let batch_rows = 1 + rng.below(50) as usize;
            let whole = PartialFile::whole(bytes.clone()).unwrap();
            assert_eq!(
                find(&whole, kind, &sought, batch_rows),
                expected,
                "case {case}: {layout:?}, sought {sought:?}"
            );
            let store = OneFile::new(bytes.clone());
            let file = block_on(PartialFile::open(store.clone(), "f", bytes.len() as u64)).unwrap();
            assert_eq!(
                find(&file, kind, &sought, batch_rows),
                expected,
                "case {case}"
            );
            assert_eq!(*store.gets.lock().unwrap(), 0, "case {case}: read whole");
        }
    }

    #[test]
    fn row_ids_are_read_at_their_positions() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for case in 0..100 {
            let rows = 1 + rng.below(300);
            let mut layout = Layout::sorted(
                Kind::IntString,
                rows,
                1 + rng.below(12) as usize,
                1 + rng.below(80) as usize,
            );
            let ids: Option<Vec<Option<i64>>> = (case % 4 > 0).then(|| {
                (0..rows)
                    .map(|k| (rng.below(3) > 0).then_some(k as i64 + 100))
                    .collect()
            });
            layout.row_ids = ids.clone();
            let mut positions: Vec<u64> = (0..rng.below(10)).map(|_| rng.below(rows)).collect();
            positions.sort_unstable();
            positions.dedup();
            let expected: Vec<Option<i64>> = positions
                .iter()
                .map(|&at| ids.as_ref().and_then(|ids| ids[at as usize]))
                .collect();
            let file = PartialFile::whole(layout.write()).unwrap();
            let read = block_on(file.row_ids(&positions, 1 + rng.below(20) as usize)).unwrap();
            assert_eq!(read, expected, "case {case}: {positions:?}");
        }
    }

    #[test]
    fn ranges_close_together_are_read_as_one() {
        let gap = COALESCE_GAP;
        assert_eq!(
            coalesce(&[10..20, 0..5, 20 + gap + 1..30 + gap, 5..8]),
            vec![0..20, 20 + gap + 1..30 + gap]
        );
        let mut source = Source::Bytes(Bytes::from_static(b"0123456789"));
        let read = block_on(source.get_byte_ranges(vec![6..8, 1..3, 2..5])).unwrap();
        assert_eq!(read, vec![&b"67"[..], &b"12"[..], &b"234"[..]]);
        // Ranges far apart, each its own read: each gets its own bytes.
        let far = 3 * gap;
        let bytes: Vec<u8> = (0..far + 10).map(|i| (i % 251) as u8).collect();
        let mut source = Source::Bytes(Bytes::from(bytes.clone()));
        let ranges = vec![far..far + 4, 2..6, gap + 100..gap + 103];
        let read = block_on(source.get_byte_ranges(ranges.clone())).unwrap();
        let expected: Vec<&[u8]> = ranges.iter().map(|r| &bytes[r.clone()]).collect();
        assert_eq!(read, expected);
    }
}
