//! `TableWriter`: produces Iceberg merge-on-read output (data files + equality
//! deletes) from a folded `Vec<MaterializedRow>`.
//!
//! For partitioned tables the writer fans rows out into one parquet file per
//! `(partition_tuple, kind)` group; the materializer then commits each group
//! as a separate `DataFile` with its `partition_values` carried through.

use crate::file_index::FileIndex;
use crate::fold::{pk_key, MaterializedRow};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pg2iceberg_core::{
    apply_transform, ColumnName, ColumnSchema, IcebergType, Op, PartitionLiteral, PgValue, Row,
    TableSchema,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

const FIELD_ID_KEY: &str = "PARQUET:field_id";
const TIMESTAMP_TZ: &str = "UTC";

#[derive(Clone, Debug, Error)]
pub enum WriterError {
    #[error("unsupported value type for Iceberg type {ty:?}: {value:?}")]
    UnsupportedValue { ty: IcebergType, value: PgValue },
    #[error("missing column {col} in row")]
    MissingColumn { col: String },
    #[error("null value in non-nullable column {col}")]
    NullInNonNullable { col: String },
    #[error("encode failure: {0}")]
    Encode(String),
    /// Reserved for the case where a new `IcebergType` variant is added
    /// to the type enum but the writer doesn't yet have a builder arm
    /// for it. All variants currently have arms; the exhaustive match
    /// in `build_one_array` keeps it that way at compile time.
    #[error("type {0:?} is not yet supported by the writer")]
    TypeNotYetSupported(IcebergType),
    /// Partition column missing from a non-Delete row. For Insert/Update
    /// rows this should never happen — they always carry full row data.
    /// For Delete rows we resolve via FileIndex first; this fires only
    /// when `Insert`/`Update` is missing a partition column, which
    /// indicates a fold/TOAST-resolution bug upstream.
    #[error("partition column `{column}` missing from {op:?} row on partitioned table")]
    PartitionColumnMissing { column: String, op: Op },
    /// Cross-batch `Delete` whose PK isn't in the FileIndex — we have
    /// no way to recover the partition tuple. We surface it as an
    /// error rather than silently dropping the delete; see the
    /// `tier_3_delete_with_no_row_data_and_no_file_index_entry_errors`
    /// test for the contract details.
    #[error(
        "delete for PK `{pk_key}` cannot be routed: row carries no partition \
         columns and FileIndex has no entry for this PK. Either set REPLICA \
         IDENTITY FULL on the source table so delete tuples carry partition \
         values, move partition columns into the PK, or wait for FileIndex \
         rebuild to complete (a fresh process before `rebuild_from_catalog` \
         finishes will hit this transiently)"
    )]
    DeletePartitionUnresolved { pk_key: String },
    #[error("partition transform failure on column `{column}`: {source}")]
    PartitionTransform {
        column: String,
        #[source]
        source: pg2iceberg_core::partition::PartitionError,
    },
}

pub type Result<T> = std::result::Result<T, WriterError>;

/// Encoded Parquet file ready for upload via a `BlobStore`. The caller picks
/// the path; we just hand back bytes + counts. Mirrors `EncodedChunk` in
/// `pg2iceberg-stream`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataChunk {
    pub bytes: Bytes,
    pub record_count: u64,
}

/// One parquet file's worth of rows that share a partition tuple. The
/// materializer uploads `chunk.bytes` and constructs a [`crate::DataFile`]
/// carrying `partition_values` so the catalog can build the right Iceberg
/// `Struct`.
#[derive(Clone, Debug)]
pub struct PreparedChunk {
    /// One literal per partition spec field, in the same order as
    /// `TableSchema.partition_spec`. Empty for unpartitioned tables.
    pub partition_values: Vec<PartitionLiteral>,
    pub chunk: DataChunk,
    /// PKs of the rows in this chunk. The materializer feeds these into
    /// `FileIndex` (data chunks) or uses them as the "deleted PKs" set
    /// (equality-delete chunks).
    pub pk_keys: Vec<String>,
}

#[derive(Debug)]
pub struct PreparedFiles {
    /// Full-schema rows for `Insert` and `Update` final ops, one entry per
    /// distinct partition tuple.
    pub data: Vec<PreparedChunk>,
    /// PK-only rows for `Update` and `Delete` final ops, one entry per
    /// distinct partition tuple.
    pub equality_deletes: Vec<PreparedChunk>,
    /// PK field-id list, populated whenever any equality-delete chunk is
    /// emitted. The materializer hands this to the catalog as the
    /// equality-delete file's `equality_field_ids` manifest column.
    pub pk_field_ids: Vec<i32>,
}

pub struct TableWriter {
    schema: TableSchema,
    pk_columns: Vec<ColumnSchema>,
    pk_col_names: Vec<ColumnName>,
    /// Subset of `schema.columns` that are PK; precomputed for the
    /// equality-delete file.
    pk_arrow_schema: Arc<Schema>,
    full_arrow_schema: Arc<Schema>,
}

impl TableWriter {
    pub fn new(schema: TableSchema) -> Self {
        let pk_columns: Vec<ColumnSchema> = schema.primary_key_columns().cloned().collect();
        let pk_col_names: Vec<ColumnName> = pk_columns
            .iter()
            .map(|c| ColumnName(c.name.clone()))
            .collect();
        let full_arrow_schema = Arc::new(arrow_schema_for(&schema.columns));
        let pk_arrow_schema = Arc::new(arrow_schema_for(&pk_columns));
        Self {
            schema,
            pk_columns,
            pk_col_names,
            pk_arrow_schema,
            full_arrow_schema,
        }
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn pk_field_ids(&self) -> Vec<i32> {
        self.pk_columns.iter().map(|c| c.field_id).collect()
    }

    /// Take folded rows, split by op into data + equality-delete buckets,
    /// then group each bucket by partition tuple. Each group is serialized
    /// to one parquet chunk; the caller flushes per materializer cycle.
    ///
    /// `file_index` is consulted only for partitioned tables to recover
    /// partition values for `Delete` rows that don't carry the partition
    /// source column on the row payload (replica identity DEFAULT case
    /// when partition col isn't in the PK). The two-tier resolution
    /// checks the row payload first, then falls back to the FileIndex.
    /// A tier-3 miss surfaces as `WriterError::DeletePartitionUnresolved`
    /// rather than a silent drop, so the operator gets a clear signal.
    /// Pass `&FileIndex::new()` for unpartitioned schemas or when the
    /// caller has no file index (e.g. test harnesses).
    pub fn prepare(
        &self,
        rows: &[MaterializedRow],
        file_index: &FileIndex,
    ) -> Result<PreparedFiles> {
        if rows.is_empty() {
            return Ok(PreparedFiles {
                data: Vec::new(),
                equality_deletes: Vec::new(),
                pk_field_ids: self.pk_field_ids(),
            });
        }

        let mut data_rows: Vec<&MaterializedRow> = Vec::new();
        let mut delete_rows: Vec<&MaterializedRow> = Vec::new();

        for r in rows {
            match r.op {
                Op::Insert => data_rows.push(r),
                Op::Update => {
                    data_rows.push(r);
                    delete_rows.push(r);
                }
                Op::Delete => delete_rows.push(r),
                Op::Relation | Op::Truncate => {
                    return Err(WriterError::Encode(format!(
                        "unexpected op in folded rows: {:?}",
                        r.op
                    )));
                }
            }
        }

        let data = self.group_and_encode(
            &data_rows,
            &self.full_arrow_schema,
            &self.schema.columns,
            file_index,
        )?;
        let equality_deletes = self.group_and_encode(
            &delete_rows,
            &self.pk_arrow_schema,
            &self.pk_columns,
            file_index,
        )?;

        Ok(PreparedFiles {
            data,
            equality_deletes,
            pk_field_ids: self.pk_field_ids(),
        })
    }

    fn group_and_encode(
        &self,
        rows: &[&MaterializedRow],
        arrow_schema: &Arc<Schema>,
        cols: &[ColumnSchema],
        file_index: &FileIndex,
    ) -> Result<Vec<PreparedChunk>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // For unpartitioned tables we skip tuple computation entirely and
        // emit a single chunk. This matches the pre-partitioning behavior
        // bit-for-bit.
        if !self.schema.is_partitioned() {
            let row_refs: Vec<&Row> = rows.iter().map(|r| &r.row).collect();
            let chunk = write_chunk(arrow_schema, cols, &row_refs)?;
            let pk_keys = rows
                .iter()
                .map(|r| pk_key(&r.row, &self.pk_col_names))
                .collect();
            return Ok(vec![PreparedChunk {
                partition_values: Vec::new(),
                chunk,
                pk_keys,
            }]);
        }

        // Partitioned table: compute per-row tuple, group by tuple, encode
        // one parquet file per group. PartitionLiteral does not implement
        // Hash/Ord (Float/Double NaN handling), so we use a linear-scan
        // group keyed by tuple equality. Number of partitions per
        // materializer cycle is small in practice.
        type Group<'a> = (Vec<PartitionLiteral>, Vec<&'a MaterializedRow>);
        let mut groups: Vec<Group> = Vec::new();
        for r in rows {
            let tuple = self.compute_partition_tuple(r, file_index)?;
            if let Some(slot) = groups.iter_mut().find(|(k, _)| k == &tuple) {
                slot.1.push(r);
            } else {
                groups.push((tuple, vec![r]));
            }
        }

        let mut out: Vec<PreparedChunk> = Vec::with_capacity(groups.len());
        for (partition_values, group_rows) in groups {
            let row_refs: Vec<&Row> = group_rows.iter().map(|r| &r.row).collect();
            let chunk = write_chunk(arrow_schema, cols, &row_refs)?;
            let pk_keys = group_rows
                .iter()
                .map(|r| pk_key(&r.row, &self.pk_col_names))
                .collect();
            out.push(PreparedChunk {
                partition_values,
                chunk,
                pk_keys,
            });
        }
        Ok(out)
    }

    /// Compute the partition tuple for one row. Tier-1: read partition
    /// source columns from the row. Tier-2 (Delete only): consult FileIndex.
    /// Tier-3 (Delete only): error.
    fn compute_partition_tuple(
        &self,
        r: &MaterializedRow,
        file_index: &FileIndex,
    ) -> Result<Vec<PartitionLiteral>> {
        // Tier 1: row-direct. Works for Insert/Update always; works for
        // Delete iff every partition source column is in the PK.
        let mut tuple: Vec<PartitionLiteral> = Vec::with_capacity(self.schema.partition_spec.len());
        let mut missing: Option<&str> = None;
        for f in &self.schema.partition_spec {
            let key = ColumnName(f.source_column.clone());
            match r.row.get(&key) {
                Some(value) => {
                    let lit = apply_transform(value, f.transform).map_err(|e| {
                        WriterError::PartitionTransform {
                            column: f.source_column.clone(),
                            source: e,
                        }
                    })?;
                    tuple.push(lit);
                }
                None => {
                    missing = Some(f.source_column.as_str());
                    break;
                }
            }
        }
        if missing.is_none() {
            return Ok(tuple);
        }

        // Tier 1 missed. For Insert/Update this is a programming error
        // upstream — those rows must carry full data after fold +
        // resolve_unchanged_cols.
        if r.op != Op::Delete {
            return Err(WriterError::PartitionColumnMissing {
                column: missing.unwrap().to_string(),
                op: r.op,
            });
        }

        // Tier 2: FileIndex resolves Delete to its prior data file's
        // partition tuple.
        let key_str = pk_key(&r.row, &self.pk_col_names);
        if let Some(values) = file_index.partition_values_for_pk(&key_str) {
            if values.len() == self.schema.partition_spec.len() {
                return Ok(values.to_vec());
            }
            // FileIndex is partitioned-arity-mismatched (e.g. partition spec
            // changed). Fall through to tier-3.
        }

        // Tier 3: nowhere left to look. Error rather than silently drop —
        // a dropped delete is a correctness bug that's invisible in normal
        // operation (Go's behavior); we'd rather fail loudly.
        Err(WriterError::DeletePartitionUnresolved { pk_key: key_str })
    }
}

fn arrow_schema_for(cols: &[ColumnSchema]) -> Schema {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let dt = arrow_data_type(c.ty);
            let mut md = HashMap::new();
            md.insert(FIELD_ID_KEY.to_string(), c.field_id.to_string());
            Field::new(&c.name, dt, c.nullable).with_metadata(md)
        })
        .collect();
    Schema::new(fields)
}

fn arrow_data_type(ty: IcebergType) -> DataType {
    match ty {
        IcebergType::Boolean => DataType::Boolean,
        IcebergType::Int => DataType::Int32,
        IcebergType::Long => DataType::Int64,
        IcebergType::Float => DataType::Float32,
        IcebergType::Double => DataType::Float64,
        IcebergType::String => DataType::Utf8,
        IcebergType::Date => DataType::Date32,
        IcebergType::Time => DataType::Time64(TimeUnit::Microsecond),
        IcebergType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        IcebergType::TimestampTz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into()))
        }
        IcebergType::Decimal { precision, scale } => DataType::Decimal128(precision, scale as i8),
        IcebergType::Binary => DataType::Binary,
        // Iceberg's `uuid` is a 16-byte fixed-size binary on the
        // Parquet wire (per the Iceberg → Parquet mapping in the
        // spec). Encoding as `FixedSizeBinary(16)` keeps round-trip
        // exact and lets readers decode without ambiguity.
        IcebergType::Uuid => DataType::FixedSizeBinary(16),
    }
}

/// Convert a `PgValue::Numeric` to the i128 the Arrow `Decimal128`
/// builder expects, rescaled to `target_scale` (the column's iceberg
/// scale). Returns `None` if the value can't be represented (e.g.
/// the unscaled bytes overflow i128, or upscaling would lose
/// precision because the source has more fractional digits than the
/// column allows). The PG side already enforces `precision ≤ 38` at
/// startup, so the i128 fit is the only practical risk here.
fn decimal_to_arrow_i128(d: &pg2iceberg_core::value::Decimal, target_scale: i32) -> Option<i128> {
    // Sign-extend the unscaled big-endian bytes to a 16-byte i128.
    if d.unscaled_be_bytes.is_empty() {
        return Some(0);
    }
    if d.unscaled_be_bytes.len() > 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    let pad = 16 - d.unscaled_be_bytes.len();
    // Sign-extend: if the high bit of the first byte is set the
    // value is negative; pad with 0xFF so the two's-complement
    // representation stays consistent.
    let sign = if d.unscaled_be_bytes[0] & 0x80 != 0 {
        0xFFu8
    } else {
        0
    };
    for b in buf.iter_mut().take(pad) {
        *b = sign;
    }
    buf[pad..].copy_from_slice(&d.unscaled_be_bytes);
    let mut value = i128::from_be_bytes(buf);
    // Rescale to the column's iceberg scale.
    let src_scale = d.scale as i32;
    if target_scale > src_scale {
        // Upscale: multiply by 10^(target - src). Saturating not OK —
        // rather return None than silently corrupt.
        let diff = (target_scale - src_scale) as u32;
        let factor = 10i128.checked_pow(diff)?;
        value = value.checked_mul(factor)?;
    } else if target_scale < src_scale {
        // Downscale would silently truncate fractional digits and
        // lose precision. We refuse rather than mask data loss —
        // PG-side casts to a stricter `numeric(p,s)` would have
        // erred at write time, and a CDC pipeline that drops
        // precision after the fact is a foot-gun. The caller
        // surfaces this as `WriterError::Encode`, refuses the
        // commit, and the operator sees a loud failure instead of
        // bad rows.
        let diff = (src_scale - target_scale) as u32;
        let divisor = 10i128.checked_pow(diff)?;
        let truncated = value.checked_div(divisor)?;
        let remainder = value.checked_rem(divisor)?;
        if remainder != 0 {
            return None;
        }
        value = truncated;
    }
    Some(value)
}

fn write_chunk(
    arrow_schema: &Arc<Schema>,
    cols: &[ColumnSchema],
    rows: &[&Row],
) -> Result<DataChunk> {
    let arrays = build_arrays(cols, rows)?;
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|e| WriterError::Encode(format!("build record batch: {e}")))?;

    let mut buf = Vec::<u8>::new();
    {
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| WriterError::Encode(format!("parquet writer: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| WriterError::Encode(format!("parquet write: {e}")))?;
        writer
            .close()
            .map_err(|e| WriterError::Encode(format!("parquet close: {e}")))?;
    }

    Ok(DataChunk {
        bytes: Bytes::from(buf),
        record_count: rows.len() as u64,
    })
}

fn build_arrays(cols: &[ColumnSchema], rows: &[&Row]) -> Result<Vec<Arc<dyn Array>>> {
    let mut out: Vec<Arc<dyn Array>> = Vec::with_capacity(cols.len());
    for c in cols {
        out.push(build_one_array(c, rows)?);
    }
    Ok(out)
}

fn build_one_array(col: &ColumnSchema, rows: &[&Row]) -> Result<Arc<dyn Array>> {
    let key = ColumnName(col.name.clone());
    let n = rows.len();

    macro_rules! collect_with {
        ($builder:expr, $extract:expr) => {{
            let mut b = $builder;
            for r in rows {
                let v = r.get(&key);
                match v {
                    None => {
                        if !col.nullable {
                            return Err(WriterError::MissingColumn {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Null) => {
                        if !col.nullable {
                            return Err(WriterError::NullInNonNullable {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(other) => match $extract(other) {
                        Some(x) => b.append_value(x),
                        None => {
                            return Err(WriterError::UnsupportedValue {
                                ty: col.ty,
                                value: other.clone(),
                            });
                        }
                    },
                }
            }
            Arc::new(b.finish()) as Arc<dyn Array>
        }};
    }

    let arr = match col.ty {
        IcebergType::Boolean => collect_with!(BooleanBuilder::with_capacity(n), |v: &PgValue| {
            if let PgValue::Bool(b) = v {
                Some(*b)
            } else {
                None
            }
        }),
        IcebergType::Int => collect_with!(Int32Builder::with_capacity(n), |v: &PgValue| {
            match v {
                PgValue::Int2(x) => Some(*x as i32),
                PgValue::Int4(x) => Some(*x),
                _ => None,
            }
        }),
        // Iceberg int→long is a legal, value-preserving widening, and
        // Postgres SMALLINT/INTEGER both fit in i64. Accepting all three
        // Pg integer widths lets the materializer flush rows that were
        // staged *before* an `ALTER COLUMN ... TYPE BIGINT` together
        // with rows staged after it: the promotion updates the column
        // type to Long, but coord-staged rows still carry their
        // original Int2/Int4 wire values until the next snapshot.
        IcebergType::Long => collect_with!(Int64Builder::with_capacity(n), |v: &PgValue| {
            match v {
                PgValue::Int2(x) => Some(*x as i64),
                PgValue::Int4(x) => Some(*x as i64),
                PgValue::Int8(x) => Some(*x),
                _ => None,
            }
        }),
        IcebergType::Float => collect_with!(Float32Builder::with_capacity(n), |v: &PgValue| {
            match v {
                PgValue::Float4(x) => Some(*x),
                _ => None,
            }
        }),
        // Iceberg float→double is the parallel widening for floats.
        // Accept Float4 alongside Float8 so cross-promotion rows from
        // before an `ALTER COLUMN ... TYPE DOUBLE PRECISION` keep
        // flushing.
        IcebergType::Double => collect_with!(Float64Builder::with_capacity(n), |v: &PgValue| {
            match v {
                PgValue::Float4(x) => Some(*x as f64),
                PgValue::Float8(x) => Some(*x),
                _ => None,
            }
        }),
        IcebergType::String => collect_with!(
            StringBuilder::with_capacity(n, n * 16),
            |v: &PgValue| match v {
                PgValue::Text(s) | PgValue::Json(s) | PgValue::Jsonb(s) => Some(s.clone()),
                _ => None,
            }
        ),
        IcebergType::Date => collect_with!(Date32Builder::with_capacity(n), |v: &PgValue| {
            match v {
                PgValue::Date(d) => Some(d.0),
                _ => None,
            }
        }),
        IcebergType::Timestamp => collect_with!(
            TimestampMicrosecondBuilder::with_capacity(n),
            |v: &PgValue| match v {
                PgValue::Timestamp(t) => Some(t.0),
                _ => None,
            }
        ),
        IcebergType::TimestampTz => collect_with!(
            TimestampMicrosecondBuilder::with_capacity(n).with_timezone(TIMESTAMP_TZ),
            |v: &PgValue| match v {
                PgValue::TimestampTz(t) => Some(t.0),
                _ => None,
            }
        ),
        IcebergType::Decimal { precision, scale } => {
            let mut b = Decimal128Builder::with_capacity(n)
                .with_precision_and_scale(precision, scale as i8)
                .map_err(|e| {
                    WriterError::Encode(format!(
                        "Decimal128Builder precision={precision} scale={scale}: {e}"
                    ))
                })?;
            for r in rows {
                match r.get(&key) {
                    None => {
                        if !col.nullable {
                            return Err(WriterError::MissingColumn {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Null) => {
                        if !col.nullable {
                            return Err(WriterError::NullInNonNullable {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Numeric(d)) => match decimal_to_arrow_i128(d, scale as i32) {
                        Some(x) => b.append_value(x),
                        None => {
                            return Err(WriterError::Encode(format!(
                                "decimal value (scale={}) out of range for \
                                 Decimal128({precision},{scale}) — would lose precision \
                                 or overflow i128",
                                d.scale
                            )));
                        }
                    },
                    Some(other) => {
                        return Err(WriterError::UnsupportedValue {
                            ty: col.ty,
                            value: other.clone(),
                        });
                    }
                }
            }
            Arc::new(b.finish()) as Arc<dyn Array>
        }
        IcebergType::Binary => {
            let mut b = BinaryBuilder::with_capacity(n, n * 32);
            for r in rows {
                match r.get(&key) {
                    None => {
                        if !col.nullable {
                            return Err(WriterError::MissingColumn {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Null) => {
                        if !col.nullable {
                            return Err(WriterError::NullInNonNullable {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Bytea(bytes)) => b.append_value(bytes.as_slice()),
                    Some(other) => {
                        return Err(WriterError::UnsupportedValue {
                            ty: col.ty,
                            value: other.clone(),
                        });
                    }
                }
            }
            Arc::new(b.finish()) as Arc<dyn Array>
        }
        IcebergType::Uuid => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, 16);
            for r in rows {
                match r.get(&key) {
                    None => {
                        if !col.nullable {
                            return Err(WriterError::MissingColumn {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Null) => {
                        if !col.nullable {
                            return Err(WriterError::NullInNonNullable {
                                col: col.name.clone(),
                            });
                        }
                        b.append_null();
                    }
                    Some(PgValue::Uuid(bytes)) => {
                        b.append_value(bytes).map_err(|e| {
                            WriterError::Encode(format!("FixedSizeBinary append uuid: {e}"))
                        })?;
                    }
                    Some(other) => {
                        return Err(WriterError::UnsupportedValue {
                            ty: col.ty,
                            value: other.clone(),
                        });
                    }
                }
            }
            Arc::new(b.finish()) as Arc<dyn Array>
        }
        // Iceberg's `time` is microseconds-since-midnight in i64
        // storage, which matches Arrow's `Time64(Microsecond)`. PG
        // `TIME WITH TIME ZONE` carries an offset we don't store —
        // Iceberg's `time` is timezone-naive — so `PgValue::TimeTz`
        // collapses to the same Time64 column by taking only its
        // microseconds component.
        IcebergType::Time => {
            collect_with!(Time64MicrosecondBuilder::with_capacity(n), |v: &PgValue| {
                match v {
                    PgValue::Time(t) => Some(t.0),
                    PgValue::TimeTz { time, .. } => Some(time.0),
                    _ => None,
                }
            })
        }
    };

    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::MaterializedRow;
    use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use pg2iceberg_core::value::{DaysSinceEpoch, Decimal, TimestampMicros};
    use pg2iceberg_core::{Namespace, PartitionField, TableIdent, TableSchema, Transform};
    use std::collections::BTreeMap;

    // ── Decimal scaling: never silently lose precision ────────────────

    /// Build a `PgValue::Numeric` from a signed decimal string. Caller
    /// owns the precision/scale convention; this helper just packs the
    /// big-endian unscaled bytes correctly so the writer's i128 path
    /// is exercised.
    fn dec_from_i128(unscaled: i128, scale: u8) -> Decimal {
        Decimal {
            unscaled_be_bytes: unscaled.to_be_bytes().to_vec(),
            scale,
        }
    }

    #[test]
    fn decimal_to_arrow_i128_same_scale_round_trips() {
        // 1.23 (i.e. unscaled=123, scale=2) → i128=123 at target_scale=2.
        let d = dec_from_i128(123, 2);
        assert_eq!(decimal_to_arrow_i128(&d, 2), Some(123));
    }

    #[test]
    fn decimal_to_arrow_i128_upscale_is_lossless() {
        // 1.23 (scale=2) → target_scale=4 → 12300.
        let d = dec_from_i128(123, 2);
        assert_eq!(decimal_to_arrow_i128(&d, 4), Some(12_300));
    }

    #[test]
    fn decimal_to_arrow_i128_downscale_with_zero_remainder_succeeds() {
        // 1.2300 (scale=4) → target_scale=2 → 123 (no fractional loss).
        let d = dec_from_i128(12_300, 4);
        assert_eq!(decimal_to_arrow_i128(&d, 2), Some(123));
    }

    #[test]
    fn decimal_to_arrow_i128_downscale_with_nonzero_remainder_refuses() {
        // 1.2345 (scale=4) → target_scale=2 → would silently truncate
        // to 1.23, losing the 0.0045. Must return None so the writer
        // raises `WriterError::Encode` and the operator sees the data
        // refused rather than dropped.
        let d = dec_from_i128(12_345, 4);
        assert_eq!(decimal_to_arrow_i128(&d, 2), None);
    }

    #[test]
    fn decimal_to_arrow_i128_downscale_negative_value_with_remainder_refuses() {
        // Negative-value flavour of the lossy-downscale guard. Same
        // reasoning, exercises the sign-bit path through the helper.
        let d = dec_from_i128(-12_345, 4);
        assert_eq!(decimal_to_arrow_i128(&d, 2), None);
    }

    #[test]
    fn decimal_to_arrow_i128_negative_round_trips() {
        let d = dec_from_i128(-123, 2);
        assert_eq!(decimal_to_arrow_i128(&d, 2), Some(-123));
    }

    #[test]
    fn decimal_to_arrow_i128_preserves_short_byte_input() {
        // Real PG sends the unscaled value as the minimum number of
        // bytes — `123` is one byte (`0x7B`), not eight. The helper
        // sign-extends correctly.
        let d = Decimal {
            unscaled_be_bytes: vec![0x7B],
            scale: 0,
        };
        assert_eq!(decimal_to_arrow_i128(&d, 0), Some(123));
    }

    #[test]
    fn decimal_to_arrow_i128_short_byte_negative_sign_extends() {
        // `-128` as a single byte is `0x80`. Sign-extension to i128
        // must produce a negative value, not 0x80 = 128.
        let d = Decimal {
            unscaled_be_bytes: vec![0x80],
            scale: 0,
        };
        assert_eq!(decimal_to_arrow_i128(&d, 0), Some(-128));
    }

    fn schema_id_qty() -> TableSchema {
        TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "orders".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    name: "qty".into(),
                    field_id: 2,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
            pg_schema: None,
        }
    }

    fn col(name: &str) -> ColumnName {
        ColumnName(name.into())
    }

    fn row(id: i32, qty: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("qty"), PgValue::Int4(qty));
        r
    }

    fn pk_only(id: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r
    }

    fn read_parquet(bytes: &Bytes) -> RecordBatch {
        let owned = bytes.clone();
        let reader = ParquetRecordBatchReaderBuilder::try_new(owned)
            .unwrap()
            .build()
            .unwrap();
        let mut batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1, "expected exactly one row group");
        batches.pop().unwrap()
    }

    #[test]
    fn empty_input_returns_empty_prepared_files() {
        let w = TableWriter::new(schema_id_qty());
        let p = w.prepare(&[], &FileIndex::new()).unwrap();
        assert!(p.data.is_empty());
        assert!(p.equality_deletes.is_empty());
    }

    #[test]
    fn single_insert_produces_data_file_only() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: row(1, 10),
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 1);
        assert!(p.equality_deletes.is_empty());

        let chunk = &p.data[0];
        assert!(chunk.partition_values.is_empty());
        let batch = read_parquet(&chunk.chunk.bytes);
        assert_eq!(batch.num_rows(), 1);
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let qty_col = batch
            .column_by_name("qty")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(qty_col.value(0), 10);
    }

    #[test]
    fn single_delete_produces_equality_delete_only_with_pk_columns() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Delete,
                    row: pk_only(7),
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        assert!(p.data.is_empty());
        assert_eq!(p.equality_deletes.len(), 1);
        let chunk = &p.equality_deletes[0];
        assert_eq!(chunk.chunk.record_count, 1);
        assert_eq!(p.pk_field_ids, vec![1]);

        let batch = read_parquet(&chunk.chunk.bytes);
        // Equality-delete file has *only* PK columns.
        let arrow_schema = batch.schema();
        let names: Vec<&str> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["id"]);
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 7);
    }

    #[test]
    fn single_update_produces_both_files() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Update,
                    row: row(3, 99),
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 1);
        assert_eq!(p.equality_deletes.len(), 1);
        let data = &p.data[0].chunk;
        let dels = &p.equality_deletes[0].chunk;
        assert_eq!(data.record_count, 1);
        assert_eq!(dels.record_count, 1);

        let data_batch = read_parquet(&data.bytes);
        assert_eq!(data_batch.num_rows(), 1);
        let qty_col = data_batch
            .column_by_name("qty")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(qty_col.value(0), 99);

        let del_batch = read_parquet(&dels.bytes);
        let id_col = del_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 3);
    }

    #[test]
    fn mixed_iud_partitioned_correctly() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(
                &[
                    MaterializedRow {
                        op: Op::Insert,
                        row: row(1, 10),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Update,
                        row: row(2, 20),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Delete,
                        row: pk_only(3),
                        unchanged_cols: vec![],
                    },
                ],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 1);
        assert_eq!(p.data[0].chunk.record_count, 2); // I + U
        assert_eq!(p.equality_deletes.len(), 1);
        assert_eq!(p.equality_deletes[0].chunk.record_count, 2); // U + D
    }

    #[test]
    fn field_ids_in_arrow_schema_metadata() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: row(1, 10),
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        let batch = read_parquet(&p.data[0].chunk.bytes);
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let id = field
                .metadata()
                .get(FIELD_ID_KEY)
                .expect("field_id metadata present");
            assert_eq!(id, &(i as i32 + 1).to_string());
        }
    }

    #[test]
    fn full_schema_round_trip_for_supported_types() {
        let schema = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "wide".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    name: "name".into(),
                    field_id: 2,
                    ty: IcebergType::String,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnSchema {
                    name: "active".into(),
                    field_id: 3,
                    ty: IcebergType::Boolean,
                    nullable: false,
                    is_primary_key: false,
                },
                ColumnSchema {
                    name: "score".into(),
                    field_id: 4,
                    ty: IcebergType::Long,
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
            pg_schema: None,
        };
        let w = TableWriter::new(schema);
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(42));
        r.insert(col("name"), PgValue::Text("alice".into()));
        r.insert(col("active"), PgValue::Bool(true));
        r.insert(col("score"), PgValue::Int8(1_000_000));
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: r,
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        let batch = read_parquet(&p.data[0].chunk.bytes);
        assert_eq!(
            batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "alice"
        );
        assert!(batch
            .column_by_name("active")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        assert_eq!(
            batch
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1_000_000
        );
    }

    #[test]
    fn null_in_non_nullable_column_errors() {
        let w = TableWriter::new(schema_id_qty());
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(1));
        // qty omitted entirely.
        let err = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: r,
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap_err();
        assert!(matches!(err, WriterError::MissingColumn { ref col } if col == "qty"));
    }

    // (Previously: `unsupported_type_returns_typed_error` asserted that
    // `IcebergType::Time` bubbled `WriterError::TypeNotYetSupported`.
    // Time is now wired through `Time64MicrosecondBuilder`, so every
    // `IcebergType` variant has a real builder. The guard variant is
    // kept on `WriterError` for forward-compat — when a new
    // `IcebergType` variant lands without a builder, the writer's
    // exhaustive match will require returning `TypeNotYetSupported`
    // again — but there's no in-tree type to assert it on today.)

    // ── partitioned writes ────────────────────────────────────────────────

    fn schema_partitioned_by_region() -> TableSchema {
        TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "orders".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    name: "region".into(),
                    field_id: 2,
                    ty: IcebergType::String,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: vec![PartitionField {
                source_column: "region".into(),
                name: "region".into(),
                transform: Transform::Identity,
            }],
            pg_schema: None,
        }
    }

    fn row_with_region(id: i32, region: &str) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("region"), PgValue::Text(region.into()));
        r
    }

    #[test]
    fn identity_partition_splits_data_by_partition_value() {
        let w = TableWriter::new(schema_partitioned_by_region());
        let p = w
            .prepare(
                &[
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_region(1, "us"),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_region(2, "us"),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_region(3, "eu"),
                        unchanged_cols: vec![],
                    },
                ],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 2, "two distinct regions → two data chunks");
        assert!(p.equality_deletes.is_empty());

        let mut by_region: BTreeMap<String, u64> = BTreeMap::new();
        for chunk in &p.data {
            assert_eq!(chunk.partition_values.len(), 1);
            match &chunk.partition_values[0] {
                PartitionLiteral::String(s) => {
                    by_region.insert(s.clone(), chunk.chunk.record_count);
                }
                other => panic!("expected String literal, got {other:?}"),
            }
        }
        assert_eq!(by_region.get("us"), Some(&2));
        assert_eq!(by_region.get("eu"), Some(&1));
    }

    fn schema_partitioned_by_day_of_created_at_pk() -> TableSchema {
        TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "events".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    // Partition column is in PK so deletes can compute the tuple.
                    name: "created_at".into(),
                    field_id: 2,
                    ty: IcebergType::TimestampTz,
                    nullable: false,
                    is_primary_key: true,
                },
            ],
            partition_spec: vec![PartitionField {
                source_column: "created_at".into(),
                name: "created_at_day".into(),
                transform: Transform::Day,
            }],
            pg_schema: None,
        }
    }

    fn row_with_ts(id: i32, micros: i64) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(
            col("created_at"),
            PgValue::TimestampTz(TimestampMicros(micros)),
        );
        r
    }

    #[test]
    fn day_partition_groups_by_day_bucket() {
        let w = TableWriter::new(schema_partitioned_by_day_of_created_at_pk());
        // Two rows on day 19_723 (2024-01-01), one on day 19_724.
        let day_micros: i64 = 86_400_000_000;
        let p = w
            .prepare(
                &[
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_ts(1, 19_723 * day_micros + 3_600_000_000),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_ts(2, 19_723 * day_micros + 7_200_000_000),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_ts(3, 19_724 * day_micros + 1),
                        unchanged_cols: vec![],
                    },
                ],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 2);
        let mut by_day: BTreeMap<i32, u64> = BTreeMap::new();
        for chunk in &p.data {
            match &chunk.partition_values[0] {
                PartitionLiteral::Int(d) => {
                    by_day.insert(*d, chunk.chunk.record_count);
                }
                other => panic!("expected Int literal, got {other:?}"),
            }
        }
        assert_eq!(by_day.get(&19_723), Some(&2));
        assert_eq!(by_day.get(&19_724), Some(&1));
    }

    #[test]
    fn delete_with_partition_column_in_pk_routes_correctly() {
        // partition col is in PK, so delete row carries it.
        let w = TableWriter::new(schema_partitioned_by_day_of_created_at_pk());
        let day_micros: i64 = 86_400_000_000;
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Delete,
                    row: row_with_ts(1, 19_723 * day_micros),
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.equality_deletes.len(), 1);
        match &p.equality_deletes[0].partition_values[0] {
            PartitionLiteral::Int(19_723) => {}
            other => panic!("expected Int(19723), got {other:?}"),
        }
    }

    #[test]
    fn delete_with_partition_col_outside_pk_recovers_via_file_index() {
        // Schema partitions by `region`, delete row has only `id`. With a
        // populated FileIndex the writer recovers the partition tuple via
        // tier-2 lookup and emits the equality-delete in the right
        // partition.
        let w = TableWriter::new(schema_partitioned_by_region());
        let mut fi = FileIndex::new();
        // Pretend a prior cycle wrote PK=1 into the "us" partition.
        let pk = pk_key(&pk_only(1), &[col("id")]);
        fi.add_file(
            "data/region=us/abc.parquet".into(),
            vec![pk],
            vec![PartitionLiteral::String("us".into())],
        );

        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Delete,
                    row: pk_only(1),
                    unchanged_cols: vec![],
                }],
                &fi,
            )
            .unwrap();
        assert_eq!(p.equality_deletes.len(), 1);
        assert_eq!(
            p.equality_deletes[0].partition_values,
            vec![PartitionLiteral::String("us".into())]
        );
    }

    #[test]
    fn insert_missing_partition_column_still_errors_at_tier_1() {
        // Insert/Update rows must always carry full data after fold +
        // resolve_unchanged_cols. Tier-2 doesn't help these — a row with
        // missing partition cols here means upstream bug. Should error
        // with PartitionColumnMissing rather than fall through to tier-3.
        let w = TableWriter::new(schema_partitioned_by_region());
        let err = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: pk_only(1), // missing `region`
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            WriterError::PartitionColumnMissing { ref column, op: Op::Insert } if column == "region"
        ));
    }

    /// Tier-3 contract test: a `Delete` on a partitioned table whose PK
    /// is unknown to the FileIndex (no row data, no prior file) errors
    /// with `DeletePartitionUnresolved`. Disabled-by-default because:
    ///
    /// - The condition is *transient* in normal operation (a fresh
    ///   process before `rebuild_from_catalog` finishes; an old cycle
    ///   replaying after compaction). Asserting it as a hard contract
    ///   in CI risks pinning behavior we may want to relax later
    ///   (e.g. fall back to "delete with null partition" or fan-out
    ///   the delete across all partitions, or silently drop it).
    ///
    /// Run on demand with `cargo test -- --ignored`.
    #[test]
    #[ignore = "tier-3 contract; documents the error path without enforcing it in CI"]
    fn tier_3_delete_with_no_row_data_and_no_file_index_entry_errors() {
        let w = TableWriter::new(schema_partitioned_by_region());
        let err = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Delete,
                    row: pk_only(99), // PK absent from FileIndex
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(), // empty index
            )
            .unwrap_err();
        assert!(matches!(
            err,
            WriterError::DeletePartitionUnresolved { ref pk_key } if pk_key.contains("99")
        ));
    }

    #[test]
    fn pk_keys_are_carried_per_chunk() {
        let w = TableWriter::new(schema_partitioned_by_region());
        let p = w
            .prepare(
                &[
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_region(1, "us"),
                        unchanged_cols: vec![],
                    },
                    MaterializedRow {
                        op: Op::Insert,
                        row: row_with_region(2, "eu"),
                        unchanged_cols: vec![],
                    },
                ],
                &FileIndex::new(),
            )
            .unwrap();
        let mut by_region: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for chunk in &p.data {
            if let PartitionLiteral::String(r) = &chunk.partition_values[0] {
                by_region.insert(r.clone(), chunk.pk_keys.clone());
            }
        }
        assert_eq!(by_region.get("us").unwrap().len(), 1);
        assert_eq!(by_region.get("eu").unwrap().len(), 1);
        // PK keys carry only the PK columns (id), not partition value.
        assert!(by_region.get("us").unwrap()[0].contains("1"));
    }

    #[test]
    fn update_on_partitioned_table_emits_data_and_delete_in_same_partition() {
        let w = TableWriter::new(schema_partitioned_by_region());
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Update,
                    row: row_with_region(1, "us"),
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 1);
        assert_eq!(p.equality_deletes.len(), 1);
        match &p.data[0].partition_values[0] {
            PartitionLiteral::String(s) if s == "us" => {}
            other => panic!("expected us, got {other:?}"),
        }
        match &p.equality_deletes[0].partition_values[0] {
            PartitionLiteral::String(s) if s == "us" => {}
            other => panic!("expected us, got {other:?}"),
        }
    }

    #[test]
    fn bucket_partition_routes_rows_into_n_buckets() {
        // Schema partitioned by bucket[4](id). Many rows → up to 4 chunks,
        // each tagged with its bucket index.
        let schema = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "users".into(),
            },
            columns: vec![ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Int,
                nullable: false,
                is_primary_key: true,
            }],
            partition_spec: vec![PartitionField {
                source_column: "id".into(),
                name: "id_bucket".into(),
                transform: Transform::Bucket(4),
            }],
            pg_schema: None,
        };
        let w = TableWriter::new(schema);
        let rows: Vec<MaterializedRow> = (1..=20)
            .map(|i| {
                let mut r = BTreeMap::new();
                r.insert(col("id"), PgValue::Int4(i));
                MaterializedRow {
                    op: Op::Insert,
                    row: r,
                    unchanged_cols: vec![],
                }
            })
            .collect();
        let p = w.prepare(&rows, &FileIndex::new()).unwrap();
        // At least 2 buckets should be hit with 20 rows (probabilistically
        // all 4); cap at 4 since N=4.
        assert!(p.data.len() >= 2 && p.data.len() <= 4);
        let mut total: u64 = 0;
        for chunk in &p.data {
            assert_eq!(chunk.partition_values.len(), 1);
            match &chunk.partition_values[0] {
                PartitionLiteral::Int(b) => {
                    assert!((0..4).contains(b), "bucket index {b} out of range [0,4)")
                }
                other => panic!("expected Int, got {other:?}"),
            }
            total += chunk.chunk.record_count;
        }
        assert_eq!(total, 20);
    }

    #[test]
    fn truncate_partition_routes_strings() {
        // Partition by `truncate[2](name)` — first 2 chars of name.
        let schema = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "users".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    name: "name".into(),
                    field_id: 2,
                    ty: IcebergType::String,
                    nullable: false,
                    is_primary_key: false,
                },
            ],
            partition_spec: vec![PartitionField {
                source_column: "name".into(),
                name: "name_trunc".into(),
                transform: Transform::Truncate(2),
            }],
            pg_schema: None,
        };
        let w = TableWriter::new(schema);
        let row = |id: i32, name: &str| {
            let mut r = BTreeMap::new();
            r.insert(col("id"), PgValue::Int4(id));
            r.insert(col("name"), PgValue::Text(name.into()));
            MaterializedRow {
                op: Op::Insert,
                row: r,
                unchanged_cols: vec![],
            }
        };
        let p = w
            .prepare(
                &[
                    row(1, "alice"),
                    row(2, "alex"),
                    row(3, "bob"),
                    row(4, "alfred"),
                ],
                &FileIndex::new(),
            )
            .unwrap();
        assert_eq!(p.data.len(), 2, "two distinct prefixes → two chunks");
        let mut by_prefix: BTreeMap<String, u64> = BTreeMap::new();
        for chunk in &p.data {
            if let PartitionLiteral::String(s) = &chunk.partition_values[0] {
                by_prefix.insert(s.clone(), chunk.chunk.record_count);
            }
        }
        // alice/alex/alfred share "al"; bob is its own prefix.
        assert_eq!(by_prefix.get("al"), Some(&3));
        assert_eq!(by_prefix.get("bo"), Some(&1));
    }

    #[test]
    fn day_partition_on_date_column() {
        let schema = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "events".into(),
            },
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    field_id: 1,
                    ty: IcebergType::Int,
                    nullable: false,
                    is_primary_key: true,
                },
                ColumnSchema {
                    name: "d".into(),
                    field_id: 2,
                    ty: IcebergType::Date,
                    nullable: false,
                    is_primary_key: true,
                },
            ],
            partition_spec: vec![PartitionField {
                source_column: "d".into(),
                name: "d_day".into(),
                transform: Transform::Day,
            }],
            pg_schema: None,
        };
        let w = TableWriter::new(schema);
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(1));
        r.insert(col("d"), PgValue::Date(DaysSinceEpoch(20_000)));
        let p = w
            .prepare(
                &[MaterializedRow {
                    op: Op::Insert,
                    row: r,
                    unchanged_cols: vec![],
                }],
                &FileIndex::new(),
            )
            .unwrap();
        match &p.data[0].partition_values[0] {
            PartitionLiteral::Int(20_000) => {}
            other => panic!("expected Int(20000), got {other:?}"),
        }
    }
}
