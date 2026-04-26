//! `TableWriter`: produces Iceberg merge-on-read output (data files + equality
//! deletes) from a folded `Vec<MaterializedRow>`.
//!
//! Mirrors `iceberg/tablewriter.go:130-380` (`Prepare`). Phase 7 first-pass
//! deferrals: partitioning, FileIndex/TOAST resolution, schema evolution,
//! decimal/uuid/binary value encoding. Each is a Phase 7.5 task.
//!
//! ## Output shape
//!
//! - Final `Insert` for a PK → 1 row in the data file.
//! - Final `Update` for a PK → 1 row in the data file *and* 1 row in the
//!   equality-delete file (drops any prior row with this PK).
//! - Final `Delete` for a PK → 1 row in the equality-delete file *only*.
//!
//! Pure inserts produce no equality delete; that mirrors `tablewriter.go:301`'s
//! `if rs.Op == "U" || rs.Op == "D"` filter and keeps Phase 7.5 (re-insert
//! after delete) honest — the materializer will emit an equality delete
//! through a separate path once FileIndex tells it the PK existed before.

use crate::fold::MaterializedRow;
use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pg2iceberg_core::{ColumnName, ColumnSchema, IcebergType, Op, PgValue, Row, TableSchema};
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
    /// Phase 7 doesn't yet wire decimal/uuid/binary builders.
    #[error("type {0:?} is not yet supported by the writer (Phase 7.5)")]
    TypeNotYetSupported(IcebergType),
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

#[derive(Debug)]
pub struct PreparedFiles {
    /// Full-schema rows for `Insert` and `Update` final ops.
    pub data: Option<DataChunk>,
    /// PK-only rows for `Update` and `Delete` final ops.
    pub equality_deletes: Option<DataChunk>,
    /// PK field-id list, populated whenever `equality_deletes` is `Some`. The
    /// materializer hands this to the catalog as the equality-delete file's
    /// `equality_field_ids` manifest column.
    pub pk_field_ids: Vec<i32>,
}

pub struct TableWriter {
    schema: TableSchema,
    pk_columns: Vec<ColumnSchema>,
    /// Subset of `schema.columns` that are PK; precomputed for the
    /// equality-delete file.
    pk_arrow_schema: Arc<Schema>,
    full_arrow_schema: Arc<Schema>,
}

impl TableWriter {
    pub fn new(schema: TableSchema) -> Self {
        let pk_columns: Vec<ColumnSchema> = schema.primary_key_columns().cloned().collect();
        let full_arrow_schema = Arc::new(arrow_schema_for(&schema.columns));
        let pk_arrow_schema = Arc::new(arrow_schema_for(&pk_columns));
        Self {
            schema,
            pk_columns,
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

    /// Take folded rows, partition by op into data + equality-delete builders,
    /// and serialize each to a single Parquet chunk. Phase 7 doesn't split
    /// across multiple chunks (no rolling threshold yet); the caller flushes
    /// per materializer cycle.
    pub fn prepare(&self, rows: &[MaterializedRow]) -> Result<PreparedFiles> {
        if rows.is_empty() {
            return Ok(PreparedFiles {
                data: None,
                equality_deletes: None,
                pk_field_ids: self.pk_field_ids(),
            });
        }

        let mut data_rows: Vec<&Row> = Vec::new();
        let mut delete_rows: Vec<&Row> = Vec::new();

        for r in rows {
            match r.op {
                Op::Insert => data_rows.push(&r.row),
                Op::Update => {
                    data_rows.push(&r.row);
                    delete_rows.push(&r.row);
                }
                Op::Delete => delete_rows.push(&r.row),
                Op::Relation | Op::Truncate => {
                    // Materializer fold should never produce these; treat as
                    // programmer error if it ever happens.
                    return Err(WriterError::Encode(format!(
                        "unexpected op in folded rows: {:?}",
                        r.op
                    )));
                }
            }
        }

        let data = if data_rows.is_empty() {
            None
        } else {
            Some(write_chunk(
                &self.full_arrow_schema,
                &self.schema.columns,
                &data_rows,
            )?)
        };
        let equality_deletes = if delete_rows.is_empty() {
            None
        } else {
            Some(write_chunk(
                &self.pk_arrow_schema,
                &self.pk_columns,
                &delete_rows,
            )?)
        };

        Ok(PreparedFiles {
            data,
            equality_deletes,
            pk_field_ids: self.pk_field_ids(),
        })
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
        // Phase 7.5 wires these.
        IcebergType::Decimal { .. } | IcebergType::Binary | IcebergType::Uuid => {
            DataType::Null // placeholder; we error before constructing arrays
        }
    }
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
        IcebergType::Long => collect_with!(Int64Builder::with_capacity(n), |v: &PgValue| {
            match v {
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
        IcebergType::Double => collect_with!(Float64Builder::with_capacity(n), |v: &PgValue| {
            match v {
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
        IcebergType::Time
        | IcebergType::Decimal { .. }
        | IcebergType::Binary
        | IcebergType::Uuid => {
            return Err(WriterError::TypeNotYetSupported(col.ty));
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
    use pg2iceberg_core::{Namespace, TableIdent, TableSchema};
    use std::collections::BTreeMap;

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
        let p = w.prepare(&[]).unwrap();
        assert!(p.data.is_none());
        assert!(p.equality_deletes.is_none());
    }

    #[test]
    fn single_insert_produces_data_file_only() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(&[MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            }])
            .unwrap();
        assert!(p.data.is_some());
        assert!(p.equality_deletes.is_none());

        let batch = read_parquet(&p.data.as_ref().unwrap().bytes);
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
            .prepare(&[MaterializedRow {
                op: Op::Delete,
                row: pk_only(7),
                unchanged_cols: vec![],
            }])
            .unwrap();
        assert!(p.data.is_none());
        let chunk = p.equality_deletes.unwrap();
        assert_eq!(chunk.record_count, 1);
        assert_eq!(p.pk_field_ids, vec![1]);

        let batch = read_parquet(&chunk.bytes);
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
            .prepare(&[MaterializedRow {
                op: Op::Update,
                row: row(3, 99),
                unchanged_cols: vec![],
            }])
            .unwrap();
        let data = p.data.unwrap();
        let dels = p.equality_deletes.unwrap();
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
            .prepare(&[
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
            ])
            .unwrap();
        assert_eq!(p.data.unwrap().record_count, 2); // I + U
        assert_eq!(p.equality_deletes.unwrap().record_count, 2); // U + D
    }

    #[test]
    fn field_ids_in_arrow_schema_metadata() {
        let w = TableWriter::new(schema_id_qty());
        let p = w
            .prepare(&[MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            }])
            .unwrap();
        let batch = read_parquet(&p.data.as_ref().unwrap().bytes);
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
        };
        let w = TableWriter::new(schema);
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(42));
        r.insert(col("name"), PgValue::Text("alice".into()));
        r.insert(col("active"), PgValue::Bool(true));
        r.insert(col("score"), PgValue::Int8(1_000_000));
        let p = w
            .prepare(&[MaterializedRow {
                op: Op::Insert,
                row: r,
                unchanged_cols: vec![],
            }])
            .unwrap();
        let batch = read_parquet(&p.data.unwrap().bytes);
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
            .prepare(&[MaterializedRow {
                op: Op::Insert,
                row: r,
                unchanged_cols: vec![],
            }])
            .unwrap_err();
        assert!(matches!(err, WriterError::MissingColumn { ref col } if col == "qty"));
    }

    #[test]
    fn unsupported_type_returns_typed_error() {
        let schema = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "needs_uuid".into(),
            },
            columns: vec![ColumnSchema {
                name: "id".into(),
                field_id: 1,
                ty: IcebergType::Uuid,
                nullable: false,
                is_primary_key: true,
            }],
            partition_spec: Vec::new(),
        };
        let w = TableWriter::new(schema);
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Uuid([0u8; 16]));
        let err = w
            .prepare(&[MaterializedRow {
                op: Op::Insert,
                row: r,
                unchanged_cols: vec![],
            }])
            .unwrap_err();
        assert!(matches!(
            err,
            WriterError::TypeNotYetSupported(IcebergType::Uuid)
        ));
    }
}
