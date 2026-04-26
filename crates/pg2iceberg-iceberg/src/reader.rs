//! Decode an Iceberg-shaped Parquet data file into `Vec<Row>`.
//!
//! Inverse of [`crate::writer::TableWriter::prepare`]'s data-file output.
//! Used by Phase 7.5 TOAST resolution and (eventually) the Phase 12 verify
//! subcommand.

use crate::writer::WriterError;
use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pg2iceberg_core::value::{DaysSinceEpoch, TimestampMicros};
use pg2iceberg_core::{ColumnName, ColumnSchema, IcebergType, PgValue, Row};
use std::collections::BTreeMap;

pub type Result<T> = std::result::Result<T, WriterError>;

/// Decode a Parquet file produced by [`crate::TableWriter`] back into rows.
/// `cols` describes the schema the file was written with — usually that's
/// the table's full schema for data files, or the PK-only subset for
/// equality-delete files.
pub fn read_data_file(bytes: &[u8], cols: &[ColumnSchema]) -> Result<Vec<Row>> {
    let owned = Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(owned)
        .map_err(|e| WriterError::Encode(format!("parquet reader: {e}")))?;
    let reader = builder
        .build()
        .map_err(|e| WriterError::Encode(format!("parquet reader build: {e}")))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| WriterError::Encode(format!("read batch: {e}")))?;
        for i in 0..batch.num_rows() {
            let mut row: Row = BTreeMap::new();
            for col in cols {
                let arr =
                    batch
                        .column_by_name(&col.name)
                        .ok_or_else(|| WriterError::MissingColumn {
                            col: col.name.clone(),
                        })?;
                let key = ColumnName(col.name.clone());
                if arr.is_null(i) {
                    row.insert(key, PgValue::Null);
                } else {
                    row.insert(key, decode_value(col.ty, arr.as_ref(), i)?);
                }
            }
            out.push(row);
        }
    }
    Ok(out)
}

fn decode_value(ty: IcebergType, arr: &dyn Array, i: usize) -> Result<PgValue> {
    macro_rules! cast {
        ($t:ty) => {
            arr.as_any().downcast_ref::<$t>().ok_or_else(|| {
                WriterError::Encode(format!(
                    "downcast to {} failed for {:?}",
                    stringify!($t),
                    ty
                ))
            })
        };
    }
    Ok(match ty {
        IcebergType::Boolean => PgValue::Bool(cast!(BooleanArray)?.value(i)),
        IcebergType::Int => PgValue::Int4(cast!(Int32Array)?.value(i)),
        IcebergType::Long => PgValue::Int8(cast!(Int64Array)?.value(i)),
        IcebergType::Float => PgValue::Float4(cast!(Float32Array)?.value(i)),
        IcebergType::Double => PgValue::Float8(cast!(Float64Array)?.value(i)),
        IcebergType::String => PgValue::Text(cast!(StringArray)?.value(i).to_string()),
        IcebergType::Date => PgValue::Date(DaysSinceEpoch(cast!(Date32Array)?.value(i))),
        IcebergType::Timestamp => {
            PgValue::Timestamp(TimestampMicros(cast!(TimestampMicrosecondArray)?.value(i)))
        }
        IcebergType::TimestampTz => {
            PgValue::TimestampTz(TimestampMicros(cast!(TimestampMicrosecondArray)?.value(i)))
        }
        IcebergType::Time
        | IcebergType::Decimal { .. }
        | IcebergType::Binary
        | IcebergType::Uuid => {
            return Err(WriterError::TypeNotYetSupported(ty));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fold::MaterializedRow;
    use crate::writer::TableWriter;
    use pg2iceberg_core::{Namespace, Op, TableIdent, TableSchema};

    fn col(name: &str) -> ColumnName {
        ColumnName(name.into())
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
        }
    }

    fn row(id: i32, qty: i32) -> Row {
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(id));
        r.insert(col("qty"), PgValue::Int4(qty));
        r
    }

    #[test]
    fn round_trip_through_writer_and_reader() {
        let schema = schema_id_qty();
        let w = TableWriter::new(schema.clone());
        let rows = vec![
            MaterializedRow {
                op: Op::Insert,
                row: row(1, 10),
                unchanged_cols: vec![],
            },
            MaterializedRow {
                op: Op::Insert,
                row: row(2, 20),
                unchanged_cols: vec![],
            },
        ];
        let prepared = w.prepare(&rows).unwrap();
        let bytes = prepared.data.unwrap().bytes;
        let decoded = read_data_file(&bytes, &schema.columns).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], row(1, 10));
        assert_eq!(decoded[1], row(2, 20));
    }

    #[test]
    fn round_trip_preserves_nulls() {
        let schema = TableSchema {
            ident: TableIdent {
                namespace: Namespace(vec!["public".into()]),
                name: "nullable".into(),
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
                    name: "note".into(),
                    field_id: 2,
                    ty: IcebergType::String,
                    nullable: true,
                    is_primary_key: false,
                },
            ],
            partition_spec: Vec::new(),
        };
        let w = TableWriter::new(schema.clone());
        let mut r = BTreeMap::new();
        r.insert(col("id"), PgValue::Int4(7));
        r.insert(col("note"), PgValue::Null);
        let prepared = w
            .prepare(&[MaterializedRow {
                op: Op::Insert,
                row: r,
                unchanged_cols: vec![],
            }])
            .unwrap();
        let decoded = read_data_file(&prepared.data.unwrap().bytes, &schema.columns).unwrap();
        assert_eq!(decoded[0].get(&col("note")), Some(&PgValue::Null));
    }
}
