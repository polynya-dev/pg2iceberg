//! Deletion vectors (Iceberg v3): a data file's deleted rows, as their
//! positions in it. A vector is a Roaring bitmap of positions, framed as a
//! `deletion-vector-v1` blob, in a Puffin file holding the vectors of
//! every data file one commit deletes rows of ([`PuffinWriter`]).
//!
//! The blob: its length (big-endian, of what follows but the checksum),
//! the magic `D1 D3 39 64`, the positions as a 64-bit Roaring bitmap in
//! its portable form — a count of 32-bit bitmaps, then each with its key,
//! the positions' high 32 bits, in ascending order — and a CRC-32 of the
//! magic and the bitmap. A Puffin file: the magic `PFA1`, the blobs, and a
//! footer describing each — its offset and length, and as properties the
//! data file it names and how many positions it holds.

use crate::DeletionVector;
use bytes::Bytes;
use pg2iceberg_stream::{BlobStore, StreamError};
use roaring::{RoaringBitmap, RoaringTreemap};
use std::collections::BTreeMap;

/// The blob type of a deletion vector.
pub const DELETION_VECTOR_V1: &str = "deletion-vector-v1";
/// A blob's magic, after its length.
const BLOB_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
/// A Puffin file's magic, at its start and end.
const PUFFIN_MAGIC: &[u8; 4] = b"PFA1";
/// The field id of `_pos`, the column a deletion vector's positions are
/// of: what Java records as the blob's field.
const ROW_POSITION_FIELD_ID: i32 = 2147483645;

type Result<T> = std::result::Result<T, StreamError>;

fn corrupt(what: impl std::fmt::Display) -> StreamError {
    StreamError::Decode(format!("deletion vector: {what}"))
}

/// The positions of one data file's deleted rows.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Positions(RoaringTreemap);

impl Positions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `position`; whether it wasn't there yet.
    pub fn insert(&mut self, position: u64) -> bool {
        self.0.insert(position)
    }

    pub fn contains(&self, position: u64) -> bool {
        self.0.contains(position)
    }

    /// How many positions it holds.
    pub fn len(&self) -> u64 {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Its positions, ascending.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.iter()
    }

    /// Add every position of `other`.
    pub fn union_with(&mut self, other: &Positions) {
        self.0 |= &other.0;
    }

    /// As a `deletion-vector-v1` blob.
    pub fn to_blob(&self) -> Vec<u8> {
        let mut bitmap = self.0.clone();
        // Runs of positions as runs, as Java writes them.
        bitmap.optimize();
        let mut body = BLOB_MAGIC.to_vec();
        bitmap
            .serialize_into(&mut body)
            .expect("writing to a Vec doesn't fail");
        let mut blob = Vec::with_capacity(body.len() + 8);
        blob.extend_from_slice(&(body.len() as u32).to_be_bytes());
        blob.extend_from_slice(&body);
        blob.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
        blob
    }

    /// From a `deletion-vector-v1` blob, checked whole: its length,
    /// magic and checksum, and its bitmap's keys ascending, each bitmap
    /// valid, and nothing after it.
    pub fn from_blob(blob: &[u8]) -> Result<Self> {
        if blob.len() < 12 {
            return Err(corrupt(format!("{} bytes", blob.len())));
        }
        let length = u32::from_be_bytes(blob[..4].try_into().expect("4 bytes")) as usize;
        if length + 8 != blob.len() {
            return Err(corrupt(format!(
                "blob of {} bytes says {length}",
                blob.len()
            )));
        }
        let body = &blob[4..4 + length];
        let crc = u32::from_be_bytes(blob[4 + length..].try_into().expect("4 bytes"));
        if crc32fast::hash(body) != crc {
            return Err(corrupt("checksum mismatch"));
        }
        if body[..4] != BLOB_MAGIC {
            return Err(corrupt("bad magic"));
        }
        let mut reader = &body[4..];
        let count = take_u64(&mut reader)?;
        let mut bitmaps = Vec::new();
        let mut last: Option<u32> = None;
        for _ in 0..count {
            let key = take_u32(&mut reader)?;
            if last.is_some_and(|last| last >= key) {
                return Err(corrupt("bitmap keys out of order"));
            }
            last = Some(key);
            // Positions are below 2^63.
            if key >= 1 << 31 {
                return Err(corrupt(format!("position key {key}")));
            }
            let bitmap = RoaringBitmap::deserialize_from(&mut reader).map_err(corrupt)?;
            bitmaps.push((key, bitmap));
        }
        if !reader.is_empty() {
            return Err(corrupt(format!("{} bytes after the bitmap", reader.len())));
        }
        Ok(Self(RoaringTreemap::from_bitmaps(bitmaps)))
    }
}

impl FromIterator<u64> for Positions {
    fn from_iter<I: IntoIterator<Item = u64>>(positions: I) -> Self {
        Self(positions.into_iter().collect())
    }
}

fn take_u64(reader: &mut &[u8]) -> Result<u64> {
    let (head, rest) = reader
        .split_first_chunk::<8>()
        .ok_or_else(|| corrupt("truncated"))?;
    *reader = rest;
    Ok(u64::from_le_bytes(*head))
}

fn take_u32(reader: &mut &[u8]) -> Result<u32> {
    let (head, rest) = reader
        .split_first_chunk::<4>()
        .ok_or_else(|| corrupt("truncated"))?;
    *reader = rest;
    Ok(u32::from_le_bytes(*head))
}

/// A Puffin file of deletion vectors, built in memory: one per data file
/// a commit deletes rows of.
pub struct PuffinWriter {
    bytes: Vec<u8>,
    blobs: Vec<serde_json::Value>,
}

impl Default for PuffinWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl PuffinWriter {
    pub fn new() -> Self {
        Self {
            bytes: PUFFIN_MAGIC.to_vec(),
            blobs: Vec::new(),
        }
    }

    /// Add the vector of `referenced_data_file`'s deleted rows: its
    /// blob's offset in the file, and length.
    pub fn add(&mut self, referenced_data_file: &str, positions: &Positions) -> (u64, u64) {
        let blob = positions.to_blob();
        let offset = self.bytes.len() as u64;
        let length = blob.len() as u64;
        self.bytes.extend_from_slice(&blob);
        self.blobs.push(serde_json::json!({
            "type": DELETION_VECTOR_V1,
            "fields": [ROW_POSITION_FIELD_ID],
            "snapshot-id": -1,
            "sequence-number": -1,
            "offset": offset,
            "length": length,
            "properties": {
                "referenced-data-file": referenced_data_file,
                "cardinality": positions.len().to_string(),
            },
        }));
        (offset, length)
    }

    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    /// The file: its blobs, and the footer describing them.
    pub fn finish(self) -> Bytes {
        let payload = serde_json::to_vec(&serde_json::json!({
            "blobs": self.blobs,
            "properties": { "created-by": "pg2iceberg" },
        }))
        .expect("JSON serializes");
        let mut bytes = self.bytes;
        bytes.extend_from_slice(PUFFIN_MAGIC);
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&(payload.len() as i32).to_le_bytes());
        // Flags: the payload isn't compressed.
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(PUFFIN_MAGIC);
        Bytes::from(bytes)
    }
}

/// `dv`'s positions: its blob, read from its Puffin file by its offset and
/// length. Fails unless it holds as many as the vector's entry says.
pub async fn read_deletion_vector(
    blob_store: &dyn BlobStore,
    dv: &DeletionVector,
) -> Result<Positions> {
    let blob = blob_store
        .get_range(&dv.path, dv.offset..dv.offset + dv.length)
        .await?;
    decode(dv, &blob)
}

/// Each of `vectors`' positions, in their order: one read per Puffin
/// file holding several of them — read whole — and a ranged one for each
/// other.
pub async fn read_deletion_vectors(
    blob_store: &dyn BlobStore,
    vectors: &[&DeletionVector],
) -> Result<Vec<Positions>> {
    let mut by_file: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, dv) in vectors.iter().enumerate() {
        by_file.entry(dv.path.as_str()).or_default().push(i);
    }
    let mut out: Vec<Option<Positions>> = vec![None; vectors.len()];
    for (path, held) in by_file {
        if let [i] = held[..] {
            out[i] = Some(read_deletion_vector(blob_store, vectors[i]).await?);
            continue;
        }
        let file = blob_store.get(path).await?;
        for i in held {
            let dv = vectors[i];
            let blob = usize::try_from(dv.offset)
                .ok()
                .zip(usize::try_from(dv.offset + dv.length).ok())
                .and_then(|(start, end)| file.get(start..end))
                .ok_or_else(|| corrupt(format!("{path} has no blob at {}", dv.offset)))?;
            out[i] = Some(decode(dv, blob)?);
        }
    }
    Ok(out.into_iter().map(|p| p.expect("read above")).collect())
}

fn decode(dv: &DeletionVector, blob: &[u8]) -> Result<Positions> {
    let positions = Positions::from_blob(blob)
        .map_err(|e| StreamError::Decode(format!("{}@{}: {e}", dv.path, dv.offset)))?;
    if positions.len() != dv.cardinality {
        return Err(corrupt(format!(
            "{}@{} holds {} positions, its entry says {}",
            dv.path,
            dv.offset,
            positions.len(),
            dv.cardinality
        )));
    }
    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn positions(p: impl IntoIterator<Item = u64>) -> Positions {
        p.into_iter().collect()
    }

    #[test]
    fn a_blob_round_trips() {
        for p in [
            positions([]),
            positions([0]),
            positions([1, 3, 5, 1 << 20]),
            // A run, and positions past one 32-bit bitmap.
            positions((100..9000).chain([(1 << 32) + 7])),
        ] {
            let blob = p.to_blob();
            assert_eq!(Positions::from_blob(&blob).unwrap(), p);
        }
    }

    /// The blob of positions {1, 3, 5} as the spec frames it — length,
    /// magic, one 32-bit bitmap (key 0) holding an array container, CRC.
    #[test]
    fn a_blob_is_framed_as_the_spec_says() {
        let blob = positions([1, 3, 5]).to_blob();
        let length = u32::from_be_bytes(blob[..4].try_into().unwrap()) as usize;
        assert_eq!(length, blob.len() - 8);
        assert_eq!(blob[4..8], BLOB_MAGIC);
        // One bitmap, key 0.
        assert_eq!(blob[8..16], 1u64.to_le_bytes());
        assert_eq!(blob[16..20], 0u32.to_le_bytes());
        // No run containers: cookie 12346, one container.
        assert_eq!(blob[20..24], 12346u32.to_le_bytes());
        let crc = u32::from_be_bytes(blob[blob.len() - 4..].try_into().unwrap());
        assert_eq!(crc, crc32fast::hash(&blob[4..blob.len() - 4]));
    }

    #[test]
    fn a_corrupt_blob_is_refused() {
        let blob = positions([1, 3, 5]).to_blob();
        let mut flipped = blob.clone();
        flipped[20] ^= 1;
        assert!(Positions::from_blob(&flipped).is_err());
        assert!(Positions::from_blob(&blob[..blob.len() - 1]).is_err());
        let mut longer = blob.clone();
        longer.push(0);
        assert!(Positions::from_blob(&longer).is_err());
    }

    #[test]
    fn a_puffin_file_holds_each_vector_where_its_entry_says() {
        let a = positions([1, 2]);
        let b = positions(0..5000);
        let mut writer = PuffinWriter::new();
        let (a_at, a_len) = writer.add("a.parquet", &a);
        let (b_at, b_len) = writer.add("b.parquet", &b);
        let file = writer.finish();
        assert_eq!(&file[..4], PUFFIN_MAGIC);
        assert_eq!(&file[file.len() - 4..], PUFFIN_MAGIC);
        assert_eq!(a_at, 4);
        let blob = |at: u64, len: u64| &file[at as usize..(at + len) as usize];
        assert_eq!(Positions::from_blob(blob(a_at, a_len)).unwrap(), a);
        assert_eq!(Positions::from_blob(blob(b_at, b_len)).unwrap(), b);
        // The footer: its payload, its size, no flags.
        let size_at = file.len() - 12;
        let size = i32::from_le_bytes(file[size_at..size_at + 4].try_into().unwrap()) as usize;
        let payload: serde_json::Value =
            serde_json::from_slice(&file[size_at - size..size_at]).unwrap();
        assert_eq!(&file[size_at - size - 4..size_at - size], PUFFIN_MAGIC);
        assert_eq!(payload["blobs"][1]["offset"], b_at);
        assert_eq!(
            payload["blobs"][1]["properties"]["referenced-data-file"],
            "b.parquet"
        );
        assert_eq!(payload["blobs"][1]["properties"]["cardinality"], "5000");
    }
}
