//! An independent reader of deletion vectors, for the oracle: Puffin files
//! and the `deletion-vector-v1` blobs in them, decoded by hand from the
//! Puffin spec and the Roaring format spec. It shares no code with any
//! writer under test — pg2iceberg's, iceberg-rust's, or the `roaring`
//! crate's — so a mistake in one of them shows as a disagreement here.

use serde_json::Value;
use std::collections::BTreeMap;

/// Where a deletion vector is, as its delete manifest entry locates it: a
/// blob in a Puffin file.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeletionVectorRef {
    /// The data file whose rows it deletes (`referenced_data_file`).
    pub referenced_data_file: String,
    /// Where its blob starts in the Puffin file (`content_offset`).
    pub offset: u64,
    /// Its blob's length (`content_size_in_bytes`).
    pub length: u64,
}

/// A Puffin blob type: a deletion vector.
pub const DELETION_VECTOR_V1: &str = "deletion-vector-v1";

const PUFFIN_MAGIC: [u8; 4] = *b"PFA1";
const DV_MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
/// A 32-bit Roaring bitmap's cookie when it has run containers; its high
/// 16 bits hold the number of containers less one.
const SERIAL_COOKIE: u32 = 12347;
/// A 32-bit Roaring bitmap's cookie when it has none.
const SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;
/// From this many containers, a bitmap with run containers has an offset
/// header too.
const NO_OFFSET_THRESHOLD: usize = 4;
/// Most values an array container holds; more take a bitmap container.
const ARRAY_MAX: u32 = 4096;

/// One blob of a Puffin file, as its footer describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PuffinBlob {
    pub blob_type: String,
    pub fields: Vec<i32>,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub offset: u64,
    pub length: u64,
    pub compression_codec: Option<String>,
    pub properties: BTreeMap<String, String>,
}

/// The blobs a Puffin file's footer lists: `Magic Blob… Footer`, where the
/// footer is `Magic FooterPayload FooterPayloadSize Flags Magic`.
pub fn puffin_blobs(file: &[u8]) -> Result<Vec<PuffinBlob>, String> {
    let n = file.len();
    if n < 4 + 4 + 12 || file[..4] != PUFFIN_MAGIC || file[n - 4..] != PUFFIN_MAGIC {
        return Err(format!("not a Puffin file ({n} bytes)"));
    }
    let flags = &file[n - 8..n - 4];
    if flags[0] & 1 != 0 {
        return Err("its footer payload is compressed".into());
    }
    let size = i32::from_le_bytes(file[n - 12..n - 8].try_into().expect("4 bytes"));
    let size = usize::try_from(size).map_err(|_| format!("footer payload size {size}"))?;
    let payload_start = (n - 12)
        .checked_sub(size)
        .filter(|start| *start >= 8)
        .ok_or_else(|| format!("footer payload size {size} overruns the file"))?;
    let footer_start = payload_start - 4;
    if file[footer_start..payload_start] != PUFFIN_MAGIC {
        return Err("no magic opening the footer".into());
    }
    let payload: Value = serde_json::from_slice(&file[payload_start..n - 12])
        .map_err(|e| format!("footer payload: {e}"))?;
    let blobs = payload
        .get("blobs")
        .and_then(Value::as_array)
        .ok_or("footer payload has no `blobs` list")?;
    blobs
        .iter()
        .map(|b| {
            let blob = parse_blob(b)?;
            let end = blob.offset.checked_add(blob.length);
            if blob.offset < 4 || end.is_none_or(|end| end > footer_start as u64) {
                return Err(format!(
                    "blob at {} of {} bytes lies outside the blobs, which end at {footer_start}",
                    blob.offset, blob.length
                ));
            }
            Ok(blob)
        })
        .collect()
}

fn parse_blob(b: &Value) -> Result<PuffinBlob, String> {
    let field = |name: &str| {
        b.get(name)
            .ok_or_else(|| format!("blob {b} has no `{name}`"))
    };
    let long = |name: &str| {
        field(name)?
            .as_i64()
            .ok_or_else(|| format!("blob {b}: `{name}` isn't a long"))
    };
    let unsigned = |name: &str| {
        u64::try_from(long(name)?).map_err(|_| format!("blob {b}: `{name}` is negative"))
    };
    let fields = field("fields")?
        .as_array()
        .ok_or_else(|| format!("blob {b}: `fields` isn't a list"))?
        .iter()
        .map(|f| {
            f.as_i64()
                .and_then(|f| i32::try_from(f).ok())
                .ok_or_else(|| format!("blob {b}: field {f} isn't an int"))
        })
        .collect::<Result<_, _>>()?;
    let properties = match b.get("properties") {
        None => BTreeMap::new(),
        Some(p) => p
            .as_object()
            .ok_or_else(|| format!("blob {b}: `properties` isn't an object"))?
            .iter()
            .map(|(k, v)| {
                v.as_str()
                    .map(|v| (k.clone(), v.to_string()))
                    .ok_or_else(|| format!("blob {b}: property {k} isn't a string"))
            })
            .collect::<Result<_, _>>()?,
    };
    Ok(PuffinBlob {
        blob_type: field("type")?
            .as_str()
            .ok_or_else(|| format!("blob {b}: `type` isn't a string"))?
            .to_string(),
        fields,
        snapshot_id: long("snapshot-id")?,
        sequence_number: long("sequence-number")?,
        offset: unsigned("offset")?,
        length: unsigned("length")?,
        compression_codec: b
            .get("compression-codec")
            .map(|c| {
                c.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("blob {b}: `compression-codec` isn't a string"))
            })
            .transpose()?,
        properties,
    })
}

/// The positions a `deletion-vector-v1` blob sets, ascending. The blob is
/// its combined length of magic and vector (4 bytes, big-endian), the magic
/// `D1 D3 39 64`, the vector — 64-bit positions in the Roaring "portable"
/// format — and a CRC-32 of magic and vector (4 bytes, big-endian).
pub fn decode_blob(blob: &[u8]) -> Result<Vec<u64>, String> {
    if blob.len() < 12 {
        return Err(format!("a {}-byte deletion vector", blob.len()));
    }
    let len = u32::from_be_bytes(blob[..4].try_into().expect("4 bytes")) as usize;
    if len + 8 != blob.len() {
        return Err(format!(
            "deletion vector's length is {len}, but its blob is {} bytes",
            blob.len()
        ));
    }
    let body = &blob[4..4 + len];
    if body[..4] != DV_MAGIC {
        return Err(format!("deletion vector magic {:02x?}", &body[..4]));
    }
    let stored = u32::from_be_bytes(blob[4 + len..].try_into().expect("4 bytes"));
    let computed = crc32(body);
    if stored != computed {
        return Err(format!(
            "deletion vector CRC {stored:#010x}, but its bytes give {computed:#010x}"
        ));
    }
    let mut r = Reader {
        bytes: &body[4..],
        at: 0,
    };
    let count = r.u64()?;
    let mut positions = Vec::new();
    let mut last_key: Option<u32> = None;
    for _ in 0..count {
        let key = r.u32()?;
        if last_key.is_some_and(|last| key <= last) {
            return Err(format!("bitmap key {key} after {last_key:?}"));
        }
        if key >> 31 != 0 {
            return Err(format!(
                "bitmap key {key:#x}: positions' most significant bit must be 0"
            ));
        }
        last_key = Some(key);
        for low in roaring_32(&mut r)? {
            positions.push(u64::from(key) << 32 | u64::from(low));
        }
    }
    if r.at != r.bytes.len() {
        return Err(format!(
            "{} bytes after the deletion vector's bitmaps",
            r.bytes.len() - r.at
        ));
    }
    Ok(positions)
}

/// A 32-bit Roaring bitmap in its portable format, ascending.
fn roaring_32(r: &mut Reader) -> Result<Vec<u32>, String> {
    let cookie = r.u32()?;
    let (size, runs) = if cookie & 0xFFFF == SERIAL_COOKIE {
        let size = (cookie >> 16) as usize + 1;
        (size, Some(r.take(size.div_ceil(8))?.to_vec()))
    } else if cookie == SERIAL_COOKIE_NO_RUNCONTAINER {
        (r.u32()? as usize, None)
    } else {
        return Err(format!("Roaring cookie {cookie:#x}"));
    };
    if size > 1 << 16 {
        return Err(format!("{size} Roaring containers"));
    }
    let mut header = Vec::with_capacity(size);
    for _ in 0..size {
        header.push((r.u16()?, u32::from(r.u16()?) + 1));
    }
    if header.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(format!("Roaring container keys out of order: {header:?}"));
    }
    if runs.is_none() || size >= NO_OFFSET_THRESHOLD {
        r.take(4 * size)?;
    }
    let mut values = Vec::new();
    for (i, (key, cardinality)) in header.into_iter().enumerate() {
        let is_run = runs.as_ref().is_some_and(|b| b[i / 8] >> (i % 8) & 1 == 1);
        let mut lows: Vec<u32> = Vec::new();
        if is_run {
            for _ in 0..r.u16()? {
                let start = u32::from(r.u16()?);
                let end = start + u32::from(r.u16()?);
                if end > 0xFFFF {
                    return Err(format!("Roaring run {start}..={end} past 16 bits"));
                }
                lows.extend(start..=end);
            }
        } else if cardinality <= ARRAY_MAX {
            for _ in 0..cardinality {
                lows.push(u32::from(r.u16()?));
            }
        } else {
            for word in 0..1024u32 {
                let bits = r.u64()?;
                lows.extend(
                    (0..64)
                        .filter(|b| bits >> b & 1 == 1)
                        .map(|b| word * 64 + b),
                );
            }
        }
        if lows.len() as u32 != cardinality {
            return Err(format!(
                "Roaring container {key} holds {} values, its header says {cardinality}",
                lows.len()
            ));
        }
        if lows.windows(2).any(|w| w[0] >= w[1]) {
            return Err(format!("Roaring container {key} out of order"));
        }
        values.extend(lows.into_iter().map(|low| u32::from(key) << 16 | low));
    }
    Ok(values)
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at + n;
        let out = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| format!("deletion vector ends at {}, before {end}", self.bytes.len()))?;
        self.at = end;
        Ok(out)
    }
    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("2")))
    }
    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8")))
    }
}

/// CRC-32 (IEEE 802.3, as `java.util.zip.CRC32`).
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Test-only writers of what this module reads, from the same specs.
#[cfg(test)]
pub(crate) mod write {
    use super::*;

    /// How [`blob`] stores a container.
    #[derive(Clone, Copy)]
    pub enum Container {
        Array,
        Bitmap,
        Run,
    }

    /// A `deletion-vector-v1` blob of `positions` (ascending), each
    /// container stored as `kind` says — or as the format requires.
    pub fn blob(positions: &[u64], kind: impl Fn(usize) -> Container) -> Vec<u8> {
        let mut by_key: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for p in positions {
            by_key.entry((p >> 32) as u32).or_default().push(*p as u32);
        }
        let mut vector = (by_key.len() as u64).to_le_bytes().to_vec();
        let mut container = 0;
        for (key, lows) in by_key {
            vector.extend(key.to_le_bytes());
            let mut by_high: BTreeMap<u16, Vec<u16>> = BTreeMap::new();
            for low in lows {
                by_high
                    .entry((low >> 16) as u16)
                    .or_default()
                    .push(low as u16);
            }
            // The format leaves no choice between an array and a bitmap:
            // a reader tells them apart by cardinality alone.
            let kinds: Vec<Container> = by_high
                .values()
                .enumerate()
                .map(|(i, values)| match kind(container + i) {
                    Container::Run => Container::Run,
                    _ if values.len() as u32 > ARRAY_MAX => Container::Bitmap,
                    _ => Container::Array,
                })
                .collect();
            container += by_high.len();
            let runs = kinds.iter().any(|k| matches!(k, Container::Run));
            let size = by_high.len();
            if runs {
                vector.extend((SERIAL_COOKIE | ((size as u32 - 1) << 16)).to_le_bytes());
                let mut bits = vec![0u8; size.div_ceil(8)];
                for (i, k) in kinds.iter().enumerate() {
                    if matches!(k, Container::Run) {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                vector.extend(bits);
            } else {
                vector.extend(SERIAL_COOKIE_NO_RUNCONTAINER.to_le_bytes());
                vector.extend((size as u32).to_le_bytes());
            }
            for (high, values) in &by_high {
                vector.extend(high.to_le_bytes());
                vector.extend((values.len() as u16 - 1).to_le_bytes());
            }
            let bodies: Vec<Vec<u8>> = by_high
                .values()
                .zip(&kinds)
                .map(|(values, k)| match k {
                    Container::Array => values.iter().flat_map(|v| v.to_le_bytes()).collect(),
                    Container::Bitmap => {
                        let mut words = [0u64; 1024];
                        for v in values {
                            words[*v as usize / 64] |= 1 << (v % 64);
                        }
                        words.iter().flat_map(|w| w.to_le_bytes()).collect()
                    }
                    Container::Run => {
                        let mut runs: Vec<(u16, u16)> = Vec::new();
                        for v in values {
                            match runs.last_mut() {
                                Some((start, len))
                                    if *start as u32 + *len as u32 + 1 == *v as u32 =>
                                {
                                    *len += 1
                                }
                                _ => runs.push((*v, 0)),
                            }
                        }
                        let mut out = (runs.len() as u16).to_le_bytes().to_vec();
                        for (start, len) in runs {
                            out.extend(start.to_le_bytes());
                            out.extend(len.to_le_bytes());
                        }
                        out
                    }
                })
                .collect();
            if !runs || size >= NO_OFFSET_THRESHOLD {
                // Offsets from the start of this bitmap's containers; a
                // reader may skip them, as this module's does.
                let mut offset = 0u32;
                for body in &bodies {
                    vector.extend(offset.to_le_bytes());
                    offset += body.len() as u32;
                }
            }
            for body in bodies {
                vector.extend(body);
            }
        }
        let mut body = DV_MAGIC.to_vec();
        body.extend(vector);
        let mut out = (body.len() as u32).to_be_bytes().to_vec();
        out.extend(&body);
        out.extend(crc32(&body).to_be_bytes());
        out
    }

    /// A Puffin file of deletion vectors — `(referenced data file, blob)`
    /// — and where each blob is in it.
    pub fn puffin(dvs: &[(&str, Vec<u8>, u64)]) -> (Vec<u8>, Vec<(u64, u64)>) {
        let mut file = PUFFIN_MAGIC.to_vec();
        let mut at = Vec::new();
        let mut blobs = Vec::new();
        for (referenced, blob, cardinality) in dvs {
            let offset = file.len() as u64;
            file.extend(blob);
            at.push((offset, blob.len() as u64));
            blobs.push(serde_json::json!({
                "type": DELETION_VECTOR_V1,
                "fields": [2147483545i64],
                "snapshot-id": -1,
                "sequence-number": -1,
                "offset": offset,
                "length": blob.len(),
                "properties": {
                    "referenced-data-file": referenced,
                    "cardinality": cardinality.to_string(),
                },
            }));
        }
        let payload = serde_json::to_vec(&serde_json::json!({ "blobs": blobs })).unwrap();
        file.extend(PUFFIN_MAGIC);
        file.extend(&payload);
        file.extend((payload.len() as i32).to_le_bytes());
        file.extend([0u8; 4]);
        file.extend(PUFFIN_MAGIC);
        (file, at)
    }
}

#[cfg(test)]
mod tests {
    use super::write::{blob, puffin, Container};
    use super::*;

    #[test]
    fn crc32_is_ieee() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn reads_every_kind_of_container() {
        // Three containers under key 0 (array, bitmap, run), one past 32
        // bits, and one bitmap container the format forces.
        let mut positions: Vec<u64> = vec![1, 7, 65_535];
        positions.extend((65_536..65_536 + 5000).step_by(3));
        positions.extend(131_072..131_200);
        positions.extend((196_608..196_608 + 4097).map(|p| p as u64));
        positions.push((3u64 << 32) | 42);
        positions.sort();
        positions.dedup();
        for kinds in [
            [
                Container::Array,
                Container::Bitmap,
                Container::Run,
                Container::Array,
                Container::Array,
            ],
            [Container::Run; 5],
            [Container::Array; 5],
            [Container::Bitmap; 5],
        ] {
            let bytes = blob(&positions, |i| kinds[i]);
            assert_eq!(decode_blob(&bytes).unwrap(), positions);
        }
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Blobs Iceberg-Java writes (its `TestBitmapPositionDeleteIndex`
    /// golden files, apache/iceberg `core/src/test/resources/org/apache/
    /// iceberg/deletes/*-position-index.bin`, Apache-2.0): the last with
    /// run containers, as Java run-optimizes before writing.
    #[test]
    fn reads_what_iceberg_java_writes() {
        let bitmap = 1u64 << 32;
        let container = 1u64 << 16;
        let mut all_types: Vec<u64> = Vec::new();
        for b in [0, bitmap] {
            let (small, (array_from, array_to)) = if b == 0 {
                ([5, 7], (1, 1000))
            } else {
                ([10, 20], (10, 500))
            };
            all_types.extend(small.map(|v| b + v));
            all_types.extend((b + container + array_from)..(b + container + array_to));
            all_types.extend((b + 2 * container + 1)..(b + 3 * container - 1));
        }
        for (name, bytes, positions) in [
            ("empty", "0000000cd1d339640000000000000000bf18480c", vec![]),
            (
                "small-alternating-values",
                "0000002ad1d339640100000000000000000000003a3000000100000000000400100000000100\
                 0300050007000900b3fb20be",
                vec![1, 3, 5, 7, 9],
            ),
            (
                "small-and-large-values",
                "00000030d1d339640100000000000000000000003a30000002000000000001000080010018000000\
                 1c000000640065006300640015eb1c7c",
                vec![100, 101, i32::MAX as u64 + 100, i32::MAX as u64 + 101],
            ),
            (
                "all-container-types",
                "00000056d1d339640200000000000000000000003b30020006000001000100e6030200fdff0500\
                 070001000100e60301000100fdff010000003b30020006000001000100e9010200fdff0a001400\
                 01000a00e90101000100fdffc89be397",
                all_types,
            ),
        ] {
            assert_eq!(decode_blob(&hex(bytes)).unwrap(), positions, "{name}");
        }
    }

    #[test]
    fn reads_an_empty_vector() {
        assert_eq!(
            decode_blob(&blob(&[], |_| Container::Array)).unwrap(),
            Vec::<u64>::new()
        );
    }

    #[test]
    fn refuses_a_corrupt_blob() {
        let good = blob(&[1, 2, 3], |_| Container::Array);
        let mut crc = good.clone();
        let last = crc.len() - 1;
        crc[last] ^= 1;
        assert!(decode_blob(&crc).unwrap_err().contains("CRC"));
        let mut magic = good.clone();
        magic[4] = 0;
        assert!(decode_blob(&magic).unwrap_err().contains("magic"));
        let mut short = good.clone();
        short.pop();
        assert!(decode_blob(&short).unwrap_err().contains("length"));
    }

    #[test]
    fn finds_each_blob_in_a_puffin_file() {
        let a = blob(&[0, 5], |_| Container::Array);
        let b = blob(&[9], |_| Container::Run);
        let (file, at) = puffin(&[
            ("s3://t/a.parquet", a.clone(), 2),
            ("s3://t/b.parquet", b.clone(), 1),
        ]);
        let blobs = puffin_blobs(&file).unwrap();
        assert_eq!(blobs.len(), 2);
        for ((blob_meta, (offset, length)), (bytes, referenced)) in blobs
            .iter()
            .zip(&at)
            .zip([(&a, "s3://t/a.parquet"), (&b, "s3://t/b.parquet")])
        {
            assert_eq!(blob_meta.blob_type, DELETION_VECTOR_V1);
            assert_eq!((blob_meta.offset, blob_meta.length), (*offset, *length));
            assert_eq!(
                &file[*offset as usize..(*offset + *length) as usize],
                &bytes[..]
            );
            assert_eq!(blob_meta.properties["referenced-data-file"], referenced);
            assert_eq!((blob_meta.snapshot_id, blob_meta.sequence_number), (-1, -1));
        }
    }

    /// pg2iceberg's writer, read by this reader — which reads what Java
    /// writes: runs, arrays and bitmaps, and positions past 2^32.
    #[test]
    fn reads_what_pg2iceberg_writes() {
        use pg2iceberg_iceberg::deletion_vector::{Positions, PuffinWriter};
        let vectors: Vec<(&str, Vec<u64>)> = vec![
            ("s3://t/a.parquet", vec![0, 5, 7]),
            ("s3://t/b.parquet", (100..9000).collect()),
            (
                "s3://t/c.parquet",
                (0..5000).step_by(2).chain([1 << 33]).collect(),
            ),
            ("s3://t/d.parquet", vec![]),
        ];
        let mut writer = PuffinWriter::new();
        let at: Vec<(u64, u64)> = vectors
            .iter()
            .map(|(file, p)| writer.add(file, &p.iter().copied().collect::<Positions>()))
            .collect();
        let file = writer.finish();
        let blobs = puffin_blobs(&file).unwrap();
        assert_eq!(blobs.len(), vectors.len());
        for ((meta, (offset, length)), (referenced, positions)) in
            blobs.iter().zip(&at).zip(&vectors)
        {
            assert_eq!(meta.blob_type, DELETION_VECTOR_V1);
            assert_eq!((meta.offset, meta.length), (*offset, *length));
            assert_eq!(meta.compression_codec, None);
            assert_eq!((meta.snapshot_id, meta.sequence_number), (-1, -1));
            assert_eq!(meta.properties["referenced-data-file"], *referenced);
            assert_eq!(meta.properties["cardinality"], positions.len().to_string());
            let blob = &file[*offset as usize..(*offset + *length) as usize];
            assert_eq!(&decode_blob(blob).unwrap(), positions);
        }
    }

    #[test]
    fn refuses_a_file_that_isnt_puffin() {
        assert!(puffin_blobs(b"PAR1....PAR1").is_err());
        let (mut file, _) = puffin(&[("a", blob(&[1], |_| Container::Array), 1)]);
        let n = file.len();
        file[n - 8] = 1;
        assert!(puffin_blobs(&file).unwrap_err().contains("compressed"));
    }
}
