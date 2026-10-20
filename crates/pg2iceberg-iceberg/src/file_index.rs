//! In-memory PK ↔ file map for a single materialized table.
//!
//! Mirrors `iceberg/tablewriter.go:84-126`. The materializer maintains one
//! `FileIndex` per table and updates it after each commit:
//! - Newly-written data files contribute their PKs ([`FileIndex::add_file`]).
//! - Equality-deleted PKs get removed ([`FileIndex::remove_pks`]).
//!
//! Used for:
//! 1. **TOAST resolution.** TOAST `unchanged_cols` placeholders need the prior
//!    column values, which live in some prior data file. The materializer
//!    asks the index for the file path, fetches it, and copies the unchanged
//!    columns in.
//! 2. **Re-insert promotion.** An `Insert` whose PK already lives in a prior
//!    data file must be downgraded to `Update` so the writer emits an
//!    equality delete; otherwise readers would see two rows for that PK.
//! 3. Placing an equality delete in its row's partition, expanding a
//!    `TRUNCATE`, filling a column added with a default, and counting each
//!    file's live rows for compaction.
//!
//! Keys are [`PkKey`]s, which compare stored values: a key built from a
//! WAL event equals the key read back from a data file.
//!
//! ## Bounded
//!
//! A key a live row would make the index a table's worth of keys: it
//! holds at most a memory budget's ([`FileIndex::with_memory`]). Past it,
//! the oldest files' keys leave the map — the files are *evicted* — for a
//! Bloom filter, and each evicted file keeps the range of its keys and a
//! count of its live rows that's never too high. A key the map doesn't
//! hold then may still live in an evicted file whose range holds it
//! ([`FileIndex::candidates`]), if the filter says it may: finding it
//! takes reading those files. Within its budget the index is exact.
//!
//! On materializer restart, the index is rebuilt from the catalog's
//! snapshot history — see [`rebuild_from_catalog`].

use crate::pk::PkKey;
use pg2iceberg_core::PartitionLiteral;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};

/// A data file's slot in [`FileIndex::files`].
type FileId = u32;

/// A key as the map holds it: its encoding, exactly, when that's under 16
/// bytes — an integer, or a few — else a 120-bit hash of it, which two
/// keys share with negligible odds.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Slot([u8; 16]);

/// A [`Slot`]'s first byte when it holds a hash: an encoding's length is
/// under 16.
const HASHED: u8 = 0xff;

impl Slot {
    fn of(key: &PkKey) -> Self {
        let bytes = key.as_bytes();
        let mut slot = [0u8; 16];
        if bytes.len() < 16 {
            slot[0] = bytes.len() as u8;
            slot[1..=bytes.len()].copy_from_slice(bytes);
        } else {
            slot[0] = HASHED;
            slot[1..9].copy_from_slice(&seeded_hash(0, bytes).to_le_bytes());
            slot[9..].copy_from_slice(&seeded_hash(1, bytes).to_le_bytes()[..7]);
        }
        Self(slot)
    }

    /// The key, if held exactly.
    fn key(&self) -> Option<PkKey> {
        let len = self.0[0];
        (len != HASHED).then(|| PkKey::from_bytes(&self.0[1..=len as usize]))
    }

    /// Two independent hashes, for [`KeyFilter`]'s probes.
    fn hashes(&self) -> (u64, u64) {
        let lo = u64::from_le_bytes(self.0[..8].try_into().expect("8 bytes"));
        let hi = u64::from_le_bytes(self.0[8..].try_into().expect("8 bytes"));
        (
            mix(lo ^ mix(hi)),
            mix(hi ^ mix(lo ^ 0x9e37_79b9_7f4a_7c15)) | 1,
        )
    }
}

fn seeded_hash(seed: u8, bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    bytes.hash(&mut h);
    h.finish()
}

/// splitmix64's finalizer: every bit of `x` moves every bit out.
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// The keys of evicted files, as a Bloom filter: whether a key may be one
/// of them — never wrongly not. It grows a layer at a time, each holding
/// twice the keys of the last and erring half as often, while its budget
/// lasts; past it, the last layer takes the rest, erring more and more.
#[derive(Clone, Default)]
struct KeyFilter {
    layers: Vec<Layer>,
    max_bytes: usize,
    /// The bytes `layers` take.
    bytes: usize,
    /// Its budget holds no more layers: the last takes the rest.
    full: bool,
}

#[derive(Clone)]
struct Layer {
    bits: Vec<u64>,
    probes: u32,
    capacity: usize,
    len: usize,
}

/// How often the filter takes a key for one it holds, at most, while its
/// budget lasts.
const FILTER_ERROR: f64 = 0.01;
/// Keys the filter's first layer holds.
const FIRST_LAYER: usize = 1 << 16;

impl Layer {
    fn new(capacity: usize, error: f64) -> Self {
        let words = Self::words(capacity, error);
        let probes = (words as f64 * 64.0 / capacity as f64 * std::f64::consts::LN_2)
            .round()
            .max(1.0) as u32;
        Self {
            bits: vec![0; words],
            probes,
            capacity,
            len: 0,
        }
    }

    /// The 64-bit words a layer of `capacity` keys erring `error` takes.
    fn words(capacity: usize, error: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        let bits = (capacity as f64 * -error.ln() / (ln2 * ln2)).ceil() as usize;
        bits.div_ceil(64).max(1)
    }

    fn positions(&self, slot: &Slot) -> impl Iterator<Item = usize> {
        let (h1, h2) = slot.hashes();
        let bits = self.bits.len() as u64 * 64;
        (0..u64::from(self.probes))
            .map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % bits) as usize)
    }

    fn insert(&mut self, slot: &Slot) {
        for p in self.positions(slot) {
            self.bits[p / 64] |= 1 << (p % 64);
        }
        self.len += 1;
    }

    fn may_contain(&self, slot: &Slot) -> bool {
        self.positions(slot)
            .all(|p| self.bits[p / 64] & (1 << (p % 64)) != 0)
    }
}

impl KeyFilter {
    fn insert(&mut self, slot: &Slot) {
        if !self.full && self.layers.last().is_none_or(|l| l.len >= l.capacity) {
            let capacity = self.layers.last().map_or(FIRST_LAYER, |l| l.capacity * 2);
            let error = FILTER_ERROR * 0.5f64.powi(self.layers.len() as i32 + 1);
            let bytes = Layer::words(capacity, error) * 8;
            if self.layers.is_empty() || self.bytes + bytes <= self.max_bytes {
                self.layers.push(Layer::new(capacity, error));
                self.bytes += bytes;
            } else {
                self.full = true;
            }
        }
        self.layers.last_mut().expect("a layer").insert(slot);
    }

    fn may_contain(&self, slot: &Slot) -> bool {
        self.layers.iter().any(|l| l.may_contain(slot))
    }

    fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Bytes a key takes in [`FileIndex::keys`]: its [`Slot`], its file and
/// the map's control byte.
const KEY_BYTES: usize = 16 + 4 + 1;

/// Live PK → data file map. One entry per live row, up to its budget,
/// so it is kept compact: a 16-byte [`Slot`] plus a 4-byte file number,
/// with each file's path stored once.
#[derive(Clone)]
pub struct FileIndex {
    /// PK → the file holding its current row: every live key of the files
    /// not evicted, and the keys added to evicted ones since.
    keys: HashMap<Slot, FileId>,
    /// Data files holding at least one live row; `None` slots are free.
    files: Vec<Option<IndexedFile>>,
    free: Vec<FileId>,
    ids: HashMap<String, FileId>,
    /// The keys of evicted files.
    evicted_keys: KeyFilter,
    /// Files evicted, while in the index.
    evicted: usize,
    /// The smallest and largest keys of the files evicted, ever: a key
    /// outside them is in none.
    evicted_range: Option<(PkKey, PkKey)>,
    /// Most keys `keys` holds before files are evicted.
    max_keys: usize,
    /// Files added so far: the next one's [`IndexedFile::generation`].
    added: u64,
    /// Some key is held as a hash: `keys` can't give every key back.
    hashed: bool,
}

#[derive(Clone)]
struct IndexedFile {
    path: String,
    /// One literal per partition spec field; empty for unpartitioned
    /// tables. Lets a cross-batch `Delete` on a partitioned table recover
    /// the partition tuple of its PK's prior data file.
    partition_values: Vec<PartitionLiteral>,
    /// Its live rows: exactly, until it's evicted; then never too many,
    /// nor fewer than `mapped`. Unevicted, it leaves the index at zero.
    live: u64,
    /// Keys in `keys` pointing at it: rows surely live.
    mapped: u64,
    /// Its smallest and largest key: where an evicted file's live keys
    /// can be.
    range: Option<(PkKey, PkKey)>,
    /// Where it stands among the files, oldest first: when it was added,
    /// or for a compaction's output, where its newest input stood. Files
    /// are evicted oldest first, and the newest holding a key has its
    /// current row.
    generation: u64,
    /// Its live keys may be missing from `keys`.
    evicted: bool,
    /// Rows it may have lost unseen: keys deleted or written again that
    /// `keys` didn't hold, in its range.
    unseen_deaths: u64,
}

impl IndexedFile {
    fn may_hold(&self, key: &PkKey) -> bool {
        self.range
            .as_ref()
            .is_some_and(|(lo, hi)| lo <= key && key <= hi)
    }

    fn widen(&mut self, key: &PkKey) {
        match &mut self.range {
            None => self.range = Some((key.clone(), key.clone())),
            Some((lo, hi)) => {
                if key < lo {
                    *lo = key.clone();
                } else if key > hi {
                    *hi = key.clone();
                }
            }
        }
    }
}

impl Default for FileIndex {
    fn default() -> Self {
        Self {
            keys: HashMap::new(),
            files: Vec::new(),
            free: Vec::new(),
            ids: HashMap::new(),
            evicted_keys: KeyFilter::default(),
            evicted: 0,
            evicted_range: None,
            max_keys: usize::MAX,
            added: 0,
            hashed: false,
        }
    }
}

impl FileIndex {
    /// An index without a budget: it holds every key.
    pub fn new() -> Self {
        Self::default()
    }

    /// An index of at most about `bytes`: a quarter for the filter of
    /// evicted keys, the rest for the map, at its largest — while it
    /// grows, its old table and new one both.
    pub fn with_memory(bytes: usize) -> Self {
        let mut index = Self::default();
        index.set_memory(bytes);
        index
    }

    /// An empty index of the same budget.
    pub fn cleared(&self) -> Self {
        Self {
            max_keys: self.max_keys,
            evicted_keys: KeyFilter {
                max_bytes: self.evicted_keys.max_bytes,
                ..KeyFilter::default()
            },
            ..Self::default()
        }
    }

    /// Budget the index `bytes` from now on ([`Self::with_memory`]).
    pub fn set_memory(&mut self, bytes: usize) {
        let map_bytes = bytes / 4 * 3;
        // The map grows by doubling its table, of a power of two buckets,
        // once 7/8 of them hold keys; both tables live while it grows.
        let mut buckets: usize = 1;
        while (buckets * 2).saturating_mul(KEY_BYTES) * 3 / 2 <= map_bytes {
            buckets *= 2;
        }
        self.max_keys = (buckets / 8 * 7).max(1);
        self.evicted_keys.max_bytes = bytes / 4;
    }

    /// Register all PKs in a freshly written data file. Replaces any prior
    /// PK→file mapping (the new file is now authoritative for these PKs).
    /// `partition_values` is the data file's partition tuple (empty for
    /// unpartitioned tables).
    pub fn add_file(
        &mut self,
        path: String,
        pk_keys: impl IntoIterator<Item = PkKey>,
        partition_values: Vec<PartitionLiteral>,
    ) {
        self.add_file_at(path, pk_keys, partition_values, None);
    }

    /// [`Self::add_file`], standing among the files at `generation` (see
    /// [`IndexedFile::generation`]) if it's new.
    pub fn add_file_at(
        &mut self,
        path: String,
        pk_keys: impl IntoIterator<Item = PkKey>,
        partition_values: Vec<PartitionLiteral>,
        generation: Option<u64>,
    ) {
        let id = self.file_id(path, partition_values, generation);
        let mut range: Option<(PkKey, PkKey)> = None;
        for pk in pk_keys {
            let slot = Slot::of(&pk);
            self.hashed |= slot.0[0] == HASHED;
            match &mut range {
                None => range = Some((pk.clone(), pk.clone())),
                Some((lo, hi)) => {
                    if pk < *lo {
                        *lo = pk.clone();
                    } else if pk > *hi {
                        *hi = pk.clone();
                    }
                }
            }
            match self.keys.insert(slot, id) {
                Some(prev) if prev == id => {}
                Some(prev) => {
                    let file = self.file_mut(id);
                    file.live += 1;
                    file.mapped += 1;
                    self.release(prev);
                }
                None => {
                    let file = self.file_mut(id);
                    file.live += 1;
                    file.mapped += 1;
                    // Its row before this one, if any, is dead now.
                    self.lose_unseen(&slot, &pk, Some(id));
                    self.evict_if_full();
                }
            }
        }
        if let Some((lo, hi)) = range {
            if let Some(file) = self.files[id as usize].as_mut() {
                file.widen(&lo);
                file.widen(&hi);
                // Evicted while its keys went in, maybe.
                self.widen_evicted_range(id);
            }
        }
        if self.files[id as usize]
            .as_ref()
            .is_some_and(|f| f.live == 0 && !f.evicted)
        {
            self.drop_file(id);
        }
    }

    /// Count `rows` more live rows of `path`'s, and `keys` in its range,
    /// without holding their keys: they're of files evicted. `path` is
    /// evicted, if it wasn't.
    pub fn add_unseen(
        &mut self,
        path: String,
        rows: u64,
        keys: impl IntoIterator<Item = PkKey>,
        partition_values: Vec<PartitionLiteral>,
        generation: Option<u64>,
    ) {
        let id = self.file_id(path, partition_values, generation);
        let file = self.file_mut(id);
        for pk in keys {
            file.widen(&pk);
        }
        file.live += rows;
        if !file.evicted {
            file.evicted = true;
            self.evicted += 1;
        }
        self.widen_evicted_range(id);
    }

    /// Take `id`'s range into [`Self::evicted_range`], if it's evicted.
    fn widen_evicted_range(&mut self, id: FileId) {
        let file = self.files[id as usize].as_ref().expect("live file id");
        let Some((lo, hi)) = file.range.as_ref().filter(|_| file.evicted) else {
            return;
        };
        match &mut self.evicted_range {
            None => self.evicted_range = Some((lo.clone(), hi.clone())),
            Some((min, max)) => {
                if lo < min {
                    *min = lo.clone();
                }
                if hi > max {
                    *max = hi.clone();
                }
            }
        }
    }

    fn file_id(
        &mut self,
        path: String,
        partition_values: Vec<PartitionLiteral>,
        generation: Option<u64>,
    ) -> FileId {
        match self.ids.get(&path) {
            Some(&id) => {
                if !partition_values.is_empty() {
                    self.file_mut(id).partition_values = partition_values;
                }
                id
            }
            None => self.insert_file(path, partition_values, generation),
        }
    }

    /// The file holding `pk_key`'s current row, if the map holds the key.
    /// A key it doesn't may still be in an evicted file
    /// ([`Self::candidates`]).
    pub fn lookup(&self, pk_key: &PkKey) -> Option<&str> {
        self.keys
            .get(&Slot::of(pk_key))
            .map(|&id| self.file(id).path.as_str())
    }

    /// Whether the table may hold `pk_key`: never wrongly not. Exact while
    /// no file is evicted.
    pub fn may_contain(&self, pk_key: &PkKey) -> bool {
        let slot = Slot::of(pk_key);
        self.keys.contains_key(&slot) || self.may_be_evicted(&slot, pk_key)
    }

    /// Whether `pk_key` may be an evicted file's: in their range, and the
    /// filter's.
    fn may_be_evicted(&self, slot: &Slot, pk_key: &PkKey) -> bool {
        self.evicted > 0
            && self
                .evicted_range
                .as_ref()
                .is_some_and(|(lo, hi)| lo <= pk_key && pk_key <= hi)
            && self.evicted_keys.may_contain(slot)
    }

    /// The evicted files that may hold `pk_key`'s current row, newest
    /// first — the newest holding it does — when the map doesn't hold it.
    pub fn candidates(&self, pk_key: &PkKey) -> Vec<&str> {
        if self.keys.contains_key(&Slot::of(pk_key)) {
            return Vec::new();
        }
        let mut files = self.evicted_holding(&Slot::of(pk_key), pk_key, None);
        files.sort_by_key(|&id| std::cmp::Reverse(self.file(id).generation));
        files
            .into_iter()
            .map(|id| self.file(id).path.as_str())
            .collect()
    }

    /// The partition tuples of the files that may hold `pk_key`'s row:
    /// the one the map holds it in, else the evicted ones that may. Empty
    /// when the table can't hold it, or its files are unpartitioned. Used
    /// by `TableWriter::prepare` to place an equality delete whose row
    /// payload doesn't carry the partition source columns — in each, if
    /// several: in a partition without the key, it deletes nothing.
    pub fn partitions_for_pk(&self, pk_key: &PkKey) -> Vec<&[PartitionLiteral]> {
        let slot = Slot::of(pk_key);
        let ids = match self.keys.get(&slot) {
            Some(&id) => vec![id],
            None => self.evicted_holding(&slot, pk_key, None),
        };
        let mut out: Vec<&[PartitionLiteral]> = Vec::new();
        for id in ids {
            let values = self.file(id).partition_values.as_slice();
            if !values.is_empty() && !out.contains(&values) {
                out.push(values);
            }
        }
        out
    }

    /// The evicted files, but `except`, whose range holds `pk_key`, if the
    /// filter says an evicted file may.
    fn evicted_holding(&self, slot: &Slot, pk_key: &PkKey, except: Option<FileId>) -> Vec<FileId> {
        if !self.may_be_evicted(slot, pk_key) {
            return Vec::new();
        }
        self.files
            .iter()
            .enumerate()
            .filter_map(|(id, f)| f.as_ref().map(|f| (id as FileId, f)))
            .filter(|(id, f)| f.evicted && Some(*id) != except && f.may_hold(pk_key))
            .map(|(id, _)| id)
            .collect()
    }

    /// Forget `path` (rewritten by compaction) and every PK still pointing
    /// at it. Add the compaction outputs with [`Self::add_file`] first:
    /// that moves the surviving PKs off `path`, leaving nothing to scan.
    pub fn remove_file(&mut self, path: &str) {
        let Some(&id) = self.ids.get(path) else {
            return;
        };
        if self.file(id).mapped > 0 {
            self.keys.retain(|_, f| *f != id);
        }
        self.drop_file(id);
    }

    /// Mark these PKs as deleted. A file leaves the index once none of
    /// its PKs are live, unless it's evicted.
    pub fn remove_pks<'a>(&mut self, pk_keys: impl IntoIterator<Item = &'a PkKey>) {
        for pk in pk_keys {
            let slot = Slot::of(pk);
            match self.keys.remove(&slot) {
                Some(id) => self.release(id),
                None => self.lose_unseen(&slot, pk, None),
            }
        }
    }

    /// Returns the set of file paths that contain at least one of the given
    /// PKs. Used by the materializer to know which files to fetch for TOAST
    /// resolution.
    pub fn affected_files(&self, pk_keys: &[PkKey]) -> BTreeSet<String> {
        pk_keys
            .iter()
            .filter_map(|pk| self.lookup(pk))
            .map(str::to_string)
            .collect()
    }

    /// Data files that may hold a live row, sorted.
    pub fn live_files(&self) -> Vec<&str> {
        let mut out: Vec<&str> = self.live().map(|f| f.path.as_str()).collect();
        out.sort_unstable();
        out
    }

    /// The evicted files, sorted.
    pub fn evicted_files(&self) -> Vec<&str> {
        let mut out: Vec<&str> = self
            .live()
            .filter(|f| f.evicted)
            .map(|f| f.path.as_str())
            .collect();
        out.sort_unstable();
        out
    }

    /// Whether the table may hold a row.
    pub fn holds_rows(&self) -> bool {
        self.live().next().is_some()
    }

    /// Keys the map holds: every live row's, while no file is evicted.
    pub fn live_pk_count(&self) -> usize {
        self.keys.len()
    }

    /// Live rows per data file: never too many. A file whose live rows
    /// are fewer than its records holds dead rows, which compaction can
    /// drop.
    pub fn live_rows_per_file(&self) -> BTreeMap<&str, u64> {
        self.live().map(|f| (f.path.as_str(), f.live)).collect()
    }

    /// Each evicted file's [`IndexedFile::unseen_deaths`]: a compaction
    /// pass prepared now, committed later, subtracts those since from what
    /// it carries over of these files' rows.
    pub fn unseen_deaths(&self) -> BTreeMap<String, u64> {
        self.live()
            .filter(|f| f.evicted)
            .map(|f| (f.path.clone(), f.unseen_deaths))
            .collect()
    }

    /// Whether `path` is evicted: its live keys may be missing from the
    /// map.
    pub fn is_evicted(&self, path: &str) -> bool {
        self.ids.get(path).is_some_and(|&id| self.file(id).evicted)
    }

    /// Where `path` stands among the files ([`IndexedFile::generation`]).
    pub fn generation(&self, path: &str) -> Option<u64> {
        self.ids.get(path).map(|&id| self.file(id).generation)
    }

    /// Every currently-live PK key, in no particular order — or `None`, if
    /// the map can't give them all back: a file is evicted, or a key is
    /// held as a hash. Used by the materializer's TRUNCATE expansion: a
    /// `TRUNCATE` event has no per-row payload, so we materialize it by
    /// emitting one equality-delete per known PK before continuing the
    /// cycle.
    pub fn all_pks(&self) -> Option<Vec<PkKey>> {
        if self.evicted > 0 || self.hashed {
            return None;
        }
        Some(self.keys.keys().filter_map(Slot::key).collect())
    }

    /// The keys the map holds — those it holds exactly: of under 16
    /// bytes encoded.
    pub fn mapped_pks(&self) -> Vec<PkKey> {
        self.keys.keys().filter_map(Slot::key).collect()
    }

    /// About how many bytes the index takes.
    pub fn memory_bytes(&self) -> usize {
        // A map's capacity is 7/8 of its buckets.
        self.keys.capacity() / 7 * 8 * KEY_BYTES
            + self.evicted_keys.bytes()
            + self
                .live()
                .map(|f| std::mem::size_of::<IndexedFile>() + f.path.len())
                .sum::<usize>()
    }

    fn live(&self) -> impl Iterator<Item = &IndexedFile> {
        self.files.iter().flatten()
    }

    fn file(&self, id: FileId) -> &IndexedFile {
        self.files[id as usize].as_ref().expect("live file id")
    }

    fn file_mut(&mut self, id: FileId) -> &mut IndexedFile {
        self.files[id as usize].as_mut().expect("live file id")
    }

    fn insert_file(
        &mut self,
        path: String,
        partition_values: Vec<PartitionLiteral>,
        generation: Option<u64>,
    ) -> FileId {
        let file = IndexedFile {
            path: path.clone(),
            partition_values,
            live: 0,
            mapped: 0,
            range: None,
            generation: generation.unwrap_or(self.added),
            evicted: false,
            unseen_deaths: 0,
        };
        self.added += 1;
        let id = match self.free.pop() {
            Some(id) => {
                self.files[id as usize] = Some(file);
                id
            }
            None => {
                let id = FileId::try_from(self.files.len()).expect("under 2^32 live data files");
                self.files.push(Some(file));
                id
            }
        };
        self.ids.insert(path, id);
        id
    }

    /// A key the map held for `id` died.
    fn release(&mut self, id: FileId) {
        let file = self.file_mut(id);
        file.live -= 1;
        file.mapped -= 1;
        if file.live == 0 && !file.evicted {
            self.drop_file(id);
        }
    }

    /// `pk`, which the map doesn't hold, died — deleted, or written again:
    /// if it lived in an evicted file, one in its range lost a row. Each
    /// such file, but `except`, counts one fewer, as one did.
    fn lose_unseen(&mut self, slot: &Slot, pk: &PkKey, except: Option<FileId>) {
        for id in self.evicted_holding(slot, pk, except) {
            let file = self.file_mut(id);
            file.live = file.live.saturating_sub(1).max(file.mapped);
            file.unseen_deaths += 1;
        }
    }

    /// Past its most keys, evict the oldest files' down to three quarters
    /// of it: their keys leave the map for the filter.
    fn evict_if_full(&mut self) {
        if self.keys.len() <= self.max_keys {
            return;
        }
        let target = self.max_keys - self.max_keys / 4;
        let mut oldest: Vec<(u64, FileId)> = self
            .files
            .iter()
            .enumerate()
            .filter_map(|(id, f)| f.as_ref().map(|f| (id as FileId, f)))
            .filter(|(_, f)| f.mapped > 0)
            .map(|(id, f)| (f.generation, id))
            .collect();
        oldest.sort_unstable();
        let mut evicting = vec![false; self.files.len()];
        let mut left = self.keys.len();
        for (_, id) in oldest {
            if left <= target {
                break;
            }
            evicting[id as usize] = true;
            left -= self.file(id).mapped as usize;
        }
        let filter = &mut self.evicted_keys;
        self.keys.retain(|slot, id| {
            let evict = evicting[*id as usize];
            if evict {
                filter.insert(slot);
            }
            !evict
        });
        for (id, evict) in evicting.into_iter().enumerate() {
            if evict {
                let file = self.files[id].as_mut().expect("live file id");
                file.mapped = 0;
                if !file.evicted {
                    file.evicted = true;
                    self.evicted += 1;
                }
                self.widen_evicted_range(id as FileId);
            }
        }
    }

    fn drop_file(&mut self, id: FileId) {
        if let Some(file) = self.files[id as usize].take() {
            if file.evicted {
                self.evicted -= 1;
            }
            self.ids.remove(&file.path);
            self.free.push(id);
        }
    }

    /// PK → path, and path → partition tuple: what the index means,
    /// independent of file numbering.
    #[allow(clippy::type_complexity)]
    fn view(&self) -> (BTreeMap<&Slot, &str>, BTreeMap<&str, &[PartitionLiteral]>) {
        let pks = self
            .keys
            .iter()
            .map(|(pk, &id)| (pk, self.file(id).path.as_str()))
            .collect();
        let partitions = self
            .live()
            .map(|f| (f.path.as_str(), f.partition_values.as_slice()))
            .collect();
        (pks, partitions)
    }
}

impl PartialEq for FileIndex {
    fn eq(&self, other: &Self) -> bool {
        self.keys.len() == other.keys.len() && self.view() == other.view()
    }
}

impl fmt::Debug for FileIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (pks, partitions) = self.view();
        let pks: BTreeMap<String, &str> = pks
            .into_iter()
            .map(|(slot, path)| {
                let key = slot
                    .key()
                    .map_or_else(|| "#hashed".into(), |k| k.to_string());
                (key, path)
            })
            .collect();
        f.debug_struct("FileIndex")
            .field("pks", &pks)
            .field("partitions", &partitions)
            .field("evicted", &self.evicted_files())
            .finish()
    }
}

/// Rebuild a `FileIndex` for `ident` from the catalog's snapshot history.
///
/// Used on materializer restart so re-insert promotion keeps working —
/// without this, a freshly-booted process has an empty FileIndex and a
/// re-insert of a previously-materialized PK won't emit the equality
/// delete that's needed to void the prior data file row, producing
/// duplicate rows in MoR readers.
///
/// MoR semantics: an equality-delete file with sequence number `N` voids
/// data file rows whose PK matches at sequence numbers `< N`. Replaying
/// the live files in sequence-number order — at each, its deletes, then
/// its data files — therefore leaves exactly the live PKs, as the
/// materializer's own updates do. Files are streamed a batch at a time and
/// only their PK columns decoded, so the rebuild holds little beyond the
/// index itself.
pub async fn rebuild_from_catalog(
    catalog: &dyn pg2iceberg_iceberg_dyn::DynCatalog,
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    ident: &pg2iceberg_core::TableIdent,
    schema: &pg2iceberg_core::TableSchema,
    pk_cols: &[pg2iceberg_core::ColumnName],
) -> std::result::Result<FileIndex, crate::verify::VerifyError> {
    let mut fi = FileIndex::new();
    catch_up_from_catalog(&mut fi, None, catalog, blob_store, ident, schema, pk_cols).await?;
    Ok(fi)
}

/// Bring `fi` — `ident`'s index as of its snapshot `at` (`None`: before
/// the first, so `fi` is empty) — up to the catalog's current snapshot,
/// and return that snapshot.
///
/// Replays only the snapshots after `at`, while history holds every one
/// of them. Past an expired one it rebuilds instead: a stand-in lists the
/// files its snapshot added that are still live, not what it removed.
pub async fn catch_up_from_catalog(
    fi: &mut FileIndex,
    at: Option<i64>,
    catalog: &dyn pg2iceberg_iceberg_dyn::DynCatalog,
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    ident: &pg2iceberg_core::TableIdent,
    schema: &pg2iceberg_core::TableSchema,
    pk_cols: &[pg2iceberg_core::ColumnName],
) -> std::result::Result<Option<i64>, crate::verify::VerifyError> {
    use crate::verify::VerifyError;

    let snapshots = catalog
        .snapshots(ident)
        .await
        .map_err(VerifyError::from_dyn)?;
    let current = snapshots.last().map(|s| s.id);
    let newer = match at {
        Some(at) => snapshots
            .iter()
            .position(|s| s.id > at)
            .unwrap_or(snapshots.len()),
        None => 0,
    };
    // Sequence numbers count snapshots: one missing is expired too.
    let whole = snapshots[newer..]
        .iter()
        .zip(at.unwrap_or(0) + 1..)
        .all(|(s, seq)| s.id == seq && !s.expired);

    let pk_schema: Vec<pg2iceberg_core::ColumnSchema> = schema
        .columns
        .iter()
        .filter(|c| c.is_primary_key)
        .cloned()
        .collect();

    if !(at.is_some() && whole && current >= at) {
        // Rebuild from the live files. Files superseded by a Replace
        // snapshot don't contribute — as for the verifier.
        *fi = fi.cleared();
        let removed_paths: BTreeSet<&str> = snapshots
            .iter()
            .flat_map(|s| s.removed_paths.iter().map(String::as_str))
            .collect();
        let mut live: Vec<(i64, bool, &crate::DataFile)> = Vec::new();
        for snap in &snapshots {
            let files = (snap.delete_files.iter().map(|f| (false, f)))
                .chain(snap.data_files.iter().map(|f| (true, f)));
            for (data, f) in files {
                if !removed_paths.contains(f.path.as_str()) {
                    live.push((f.sequence_number_in(snap), data, f));
                }
            }
        }
        // A compaction's output keeps an older sequence number than the
        // snapshot that added it: deletes since apply to it.
        live.sort_by_key(|(seq, data, _)| (*seq, *data));
        for (_, data, df) in live {
            for_each_pk_batch(blob_store, &df.path, &pk_schema, pk_cols, |pks| {
                if data {
                    fi.add_file(df.path.clone(), pks, df.partition_values.clone())
                } else {
                    fi.remove_pks(&pks)
                }
            })
            .await?;
        }
        return Ok(current);
    }

    // Replay what happened since `at`, in order.
    for snap in &snapshots[newer..] {
        for df in &snap.delete_files {
            for_each_pk_batch(blob_store, &df.path, &pk_schema, pk_cols, |pks| {
                fi.remove_pks(&pks)
            })
            .await?;
        }
        // A Replace's outputs hold its inputs' rows as of the snapshot its
        // pass read: they take over the keys still in those inputs. A key
        // deleted or rewritten since is elsewhere, or nowhere. One the map
        // doesn't hold, of an evicted input, may be live: the output is
        // evicted too, counting only the rows it surely holds.
        let inputs: BTreeSet<&str> = snap.removed_paths.iter().map(String::as_str).collect();
        let evicted_inputs = inputs.iter().any(|p| fi.is_evicted(p));
        let generation = inputs.iter().filter_map(|p| fi.generation(p)).max();
        for df in &snap.data_files {
            for_each_pk_batch(blob_store, &df.path, &pk_schema, pk_cols, |pks| {
                if inputs.is_empty() {
                    return fi.add_file(df.path.clone(), pks, df.partition_values.clone());
                }
                if evicted_inputs {
                    fi.add_unseen(
                        df.path.clone(),
                        0,
                        pks.iter().cloned(),
                        df.partition_values.clone(),
                        generation,
                    );
                }
                let moved: Vec<PkKey> = pks
                    .into_iter()
                    .filter(|pk| fi.lookup(pk).is_some_and(|p| inputs.contains(p)))
                    .collect();
                fi.add_file_at(
                    df.path.clone(),
                    moved,
                    df.partition_values.clone(),
                    generation,
                )
            })
            .await?;
        }
        for path in inputs {
            fi.remove_file(path);
        }
    }
    Ok(current)
}

/// Calls `f` with the PKs of each batch of rows in the file at `path`.
async fn for_each_pk_batch(
    blob_store: &dyn pg2iceberg_stream::BlobStore,
    path: &str,
    pk_schema: &[pg2iceberg_core::ColumnSchema],
    pk_cols: &[pg2iceberg_core::ColumnName],
    mut f: impl FnMut(Vec<PkKey>),
) -> std::result::Result<(), crate::verify::VerifyError> {
    use crate::verify::VerifyError;
    let bytes = blob_store.get(path).await.map_err(VerifyError::Blob)?;
    let batches =
        crate::reader::RowBatches::new(bytes, pk_schema, crate::compact::DECODE_BATCH_ROWS)
            .map_err(VerifyError::Decode)?;
    for batch in batches {
        let rows = batch.map_err(VerifyError::Decode)?;
        f(rows.iter().map(|r| PkKey::from_row(r, pk_cols)).collect());
    }
    Ok(())
}

/// Avoid a circular module reference by re-exporting `DynCatalog` through a
/// private module. `verify::DynCatalog` is the canonical name.
mod pg2iceberg_iceberg_dyn {
    pub use crate::verify::DynCatalog;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2iceberg_core::{ColumnName, PgValue, Row};

    fn k(id: &str) -> PkKey {
        let row: Row = [(ColumnName("id".into()), PgValue::Text(id.into()))].into();
        PkKey::from_row(&row, &[ColumnName("id".into())])
    }

    fn ks(ids: &[&str]) -> Vec<PkKey> {
        ids.iter().map(|id| k(id)).collect()
    }

    #[test]
    fn add_then_lookup() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        assert_eq!(fi.lookup(&k("k1")), Some("p0"));
        assert_eq!(fi.lookup(&k("k2")), Some("p0"));
        assert_eq!(fi.lookup(&k("missing")), None);
        assert!(fi.may_contain(&k("k1")));
        assert!(!fi.may_contain(&k("missing")));
    }

    #[test]
    fn remove_pks_clears_mapping_and_drops_empty_files() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        fi.remove_pks(&ks(&["k1"]));
        assert_eq!(fi.lookup(&k("k1")), None);
        assert_eq!(fi.lookup(&k("k2")), Some("p0"));
        assert_eq!(fi.live_pk_count(), 1);

        fi.remove_pks(&ks(&["k2"]));
        assert!(fi.live_files().is_empty());
        assert_eq!(fi.live_pk_count(), 0);
    }

    #[test]
    fn add_file_with_overlapping_pk_remaps_to_new_file() {
        // Re-insert flow: a PK lives in p0, then a new file p1 covers it.
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("p1".into(), ks(&["k1"]), Vec::new());
        assert_eq!(fi.lookup(&k("k1")), Some("p1"));
        assert_eq!(
            fi.live_rows_per_file(),
            BTreeMap::from([("p0", 1), ("p1", 1)])
        );
        // p0's last live PK moves too: p0 leaves the index.
        fi.add_file("p2".into(), ks(&["k2"]), Vec::new());
        assert_eq!(fi.live_files(), vec!["p1", "p2"]);
    }

    #[test]
    fn affected_files_collects_distinct_paths() {
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("p1".into(), ks(&["k3"]), Vec::new());
        let s = fi.affected_files(&ks(&["k1", "k3", "missing"]));
        let v: Vec<&String> = s.iter().collect();
        assert_eq!(v, vec![&"p0".to_string(), &"p1".to_string()]);
    }

    #[test]
    fn partitions_for_pk_returns_files_partition_tuple() {
        let mut fi = FileIndex::new();
        fi.add_file(
            "p0".into(),
            ks(&["k1"]),
            vec![PartitionLiteral::String("us".into())],
        );
        fi.add_file(
            "p1".into(),
            ks(&["k2"]),
            vec![PartitionLiteral::String("eu".into())],
        );
        assert_eq!(
            fi.partitions_for_pk(&k("k1")),
            vec![&[PartitionLiteral::String("us".into())][..]]
        );
        assert_eq!(
            fi.partitions_for_pk(&k("k2")),
            vec![&[PartitionLiteral::String("eu".into())][..]]
        );
        // Missing PK → none.
        assert!(fi.partitions_for_pk(&k("missing")).is_empty());
    }

    #[test]
    fn partitions_for_unpartitioned_file_are_none() {
        // Unpartitioned files store no partition_values; lookup returns
        // none even though the PK is indexed. Callers should only consult
        // this for partitioned schemas.
        let mut fi = FileIndex::new();
        fi.add_file("p0".into(), ks(&["k1"]), Vec::new());
        assert!(fi.partitions_for_pk(&k("k1")).is_empty());
        assert!(fi.may_contain(&k("k1")));
    }

    #[test]
    fn remove_file_after_its_pks_moved_needs_no_scan_and_forgets_it() {
        let mut fi = FileIndex::new();
        fi.add_file("in".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("out".into(), ks(&["k1", "k2"]), Vec::new());
        fi.remove_file("in");
        assert_eq!(fi.live_files(), vec!["out"]);
        assert_eq!(fi.live_pk_count(), 2);
    }

    #[test]
    fn remove_file_drops_pks_still_pointing_at_it() {
        let mut fi = FileIndex::new();
        fi.add_file("a".into(), ks(&["k1", "k2"]), Vec::new());
        fi.add_file("b".into(), ks(&["k3"]), Vec::new());
        fi.remove_file("a");
        assert_eq!(fi.live_pk_count(), 1);
        assert_eq!(fi.lookup(&k("k3")), Some("b"));
        assert_eq!(fi.live_files(), vec!["b"]);
    }

    #[test]
    fn file_slots_are_reused() {
        let mut fi = FileIndex::new();
        for i in 0..100 {
            fi.add_file(format!("p{i}"), ks(&["k"]), Vec::new());
        }
        assert_eq!(fi.live_files(), vec!["p99"]);
        assert!(
            fi.files.len() <= 2,
            "{} slots for one live file",
            fi.files.len()
        );
    }

    #[test]
    fn equality_ignores_file_numbering() {
        let mut a = FileIndex::new();
        a.add_file("p0".into(), ks(&["k1"]), Vec::new());
        a.add_file("p1".into(), ks(&["k2"]), Vec::new());
        let mut b = FileIndex::new();
        b.add_file("p1".into(), ks(&["k2"]), Vec::new());
        b.add_file("p0".into(), ks(&["k1"]), Vec::new());
        assert_eq!(a, b);
        b.add_file("p1".into(), ks(&["k1"]), Vec::new());
        assert_ne!(a, b);
    }

    fn n(id: i64) -> PkKey {
        PkKey::from_values(&[PgValue::Int8(id)])
    }

    /// A seeded xorshift: the same history every run.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// `bounded` errs only safely against `exact`, the same history's
    /// without a budget: it never counts a file's live rows too high,
    /// holds no key wrongly, and finds every live key — in the map, or
    /// among the evicted files it names for it.
    fn assert_safe(bounded: &FileIndex, exact: &FileIndex) {
        let truly = exact.live_rows_per_file();
        for (path, live) in bounded.live_rows_per_file() {
            let held = truly.get(path).copied().unwrap_or(0);
            assert!(live <= held, "{path}: {live} live, holds {held}");
        }
        for pk in bounded.mapped_pks() {
            assert_eq!(bounded.lookup(&pk), exact.lookup(&pk), "{pk}");
        }
        for pk in exact.all_pks().expect("exact") {
            let holding = exact.lookup(&pk).expect("held");
            match bounded.lookup(&pk) {
                Some(path) => assert_eq!(path, holding, "{pk}"),
                None => {
                    assert!(bounded.may_contain(&pk), "{pk} taken for absent");
                    assert!(
                        bounded.candidates(&pk).contains(&holding),
                        "{pk} not in its file's range"
                    );
                }
            }
        }
    }

    /// Past its budget, the index holds what the budget does: two
    /// million keys in about a megabyte, none taken for absent.
    #[test]
    fn a_budget_bounds_the_index() {
        const BUDGET: usize = 1 << 20;
        let mut fi = FileIndex::with_memory(BUDGET);
        for f in 0..200 {
            let keys = (0..10_000).map(|i| n(f * 10_000 + i));
            fi.add_file(format!("f{f}"), keys, Vec::new());
            assert!(fi.memory_bytes() <= BUDGET, "{} bytes", fi.memory_bytes());
        }
        assert!(!fi.evicted_files().is_empty());
        for id in 0..2_000_000 {
            assert!(fi.may_contain(&n(id)), "{id}");
        }
        // Evicted oldest first.
        assert!(fi.is_evicted("f0"));
        assert!(!fi.is_evicted("f199"));
        assert_eq!(fi.all_pks(), None);
    }

    /// Over a random history of files written and keys deleted, a bounded
    /// index errs only safely ([`assert_safe`]).
    #[test]
    fn a_bounded_index_errs_only_safely() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut bounded = FileIndex::new();
        bounded.max_keys = 40;
        let mut exact = FileIndex::new();
        for step in 0..600 {
            if rng.below(3) == 0 {
                let gone: Vec<PkKey> = (0..rng.below(20))
                    .map(|_| n(rng.below(500) as i64))
                    .collect();
                bounded.remove_pks(&gone);
                exact.remove_pks(&gone);
            } else {
                // Clustered keys, as a table's files hold: a run of them.
                let start = rng.below(500) as i64;
                let keys: Vec<PkKey> = (0..rng.below(30) as i64 + 1)
                    .map(|i| n(start + i))
                    .collect();
                bounded.add_file(format!("f{step}"), keys.clone(), Vec::new());
                exact.add_file(format!("f{step}"), keys, Vec::new());
            }
            assert_safe(&bounded, &exact);
        }
        assert!(bounded.evicted > 0);
    }

    /// An equality delete of a key the index evicted goes to each
    /// partition it may be in.
    #[test]
    fn an_evicted_key_may_be_in_each_partition_holding_its_range() {
        let us = vec![PartitionLiteral::String("us".into())];
        let eu = vec![PartitionLiteral::String("eu".into())];
        let mut fi = FileIndex::new();
        fi.max_keys = 3;
        fi.add_file("us".into(), [n(1), n(5)], us.clone());
        fi.add_file("eu".into(), [n(2), n(6)], eu.clone());
        fi.add_file("later".into(), (100..104).map(n), Vec::new());
        assert!(fi.is_evicted("us") && fi.is_evicted("eu"));

        // 5 is in us, and in eu's range too.
        let partitions = fi.partitions_for_pk(&n(5));
        assert!(partitions.contains(&us.as_slice()) && partitions.contains(&eu.as_slice()));
        assert_eq!(fi.partitions_for_pk(&n(6)), vec![eu.as_slice()]);
        assert!(fi.partitions_for_pk(&n(3)).is_empty(), "never in the table");
    }
}
