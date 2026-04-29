//! In-memory `BlobStore` impl for tests and the DST harness.

use async_trait::async_trait;
use bytes::Bytes;
use pg2iceberg_stream::{BlobInfo, BlobStore, Result, StreamError};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

#[derive(Debug)]
struct Entry {
    bytes: Bytes,
    last_modified_ms: i64,
}

#[derive(Default)]
pub struct MemoryBlobStore {
    inner: Mutex<BTreeMap<String, Entry>>,
    /// Monotonic counter (in milliseconds) handed out as the
    /// `last_modified_ms` for each `put`. Lets orphan-cleanup tests
    /// discriminate "old" vs "fresh" blobs without a real wall clock.
    /// Tests can also override via [`MemoryBlobStore::set_clock_ms`].
    clock_ms: AtomicI64,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test-only: enumerate every stored path.
    pub fn paths(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }

    /// Test-only: total stored byte count.
    pub fn total_bytes(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .values()
            .map(|e| e.bytes.len())
            .sum()
    }

    /// Test-only: override the in-memory clock used for `last_modified_ms`
    /// stamping. Subsequent `put`s record this exact value (and the
    /// counter advances from there).
    pub fn set_clock_ms(&self, ms: i64) {
        self.clock_ms.store(ms, Ordering::SeqCst);
    }
}

#[async_trait]
impl BlobStore for MemoryBlobStore {
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()> {
        let ms = self.clock_ms.fetch_add(1, Ordering::SeqCst);
        self.inner.lock().unwrap().insert(
            path.to_string(),
            Entry {
                bytes,
                last_modified_ms: ms,
            },
        );
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        self.inner
            .lock()
            .unwrap()
            .get(path)
            .map(|e| e.bytes.clone())
            .ok_or_else(|| StreamError::Io(format!("not found: {path}")))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobInfo>> {
        let lock = self.inner.lock().unwrap();
        Ok(lock
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| BlobInfo {
                path: k.clone(),
                size: v.bytes.len() as u64,
                last_modified_ms: v.last_modified_ms,
            })
            .collect())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        // Phantom deletes (path doesn't exist) are explicitly OK — see
        // the trait doc comment.
        self.inner.lock().unwrap().remove(path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pollster::block_on;

    #[test]
    fn put_then_get_round_trips_bytes() {
        let store = MemoryBlobStore::new();
        let payload = Bytes::from_static(b"hello");
        block_on(store.put("foo/bar.parquet", payload.clone())).unwrap();
        let got = block_on(store.get("foo/bar.parquet")).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn get_missing_path_errors() {
        let store = MemoryBlobStore::new();
        let err = block_on(store.get("does/not/exist")).unwrap_err();
        assert!(matches!(err, StreamError::Io(_)));
    }

    #[test]
    fn paths_lists_all_stored_keys() {
        let store = MemoryBlobStore::new();
        block_on(store.put("a", Bytes::from_static(b"1"))).unwrap();
        block_on(store.put("b", Bytes::from_static(b"22"))).unwrap();
        let mut paths = store.paths();
        paths.sort();
        assert_eq!(paths, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(store.total_bytes(), 3);
    }

    #[test]
    fn list_filters_by_prefix() {
        let store = MemoryBlobStore::new();
        block_on(store.put("data/a.parquet", Bytes::from_static(b"x"))).unwrap();
        block_on(store.put("data/b.parquet", Bytes::from_static(b"yy"))).unwrap();
        block_on(store.put("metadata/m.json", Bytes::from_static(b"zzz"))).unwrap();

        let mut got = block_on(store.list("data/")).unwrap();
        got.sort_by_key(|b| b.path.clone());
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].path, "data/a.parquet");
        assert_eq!(got[0].size, 1);
        assert_eq!(got[1].path, "data/b.parquet");
        assert_eq!(got[1].size, 2);
    }

    #[test]
    fn list_assigns_increasing_last_modified_ms() {
        let store = MemoryBlobStore::new();
        store.set_clock_ms(100);
        block_on(store.put("a", Bytes::from_static(b"1"))).unwrap();
        block_on(store.put("b", Bytes::from_static(b"2"))).unwrap();
        let infos = block_on(store.list("")).unwrap();
        let by_path: std::collections::BTreeMap<String, i64> = infos
            .into_iter()
            .map(|b| (b.path, b.last_modified_ms))
            .collect();
        assert_eq!(by_path.get("a"), Some(&100));
        assert_eq!(by_path.get("b"), Some(&101));
    }

    #[test]
    fn delete_removes_path_and_phantom_delete_is_noop() {
        let store = MemoryBlobStore::new();
        block_on(store.put("k", Bytes::from_static(b"v"))).unwrap();
        block_on(store.delete("k")).unwrap();
        assert!(block_on(store.get("k")).is_err());
        // Phantom delete on missing path is fine.
        block_on(store.delete("never-existed")).unwrap();
    }
}
