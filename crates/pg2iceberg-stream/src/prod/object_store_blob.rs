//! `BlobStore` impl over `object_store::ObjectStore`.
//!
//! Production binary constructs an `Arc<dyn ObjectStore>` (e.g. via
//! `object_store::aws::AmazonS3Builder` for S3, `object_store::local::LocalFileSystem`
//! for tests, etc.) and wraps it in `ObjectStoreBlobStore`. Everything
//! downstream stays trait-bound.

use crate::{BlobInfo, BlobStore, Result, StreamError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

pub struct ObjectStoreBlobStore {
    inner: Arc<dyn ObjectStore>,
}

impl ObjectStoreBlobStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Borrow the underlying `ObjectStore`. Useful for callers that need
    /// features beyond put/get (multipart, listing) without going through
    /// our narrowed trait.
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }
}

fn parse_path(input: &str) -> Result<Path> {
    Path::parse(input).map_err(|e| StreamError::Io(format!("invalid object path {input:?}: {e}")))
}

#[async_trait]
impl BlobStore for ObjectStoreBlobStore {
    async fn put(&self, path: &str, bytes: Bytes) -> Result<()> {
        let p = parse_path(path)?;
        self.inner
            .put(&p, bytes.into())
            .await
            .map_err(|e| StreamError::Io(format!("object_store put {path:?}: {e}")))?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let p = parse_path(path)?;
        let result = self
            .inner
            .get(&p)
            .await
            .map_err(|e| StreamError::Io(format!("object_store get {path:?}: {e}")))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| StreamError::Io(format!("object_store get bytes {path:?}: {e}")))?;
        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobInfo>> {
        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(parse_path(prefix.trim_end_matches('/'))?)
        };
        let mut stream = self.inner.list(prefix_path.as_ref());
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta =
                item.map_err(|e| StreamError::Io(format!("object_store list {prefix:?}: {e}")))?;
            out.push(BlobInfo {
                path: meta.location.to_string(),
                size: meta.size as u64,
                last_modified_ms: meta.last_modified.timestamp_millis(),
            });
        }
        Ok(out)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let p = parse_path(path)?;
        match self.inner.delete(&p).await {
            Ok(()) => Ok(()),
            // Phantom delete (path doesn't exist) is a no-op per the
            // BlobStore contract.
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(StreamError::Io(format!(
                "object_store delete {path:?}: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use pollster::block_on;

    fn store() -> ObjectStoreBlobStore {
        ObjectStoreBlobStore::new(Arc::new(InMemory::new()))
    }

    #[test]
    fn put_then_get_round_trips_bytes() {
        let s = store();
        let payload = Bytes::from_static(b"hello world");
        block_on(s.put("dir/foo.parquet", payload.clone())).unwrap();
        let got = block_on(s.get("dir/foo.parquet")).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn get_missing_returns_io_error() {
        let s = store();
        let err = block_on(s.get("does/not/exist")).unwrap_err();
        assert!(matches!(err, StreamError::Io(_)));
    }

    #[test]
    fn put_overwrites_prior_value() {
        let s = store();
        block_on(s.put("k", Bytes::from_static(b"first"))).unwrap();
        block_on(s.put("k", Bytes::from_static(b"second"))).unwrap();
        let got = block_on(s.get("k")).unwrap();
        assert_eq!(&got[..], b"second");
    }

    #[test]
    fn invalid_path_rejected() {
        let s = store();
        // object_store::path::Path doesn't allow leading slashes followed
        // by null/control chars; pick something pathological. The exact
        // failure depends on the upstream parser, but any error variant
        // routes to StreamError::Io.
        let err = block_on(s.put("\0bad", Bytes::from_static(b"x"))).unwrap_err();
        assert!(matches!(err, StreamError::Io(_)));
    }

    #[test]
    fn nested_paths_round_trip() {
        let s = store();
        let nested = "warehouse/db/table/data/snap-1/00000.parquet";
        block_on(s.put(nested, Bytes::from_static(b"data"))).unwrap();
        let got = block_on(s.get(nested)).unwrap();
        assert_eq!(&got[..], b"data");
    }

    #[test]
    fn empty_bytes_round_trip() {
        let s = store();
        block_on(s.put("k", Bytes::new())).unwrap();
        let got = block_on(s.get("k")).unwrap();
        assert!(got.is_empty());
    }
}
