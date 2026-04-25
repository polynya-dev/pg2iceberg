//! Production-impl glue for the `BlobStore` trait.
//!
//! Wraps [`object_store::ObjectStore`] — the upstream abstraction for
//! S3/GCS/Azure/local-fs/HTTP. We expose it through our narrower
//! [`crate::BlobStore`] trait so the rest of the codebase doesn't need to
//! depend on `object_store` types directly.
//!
//! Allowlisted to use real IO via `crates/pg2iceberg-stream/src/prod/`
//! per the banned-calls CI script.

mod object_store_blob;

pub use object_store_blob::ObjectStoreBlobStore;
