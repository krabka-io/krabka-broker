//! The shared object-store operation surface.
//!
//! This module holds an async, mockable [`ObjectOps`] trait and its single
//! concrete implementation [`ObjectStoreClient`] over `object_store`. Consumers
//! route their put, get, delete, and multipart calls through it, so the
//! operation logic lives in one place. That logic includes the
//! multipart-threshold branch and the `object_store::Error` to
//! [`ObjectStoreError`] mapping.

use bytes::Bytes;
/// Precondition for a write, re-exported so callers need not depend on
/// `object_store` directly.
pub use object_store::PutMode;
use object_store::{GetRange, ObjectMeta, path::Path};

mod client;

#[cfg(test)]
mod tests;

pub use self::client::ObjectStoreClient;
use crate::error::ObjectStoreError;

/// How one put should behave.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutRequest {
    /// Precondition for the write. Defaults to `PutMode::Overwrite`.
    pub mode: PutMode,
    /// Whether to compute a SHA-256 digest of the payload during upload.
    /// Defaults to `false`; only the WORM archive path needs it, and hashing
    /// every tiered byte is not free.
    pub digest: bool,
}

/// What a put produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// Bytes written.
    pub size_bytes: u64,
    /// Whole-object SHA-256, present only when `PutRequest::digest` was set.
    pub sha256: Option<[u8; 32]>,
    /// Backend entity tag, when the backend returned one.
    pub e_tag: Option<String>,
    /// Version id on a versioned bucket, when the backend returned one.
    pub version_id: Option<String>,
    /// Whether the backend applied an atomic create precondition to this put.
    /// Multipart uploads currently return `false`: their absence check is a
    /// replay guard, while bucket retention closes the race until completion.
    pub create_precondition: bool,
}

/// Async object-store operations. The trait is `Send + Sync`, so tasks can
/// share it.
///
/// The trait stays dyn-safe and `#[automock]`-able. It expresses multipart
/// upload as [`ObjectOps::put_from_path`] over a filesystem path, not over a
/// generic reader. The trait thus mocks cleanly for mutation-testable IO
/// decision logic.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ObjectOps: Send + Sync {
    /// Single-PUT an in-memory payload under the preconditions in `req`.
    async fn put(
        &self,
        key: &Path,
        bytes: Bytes,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError>;

    /// Upload a local file. The method uses single-PUT below `threshold`
    /// bytes, and streaming multipart in `chunk_size` parts at or above
    /// `threshold`.
    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError>;

    /// Fetch a whole object.
    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError>;

    /// Fetch a byte range of an object.
    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError>;

    /// Fetch object metadata, such as the size and the etag.
    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError>;

    /// List objects under an optional prefix.
    async fn list(&self, prefix: Option<Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError>;

    /// Delete an object.
    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError>;
}
