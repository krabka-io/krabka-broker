//! The `wasm32-wasip1` stand-ins for the calls that need a cloud backend.
//!
//! `object_store` builds neither its HTTP client nor its filesystem backend for
//! that target. The functions here keep the native signatures, so the callers
//! build unchanged, and they answer every request with the error that
//! [`unavailable`] makes.

use crate::{GcsConfig, ObjectStoreError, S3Config};

/// The error for a backend that this platform cannot reach.
pub(crate) fn unavailable(backend: &str) -> ObjectStoreError {
    ObjectStoreError::InvalidConfig(format!(
        "the {backend} object store is unavailable on this platform"
    ))
}

/// Confirms that an S3 bucket can protect multipart WORM objects.
///
/// # Errors
///
/// Always returns [`ObjectStoreError::InvalidConfig`], because this platform
/// has no S3 client.
pub async fn verify_s3_worm_bucket(_cfg: &S3Config) -> Result<(), ObjectStoreError> {
    Err(unavailable("S3"))
}

/// Confirms that a GCS bucket can protect WORM objects.
///
/// # Errors
///
/// Always returns [`ObjectStoreError::InvalidConfig`], because this platform
/// has no GCS client.
pub async fn verify_gcs_worm_bucket(_cfg: &GcsConfig) -> Result<(), ObjectStoreError> {
    Err(unavailable("GCS"))
}
