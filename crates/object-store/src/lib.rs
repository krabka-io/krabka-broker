//! Unified object-store construction for Krabka.
//!
//! Krabka's KIP-405 tiered storage, `krabka-remote-storage`, and the
//! observability blockstore, `krabka-blockstore`, share `krabka-object-store`.
//!
//! The scope is the object-store access and plumbing layer only. The crate
//! turns a typed `ObjectStoreConfig` into an `object_store::ObjectStore`
//! handle. The data representation stays in the respective consumer crates.
//! That representation is verbatim Kafka segment bytes or Parquet blocks.
//!
//! On `wasm32-wasip1` the crate has only the in-memory backend. The S3, GCS
//! and local-filesystem backends need an HTTP client stack or a filesystem
//! walk that `object_store` does not build for that target, so their
//! configurations fail with [`ObjectStoreError::InvalidConfig`] there, and so
//! do the WORM bucket checks. The multipart-upload listing is not exported.

mod build;
mod config;
mod error;
pub mod fault;
#[cfg(not(target_family = "wasm"))]
mod multipart;
mod ops;
mod read;
#[cfg(target_family = "wasm")]
mod unavailable;
#[cfg(not(target_family = "wasm"))]
mod worm;

pub use build::build_object_store;
pub use config::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAX_RETRIES, DEFAULT_MULTIPART_CHUNK_SIZE,
    DEFAULT_MULTIPART_THRESHOLD, DEFAULT_REQUEST_TIMEOUT, DEFAULT_RETRY_TIMEOUT, GcsConfig,
    ObjectStoreConfig, S3Config,
};
pub use error::ObjectStoreError;
#[cfg(not(target_family = "wasm"))]
pub use multipart::{IncompleteMultipartUpload, list_s3_multipart_uploads};
pub use ops::{ObjectOps, ObjectStoreClient, PutMode, PutOutcome, PutRequest};
pub use read::read_capped;
#[cfg(target_family = "wasm")]
pub use unavailable::{verify_gcs_worm_bucket, verify_s3_worm_bucket};
#[cfg(not(target_family = "wasm"))]
pub use worm::{verify_gcs_worm_bucket, verify_s3_worm_bucket};
