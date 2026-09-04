//! Unified object-store construction for Krabka.
//!
//! Krabka's KIP-405 tiered storage, `krabka-remote-storage`, and the
//! observability blockstore, `krabka-blockstore`, share `krabka-object-store`.
//!
//! The scope is the object-store access and plumbing layer only. The crate
//! turns a typed `ObjectStoreConfig` into an `object_store::ObjectStore`
//! handle. The data representation stays in the respective consumer crates.
//! That representation is verbatim Kafka segment bytes or Parquet blocks.

mod build;
mod config;
mod error;
pub mod fault;
mod multipart;
mod ops;
mod read;
mod worm;

pub use build::build_object_store;
pub use config::{
    DEFAULT_CONNECT_TIMEOUT, DEFAULT_MAX_RETRIES, DEFAULT_MULTIPART_CHUNK_SIZE,
    DEFAULT_MULTIPART_THRESHOLD, DEFAULT_REQUEST_TIMEOUT, DEFAULT_RETRY_TIMEOUT, GcsConfig,
    ObjectStoreConfig, S3Config,
};
pub use error::ObjectStoreError;
pub use multipart::{IncompleteMultipartUpload, list_s3_multipart_uploads};
pub use ops::{ObjectOps, ObjectStoreClient, PutMode, PutOutcome, PutRequest};
pub use read::read_capped;
pub use worm::{verify_gcs_worm_bucket, verify_s3_worm_bucket};
