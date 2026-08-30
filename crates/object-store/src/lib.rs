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
mod multipart;
mod ops;
mod read;

pub use build::build_object_store;
pub use config::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, GcsConfig, ObjectStoreConfig,
    S3Config,
};
pub use error::ObjectStoreError;
pub use multipart::{IncompleteMultipartUpload, list_s3_multipart_uploads};
pub use ops::{ObjectOps, ObjectStoreClient, PutMode, PutOutcome, PutRequest};
pub use read::read_capped;
