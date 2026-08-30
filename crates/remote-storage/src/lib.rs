//! KIP-405 tiered-storage SPI and reference implementations for Krabka.
//!
//! This crate is the foundation layer for Krabka's tiered storage. It
//! defines the two plugin SPIs and the data model that they exchange, and
//! it supplies the two reference implementations that the rest of the
//! tiered-storage stack builds and tests against. It mirrors the shapes
//! of Apache Kafka's `storage-api` module
//! (`org.apache.kafka.server.log.remote.storage`).
//!
//! It also holds the write-once (WORM) archive layer, which turns an
//! object-store tier into a compliance archive: every object is a conditional
//! create, every segment copy seals a signed and hash-chained manifest, and
//! the backend refuses every delete. See
//! [Write-once archive mode](#write-once-worm-archive-mode).
//!
//! ## What this crate provides
//!
//! - [`RemoteStorageManager`] copies, fetches, and deletes segment data and
//!   indexes to and from the remote tier.
//! - [`RemoteLogMetadataManager`] stores and queries remote-segment metadata,
//!   with a strict lifecycle state machine.
//! - The data model: [`TopicIdPartition`], [`RemoteLogSegmentId`],
//!   [`RemoteLogSegmentMetadata`] / [`RemoteLogSegmentMetadataUpdate`],
//!   [`RemoteLogSegmentState`], [`LogSegmentData`], [`IndexType`],
//!   [`CustomMetadata`], and the partition-delete lifecycle
//!   ([`RemotePartitionDeleteMetadata`] / [`RemotePartitionDeleteState`]).
//! - The archive key codec ([`partition_dir_name`] / [`parse_partition_dir_name`],
//!   [`segment_file_name`] / [`parse_segment_file_name`]) and the index
//!   decoders in [`index`], which together let a reader discover and decode an
//!   archive from object storage alone.
//! - [`LocalTieredStorage`] is a filesystem [`RemoteStorageManager`].
//! - [`InmemoryRemoteLogMetadataManager`] is a process-memory
//!   [`RemoteLogMetadataManager`].
//! - [`S3RemoteStorage`] is an object-store [`RemoteStorageManager`] for S3 and
//!   for Google Cloud Storage.
//! - The write-once (WORM) archive layer: [`WormConfig`] turns the mode on
//!   through [`S3RemoteStorage::with_worm`], [`WormArchiver`] seals each
//!   [`SegmentManifest`] onto the partition chain, and [`verify_archive`]
//!   audits a finished archive into an [`ArchiveVerifyReport`].
//!
//! ## Boundary with the broker
//!
//! This crate is the SPI and reference-implementation layer. Broker-specific
//! behavior such as segment-copy scheduling, `Fetch` remote reads,
//! local-vs-remote retention policy, and topic config parsing lives in the
//! broker.
//!
//! The SPIs are intentionally **synchronous**. They mirror Kafka's
//! blocking `RemoteStorageManager` / `RemoteLogMetadataManager`, which the
//! broker drives from a thread pool. The broker wraps the calls in
//! `spawn_blocking`. Because the SPIs stay synchronous, the segment-copy and
//! segment-fetch paths here need no async runtime of their own. The archive
//! verifier is the exception: [`verify_archive`] is an `async fn` over the
//! object store, and the `krabka-worm-verify` binary starts a Tokio runtime to
//! drive it.
//!
//! ## Write-once (WORM) archive mode
//!
//! [`S3RemoteStorage::with_worm`] puts an object-store backend into archive
//! mode. Each copy writes its objects with `PutMode::Create`, records a
//! `SHA-256` digest per object in a [`SegmentManifest`], chains that manifest
//! onto the partition's previous head, and signs the head with an Ed25519 key.
//! The backend then refuses every delete, and a [`WormConfig::write_only`]
//! archive refuses every remote fetch as well.
//!
//! The bucket enforces the retention, not this crate. An operator configures S3
//! Object Lock in compliance mode with a default retention period.
//! `object_store` 0.13 models no `x-amz-object-lock-*` header, so the archive
//! layer cannot set one. What the layer adds is a writer that never deletes and
//! never overwrites, plus a chain that shows what the archive held.
//!
//! [`verify_archive`] reads a finished archive back with nothing but the
//! objects. It recomputes every chain head, checks every signature against a
//! [`TrustedManifestKeys`] set, and confirms that every object a manifest names
//! is present with the recorded size. Tail truncation is the one attack the
//! archive cannot reveal on its own, so a caller that holds an independently
//! recorded tip passes it as [`VerifyRequest::expect_head`]. The
//! `krabka-worm-verify` binary is the same check with a command line and graded
//! exit codes.
//!
//! ## Filesystem-backed remote tier
//!
//! ```no_run
//! use std::{collections::BTreeMap, path::PathBuf};
//!
//! use bytes::Bytes;
//! use krabka_ids::LeaderEpoch;
//! use krabka_remote_storage::{
//!     IndexType, LocalTieredStorage, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
//!     RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
//! };
//! use uuid::Uuid;
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let storage = LocalTieredStorage::new(PathBuf::from("/var/lib/krabka-remote"));
//! let topic_partition = TopicIdPartition::new(Uuid::new_v4(), "orders", 0);
//! let segment_id = RemoteLogSegmentId::new(topic_partition, Uuid::new_v4());
//! let mut leader_epochs = BTreeMap::new();
//! leader_epochs.insert(LeaderEpoch(0), 0);
//! let metadata = RemoteLogSegmentMetadata::new(
//!     segment_id,
//!     0,
//!     999,
//!     1_713_000_000_000,
//!     1,
//!     1_713_000_000_000,
//!     RemoteLogSegmentDetails::new(
//!         1_048_576,
//!         RemoteLogSegmentState::CopySegmentStarted,
//!         leader_epochs,
//!     ),
//! )?;
//!
//! // The broker fills these paths from a closed local log segment.
//! let segment = LogSegmentData {
//!     log_segment: PathBuf::from("/var/lib/krabka/orders-0/00000000000000000000.log"),
//!     offset_index: PathBuf::from("/var/lib/krabka/orders-0/00000000000000000000.index"),
//!     time_index: PathBuf::from("/var/lib/krabka/orders-0/00000000000000000000.timeindex"),
//!     transaction_index: None,
//!     producer_snapshot_index: None,
//!     leader_epoch_index: Bytes::new(),
//! };
//! let _custom_metadata = storage.copy_log_segment_data(&metadata, &segment)?;
//! let bytes = storage.fetch_index(&metadata, IndexType::Offset)?;
//! # let _ = bytes;
//! # Ok(())
//! # }
//! ```

#![doc(html_root_url = "https://docs.rs/krabka-remote-storage/0.5.0")]

mod cache;
pub mod dump;
mod error;
mod gcs;
pub mod index;
mod inmemory;
mod local;
mod metadata;
mod metadata_manager;
mod s3;
mod storage_manager;
mod worm;

pub use dump::{PartitionDump, RlmmCacheDump};
pub use error::RemoteStorageError;
pub use index::{
    AbortedTxnIndexEntry, BytePosition, LogOffset, OffsetIndexEntry, RelativeOffset,
    TimeIndexEntry, TimestampMs, corrupt_log, end_position_for, first_batch_at_or_after,
    first_record_at_or_after_timestamp, parse_offset_index, parse_time_index, parse_txn_index,
    position_for_relative_offset, relative_offset_floor_for_timestamp, txn_overlaps,
};
pub use inmemory::InmemoryRemoteLogMetadataManager;
pub use krabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, GcsConfig, ObjectStoreConfig,
    S3Config,
};
pub use local::LocalTieredStorage;
pub use metadata::{
    CustomMetadata, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
    RemotePartitionDeleteState, TopicIdPartition,
};
pub use metadata_manager::RemoteLogMetadataManager;
pub use s3::S3RemoteStorage;
pub use storage_manager::{
    IndexType, LOG_FILE_SUFFIX, LogSegmentData, PartitionDirName, RemoteStorageManager,
    SegmentFileName, decode_kafka_uuid, kafka_uuid, parse_partition_dir_name,
    parse_segment_file_name, partition_dir_name, segment_file_name,
};
pub use worm::{
    ArchiveVerifyReport, ChainHead, ChainStamp, EpochId, EpochSpan, HexBytes, MANIFEST_BODY_DOMAIN,
    MANIFEST_DOMAIN, MANIFEST_FORMAT_VERSION, MANIFEST_SUFFIX, MAX_MANIFEST_BYTES, ManifestBody,
    ManifestSeq, ManifestSignature, ObjectEntry, OffsetGap, PartitionVerifyReport, SealedManifest,
    SegmentIdentity, SegmentManifest, Sha256Digest, TrustedManifestKeys, VerifyBreak, VerifyDepth,
    VerifyRequest, WormArchiver, WormChainRecord, WormConfig, WormError, canonical_manifest_bytes,
    manifest_head, manifest_signing_bytes, next_chain_stamp, verify_archive,
    verify_manifest_signature,
};
