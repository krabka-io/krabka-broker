//! [`S3RemoteStorage`] is an S3-compatible object-store
//! [`RemoteStorageManager`], the KIP-405 production backend.
//!
//! This backend is built on the `object_store` crate, so it works against
//! any `S3-API` endpoint: AWS S3, `MinIO`, and Cloudflare R2. Google Cloud
//! Storage has a dedicated native backend. See
//! [`from_gcs_config`](S3RemoteStorage::from_gcs_config) in [`crate::gcs`],
//! which supports keyless GKE Workload Identity instead of the legacy
//! S3-compatibility/HMAC shim.
//!
//! The trait method bodies are synchronous, and they mirror Kafka's blocking
//! `RemoteStorageManager`. The broker drives them from `spawn_blocking`.
//! Inside them this crate blocks on the async `object_store` calls with the
//! current Tokio runtime handle. That handle is always present inside a
//! `spawn_blocking` worker that Tokio spawned.
//!
//! ## Object-key layout
//!
//! ```text
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.log
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.index
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.timeindex
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.snapshot
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.leader_epoch_checkpoint
//! <prefix?>/<topic>-<partition>-<topic_id_base64>/<base_offset>-<segment_id_base64>.txnindex
//! ```
//!
//! Keys mirror [`LocalTieredStorage`](crate::LocalTieredStorage)'s
//! directory layout so the two backends are observationally equivalent.

use std::sync::Arc;

use bytes::Bytes;
use krabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectOps, ObjectStoreClient,
    ObjectStoreConfig, ObjectStoreError, PutMode, PutOutcome, PutRequest, S3Config,
    build_object_store,
};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};
use object_store::{GetRange, ObjectStore, path::Path as ObjectPath};
use tracing::instrument;

use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
    storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
    worm::{MANIFEST_SUFFIX, ObjectEntry, Sha256Digest, WormArchiver, WormConfig, WormError},
};

/// A [`RemoteStorageManager`] backed by any S3-compatible object store.
///
/// Construct it with [`S3RemoteStorage::with_store`], which accepts any
/// `ObjectStore` impl, for in-process tests. Use
/// [`S3RemoteStorage::from_s3_config`] for the production path, which builds
/// an `AmazonS3` client from credentials, endpoint, and bucket.
pub struct S3RemoteStorage {
    ops: ObjectStoreClient,
    /// Optional key prefix. The store joins it to every object key with
    /// `/`. This lets multiple Krabka clusters share a bucket safely.
    prefix: Option<String>,
    /// File-size threshold above which uploads switch to S3 multipart.
    multipart_threshold: ByteSize,
    /// Per-part size used by the multipart path.
    multipart_chunk_size: ByteSize,
    /// `Some` when this backend is a write-once archive.
    worm: Option<WormMode>,
}

/// Resolved WORM state: the archiver (which owns the loaded key) plus the
/// credential posture.
struct WormMode {
    archiver: WormArchiver,
    write_only: bool,
}

/// Where one copied object's body comes from.
///
/// The two arms differ in more than the source: a file goes through the
/// multipart threshold, an in-memory payload is always a single PUT. Naming
/// them lets the copy walk one ordered list instead of six near-identical
/// call sites, which is what makes collecting a digest per object cheap.
enum ObjectBody<'a> {
    /// A file on disk.
    Path(&'a std::path::Path),
    /// An in-memory payload.
    Memory(Bytes),
}

/// Turns one completed upload into the manifest entry that records it.
///
/// # Errors
///
/// [`WormError::MissingDigest`] when the put reported no `SHA-256`. An entry
/// with no digest makes no integrity claim, so the copy fails rather than
/// write a manifest that only looks like a proof.
fn object_entry(
    suffix: &str,
    key: &ObjectPath,
    outcome: PutOutcome,
) -> Result<ObjectEntry, WormError> {
    let sha256 = outcome.sha256.ok_or_else(|| WormError::MissingDigest {
        key: key.to_string(),
    })?;
    Ok(ObjectEntry {
        suffix: suffix.to_string(),
        key: key.to_string(),
        size_bytes: outcome.size_bytes,
        sha256: Sha256Digest(sha256),
        e_tag: outcome.e_tag,
        version_id: outcome.version_id,
    })
}

/// Lifts a raw byte count from [`krabka_object_store`]'s config layer into
/// the dimensioned domain. That config layer still uses primitive types.
///
/// The conversion saturates and does not wrap. A `usize` above `u64::MAX`
/// cannot occur on any target Krabka builds for.
pub(crate) fn size_from_usize(bytes: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(bytes).unwrap_or(u64::MAX))
}

impl std::fmt::Debug for S3RemoteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3RemoteStorage")
            .field("prefix", &self.prefix)
            // Mode only. The archiver holds live private-key material, so it
            // never reaches a formatter.
            .field("worm", &self.worm.is_some())
            .field(
                "write_only",
                &self.worm.as_ref().is_some_and(|worm| worm.write_only),
            )
            .finish_non_exhaustive()
    }
}

impl S3RemoteStorage {
    /// Wraps an arbitrary `ObjectStore`, for example
    /// `object_store::memory::InMemory` for tests. Use
    /// [`Self::from_s3_config`] for the production S3 path. Multipart
    /// tuning falls back to the [`DEFAULT_MULTIPART_THRESHOLD`] and
    /// [`DEFAULT_MULTIPART_CHUNK_SIZE`] constants. Call
    /// [`Self::with_multipart_tuning`] to override them in tests.
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self {
            ops: ObjectStoreClient::new(store),
            prefix,
            multipart_threshold: ByteSize::from_bytes(DEFAULT_MULTIPART_THRESHOLD),
            multipart_chunk_size: size_from_usize(DEFAULT_MULTIPART_CHUNK_SIZE),
            worm: None,
        }
    }

    /// Puts this backend into WORM archive mode.
    ///
    /// Every copy then seals a signed, chained `.manifest` beside the segment,
    /// every delete is refused, and a `write_only` archive refuses remote
    /// fetches as well.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::Worm`] when `cfg`'s signing key cannot be
    /// loaded.
    pub fn with_worm(mut self, cfg: &WormConfig) -> Result<Self, RemoteStorageError> {
        self.worm = Some(WormMode {
            archiver: WormArchiver::from_config(cfg)?,
            write_only: cfg.write_only,
        });
        Ok(self)
    }

    /// Overrides the multipart threshold and chunk size. Returns `self` for
    /// chained calls. Tests use this to force the multipart path on small
    /// fixtures. Production usually keeps the defaults.
    #[must_use]
    pub fn with_multipart_tuning(mut self, threshold: ByteSize, chunk_size: ByteSize) -> Self {
        self.multipart_threshold = threshold;
        self.multipart_chunk_size = chunk_size;
        self
    }

    /// Builds an `AmazonS3` client from `cfg` and wraps it.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if `object_store`'s
    /// builder rejects the bucket, region, and endpoint combination.
    pub fn from_s3_config(cfg: &S3Config) -> Result<Self, RemoteStorageError> {
        let store = build_object_store(&ObjectStoreConfig::S3(cfg.clone()))
            .map_err(|e| RemoteStorageError::InvalidArgument(e.to_string()))?;
        Ok(
            Self::with_store(store, cfg.prefix.clone()).with_multipart_tuning(
                ByteSize::from_bytes(cfg.multipart_threshold),
                size_from_usize(cfg.multipart_chunk_size),
            ),
        )
    }

    fn segment_key(&self, metadata: &RemoteLogSegmentMetadata, suffix: &str) -> ObjectPath {
        let mut key = String::new();
        if let Some(p) = &self.prefix {
            key.push_str(p);
            key.push('/');
        }
        key.push_str(&crate::storage_manager::partition_dir_name(metadata));
        key.push('/');
        key.push_str(&crate::storage_manager::segment_file_name(metadata, suffix));
        ObjectPath::from(key)
    }

    fn log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.segment_key(metadata, ".log")
    }

    fn index_key(&self, metadata: &RemoteLogSegmentMetadata, index_type: IndexType) -> ObjectPath {
        self.segment_key(metadata, index_type.suffix())
    }

    /// Krabka 0.3.8 and earlier used UUID directories and extensionless
    /// artifact names. New writes use Kafka's layout; reads and deletes keep
    /// the old keys reachable during upgrades.
    fn legacy_segment_key(&self, metadata: &RemoteLogSegmentMetadata, name: &str) -> ObjectPath {
        use std::fmt::Write as _;

        let id = metadata.remote_log_segment_id();
        let tp = &id.topic_id_partition;
        let mut key = String::new();
        if let Some(prefix) = &self.prefix {
            key.push_str(prefix);
            key.push('/');
        }
        write!(key, "{}_{}/{}/{name}", tp.topic_id, tp.partition, id.id)
            .expect("writing to String cannot fail");
        ObjectPath::from(key)
    }

    fn legacy_log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.legacy_segment_key(metadata, "log")
    }

    fn legacy_index_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> ObjectPath {
        let name = match index_type {
            IndexType::Offset => "offset_index",
            IndexType::Timestamp => "time_index",
            IndexType::ProducerSnapshot => "producer_snapshot",
            IndexType::LeaderEpoch => "leader_epoch",
            IndexType::Transaction => "txn_index",
        };
        self.legacy_segment_key(metadata, name)
    }

    /// Refuses a remote read when the archive is configured write-only.
    ///
    /// Callers must invoke this before they derive a key or reach the store,
    /// so that no request the archive is going to reject ever leaves the
    /// process.
    fn refuse_read_when_write_only(&self) -> Result<(), RemoteStorageError> {
        if self.worm.as_ref().is_some_and(|worm| worm.write_only) {
            return Err(RemoteStorageError::Worm(WormError::ReadRefused));
        }
        Ok(())
    }

    /// Runs an async [`ObjectOps`] call to completion on the current Tokio
    /// runtime. Sync trait callers reach this through `spawn_blocking`, where
    /// `Handle::current()` is always available. The `block_on` bridge lives
    /// here, never in the substrate.
    fn block_os<T, F>(fut: F) -> Result<T, ObjectStoreError>
    where
        F: std::future::Future<Output = Result<T, ObjectStoreError>>,
    {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ObjectStoreError::Backend(
                "S3RemoteStorage requires an active Tokio runtime; call from spawn_blocking".into(),
            )
        })?;
        tokio::task::block_in_place(|| handle.block_on(fut))
    }
}

impl RemoteStorageManager for S3RemoteStorage {
    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_offset = metadata.start_offset(),
            end_offset = metadata.end_offset(),
        ),
        err
    )]
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        // `ObjectOps` is a primitive-typed substrate: hand it raw counts.
        let threshold = self.multipart_threshold.bytes_u64();
        let chunk_size = self.multipart_chunk_size.bytes_usize();
        let worm = self.worm.as_ref();
        // One request shape for every data object of this copy. A WORM copy
        // writes each key once and hashes the body on the way past.
        //
        // `PutMode::Create` binds only below `multipart_threshold`:
        // object_store 0.13's `PutMultipartOptions` carries no mode and
        // `MultipartUpload::complete` takes no precondition, so a large `.log`
        // body degrades to an unconditional multipart put and depends on the
        // bucket's Object Lock policy for its write-once guarantee. The
        // manifest is always small, so its create is always conditional.
        let put = if worm.is_some() {
            PutRequest {
                mode: PutMode::Create,
                digest: true,
            }
        } else {
            PutRequest::default()
        };

        let mut uploads = vec![
            (
                ".log",
                self.log_key(metadata),
                ObjectBody::Path(&data.log_segment),
            ),
            (
                IndexType::Offset.suffix(),
                self.index_key(metadata, IndexType::Offset),
                ObjectBody::Path(&data.offset_index),
            ),
            (
                IndexType::Timestamp.suffix(),
                self.index_key(metadata, IndexType::Timestamp),
                ObjectBody::Path(&data.time_index),
            ),
        ];
        if let Some(snap) = &data.producer_snapshot_index {
            uploads.push((
                IndexType::ProducerSnapshot.suffix(),
                self.index_key(metadata, IndexType::ProducerSnapshot),
                ObjectBody::Path(snap),
            ));
        }
        uploads.push((
            IndexType::LeaderEpoch.suffix(),
            self.index_key(metadata, IndexType::LeaderEpoch),
            ObjectBody::Memory(data.leader_epoch_index.clone()),
        ));
        if let Some(txn) = &data.transaction_index {
            uploads.push((
                IndexType::Transaction.suffix(),
                self.index_key(metadata, IndexType::Transaction),
                ObjectBody::Path(txn),
            ));
        }

        // Only the WORM path reads this list. An empty `Vec` allocates
        // nothing, so the default path pays nothing to declare it.
        let mut objects = Vec::new();
        for (suffix, key, body) in uploads {
            let outcome = match body {
                ObjectBody::Path(path) => Self::block_os(self.ops.put_from_path(
                    &key,
                    path,
                    threshold,
                    chunk_size,
                    put.clone(),
                ))?,
                ObjectBody::Memory(bytes) => {
                    Self::block_os(self.ops.put(&key, bytes, put.clone()))?
                }
            };
            if worm.is_some() {
                objects.push(object_entry(suffix, &key, outcome)?);
            }
        }

        let Some(worm) = worm else {
            // Outside WORM mode the opaque CustomMetadata channel is unused —
            // every object's key is derivable from the segment metadata, so we
            // don't need to echo a separate identifier back.
            return Ok(None);
        };

        let sealed = worm.archiver.seal(metadata, objects)?;
        // The manifest goes last, deliberately: it is the commit point of the
        // copy. A crash part-way through then leaves data objects that no
        // manifest names, and a verifier reports them as orphans. Writing it
        // first would instead leave a manifest naming objects that do not
        // exist, which reads as a broken chain — far worse to meet in an
        // audit than a few unreferenced blobs.
        let manifest_put = Self::block_os(self.ops.put(
            &self.segment_key(metadata, MANIFEST_SUFFIX),
            sealed.bytes,
            PutRequest {
                mode: PutMode::Create,
                digest: false,
            },
        ))?;
        Ok(Some(
            sealed
                .receipt
                .with_manifest_version(manifest_put.version_id)
                .to_custom_metadata(),
        ))
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_position,
            end_position = ?end_position,
        ),
        err
    )]
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.refuse_read_when_write_only()?;
        let key = self.log_key(metadata);
        if let Some(end) = end_position
            && end < start_position
        {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "end_position {end} < start_position {start_position}"
            )));
        }
        let range = || match end_position {
            Some(end) => {
                // GetRange::Bounded is half-open [start, end); the trait
                // contract is inclusive end, so add 1 and saturate.
                GetRange::Bounded(u64::from(start_position)..u64::from(end).saturating_add(1))
            }
            None => GetRange::Offset(u64::from(start_position)),
        };
        let result = match Self::block_os(self.ops.get_range(&key, range())) {
            Err(ObjectStoreError::NotFound(_)) => {
                Self::block_os(self.ops.get_range(&self.legacy_log_key(metadata), range()))
            }
            result => result,
        };
        match result {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            )),
            Err(other) => Err(other.into()),
        }
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            index_type = ?index_type,
        ),
        err
    )]
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.refuse_read_when_write_only()?;
        let key = self.index_key(metadata, index_type);
        let result = match Self::block_os(self.ops.get(&key)) {
            Err(ObjectStoreError::NotFound(_)) => {
                Self::block_os(self.ops.get(&self.legacy_index_key(metadata, index_type)))
            }
            result => result,
        };
        match result {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            )),
            Err(other) => Err(other.into()),
        }
    }

    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
        ),
        err
    )]
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        if self.worm.is_some() {
            // The backstop that makes the write-once guarantee hold against
            // any caller. No delete is issued at all, not even for an object
            // that is absent: a store that is never asked cannot be talked
            // into obliging.
            return Err(RemoteStorageError::Worm(WormError::DeleteRefused {
                key: self.log_key(metadata).to_string(),
            }));
        }
        for key in [
            self.log_key(metadata),
            self.index_key(metadata, IndexType::Offset),
            self.index_key(metadata, IndexType::Timestamp),
            self.index_key(metadata, IndexType::ProducerSnapshot),
            self.index_key(metadata, IndexType::LeaderEpoch),
            self.index_key(metadata, IndexType::Transaction),
            self.legacy_log_key(metadata),
            self.legacy_index_key(metadata, IndexType::Offset),
            self.legacy_index_key(metadata, IndexType::Timestamp),
            self.legacy_index_key(metadata, IndexType::ProducerSnapshot),
            self.legacy_index_key(metadata, IndexType::LeaderEpoch),
            self.legacy_index_key(metadata, IndexType::Transaction),
        ] {
            match Self::block_os(self.ops.delete(&key)) {
                // Idempotent: deleting an absent object succeeds.
                Ok(()) | Err(ObjectStoreError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Write, path::PathBuf};

    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_units::prelude::{kibibytes, mebibytes};
    use object_store::memory::InMemory;
    use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata::{
            RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
        },
        worm::{
            ChainHead, ChainStamp, EpochId, MANIFEST_FORMAT_VERSION, ManifestSeq, SegmentIdentity,
            SegmentManifest, WormChainRecord, manifest_head, verify_manifest_signature,
        },
    };

    fn rsm(prefix: Option<&str>) -> S3RemoteStorage {
        S3RemoteStorage::with_store(Arc::new(InMemory::new()), prefix.map(str::to_string))
    }

    /// The multipart tunables cross two seams: in from
    /// [`krabka_object_store`]'s primitive config, and back out to the
    /// primitive-typed `ObjectOps` substrate. Both must be lossless for every
    /// size the config can express, so a mis-scaled conversion, such as a
    /// stray `* 1024`, cannot silently change when a segment switches to
    /// multipart.
    #[test]
    fn multipart_tuning_round_trips_through_the_primitive_seams() {
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None);
        check!(store.multipart_threshold.bytes_u64() == DEFAULT_MULTIPART_THRESHOLD);
        check!(store.multipart_chunk_size.bytes_usize() == DEFAULT_MULTIPART_CHUNK_SIZE);

        let tuned = store.with_multipart_tuning(mebibytes(64), kibibytes(512));
        check!(tuned.multipart_threshold.bytes_u64() == 64 * 1024 * 1024);
        check!(tuned.multipart_chunk_size.bytes_usize() == 512 * 1024);
    }

    /// `usize::MAX` has no `u64` image on a hypothetical 128-bit target. The
    /// lift saturates and does not wrap to a tiny chunk size.
    #[test]
    fn size_from_usize_saturates_instead_of_wrapping() {
        check!(size_from_usize(0).bytes_usize() == 0);
        check!(size_from_usize(usize::MAX).bytes_usize() == usize::MAX);
    }

    #[test]
    fn storage_debug_is_nonempty() {
        // The S3RemoteStorage Debug impl must render something (a `fmt`
        // replaced with `Ok(())` would print nothing).
        let dbg = format!("{:?}", rsm(None));
        assert!(dbg.contains("S3RemoteStorage"));
    }

    fn sample_metadata(id: u128) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
                Uuid::from_u128(id),
            ),
            0,
            99,
            123,
            1,
            456,
            crate::metadata::RemoteLogSegmentDetails::new(
                8,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .unwrap()
    }

    fn write_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> PathBuf {
        let p = dir.join(name);
        std::fs::File::create(&p)
            .unwrap()
            .write_all(contents)
            .unwrap();
        p
    }

    fn sample_data(src: &std::path::Path, with_txn: bool) -> LogSegmentData {
        LogSegmentData {
            log_segment: write_file(src, "00.log", b"0123456789"),
            offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
            time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
            transaction_index: with_txn.then(|| write_file(src, "00.txnindex", b"TXN-IDX")),
            producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn copy_then_fetch_full_segment() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            assert!(store.fetch_log_segment(&md, 0, None).unwrap() == b"0123456789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_partial_byte_ranges() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), false))
                .unwrap();
            // Inclusive [2, 5] -> "2345".
            assert!(store.fetch_log_segment(&md, 2, Some(5)).unwrap() == b"2345");
            // Open-ended from 7 -> "789".
            assert!(store.fetch_log_segment(&md, 7, None).unwrap() == b"789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_single_byte_range_start_equals_end() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(10);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), false))
                .unwrap();
            // Inclusive [3, 3] is a valid single-byte range -> "3" (the guard
            // is `end < start_position`, not `<=`/`==`).
            assert!(store.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_each_index_type() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(11);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            for (index_type, want) in [
                (IndexType::Offset, b"OFFSET-IDX".as_ref()),
                (IndexType::Timestamp, b"TIME-IDX".as_ref()),
                (IndexType::ProducerSnapshot, b"SNAP".as_ref()),
                (IndexType::LeaderEpoch, b"EPOCH-BYTES".as_ref()),
                (IndexType::Transaction, b"TXN-IDX".as_ref()),
            ] {
                check!(
                    store.fetch_index(&md, index_type).unwrap() == want,
                    "{index_type:?}"
                );
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_before_copy_is_not_found() {
        let store = rsm(None);
        let md = sample_metadata(404);
        let err = tokio::task::spawn_blocking(move || store.fetch_log_segment(&md, 0, None))
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_optional_txn_index_is_not_found() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(12);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), false))
                .unwrap();
            let err = store.fetch_index(&md, IndexType::Transaction).unwrap_err();
            assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_and_deletes_pre_kafka_layout_objects() {
        let store = rsm(None);
        let md = sample_metadata(12);
        tokio::task::spawn_blocking(move || {
            S3RemoteStorage::block_os(store.ops.put(
                &store.legacy_log_key(&md),
                Bytes::from_static(b"legacy-log"),
                PutRequest::default(),
            ))
            .unwrap();
            S3RemoteStorage::block_os(store.ops.put(
                &store.legacy_index_key(&md, IndexType::ProducerSnapshot),
                Bytes::from_static(b"legacy-snapshot"),
                PutRequest::default(),
            ))
            .unwrap();

            check!(store.fetch_log_segment(&md, 0, None).unwrap() == b"legacy-log");
            check!(
                store.fetch_index(&md, IndexType::ProducerSnapshot).unwrap() == b"legacy-snapshot"
            );
            store.delete_log_segment_data(&md).unwrap();
            check!(matches!(
                store.fetch_log_segment(&md, 0, None),
                Err(RemoteStorageError::SegmentNotFound(_))
            ));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_is_idempotent() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(13);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            store.delete_log_segment_data(&md).unwrap();
            store.delete_log_segment_data(&md).unwrap();
            assert!(matches!(
                store.fetch_log_segment(&md, 0, None).unwrap_err(),
                RemoteStorageError::SegmentNotFound(_)
            ));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_are_isolated_by_id() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let a = sample_metadata(20);
        let b = sample_metadata(21);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&a, &sample_data(src.path(), false))
                .unwrap();
            store
                .copy_log_segment_data(&b, &sample_data(src.path(), false))
                .unwrap();
            store.delete_log_segment_data(&a).unwrap();
            assert!(store.fetch_log_segment(&b, 0, None).unwrap() == b"0123456789");
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefix_isolates_clusters() {
        let store_a =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("cluster-a".to_string()));
        let _ = store_a;
        // Single cluster keys live under the prefix; we verify the key
        // construction at the unit level (no cross-cluster fixture
        // available without sharing the InMemory backend, which we don't
        // because each cluster gets its own bucket in practice).
        let md = sample_metadata(30);
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("c".to_string()));
        let key = store.log_key(&md);
        let expected = concat!(
            "c/orders-0-AAAAAAAAAAAAAAAAAAAAAQ/",
            "00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log"
        );
        assert!(
            key.as_ref() == expected,
            "unexpected Kafka-compatible object key {key:?}",
        );
    }

    fn write_log_segment(dir: &std::path::Path, len: usize) -> PathBuf {
        let p = dir.join("00.log");
        let mut f = std::fs::File::create(&p).unwrap();
        // Deterministic, position-sensitive bytes so the round-trip
        // assertion catches both reordering bugs and truncation.
        let bytes: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
        f.write_all(&bytes).unwrap();
        p
    }

    /// Files at or above `multipart_threshold` flow through the `ObjectOps`
    /// multipart path. This test picks a chunk size that gives multiple
    /// non-trailing parts, so it exercises the inner loop's tail-flush and
    /// finish path. The `InMemory` backend implements `put_multipart` and
    /// `complete` end-to-end, so a successful round-trip proves that the
    /// multipart wire calls are correct, including the per-part offsets and
    /// the final concatenation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_path_uses_multipart_above_threshold_and_round_trips() {
        // 100 KiB segment, 8 KiB threshold → multipart, 4 KiB chunks
        // → 25 parts (last one full, no tail).
        let seg_len = kibibytes(100).bytes_usize();
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_multipart_tuning(kibibytes(8), kibibytes(4));
        let src = TempDir::new().unwrap();
        let md = sample_metadata(40);
        let log_path = write_log_segment(src.path(), seg_len);
        let data = LogSegmentData {
            log_segment: log_path,
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: Some(write_file(src.path(), "00.snapshot", b"SNAP")),
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
            assert!(fetched.len() == seg_len);
            for (i, b) in fetched.iter().enumerate() {
                assert!(*b == u8::try_from(i % 251).unwrap(), "byte mismatch at {i}");
            }
        })
        .await
        .unwrap();
    }

    /// Multipart path with a tail chunk strictly smaller than `chunk_size`.
    /// `WriteMultipart::finish` flushes the partially-filled buffer as the
    /// final part, and this test asserts that it does. If it did not, the
    /// uploaded object would silently lose the last `tail_len` bytes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multipart_flushes_partial_tail_chunk() {
        let chunk = kibibytes(4);
        let seg_len = 3 * chunk.bytes_usize() + 137; // 3 full parts + tail
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_multipart_tuning(kibibytes(1), chunk);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(41);
        let log_path = write_log_segment(src.path(), seg_len);
        let data = LogSegmentData {
            log_segment: log_path,
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
            assert!(fetched.len() == seg_len);
            assert!(
                fetched.last().copied() == Some(u8::try_from((seg_len - 1) % 251).unwrap()),
                "tail byte was dropped"
            );
        })
        .await
        .unwrap();
    }

    /// Files strictly below the threshold MUST still take the single-PUT
    /// path, even when multipart tuning is configured. This test raises the
    /// threshold above the fixture size. A regression that inverted the
    /// branch would show as a hang, or as a multipart-specific error against
    /// a backend with no multipart support. It would also be a latency
    /// regression in production.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_path_stays_on_single_put_below_threshold() {
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_multipart_tuning(mebibytes(1), kibibytes(4));
        let src = TempDir::new().unwrap();
        let md = sample_metadata(42);
        let log_path = write_log_segment(src.path(), 10); // ten bytes, well under 1 MiB
        let data = LogSegmentData {
            log_segment: log_path,
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let fetched = store.fetch_log_segment(&md, 0, None).unwrap();
            assert!(fetched.len() == 10);
        })
        .await
        .unwrap();
    }

    // ---- WORM archive mode -------------------------------------------------

    const WORM_KEY_ID: &str = "s3-worm-key";

    /// The chain epoch every stamped fixture in this module belongs to.
    fn worm_epoch() -> EpochId {
        EpochId(Uuid::from_u128(0x5eed))
    }

    /// A [`WormConfig`] naming a throwaway PKCS#8 Ed25519 key written into
    /// `dir`. `ring` mints it because `krabka-audit` exposes no key generator.
    fn worm_config(dir: &std::path::Path, write_only: bool) -> WormConfig {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let path = dir.join("worm.pk8");
        std::fs::write(&path, pkcs8.as_ref()).unwrap();
        WormConfig {
            signing_key_path: Some(path),
            signing_key_id: Some(WORM_KEY_ID.to_string()),
            write_only,
        }
    }

    /// An archive backed by `store`, signing with a key under `keys`.
    fn worm_rsm(store: Arc<dyn ObjectStore>, keys: &TempDir, write_only: bool) -> S3RemoteStorage {
        S3RemoteStorage::with_store(store, None)
            .with_worm(&worm_config(keys.path(), write_only))
            .unwrap()
    }

    /// [`sample_metadata`] plus the chain stamp the broker leaves on a segment
    /// before it asks for the copy.
    fn stamped_metadata(id: u128, seq: u64, prev_head: ChainHead) -> RemoteLogSegmentMetadata {
        sample_metadata(id).with_custom_metadata(
            WormChainRecord::request(ChainStamp {
                epoch_id: worm_epoch(),
                seq: ManifestSeq(seq),
                prev_head,
            })
            .to_custom_metadata(),
        )
    }

    /// Reads back and decodes the manifest a copy wrote for `md`.
    fn read_manifest(store: &S3RemoteStorage, md: &RemoteLogSegmentMetadata) -> SegmentManifest {
        let raw = S3RemoteStorage::block_os(store.ops.get(&store.segment_key(md, MANIFEST_SUFFIX)))
            .unwrap();
        serde_json::from_slice(&raw).unwrap()
    }

    /// The manifest entry a copy must record for an object holding `body`.
    fn expected_entry(suffix: &str, key: &ObjectPath, body: &[u8], e_tag: &str) -> ObjectEntry {
        ObjectEntry {
            suffix: suffix.to_string(),
            key: key.to_string(),
            size_bytes: u64::try_from(body.len()).unwrap(),
            sha256: Sha256Digest::of(body),
            e_tag: Some(e_tag.to_string()),
            version_id: None,
        }
    }

    /// Every key the backing store currently holds, sorted.
    fn all_keys(store: &S3RemoteStorage) -> Vec<String> {
        let mut keys: Vec<String> = S3RemoteStorage::block_os(store.ops.list(None))
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();
        keys.sort();
        keys
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_copy_writes_a_manifest_next_to_the_segment() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        let md = stamped_metadata(50, 0, ChainHead::GENESIS);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();

            // The manifest is the log's key with the suffix swapped, so a
            // verifier that can list a partition prefix finds it beside the
            // data it describes.
            let manifest_key = store.segment_key(&md, MANIFEST_SUFFIX);
            check!(
                manifest_key.as_ref().trim_end_matches(MANIFEST_SUFFIX)
                    == store.log_key(&md).as_ref().trim_end_matches(".log")
            );

            let manifest = read_manifest(&store, &md);
            check!(manifest.body.segment == SegmentIdentity::from_metadata(&md));
            check!(manifest.body.format_version == MANIFEST_FORMAT_VERSION);
            assert!(let Some(signature) = manifest.signature.as_ref());
            check!(signature.key_id == WORM_KEY_ID);
            check!(verify_manifest_signature(
                &manifest,
                &signature.public_key.0
            ));
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_manifest_lists_every_object_with_its_digest() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        let md = stamped_metadata(51, 0, ChainHead::GENESIS);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();

            // `InMemory` hands out etags from a per-store counter, so a fresh
            // store numbers this copy's six objects 0..=5 in upload order.
            // The digests are computed here from the fixture bodies, never
            // from what the store reported.
            let expected = vec![
                expected_entry(".log", &store.log_key(&md), b"0123456789", "0"),
                expected_entry(
                    ".index",
                    &store.index_key(&md, IndexType::Offset),
                    b"OFFSET-IDX",
                    "1",
                ),
                expected_entry(
                    ".timeindex",
                    &store.index_key(&md, IndexType::Timestamp),
                    b"TIME-IDX",
                    "2",
                ),
                expected_entry(
                    ".snapshot",
                    &store.index_key(&md, IndexType::ProducerSnapshot),
                    b"SNAP",
                    "3",
                ),
                expected_entry(
                    ".leader_epoch_checkpoint",
                    &store.index_key(&md, IndexType::LeaderEpoch),
                    b"EPOCH-BYTES",
                    "4",
                ),
                expected_entry(
                    ".txnindex",
                    &store.index_key(&md, IndexType::Transaction),
                    b"TXN-IDX",
                    "5",
                ),
            ];
            check!(read_manifest(&store, &md).body.objects == expected);
        })
        .await
        .unwrap();
    }

    /// A segment with no transaction index and no producer snapshot lists
    /// exactly the four objects the copy wrote, and no placeholders.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_manifest_omits_objects_the_copy_did_not_write() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        let md = stamped_metadata(52, 0, ChainHead::GENESIS);
        let data = LogSegmentData {
            log_segment: write_file(src.path(), "00.log", b"0123456789"),
            offset_index: write_file(src.path(), "00.index", b"OFFSET-IDX"),
            time_index: write_file(src.path(), "00.timeindex", b"TIME-IDX"),
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        };
        tokio::task::spawn_blocking(move || {
            store.copy_log_segment_data(&md, &data).unwrap();
            let suffixes: Vec<String> = read_manifest(&store, &md)
                .body
                .objects
                .into_iter()
                .map(|object| object.suffix)
                .collect();
            check!(suffixes == [".log", ".index", ".timeindex", ".leader_epoch_checkpoint"]);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_copy_returns_a_receipt_with_the_new_head() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        let prev_head = ChainHead([1u8; 32]);
        let md = stamped_metadata(53, 3, prev_head);
        tokio::task::spawn_blocking(move || {
            assert!(let
                Ok(Some(custom)) =
                    store.copy_log_segment_data(&md, &sample_data(src.path(), false))
            );
            assert!(let Ok(receipt) = WormChainRecord::from_custom_metadata(&custom));

            check!(
                receipt
                    == WormChainRecord {
                        epoch_id: worm_epoch(),
                        seq: ManifestSeq(3),
                        prev_head,
                        head: Some(manifest_head(&read_manifest(&store, &md).body)),
                        // `InMemory` is unversioned, so the PUT reports none.
                        manifest_version_id: None,
                    }
            );
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_copy_refuses_to_overwrite_an_existing_manifest() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        let md = stamped_metadata(54, 0, ChainHead::GENESIS);
        tokio::task::spawn_blocking(move || {
            let data = sample_data(src.path(), true);
            store.copy_log_segment_data(&md, &data).unwrap();
            let first = read_manifest(&store, &md);

            // A replayed copy stops at the very first object: `PutMode::Create`
            // refuses the `.log` key before the manifest is ever reached.
            assert!(let Err(err) = store.copy_log_segment_data(&md, &data));
            check!(matches!(&err, RemoteStorageError::ObjectExists { key }
                    if *key == store.log_key(&md).to_string()));

            // With the data objects gone but the manifest still in place, the
            // conditional create on the manifest key itself is what refuses.
            for suffix in [
                ".log",
                ".index",
                ".timeindex",
                ".snapshot",
                ".leader_epoch_checkpoint",
                ".txnindex",
            ] {
                S3RemoteStorage::block_os(store.ops.delete(&store.segment_key(&md, suffix)))
                    .unwrap();
            }
            assert!(let Err(err) = store.copy_log_segment_data(&md, &data));
            check!(matches!(&err, RemoteStorageError::ObjectExists { key }
                    if *key == store.segment_key(&md, MANIFEST_SUFFIX).to_string()));
            // The manifest that is there is still the original one.
            check!(read_manifest(&store, &md) == first);
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_delete_is_refused() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        let md = stamped_metadata(55, 0, ChainHead::GENESIS);
        tokio::task::spawn_blocking(move || {
            store
                .copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap();
            let before = all_keys(&store);

            assert!(let Err(err) = store.delete_log_segment_data(&md));
            check!(
                matches!(&err, RemoteStorageError::Worm(WormError::DeleteRefused { key })
                    if *key == store.log_key(&md).to_string())
            );

            // Not one object left the archive, including the legacy-layout
            // keys the non-WORM path would also have swept.
            check!(all_keys(&store) == before);
            check!(before.len() == 7, "six objects plus the manifest");
        })
        .await
        .unwrap();
    }

    /// The write-only guard must fire before a key is derived or a request is
    /// built.
    ///
    /// The proof is the missing Tokio runtime. `block_os` reaches the store
    /// only through `Handle::try_current`, so on a plain thread any call that
    /// got as far as the store would come back as
    /// [`RemoteStorageError::Backend`]. A `ReadRefused` from that thread can
    /// therefore only mean the guard returned first. The twin archive over the
    /// same backing store proves the object is there to be read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_only_worm_refuses_fetch_without_touching_the_store() {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let src = TempDir::new().unwrap();
        let readable_keys = TempDir::new().unwrap();
        let sealed_keys = TempDir::new().unwrap();
        let readable = worm_rsm(Arc::clone(&backing), &readable_keys, false);
        let write_only = worm_rsm(backing, &sealed_keys, true);
        let md = stamped_metadata(56, 0, ChainHead::GENESIS);

        let readable_md = md.clone();
        tokio::task::spawn_blocking(move || {
            readable
                .copy_log_segment_data(&readable_md, &sample_data(src.path(), false))
                .unwrap();
            check!(readable.fetch_log_segment(&readable_md, 0, None).unwrap() == b"0123456789");
            check!(
                readable
                    .fetch_index(&readable_md, IndexType::Offset)
                    .unwrap()
                    == b"OFFSET-IDX"
            );
        })
        .await
        .unwrap();

        let (segment, index) = std::thread::spawn(move || {
            (
                write_only.fetch_log_segment(&md, 0, None),
                write_only.fetch_index(&md, IndexType::Offset),
            )
        })
        .join()
        .unwrap();

        check!(matches!(
            segment,
            Err(RemoteStorageError::Worm(WormError::ReadRefused))
        ));
        check!(matches!(
            index,
            Err(RemoteStorageError::Worm(WormError::ReadRefused))
        ));
    }

    /// Regression guard on the default path: no manifest, no receipt, and
    /// still an overwriting put rather than a conditional create.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_worm_copy_writes_no_manifest_and_returns_none() {
        let store = rsm(None);
        let src = TempDir::new().unwrap();
        let md = sample_metadata(57);
        tokio::task::spawn_blocking(move || {
            let data = sample_data(src.path(), true);
            check!(store.copy_log_segment_data(&md, &data).unwrap().is_none());
            check!(matches!(
                S3RemoteStorage::block_os(store.ops.get(&store.segment_key(&md, MANIFEST_SUFFIX))),
                Err(ObjectStoreError::NotFound(_))
            ));
            // A second copy overwrites rather than being refused.
            check!(store.copy_log_segment_data(&md, &data).unwrap().is_none());
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worm_copy_without_a_chain_stamp_is_refused() {
        let src = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let store = worm_rsm(Arc::new(InMemory::new()), &keys, false);
        // No `with_custom_metadata`: the broker did not stamp this segment.
        let md = sample_metadata(58);
        tokio::task::spawn_blocking(move || {
            assert!(let
                Err(err) = store.copy_log_segment_data(&md, &sample_data(src.path(), false))
            );
            check!(matches!(
                err,
                RemoteStorageError::Worm(WormError::MissingChainStamp)
            ));
            // Nothing was committed: the manifest is the commit point, and the
            // copy failed before it.
            check!(matches!(
                S3RemoteStorage::block_os(store.ops.get(&store.segment_key(&md, MANIFEST_SUFFIX))),
                Err(ObjectStoreError::NotFound(_))
            ));
        })
        .await
        .unwrap();
    }

    #[test]
    fn worm_debug_reports_mode_without_leaking_the_key() {
        let keys = TempDir::new().unwrap();
        let cfg = worm_config(keys.path(), true);
        let key_path = cfg.signing_key_path.clone().unwrap();
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), None)
            .with_worm(&cfg)
            .unwrap();

        let rendered = format!("{store:?}");
        check!(rendered.contains("worm: true"));
        check!(rendered.contains("write_only: true"));
        // Neither the key nor the path to it reaches a log line.
        check!(!rendered.contains(&key_path.display().to_string()));
        check!(!rendered.contains(&hex::encode(std::fs::read(&key_path).unwrap())));

        let plain = format!("{:?}", rsm(None));
        check!(plain.contains("worm: false"));
        check!(plain.contains("write_only: false"));
    }
}
