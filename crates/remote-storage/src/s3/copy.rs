//! The segment-copy path: one ordered upload per artifact of a segment, and
//! the sealed manifest that commits a WORM copy.
//!
//! Naming each body source lets the copy walk a single ordered list instead
//! of six near-identical call sites, which is what makes collecting a digest
//! per object cheap enough to do on every archive write.

use bytes::Bytes;
use krabka_object_store::{ObjectOps, PutMode, PutOutcome, PutRequest};
use krabka_units::prelude::ByteSizeExt as _;
use object_store::path::Path as ObjectPath;

use super::S3RemoteStorage;
use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
    storage_manager::{IndexType, LogSegmentData},
    worm::{MANIFEST_SUFFIX, ObjectEntry, Sha256Digest, WormError},
};

#[cfg(test)]
mod tests;

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

impl S3RemoteStorage {
    /// Uploads every artifact of one segment, and seals the manifest when
    /// this backend is a WORM archive.
    pub(super) fn copy_segment_objects(
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
}
