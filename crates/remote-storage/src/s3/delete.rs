//! Segment deletion: the idempotent sweep of every key a copy could have
//! written, and the archive backstop that refuses to issue a delete at all.
//!
//! The sweep covers both key layouts, so a segment an older Krabka wrote
//! leaves nothing behind. A WORM archive never reaches the sweep, because a
//! store that is never asked cannot be talked into obliging.

use krabka_object_store::{ObjectOps, ObjectStoreError};

use super::S3RemoteStorage;
use crate::{
    error::RemoteStorageError, metadata::RemoteLogSegmentMetadata, storage_manager::IndexType,
    worm::WormError,
};

impl S3RemoteStorage {
    /// Deletes every object of one segment, in both key layouts.
    pub(super) fn delete_segment_objects(
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
    use std::sync::Arc;

    use assert2::{assert, check};
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::{ObjectOps, RemoteStorageError, S3RemoteStorage, WormError};
    use crate::{
        s3::test_support::{rsm, sample_data, sample_metadata, stamped_metadata, worm_rsm},
        storage_manager::RemoteStorageManager,
        worm::ChainHead,
    };

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
}
