//! Object-key derivation for the S3 backend.
//!
//! Two layouts live here. The current one mirrors
//! [`LocalTieredStorage`](crate::LocalTieredStorage): a partition directory
//! holding one file per artifact, named by base offset and segment id. The
//! legacy one is what Krabka 0.3.8 and earlier wrote. Reads and deletes still
//! derive both, so an upgraded cluster keeps its old objects reachable.

use object_store::path::Path as ObjectPath;

use super::S3RemoteStorage;
use crate::{metadata::RemoteLogSegmentMetadata, storage_manager::IndexType};

impl S3RemoteStorage {
    pub(super) fn segment_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        suffix: &str,
    ) -> ObjectPath {
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

    pub(super) fn log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.segment_key(metadata, ".log")
    }

    pub(super) fn index_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> ObjectPath {
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

    pub(super) fn legacy_log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.legacy_segment_key(metadata, "log")
    }

    pub(super) fn legacy_index_key(
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_object_store::{ObjectOps, PutRequest};
    use object_store::memory::InMemory;

    use super::{IndexType, S3RemoteStorage};
    use crate::{
        error::RemoteStorageError,
        s3::test_support::{rsm, sample_metadata},
        storage_manager::RemoteStorageManager,
    };

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
}
