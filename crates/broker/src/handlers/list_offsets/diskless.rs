//! The earliest offset a diskless partition can answer.
//!
//! A diskless topic keeps its oldest records in shared object storage rather
//! than in the local log, so the `EARLIEST` sentinel takes the lower of the
//! local log start offset and the first offset the WAL index still covers.

pub(super) async fn diskless_earliest_offset(
    local_start: i64,
    diskless_read: Option<&crate::diskless::read::DisklessReadHandle>,
    topic_id: Option<uuid::Uuid>,
    partition: i32,
) -> i64 {
    let Some((handle, topic_id)) = diskless_read.zip(topic_id) else {
        return local_start;
    };
    handle
        .index
        .lock()
        .await
        .earliest_covered(topic_id, partition)
        .map_or(local_start, |object_start| local_start.min(object_start))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn diskless_earliest_uses_object_floor_but_leaves_local_floor_visible() {
        let topic_id = uuid::Uuid::from_u128(42);
        let mut cache = crate::diskless::wal_index::WalIndexCache::default();
        cache.apply(&crate::diskless::wal_index::WalFlushRecord {
            object_key: "o".into(),
            format_version: 1,
            entries: vec![crate::diskless::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 8,
                byte_start: 0,
                byte_len: 1,
            }],
        });
        let handle = crate::diskless::read::DisklessReadHandle::new(
            Arc::new(tokio::sync::Mutex::new(cache)),
            Arc::new(object_store::memory::InMemory::new()),
        );

        let earliest = diskless_earliest_offset(9, Some(&handle), Some(topic_id), 0).await;
        assert!(earliest == 0);
    }
}
