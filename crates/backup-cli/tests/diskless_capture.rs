use std::collections::HashMap;

use assert2::check;
use bytes::Bytes;
use krabka_backup::diskless::capture_projection;
use krabka_remote_storage::diskless::{WalFlushRecord, WalIndexEntry, WalIndexKey};
use krabka_remote_storage_topic::{InProcessMetadataEventLog, MetadataEventLog};
use uuid::Uuid;

#[tokio::test]
async fn capture_replays_only_committed_index_values_before_its_fences() {
    let log = InProcessMetadataEventLog::new(2);
    let entry = WalIndexEntry {
        topic_id: Uuid::from_u128(9),
        partition: 0,
        first_offset: 4,
        last_offset: 7,
        byte_start: 6,
        byte_len: 10,
        max_timestamp_ms: 1,
    };
    let key = WalIndexKey::from(&entry).to_bytes();
    let value = WalFlushRecord {
        object_key: "diskless-wal/1/a.ckwl".into(),
        format_version: WalFlushRecord::FORMAT_VERSION,
        entries: vec![entry],
    }
    .to_bytes()
    .unwrap();
    log.publish_keyed(0, key, Some(value)).await.unwrap();

    let capture = capture_projection(
        log,
        &HashMap::from([(Uuid::from_u128(9), "orders".to_owned())]),
        42,
    )
    .await
    .unwrap();
    check!(capture.captured_at_ms == 42);
    check!(capture.source_cutoffs.len() == 2);
    check!(capture.partitions[0].recovery_cutoff == 8);
    check!(capture.partitions[0].ranges[0].object_key == "diskless-wal/1/a.ckwl");
    let _ = Bytes::new();
}
