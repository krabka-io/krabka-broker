//! The lookup for a positive request timestamp.
//!
//! KIP-405 puts the oldest records in the remote tier, so a by-timestamp
//! lookup asks the remote tier first and falls back to the local log's time
//! index (KIP-734) when the remote tier holds nothing for the timestamp.

use std::time::Duration;

use super::{
    remote::await_remote,
    sentinels::{UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP},
};
use crate::broker::Broker;

pub(super) async fn resolve_timestamp_offset(
    broker: &Broker,
    partition: &crate::partition::Partition,
    topic_name: &str,
    partition_index: i32,
    topic_id: Option<uuid::Uuid>,
    timestamp: i64,
    remote_timeout: Duration,
) -> Option<(i64, i64)> {
    if let (Some(reader), Some(id)) = (broker.remote_reader.as_ref(), topic_id) {
        let topic_partition = krabka_remote_storage::TopicIdPartition::new(
            id,
            topic_name.to_string(),
            partition_index,
        );
        match await_remote(
            remote_timeout,
            reader.offset_for_timestamp(&topic_partition, timestamp),
        )
        .await
        {
            None => return None,
            Some(Ok(Some(offset_and_timestamp))) => return Some(offset_and_timestamp),
            Some(Ok(None)) => {}
            Some(Err(error)) => tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: remote offset_for_timestamp failed"),
        }
    }
    let log = partition.log.lock().expect("log mutex poisoned");
    Some(
        log.offset_for_timestamp(timestamp)
            .map_or((UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP), |(offset, matched)| {
                (offset.0, matched)
            }),
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_protocol::owned::{
        create_topics_request::CreatableTopicConfig,
        list_offsets_response::ListOffsetsPartitionResponse,
    };

    use super::*;
    use crate::{
        codes,
        handlers::list_offsets::test_support::{client_for, create_topic, list_one},
    };

    #[tokio::test]
    async fn positive_timestamp_wire_response_returns_exact_remote_record() {
        use bytes::Bytes;
        use krabka_ids::LeaderEpoch;
        use krabka_protocol::records::{Record, RecordBatch};
        use krabka_remote_storage::{
            LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
            RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
        };

        const TOPIC: &str = "list-offsets-remote-timestamp";

        let remote_dir = tempfile::tempdir().expect("remote tempdir");
        let remote_path = remote_dir.path().to_path_buf();
        let (broker, _dir) = crate::test_support::start_broker_with(move |config| {
            config.audit_enabled = false;
            config.remote_storage_backend =
                Some(crate::config::RemoteStorageBackend::Local { dir: remote_path });
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(
            &client,
            TOPIC,
            vec![CreatableTopicConfig {
                name: "remote.storage.enable".into(),
                value: Some("true".into()),
                ..Default::default()
            }],
        )
        .await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if broker
                    .partition_log_config_for_test(TOPIC, 0)
                    .is_some_and(|config| config.remote_storage_enable)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote topic config propagated");

        let source_dir = tempfile::tempdir().expect("source tempdir");
        let batches = [
            (0, &[1_000, 1_100][..]),
            (2, &[1_600, 1_700][..]),
            (4, &[2_000, 2_200, 2_400][..]),
        ];
        let mut log_bytes = BytesMut::new();
        let mut last_position = 0;
        for (base_offset, timestamps) in batches {
            if base_offset == 4 {
                last_position = u32::try_from(log_bytes.len()).expect("segment position");
            }
            let base_timestamp = timestamps[0];
            RecordBatch {
                base_offset,
                last_offset_delta: i32::try_from(timestamps.len() - 1).expect("record count"),
                base_timestamp,
                max_timestamp: *timestamps.iter().max().expect("timestamps"),
                records: timestamps
                    .iter()
                    .enumerate()
                    .map(|(offset_delta, timestamp)| Record {
                        timestamp_delta: *timestamp - base_timestamp,
                        offset_delta: i32::try_from(offset_delta).expect("offset delta"),
                        value: Some(Bytes::from_static(b"value")),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
            .encode(&mut log_bytes)
            .expect("encode batch");
        }
        let log_path = source_dir.path().join("segment.log");
        let offset_index_path = source_dir.path().join("segment.index");
        let time_index_path = source_dir.path().join("segment.timeindex");
        std::fs::write(&log_path, &log_bytes).expect("write log");
        std::fs::write(
            &offset_index_path,
            [
                0_u32.to_be_bytes(),
                0_u32.to_be_bytes(),
                4_u32.to_be_bytes(),
                last_position.to_be_bytes(),
            ]
            .concat(),
        )
        .expect("write offset index");
        let mut time_index = Vec::new();
        time_index.extend_from_slice(&1_100_i64.to_be_bytes());
        time_index.extend_from_slice(&0_u32.to_be_bytes());
        time_index.extend_from_slice(&2_400_i64.to_be_bytes());
        time_index.extend_from_slice(&4_u32.to_be_bytes());
        std::fs::write(&time_index_path, time_index).expect("write time index");

        let broker_arc = broker.broker_arc_for_test();
        let topic_id = broker_arc
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic metadata")
            .topic_id;
        let topic_partition = TopicIdPartition::new(topic_id, TOPIC, 0);
        let reader = broker_arc.remote_reader.as_ref().expect("remote reader");
        let segment_id = RemoteLogSegmentId::new(topic_partition, uuid::Uuid::new_v4());
        let metadata = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            0,
            6,
            2_400,
            1,
            2_400,
            RemoteLogSegmentDetails::new(
                i32::try_from(log_bytes.len()).expect("segment size"),
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .expect("segment metadata");
        reader
            .rlmm
            .add_remote_log_segment_metadata(metadata.clone())
            .expect("add segment metadata");
        reader
            .rsm
            .copy_log_segment_data(
                &metadata,
                &LogSegmentData {
                    log_segment: log_path,
                    offset_index: offset_index_path,
                    time_index: time_index_path,
                    transaction_index: None,
                    producer_snapshot_index: None,
                    leader_epoch_index: Bytes::from_static(b"0\n1\n0 0\n"),
                },
            )
            .expect("copy remote segment");
        reader
            .rlmm
            .update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: segment_id,
                event_timestamp_ms: 2_400,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            })
            .expect("finish segment");

        // The tier now describes offsets 0..6, so the partition committed them
        // before they were uploaded: a segment is only ever copied out of a log
        // that already acknowledged it. `ListOffsets` bounds a client's answer
        // at the high watermark, and writing the segment straight into remote
        // storage above skipped the produce path that would have advanced it.
        broker_arc
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition")
            .replica_state
            .lock()
            .await
            .hw = krabka_log::Offset(6);

        assert!(
            list_one(&client, TOPIC, 1_500).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: 1_600,
                    offset: 2,
                    leader_epoch: -1,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }
}
