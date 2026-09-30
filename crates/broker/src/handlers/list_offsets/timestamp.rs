//! The lookup for a positive request timestamp.
//!
//! KIP-405 puts the oldest records in the remote tier, so a by-timestamp
//! lookup asks the remote tier first and falls back to the local log's time
//! index (KIP-734) when the remote tier holds nothing for the timestamp.

use std::{future::Future, time::Duration};

use krabka_remote_storage::RemoteStorageError;
use krabka_units::prelude::ByteSizeExt;

use super::{
    remote::await_remote,
    sentinels::{UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP},
};
use crate::{broker::Broker, codes, partition::Partition};

/// Resolve a positive request timestamp to `(offset, record timestamp)`, or to
/// the error code the partition's row carries.
///
/// A compressed record the scan has to decompress that is above the topic's
/// Kafka trunk `max.decompressed.message.bytes` fails the lookup, on the
/// remote tier and on the local log alike, with `INVALID_RECORD`: Kafka's
/// `InvalidRecordException` reaches the response through
/// `Errors.forException`. Under Kafka 4.3.1 the limit is unset and no lookup
/// reads it.
pub(super) async fn resolve_timestamp_offset(
    broker: &Broker,
    partition: &Partition,
    topic_name: &str,
    partition_index: i32,
    topic_id: Option<uuid::Uuid>,
    timestamp: i64,
    remote_timeout: Duration,
) -> Result<(i64, i64), i16> {
    let remote = broker
        .remote_reader
        .as_ref()
        .zip(topic_id)
        .map(|(reader, id)| {
            let topic_partition = krabka_remote_storage::TopicIdPartition::new(
                id,
                topic_name.to_string(),
                partition_index,
            );
            move |max_record_body| async move {
                reader
                    .offset_for_timestamp(&topic_partition, timestamp, max_record_body)
                    .await
            }
        });
    lookup_timestamp(
        (partition, topic_name, partition_index),
        timestamp,
        remote_timeout,
        remote,
    )
    .await
}

/// [`resolve_timestamp_offset`] with the remote tier's read handed in, so a test
/// can answer it without a remote segment store.
///
/// `remote` is `None` for a broker with no remote tier. Otherwise it reads the
/// tier for the partition, given the topic's decompressed-record limit in
/// bytes, which this function takes from the partition's log configuration.
async fn lookup_timestamp<Read, Answer>(
    (partition, topic_name, partition_index): (&Partition, &str, i32),
    timestamp: i64,
    remote_timeout: Duration,
    remote: Option<Read>,
) -> Result<(i64, i64), i16>
where
    Read: FnOnce(Option<usize>) -> Answer,
    Answer: Future<Output = Result<Option<(i64, i64)>, RemoteStorageError>>,
{
    if let Some(read) = remote {
        let max_record_body = partition
            .log
            .lock()
            .expect("log mutex poisoned")
            .config_snapshot()
            .max_decompressed_record
            .map(ByteSizeExt::bytes_usize);
        match await_remote(remote_timeout, read(max_record_body)).await {
            None => return Err(codes::REQUEST_TIMED_OUT),
            Some(Ok(Some(offset_and_timestamp))) => return Ok(offset_and_timestamp),
            Some(Ok(None)) => {}
            Some(Err(error @ RemoteStorageError::RecordTooLarge { .. })) => {
                tracing::warn!(topic = topic_name, partition = partition_index,
                    error = %error, "list_offsets: remote offset_for_timestamp refused a record");
                return Err(codes::INVALID_RECORD);
            }
            Some(Err(error)) => tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: remote offset_for_timestamp failed"),
        }
    }
    let found = partition
        .log
        .lock()
        .expect("log mutex poisoned")
        .offset_for_timestamp_checked(timestamp);
    match found {
        Ok(found) => Ok(
            found.map_or((UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP), |(offset, matched)| {
                (offset.0, matched)
            }),
        ),
        Err(error) => {
            tracing::warn!(topic = topic_name, partition = partition_index,
                error = %error, "list_offsets: offset_for_timestamp refused a record");
            Err(codes::from_broker_error(&error.into()))
        }
    }
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

    /// The remote tier answers first, and what it answers decides the row: a
    /// hit is the answer, a miss or a failure falls through to the local log,
    /// and a record it refused for `max.decompressed.message.bytes` is the
    /// row's `INVALID_RECORD`, as `InvalidRecordException` is in Kafka. The
    /// local log holds a match for the timestamp, so a refusal that fell
    /// through would come back as that match.
    #[tokio::test]
    async fn a_record_the_remote_tier_refuses_answers_invalid_record_and_not_the_local_match() {
        use std::sync::{Arc, Mutex};

        use krabka_protocol::records::{Record, RecordBatch};

        let (partition, _dir) =
            crate::partition::test_support::test_partition(Arc::new(tokio::sync::Notify::new()));
        {
            let mut log = partition.log.lock().expect("partition log lock");
            log.append(&mut RecordBatch {
                base_timestamp: 1_000,
                max_timestamp: 1_000,
                records: vec![Record {
                    value: Some(bytes::Bytes::from_static(b"local")),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .expect("append the local record");
            let config = krabka_log::LogConfig {
                max_decompressed_record: Some(krabka_units::bytes(100)),
                ..log.config_snapshot()
            };
            log.set_config(config);
        }
        let local = Ok((0, 1_000));

        for (name, remote, expected) in [
            (
                "the remote tier refuses a record",
                Some(Err(RemoteStorageError::RecordTooLarge {
                    size: 1_007,
                    limit: 100,
                })),
                Err(codes::INVALID_RECORD),
            ),
            (
                "any other remote failure falls back to the local log",
                Some(Err(RemoteStorageError::Io(std::io::Error::other("boom")))),
                local,
            ),
            (
                "a remote miss falls back to the local log",
                Some(Ok(None)),
                local,
            ),
            (
                "a remote hit is the answer",
                Some(Ok(Some((7, 7_000)))),
                Ok((7, 7_000)),
            ),
            ("no remote tier reads the local log", None, local),
        ] {
            let limits = Mutex::new(Vec::new());
            let remote = remote.map(|answer| {
                |limit| {
                    limits.lock().expect("limits lock").push(limit);
                    std::future::ready(answer)
                }
            });
            let had_remote = remote.is_some();

            let got =
                lookup_timestamp((&partition, "t", 0), 500, Duration::from_secs(5), remote).await;

            assert!(got == expected, "{name}");
            // The reader is handed the topic's limit in bytes.
            assert!(
                *limits.lock().expect("limits lock")
                    == if had_remote { vec![Some(100)] } else { vec![] },
                "{name}"
            );
        }
    }

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
                    // The partition's leader recorded epoch 0 at promotion,
                    // and the remote segment's epoch index agrees: epoch 0
                    // covers offset 2.
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }
}
