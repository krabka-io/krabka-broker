//! The per-partition offset resolution: one row of a `ListOffsets` request
//! turned into one row of the response.
//!
//! The version gate runs first, then the partition lookup, then KIP-320's
//! leader-epoch fence, and then the match that sends each sentinel to the log,
//! the remote tier, or the diskless index that answers it. A partition that
//! fails any of those steps carries its own error code, because Kafka reports
//! per-partition failures in the row rather than at the top level.
//!
//! The fence sits exactly where Kafka's does. `Partition.fetchOffsetForTimestamp`
//! resolves nothing until `localLogWithEpochOrThrow` has compared the request's
//! `current_leader_epoch` against the live one, so a consumer holding stale
//! metadata is told to refresh rather than handed an offset resolved against a
//! leader it no longer believes in -- and then refused by the very next Fetch,
//! which applies the same comparison.

use std::time::Duration;

use krabka_protocol::owned::list_offsets_response::ListOffsetsPartitionResponse;

use super::{
    bound::{FetchBound, last_fetchable_offset},
    diskless::diskless_earliest_offset,
    local::{latest_offset, leader_epoch_for_offset},
    remote::await_remote,
    response::error_response,
    sentinels::{
        EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, EARLIEST_TIMESTAMP,
        LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP, UNKNOWN_EPOCH, UNKNOWN_OFFSET,
        UNKNOWN_TIMESTAMP, timestamp_supported,
    },
    timestamp::resolve_timestamp_offset,
};
use crate::{broker::Broker, codes};

pub(super) async fn resolve_partition(
    broker: &Broker,
    topic_name: &str,
    request: krabka_protocol::owned::list_offsets_request::ListOffsetsPartition,
    version: i16,
    remote_timeout: Duration,
    bound: FetchBound,
) -> ListOffsetsPartitionResponse {
    let index = request.partition_index;
    let mut response = ListOffsetsPartitionResponse {
        partition_index: index,
        timestamp: UNKNOWN_TIMESTAMP,
        ..Default::default()
    };
    if !timestamp_supported(request.timestamp, version) {
        response.error_code = codes::UNSUPPORTED_VERSION;
        response.offset = UNKNOWN_OFFSET;
        return response;
    }
    let Some(partition) = broker
        .partitions
        .get(topic_name, krabka_ids::PartitionIndex(index))
    else {
        response.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        return response;
    };
    // KIP-320. The `current_leader_epoch` field decodes from v4 up and holds
    // the `-1` sentinel below it, so a v1-v3 request never trips the fence.
    if let Some((error_code, _)) = partition.leader_epoch_fence(request.current_leader_epoch) {
        return error_response(index, error_code);
    }
    let (local_start, local_end, local_log_start, log_config) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        (
            log.log_start_offset().0,
            log.log_end_offset().0,
            log.local_log_start_offset().0,
            log.config_snapshot(),
        )
    };
    let remote_enabled = log_config.remote_storage_enable;
    let diskless = partition.diskless && broker.diskless_read.is_some();
    let topic_id = if (remote_enabled && broker.remote_reader.is_some()) || diskless {
        broker
            .controller
            .current_image()
            .topic(topic_name)
            .map(|topic| topic.topic_id)
    } else {
        None
    };
    let remote_topic_id = if remote_enabled { topic_id } else { None };
    // Kafka reads `lastFetchableOffset` for every request but EARLIEST and
    // `EARLIEST_LOCAL`, which it answers from the start of the log without
    // measuring them. Skipping the read for those two also skips the high
    // watermark's async mutex. It is read here, ahead of the match, because
    // several arms below hold the log mutex and the watermark must not be
    // awaited under it.
    let last_fetchable = if matches!(
        request.timestamp,
        EARLIEST_TIMESTAMP | EARLIEST_LOCAL_TIMESTAMP
    ) {
        None
    } else {
        Some(last_fetchable_offset(&partition, bound, local_end).await)
    };
    let (offset, timestamp) = match request.timestamp {
        EARLIEST_TIMESTAMP => {
            let mut earliest = local_start;
            if let (Some(reader), Some(id)) = (broker.remote_reader.as_ref(), remote_topic_id) {
                let topic_partition =
                    krabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(remote_timeout, reader.earliest_offset(&topic_partition)).await {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(remote_start))) => earliest = earliest.min(remote_start),
                    Some(Ok(None)) => {}
                    Some(Err(error)) => tracing::warn!(topic = topic_name, partition = index,
                        error = %error, "list_offsets: remote earliest_offset failed"),
                }
            }
            earliest = diskless_earliest_offset(
                earliest,
                broker.diskless_read.as_deref(),
                topic_id,
                index,
            )
            .await;
            (earliest, UNKNOWN_TIMESTAMP)
        }
        LATEST_TIMESTAMP => (
            latest_offset(&partition, log_config.delivery_policy, local_end),
            UNKNOWN_TIMESTAMP,
        ),
        EARLIEST_LOCAL_TIMESTAMP => {
            let offset = if remote_enabled {
                local_log_start
            } else {
                local_start
            };
            response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            (offset, UNKNOWN_TIMESTAMP)
        }
        LATEST_TIERED_TIMESTAMP => {
            if let Some((reader, id)) = broker.remote_reader.as_ref().zip(remote_topic_id) {
                let topic_partition =
                    krabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(
                    remote_timeout,
                    reader.latest_tiered_offset(&topic_partition),
                )
                .await
                {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(tiered))) => {
                        response.leader_epoch = tiered.leader_epoch.0;
                        (tiered.offset, UNKNOWN_TIMESTAMP)
                    }
                    Some(Ok(None)) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                    Some(Err(error)) => {
                        tracing::warn!(topic = topic_name, partition = index,
                            error = %error, "list_offsets: remote latest_tiered_offset failed");
                        (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
                    }
                }
            } else {
                (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
            }
        }
        EARLIEST_PENDING_UPLOAD_TIMESTAMP => {
            if let Some((reader, id)) = broker.remote_reader.as_ref().zip(remote_topic_id) {
                let topic_partition =
                    krabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(
                    remote_timeout,
                    reader.latest_tiered_offset(&topic_partition),
                )
                .await
                {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(tiered))) => {
                        // KIP-1023 deliberately allows this to be below the
                        // leader's log-start offset. That tells an empty
                        // follower the remote tier currently has no valid
                        // segment and it must rebuild from local storage.
                        let offset = tiered.offset.saturating_add(1);
                        let local_epoch = leader_epoch_for_offset(&partition, offset);
                        response.leader_epoch = if local_epoch < 0 {
                            tiered.leader_epoch.0
                        } else {
                            local_epoch
                        };
                        (offset, UNKNOWN_TIMESTAMP)
                    }
                    Some(Ok(None)) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                    Some(Err(error)) => {
                        tracing::warn!(topic = topic_name, partition = index,
                            error = %error, "list_offsets: remote earliest_pending_upload failed");
                        (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
                    }
                }
            } else {
                (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
            }
        }
        MAX_TIMESTAMP => {
            let log = partition.log.lock().expect("log mutex poisoned");
            log.max_timestamp_offset_and_ts().map_or_else(
                || (log.offset_of_max_timestamp().0, UNKNOWN_TIMESTAMP),
                |(offset, timestamp)| (offset.0, timestamp),
            )
        }
        requested_timestamp if requested_timestamp >= 0 => {
            match resolve_timestamp_offset(
                broker,
                &partition,
                topic_name,
                index,
                remote_topic_id,
                requested_timestamp,
                remote_timeout,
            )
            .await
            {
                Some(result) => result,
                None => return error_response(index, codes::REQUEST_TIMED_OUT),
            }
        }
        _ => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
    };
    // One bound, applied the two ways `Partition.fetchOffsetForTimestamp`
    // applies it. EARLIEST and `EARLIEST_LOCAL` are absent from both arms
    // because they resolve from the start of the log, which is never above the
    // bound, and Kafka returns them unmeasured.
    let (offset, timestamp) = match (request.timestamp, last_fetchable) {
        // LATEST *is* the bound: Kafka answers it with `lastFetchableOffset`
        // and never consults the log. KFC-1's delivery watermark is an
        // independent cap on the same answer, so a scheduled topic takes the
        // lower of the two. On a topic that delivers immediately `latest_offset`
        // returned the log end offset, which is at or above every bound, and
        // this collapses to the value Kafka returns.
        (LATEST_TIMESTAMP, Some(last_fetchable)) => (offset.min(last_fetchable), timestamp),
        // Every other sentinel resolves against record data, and Kafka refuses
        // the answer when it lands at or above the bound -- the test in
        // `ReplicaManager.fetchOffset` is `offset >= lastFetchableOffset`, so
        // the bound itself is already out of reach. The refusal is not an
        // error: `buildErrorResponse(Errors.NONE, partition)` reports no error
        // with `UNKNOWN_OFFSET`, `UNKNOWN_TIMESTAMP`, and the leader epoch left
        // at `UNKNOWN_EPOCH`, so an arm above that set an epoch gives it back.
        (_, Some(last_fetchable)) if offset >= last_fetchable => {
            response.leader_epoch = UNKNOWN_EPOCH;
            (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
        }
        _ => (offset, timestamp),
    };
    response.error_code = codes::NONE;
    response.offset = offset;
    response.timestamp = timestamp;
    response
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;

    use super::*;
    use crate::handlers::list_offsets::test_support::{
        client_for, create_topic, list_one, list_one_at_epoch,
    };

    #[tokio::test]
    async fn request_leader_epoch_is_fenced_the_way_the_fetch_path_fences_it() {
        const TOPIC: &str = "list-offsets-epoch";
        const CURRENT_EPOCH: i32 = 3;
        const RECORDS: usize = 4;

        let (broker, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(&client, TOPIC, Vec::new()).await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        broker
            .produce_records_for_test(TOPIC, 0, RECORDS)
            .await
            .expect("produce");
        broker
            .broker_arc_for_test()
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition")
            .test_set_leader_epoch(CURRENT_EPOCH);

        let resolved = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: codes::NONE,
            timestamp: UNKNOWN_TIMESTAMP,
            offset: i64::try_from(RECORDS).expect("record count fits an offset"),
            leader_epoch: UNKNOWN_EPOCH,
            ..Default::default()
        };
        let fenced = |error_code| ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code,
            timestamp: UNKNOWN_TIMESTAMP,
            offset: UNKNOWN_OFFSET,
            leader_epoch: UNKNOWN_EPOCH,
            ..Default::default()
        };
        let cases = [
            (
                "below the current epoch",
                CURRENT_EPOCH - 1,
                fenced(codes::FENCED_LEADER_EPOCH),
            ),
            (
                "equal to the current epoch",
                CURRENT_EPOCH,
                resolved.clone(),
            ),
            (
                "above the current epoch",
                CURRENT_EPOCH + 1,
                fenced(codes::UNKNOWN_LEADER_EPOCH),
            ),
            ("the unknown-epoch sentinel", UNKNOWN_EPOCH, resolved),
        ];
        for (name, request_epoch, expected) in cases {
            assert!(
                list_one_at_epoch(&client, TOPIC, LATEST_TIMESTAMP, request_epoch).await
                    == expected,
                "{name}"
            );
        }

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn non_tiered_sentinels_use_ordinary_earliest_and_unknown_remote_offsets() {
        const TOPIC: &str = "list-offsets-local";

        let (broker, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(&client, TOPIC, Vec::new()).await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        broker
            .produce_records_for_test(TOPIC, 0, 8)
            .await
            .expect("produce");
        broker
            .test_advance_log_start(TOPIC, 0, 5)
            .await
            .expect("advance log start");

        assert!(
            list_one(&client, TOPIC, EARLIEST_LOCAL_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );
        for timestamp in [LATEST_TIERED_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP] {
            assert!(
                list_one(&client, TOPIC, timestamp).await
                    == ListOffsetsPartitionResponse {
                        partition_index: 0,
                        error_code: codes::NONE,
                        timestamp: UNKNOWN_TIMESTAMP,
                        offset: UNKNOWN_OFFSET,
                        leader_epoch: -1,
                        ..Default::default()
                    }
            );
        }

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn tiered_sentinels_return_finished_remote_frontier_and_pending_epoch() {
        use krabka_ids::LeaderEpoch;
        use krabka_remote_storage::{
            RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
            RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
        };

        const TOPIC: &str = "list-offsets-tiered";

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
        broker
            .produce_records_for_test(TOPIC, 0, 10)
            .await
            .expect("produce");

        let broker_arc = broker.broker_arc_for_test();
        let topic_id = broker_arc
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic metadata")
            .topic_id;
        let topic_partition = TopicIdPartition::new(topic_id, TOPIC, 0);
        let rlmm = broker_arc
            .remote_reader
            .as_ref()
            .expect("remote reader")
            .rlmm
            .clone();
        let finished_id = RemoteLogSegmentId::new(topic_partition.clone(), uuid::Uuid::new_v4());
        let finished = RemoteLogSegmentMetadata::new(
            finished_id.clone(),
            0,
            4,
            0,
            1,
            0,
            RemoteLogSegmentDetails::new(
                1,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .expect("finished metadata");
        rlmm.add_remote_log_segment_metadata(finished)
            .expect("add finished segment");
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: finished_id,
            event_timestamp_ms: 0,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .expect("finish segment");
        rlmm.add_remote_log_segment_metadata(
            RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(topic_partition, uuid::Uuid::new_v4()),
                5,
                8,
                0,
                1,
                0,
                RemoteLogSegmentDetails::new(
                    1,
                    RemoteLogSegmentState::CopySegmentStarted,
                    maplit::btreemap! {LeaderEpoch(0) => 5},
                ),
            )
            .expect("started metadata"),
        )
        .expect("add in-progress segment");

        assert!(
            list_one(&client, TOPIC, LATEST_TIERED_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 4,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        // KIP-1023 requires returning the raw remote frontier even when it is
        // now below the leader log-start offset. The follower interprets that
        // relation as "no currently valid remote segments" and rebuilds from
        // the leader's local log.
        broker
            .test_advance_log_start(TOPIC, 0, 7)
            .await
            .expect("advance leader log start");
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }
}
