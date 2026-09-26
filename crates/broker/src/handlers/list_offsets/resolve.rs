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
//! which applies the same comparison. The two APIs share the comparison but
//! not the sentinel that skips it: only `-1` means "no epoch asserted" here,
//! whereas Fetch reads every negative epoch that way. See
//! `Partition::list_offsets_leader_epoch_fence`.

use std::{sync::atomic::Ordering, time::Duration};

use krabka_protocol::owned::list_offsets_response::ListOffsetsPartitionResponse;
use krabka_verified::{
    ListOffsetsEarliestFacts, ListOffsetsKind, ListOffsetsSelectionDecision,
    ListOffsetsSelectionFacts, list_offsets_earliest, list_offsets_selection_decision,
};

use super::{
    bound::{FetchBound, last_fetchable_offset},
    diskless::diskless_earliest_candidate,
    leadership::resolve_leadership,
    local::{latest_offset, leader_epoch_for_offset},
    remote::await_remote,
    response::error_response,
    sentinels::{UNKNOWN_EPOCH, UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP, timestamp_kind},
    timestamp::resolve_timestamp_offset,
};
use crate::{broker::Broker, codes};

fn earliest_pending_upload_offset(tiered_offset: i64) -> Option<i64> {
    tiered_offset.checked_add(1)
}

/// KIP-207. Right after a leader election the new leader's high watermark
/// can still sit below the start offset of its own epoch: replication has
/// not yet caught the watermark up to what was already appended in the
/// outgoing epoch's tail plus this leader's own log-prepare work. Kafka
/// refuses to answer with an offset the watermark could later step past
/// going backwards -- which would let a consumer observe a non-monotonic
/// end of partition. LATEST always targets that live, still-moving end, so
/// it is fenced immediately, before anything is resolved. A timestamp or
/// `MAX_TIMESTAMP` lookup, though, can legitimately resolve to an old,
/// already-stable offset below the current epoch's start even while this
/// window is open, and that answer carries none of the risk; only a
/// resolved candidate that lands at or above the epoch's start is fenced.
/// `Partition.maybeOffsetsError` raises it for LATEST and for any lookup
/// whose answer would land at or above the bound; `ReplicaManager.scala:1546-1554`
/// answers `OFFSET_NOT_AVAILABLE` from v5 and `LEADER_NOT_AVAILABLE` for
/// v1-v4, where the dedicated code did not exist yet. A follower or the
/// offline-debugging replica id is never bounded by the high watermark to
/// begin with, so only a client request (`replica_id == -1`) is subject to
/// the fence.
/// The values [`resolve_earliest`] needs, gathered so `resolve_partition`
/// passes them as one handle rather than one parameter each.
struct EarliestContext<'a> {
    broker: &'a Broker,
    topic_name: &'a str,
    index: i32,
    partition: &'a crate::partition::Partition,
    remote_timeout: Duration,
    remote_topic_id: Option<uuid::Uuid>,
    topic_id: Option<uuid::Uuid>,
    local_start: i64,
    deleted_below: Option<krabka_log::Offset>,
    local_log_start: i64,
}

/// Resolve the EARLIEST sentinel's offset across the local log, the remote
/// tier and the diskless index, and the leader epoch that goes with it.
///
/// Split out of [`resolve_partition`] purely to keep that function's line
/// count down.
async fn resolve_earliest(ctx: EarliestContext<'_>) -> Result<(i64, i32), i16> {
    let mut remote_candidate = None;
    if let (Some(reader), Some(id)) = (ctx.broker.remote_reader.as_ref(), ctx.remote_topic_id) {
        let topic_partition =
            krabka_remote_storage::TopicIdPartition::new(id, ctx.topic_name.to_string(), ctx.index);
        match await_remote(ctx.remote_timeout, reader.earliest_offset(&topic_partition)).await {
            None => return Err(codes::REQUEST_TIMED_OUT),
            // KIP-405: the global log start bounds the archive too. A
            // `DeleteRecords` moves the floor at once and the expiration
            // pass removes the breached segments on its own tick, so in
            // between the RLMM still lists a segment that starts below the
            // floor. Reporting its start as EARLIEST would name an offset
            // the fetch path refuses.
            //
            // Only a floor someone deleted up to bounds it. The one
            // `Log::open` infers from the segments left on disk sits above
            // the whole archive on a partition whose local segments were
            // evicted, and clamping to that would report an EARLIEST past
            // every record the tier still holds -- a `--from-beginning`
            // consumer would skip them all.
            Some(Ok(Some(remote_start))) => {
                remote_candidate = Some(match ctx.deleted_below {
                    Some(floor) => remote_start.max(floor.0),
                    None => remote_start,
                });
            }
            Some(Ok(None)) => {}
            Some(Err(error)) => tracing::warn!(topic = ctx.topic_name, partition = ctx.index,
                error = %error, "list_offsets: remote earliest_offset failed"),
        }
    }
    let diskless_candidate =
        diskless_earliest_candidate(ctx.broker.diskless_read.as_deref(), ctx.topic_id, ctx.index)
            .await;
    let facts = ListOffsetsEarliestFacts {
        local: ctx.local_start,
        has_remote: remote_candidate.is_some(),
        remote: remote_candidate.unwrap_or(0),
        has_diskless: diskless_candidate.is_some(),
        diskless: diskless_candidate.unwrap_or(0),
    };
    let Some(earliest) = list_offsets_earliest(facts) else {
        return Err(codes::KAFKA_STORAGE_ERROR);
    };
    // Kafka fills the epoch for EARLIEST when its start offset sits at or
    // below the log start, which is always true of the offset just
    // selected: it is by definition the earliest one this broker can name.
    // `UnifiedLog.java:1690-1700`.
    Ok((earliest, leader_epoch_for_offset(ctx.partition, earliest)))
}

/// Resolve the KIP-1023 `EARLIEST_PENDING_UPLOAD` sentinel: the first offset
/// this leader has not yet copied to the remote tier, or `None` when there
/// is no remote reader configured for this topic.
///
/// Split out of [`resolve_partition`] purely to keep that function's line
/// count down.
async fn resolve_earliest_pending_upload(
    ctx: EarliestContext<'_>,
) -> Result<Option<(i64, i32)>, i16> {
    let Some((reader, id)) = ctx.broker.remote_reader.as_ref().zip(ctx.remote_topic_id) else {
        return Ok(None);
    };
    let topic_partition =
        krabka_remote_storage::TopicIdPartition::new(id, ctx.topic_name.to_string(), ctx.index);
    match await_remote(
        ctx.remote_timeout,
        reader.latest_tiered_offset(&topic_partition),
    )
    .await
    {
        None => Err(codes::REQUEST_TIMED_OUT),
        Some(Ok(Some(tiered))) => {
            // `UnifiedLog.fetchEarliestPendingUploadOffset` clamps the raw
            // remote frontier with the log start offset:
            // `Math.max(curHighestRemoteOffset + 1, logStartOffset())`.
            // Without the clamp, a `DeleteRecords` call that moves the log
            // start past the last finished upload would report an offset
            // the fetch path refuses, the same failure mode EARLIEST guards
            // against above.
            let Some(raw_offset) = earliest_pending_upload_offset(tiered.offset) else {
                return Err(codes::KAFKA_STORAGE_ERROR);
            };
            let offset = raw_offset.max(ctx.local_start);
            let local_epoch = leader_epoch_for_offset(ctx.partition, offset);
            let leader_epoch = if local_epoch < 0 {
                tiered.leader_epoch.0
            } else {
                local_epoch
            };
            Ok(Some((offset, leader_epoch)))
        }
        // The RLMM lists no segment. When the local log still holds
        // everything from the log start, nothing has ever been tiered and
        // Kafka falls back to the EARLIEST answer. When the local log start
        // has moved ahead of it, segments were evicted on the strength of a
        // tier upload this leader cannot currently confirm, and Kafka
        // reports -1 rather than naming an offset it cannot stand behind.
        Some(Ok(None)) if ctx.local_log_start == ctx.local_start => Ok(Some((
            ctx.local_start,
            leader_epoch_for_offset(ctx.partition, ctx.local_start),
        ))),
        Some(Ok(None)) => Ok(None),
        Some(Err(error)) => {
            tracing::warn!(topic = ctx.topic_name, partition = ctx.index,
                error = %error, "list_offsets: remote earliest_pending_upload failed");
            Ok(None)
        }
    }
}

/// LATEST needs no resolution step -- it always targets the live end of the
/// log, which is exactly what the high watermark not having caught up to
/// the epoch start puts at risk -- so it is fenced immediately, ahead of the
/// match that resolves every other sentinel.
fn epoch_not_caught_up_to_bound(
    partition: &crate::partition::Partition,
    kind: ListOffsetsKind,
    bound: FetchBound,
    last_fetchable: Option<i64>,
    version: i16,
) -> Option<i16> {
    let last_fetchable = last_fetchable?;
    if bound.replica_id() != -1 || kind != ListOffsetsKind::Latest {
        return None;
    }
    let epoch_start = super::local::epoch_start_offset(partition)?;
    if last_fetchable >= epoch_start {
        return None;
    }
    Some(if version >= 5 {
        codes::OFFSET_NOT_AVAILABLE
    } else {
        codes::LEADER_NOT_AVAILABLE
    })
}

/// The same fence as [`epoch_not_caught_up_to_bound`], applied after a
/// timestamp or `MAX_TIMESTAMP` lookup has resolved to a candidate offset,
/// and only when that candidate actually lands in the risky range: at or
/// above the current epoch's start, while the high watermark has not yet
/// caught up to it. An old timestamp that resolves below the epoch start
/// names an offset that was already stable before this leader's election
/// and never risks going non-monotonic, so it is answered rather than
/// refused.
fn epoch_not_caught_up_to_offset(
    partition: &crate::partition::Partition,
    kind: ListOffsetsKind,
    bound: FetchBound,
    last_fetchable: Option<i64>,
    candidate_offset: i64,
    version: i16,
) -> Option<i16> {
    let last_fetchable = last_fetchable?;
    if bound.replica_id() != -1
        || !matches!(
            kind,
            ListOffsetsKind::MaxTimestamp | ListOffsetsKind::Timestamp
        )
        || candidate_offset == UNKNOWN_OFFSET
    {
        return None;
    }
    let epoch_start = super::local::epoch_start_offset(partition)?;
    if last_fetchable >= epoch_start || candidate_offset < epoch_start {
        return None;
    }
    Some(if version >= 5 {
        codes::OFFSET_NOT_AVAILABLE
    } else {
        codes::LEADER_NOT_AVAILABLE
    })
}

fn apply_selection(
    response: &mut ListOffsetsPartitionResponse,
    kind: ListOffsetsKind,
    offset: i64,
    timestamp: i64,
    last_fetchable: Option<i64>,
) -> bool {
    match list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind,
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch: response.leader_epoch,
        last_fetchable: last_fetchable.unwrap_or(0),
    }) {
        ListOffsetsSelectionDecision::RejectMalformed => false,
        ListOffsetsSelectionDecision::Unknown => {
            response.leader_epoch = UNKNOWN_EPOCH;
            response.offset = UNKNOWN_OFFSET;
            response.timestamp = UNKNOWN_TIMESTAMP;
            true
        }
        ListOffsetsSelectionDecision::Resolved {
            offset,
            timestamp,
            leader_epoch,
        } => {
            response.leader_epoch = leader_epoch;
            response.offset = offset;
            response.timestamp = timestamp;
            true
        }
    }
}

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
    let kind = timestamp_kind(request.timestamp, version);
    if kind == ListOffsetsKind::Unsupported {
        response.error_code = codes::UNSUPPORTED_VERSION;
        response.offset = UNKNOWN_OFFSET;
        return response;
    }
    // Kafka answers from whatever this node holds only when it leads the
    // partition, or the request carries the offline-debugging sentinel
    // `replica_id == -2` and this node holds it as a follower. KIP-320's
    // epoch fence -- the `current_leader_epoch` field decodes from v4 up and
    // holds the `-1` sentinel below it, so a v1-v3 request never trips it --
    // runs ahead of that leadership check, matching Kafka's order. Anyone
    // refused leadership gets `NOT_LEADER_OR_FOLLOWER` when the metadata
    // image and the installed local role do not both name this node leader,
    // `UNKNOWN_TOPIC_OR_PARTITION` when the image does not know the
    // partition at all, and `KAFKA_STORAGE_ERROR` when the partition's log
    // directory is offline. See
    // [`resolve_leadership`](super::leadership::resolve_leadership).
    let partition = match resolve_leadership(
        topic_name,
        index,
        bound.replica_id(),
        request.current_leader_epoch,
        super::leadership::LeadershipContext {
            partitions: &broker.partitions,
            log_dir_status: &broker.log_dir_status,
            image: &broker.controller.current_image(),
            node_id: broker.config.node_id,
        },
    ) {
        Ok(partition) => partition,
        Err(error_code) => return error_response(index, error_code),
    };
    let (local_start, deleted_below, local_end, local_log_start, log_config) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        (
            log.log_start_offset().0,
            log.established_log_start(),
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
        kind,
        ListOffsetsKind::Earliest | ListOffsetsKind::EarliestLocal
    ) {
        None
    } else {
        match last_fetchable_offset(&partition, bound).await {
            Some(offset) => Some(offset),
            None => return error_response(index, codes::KAFKA_STORAGE_ERROR),
        }
    };
    if let Some(error_code) =
        epoch_not_caught_up_to_bound(&partition, kind, bound, last_fetchable, version)
    {
        return error_response(index, error_code);
    }
    let (offset, timestamp) = match kind {
        ListOffsetsKind::Earliest => {
            match resolve_earliest(EarliestContext {
                broker,
                topic_name,
                index,
                partition: &partition,
                remote_timeout,
                remote_topic_id,
                topic_id,
                local_start,
                deleted_below,
                local_log_start,
            })
            .await
            {
                Ok((earliest, leader_epoch)) => {
                    response.leader_epoch = leader_epoch;
                    (earliest, UNKNOWN_TIMESTAMP)
                }
                Err(error_code) => return error_response(index, error_code),
            }
        }
        ListOffsetsKind::Latest => {
            // Kafka answers LATEST with the partition's live leader epoch,
            // not the epoch recorded for whatever offset the log end
            // happens to sit at -- the two agree once a record lands in the
            // new epoch, but LATEST must not wait for that.
            // `Partition.scala:1473-1475`.
            response.leader_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
            (
                latest_offset(&partition, log_config.delivery_policy, local_end),
                UNKNOWN_TIMESTAMP,
            )
        }
        ListOffsetsKind::EarliestLocal => {
            let offset = if remote_enabled {
                local_log_start
            } else {
                local_start
            };
            response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            (offset, UNKNOWN_TIMESTAMP)
        }
        ListOffsetsKind::LatestTiered => {
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
        ListOffsetsKind::EarliestPendingUpload => {
            match resolve_earliest_pending_upload(EarliestContext {
                broker,
                topic_name,
                index,
                partition: &partition,
                remote_timeout,
                remote_topic_id,
                topic_id,
                local_start,
                deleted_below,
                local_log_start,
            })
            .await
            {
                Ok(None) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                Ok(Some((offset, leader_epoch))) => {
                    response.leader_epoch = leader_epoch;
                    (offset, UNKNOWN_TIMESTAMP)
                }
                Err(error_code) => return error_response(index, error_code),
            }
        }
        ListOffsetsKind::MaxTimestamp => {
            let (offset, timestamp) = {
                let log = partition.log.lock().expect("log mutex poisoned");
                log.max_timestamp_offset_and_ts().map_or_else(
                    || (log.offset_of_max_timestamp().0, UNKNOWN_TIMESTAMP),
                    |(offset, timestamp)| (offset.0, timestamp),
                )
            };
            // Kafka fills the epoch with the resolved batch's own
            // `partitionLeaderEpoch`. `UnifiedLog.java:1742-1749`.
            if offset != UNKNOWN_OFFSET {
                response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            }
            (offset, timestamp)
        }
        ListOffsetsKind::Timestamp => {
            let Some((offset, timestamp)) = resolve_timestamp_offset(
                broker,
                &partition,
                topic_name,
                index,
                remote_topic_id,
                request.timestamp,
                remote_timeout,
            )
            .await
            else {
                return error_response(index, codes::REQUEST_TIMED_OUT);
            };
            // Kafka fills the epoch with the matched batch's own epoch.
            // `FileRecords.java:364-381`.
            if offset != UNKNOWN_OFFSET {
                response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            }
            (offset, timestamp)
        }
        ListOffsetsKind::Unsupported => unreachable!("unsupported timestamp returned above"),
    };
    // KIP-207 for a data-resolved lookup: fenced only once its candidate is
    // known to land in the new epoch's still-unstable range.
    if let Some(error_code) =
        epoch_not_caught_up_to_offset(&partition, kind, bound, last_fetchable, offset, version)
    {
        return error_response(index, error_code);
    }
    // One bound, applied the two ways `Partition.fetchOffsetForTimestamp`
    // applies it. EARLIEST and `EARLIEST_LOCAL` are absent from both arms
    // because they resolve from the start of the log, which is never above the
    // bound, and Kafka returns them unmeasured.
    if !apply_selection(&mut response, kind, offset, timestamp, last_fetchable) {
        return error_response(index, codes::KAFKA_STORAGE_ERROR);
    }
    response.error_code = codes::NONE;
    response
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;

    use super::*;
    use crate::{
        handlers::list_offsets::{
            handle,
            sentinels::{
                EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP,
                LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP,
            },
            test_support::{
                client_for, create_topic, decode_response, encode_request, list_one,
                list_one_at_epoch, test_context,
            },
        },
        test_support::{peer, principal},
    };

    #[test]
    fn pending_upload_offset_rejects_overflow() {
        assert!(earliest_pending_upload_offset(4) == Some(5));
        assert!(earliest_pending_upload_offset(i64::MAX).is_none());
    }

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
        broker.test_set_leader_epoch(TOPIC, 0, CURRENT_EPOCH);

        let resolved = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: codes::NONE,
            timestamp: UNKNOWN_TIMESTAMP,
            offset: i64::try_from(RECORDS).expect("record count fits an offset"),
            // LATEST reports the partition's live leader epoch, which the
            // `test_set_leader_epoch` call above bumped to `CURRENT_EPOCH`.
            leader_epoch: CURRENT_EPOCH,
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
            // Kafka's `RequestUtils.getLeaderEpoch` reads only `-1` as "no
            // epoch asserted", so a `ListOffsets` row carrying any other
            // negative epoch is compared and fenced. Fetch does not: its
            // `FetchRequest.optionalEpoch` reads every negative epoch that
            // way. Confirmed on apache/kafka:4.3.1.
            (
                "a negative epoch that is not the sentinel",
                UNKNOWN_EPOCH - 1,
                fenced(codes::FENCED_LEADER_EPOCH),
            ),
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

        // `UnifiedLog.fetchEarliestPendingUploadOffset` clamps the raw remote
        // frontier (5) with the log start offset, so once `DeleteRecords`
        // moves the log start past it, the log start wins: naming the raw
        // frontier here would report an offset the fetch path refuses.
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
                    offset: 7,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn earliest_pending_upload_falls_back_to_earliest_when_nothing_is_tiered() {
        const TOPIC: &str = "list-offsets-pending-upload-empty-tier";

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
            .produce_records_for_test(TOPIC, 0, 4)
            .await
            .expect("produce");

        // The RLMM has no segment for this partition at all, and the local
        // log still holds everything from its own start: nothing has ever
        // reached the remote tier. Kafka answers EARLIEST rather than -1 in
        // this state.
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 0,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn earliest_pending_upload_stays_unknown_when_local_segments_outrun_the_rlmm() {
        const TOPIC: &str = "list-offsets-pending-upload-unconfirmed";

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
            vec![
                CreatableTopicConfig {
                    name: "remote.storage.enable".into(),
                    value: Some("true".into()),
                    ..Default::default()
                },
                // A tiny segment size seals a fresh segment on almost every
                // produced record, so a handful of records leave several
                // sealed segments behind the active one to evict.
                CreatableTopicConfig {
                    name: "internal.segment.bytes".into(),
                    value: Some("1".into()),
                    ..Default::default()
                },
            ],
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
            .produce_records_for_test(TOPIC, 0, 6)
            .await
            .expect("produce");

        // Drop the local copies of the sealed segments directly, without
        // going through the RLMM upload path and without moving the global
        // log start. This is the state a leader that lost its RLMM cache
        // memory would observe: files gone from disk, but no record of
        // what -- if anything -- ever reached the tier. Kafka reports -1
        // here rather than guessing.
        let broker_arc = broker.broker_arc_for_test();
        let partition = broker_arc
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition");
        {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            let removed = log
                .delete_local_segments_through(krabka_log::Offset(3))
                .expect("evict sealed segments");
            assert!(removed > 0, "the tiny segment size must have sealed some");
        }

        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: UNKNOWN_OFFSET,
                    leader_epoch: UNKNOWN_EPOCH,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    /// One `ListOffsets` for partition 0 of `topic`, at a chosen wire
    /// `version`, bypassing the client's own version negotiation. KIP-207's
    /// error code depends on the version (`OFFSET_NOT_AVAILABLE` from v5,
    /// `LEADER_NOT_AVAILABLE` below it), so this suite needs to pick the
    /// version a case tests rather than always sending the client's max.
    async fn list_one_at_version(
        broker: &crate::broker::BrokerHandle,
        topic: &str,
        timestamp: i64,
        version: i16,
    ) -> ListOffsetsPartitionResponse {
        let broker_arc = broker.broker_arc_for_test();
        let admin = principal("admin");
        let peer = peer();
        let ctx = test_context(&admin, &peer);
        let req = encode_request(
            &krabka_protocol::owned::list_offsets_request::ListOffsetsRequest {
                replica_id: -1,
                topics: vec![
                    krabka_protocol::owned::list_offsets_request::ListOffsetsTopic {
                        name: topic.to_string(),
                        partitions: vec![
                            krabka_protocol::owned::list_offsets_request::ListOffsetsPartition {
                                partition_index: 0,
                                current_leader_epoch: -1,
                                timestamp,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ],
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let bytes = handle(&broker_arc, version, 123, &req, &ctx)
            .await
            .expect("handle");
        let mut response = decode_response(&bytes, version);
        response.topics.remove(0).partitions.remove(0)
    }

    /// One record, appended straight through the partition's writer with a
    /// chosen create timestamp and leader epoch, bypassing the ordinary
    /// Produce validation path (which would otherwise stamp its own
    /// timestamp). Returns the offset it landed at.
    async fn produce_one_at_timestamp(
        partition: &crate::partition::Partition,
        leader_epoch: i32,
        timestamp: i64,
    ) -> i64 {
        let batch = krabka_protocol::records::RecordBatch {
            partition_leader_epoch: leader_epoch,
            base_timestamp: timestamp,
            max_timestamp: timestamp,
            records: vec![krabka_protocol::records::Record {
                offset_delta: 0,
                value: Some(bytes::Bytes::from_static(b"v")),
                ..Default::default()
            }],
            ..Default::default()
        };
        partition.produce_batch(batch).await.expect("produce").0
    }

    /// KIP-207: a client LATEST lookup is refused unconditionally while the
    /// high watermark has not yet caught up to the start offset of the live
    /// leader epoch, because LATEST always targets the live end of the log
    /// that this window puts at risk. A timestamp or `MAX_TIMESTAMP` lookup
    /// is refused only when it actually resolves to a candidate in that same
    /// unstable range; one that resolves to an older, already-stable offset
    /// below the epoch start answers normally even while the window is
    /// open.
    #[tokio::test]
    async fn offset_not_available_is_reported_below_the_epoch_start_and_versioned_correctly() {
        const TOPIC: &str = "list-offsets-kip-207";
        const OLD_TIMESTAMP: i64 = 1_000;
        const NEW_TIMESTAMP: i64 = 5_000;

        let (broker, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;
        let client = client_for(&broker).await;
        create_topic(&client, TOPIC, Vec::new()).await;
        broker.wait_until_partition_present(TOPIC, 0).await;
        let broker_arc = broker.broker_arc_for_test();
        let partition = broker_arc
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition");

        // Epoch 0: four records at ascending, old timestamps (offsets 0-3).
        for delta in 0..4 {
            produce_one_at_timestamp(&partition, 0, OLD_TIMESTAMP + delta).await;
        }
        broker.test_set_leader_epoch(TOPIC, 0, 1);
        // Epoch 1 starts at offset 4, with new, later timestamps (offsets
        // 4-5). `NEW_TIMESTAMP` resolves into this range, `OLD_TIMESTAMP`
        // resolves below it.
        for delta in 0..2 {
            produce_one_at_timestamp(&partition, 1, NEW_TIMESTAMP + delta).await;
        }

        // Force the high watermark back below the epoch-1 start, the way a
        // fresh leader's would sit before replication (and this broker's
        // own single-replica ack) catches it up.
        partition.replica_state.lock().await.hw = krabka_log::Offset(3);

        // MAX_TIMESTAMP (KIP-734) only decodes from v7, which already sits
        // above the v5 floor `OFFSET_NOT_AVAILABLE` needs, so there is no
        // version at which it can see the older `LEADER_NOT_AVAILABLE` code.
        // LATEST and a timestamp lookup decode from v1 and see both.
        let fenced_cases: &[(&str, i64, &[i16])] = &[
            ("LATEST", LATEST_TIMESTAMP, &[4, 5, 11]),
            (
                "a timestamp lookup resolving into the new epoch",
                NEW_TIMESTAMP,
                &[4, 5, 11],
            ),
            ("MAX_TIMESTAMP", MAX_TIMESTAMP, &[7, 11]),
        ];
        for &(label, timestamp, versions) in fenced_cases {
            for &version in versions {
                let want_error = if version >= 5 {
                    codes::OFFSET_NOT_AVAILABLE
                } else {
                    codes::LEADER_NOT_AVAILABLE
                };
                assert!(
                    list_one_at_version(&broker, TOPIC, timestamp, version).await
                        == error_response(0, want_error),
                    "{label} at v{version}"
                );
            }
        }

        // A timestamp that resolves to an old, already-stable offset below
        // the epoch start is answered rather than refused, even in the same
        // window that fences LATEST above.
        assert!(
            list_one_at_version(&broker, TOPIC, OLD_TIMESTAMP, 11).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: OLD_TIMESTAMP,
                    offset: 0,
                    leader_epoch: 0,
                    ..Default::default()
                },
            "an old timestamp lookup below the epoch start"
        );

        // Let the watermark catch up to the epoch start and confirm the
        // fence lifts for LATEST too.
        partition.replica_state.lock().await.hw = krabka_log::Offset(6);
        assert!(
            list_one_at_version(&broker, TOPIC, LATEST_TIMESTAMP, 11)
                .await
                .error_code
                == codes::NONE
        );

        drop(client);
        broker.shutdown().await;
    }
}
