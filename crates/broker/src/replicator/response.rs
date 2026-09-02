//! Interpretation of one `Fetch` response.
//!
//! The module locates this task's partition in the response, appends the
//! records it carries, and maps every Kafka error code to the next action of
//! the fetch loop, including the KIP-320 in-band divergence signal and the
//! backoffs that keep a persistent error from hot-spinning the CPU.

use krabka_log::Offset;
use krabka_protocol::{
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};
use krabka_units::convert::TimeExt;
use krabka_verified::ReplicaFetchMutation;
use tracing::{info, warn};

use super::{
    Config, replication_target_changed, task_replication_target,
    truncation::{handle_epoch_fence, handle_offset_out_of_range},
};
use crate::codes;

/// Outcome of one fetch round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoopAction {
    Continue,
    StopNotLeader,
}

// KIP-320 in-band truncation + KIP-101 epoch fence add match arms
pub(super) async fn handle_response(
    mut resp: FetchResponse,
    cfg: &Config,
    request_leader_epoch: i32,
) -> LoopAction {
    // The replicator only ever requests one (topic, partition) per Fetch.
    // Match the field carried by the negotiated version (`topic` at v <= 12,
    // `topic_id` at v >= 13). If both are populated, both must agree. Require
    // one unique partition row so a contradictory or duplicated response is
    // side-effect free.
    //
    // Take `resp` BY VALUE and resolve the matching partition by *mutable*
    // reference so the record batches can be moved out (via `records.take()`)
    // and handed to the writer without a deep clone per batch.
    let Some(part_resp) = find_partition_response(&mut resp, cfg) else {
        return LoopAction::Continue;
    };

    let target_matches = !replication_target_changed(cfg);
    let reported_leader = &part_resp.current_leader;
    let reported_leader_absent =
        reported_leader.leader_id == -1 && reported_leader.leader_epoch == -1;
    let reported_leader_exact = u64::try_from(reported_leader.leader_id).ok()
        == Some(cfg.leader_node_id.0)
        && reported_leader.leader_epoch == cfg.leader_epoch.0;
    let reported_target_matches = reported_leader_absent || reported_leader_exact;
    let mutation = krabka_verified::replica_fetch_mutation(
        (true, true),
        (request_leader_epoch, cfg.leader_epoch.0),
        (target_matches, reported_target_matches),
        (
            part_resp.error_code == codes::NONE,
            part_resp.diverging_epoch.end_offset >= 0,
        ),
    );

    if mutation == ReplicaFetchMutation::Reject {
        warn!(
            topic = %cfg.topic,
            partition = cfg.partition.get(),
            leader_node_id = cfg.leader_node_id.0,
            leader_epoch = cfg.leader_epoch.0,
            request_leader_epoch,
            reported_leader_id = reported_leader.leader_id,
            reported_leader_epoch = reported_leader.leader_epoch,
            "replicator: discarding fenced Fetch response"
        );
        return LoopAction::StopNotLeader;
    }

    match mutation {
        ReplicaFetchMutation::Truncate => {
            // KIP-320: an in-band divergence signal. The leader served no
            // records and told us the epoch/offset our log must truncate to.
            // `EpochEndOffset` defaults to (epoch:-1, end_offset:-1); a
            // populated `end_offset >= 0` means "truncate here".
            // Recheck immediately before mutating: metadata may have
            // changed since the response-level guard above.
            if replication_target_changed(cfg) {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: skipping diverging_epoch truncation from stale target");
                return LoopAction::StopNotLeader;
            }
            let end_offset = part_resp.diverging_epoch.end_offset;
            if let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) {
                let _target_guard = match part
                    .lock_replication_target(task_replication_target(cfg))
                    .await
                {
                    Ok(guard) => guard,
                    Err(error) => {
                        warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
                            "replicator: skipping diverging_epoch truncation from stale local target");
                        return LoopAction::StopNotLeader;
                    }
                };
                // Wrap the wire `i64` into `Offset` for the log-layer call.
                match part.truncate_to(Offset(end_offset)).await {
                    Ok(()) => {
                        // Drop idempotent-producer dedup entries for the
                        // truncated tail, or a retried batch deduplicates
                        // against an offset the log no longer holds and its
                        // acks=all HW gate stalls forever (failover stall).
                        cfg.producer_state
                            .truncate(&cfg.topic, cfg.partition, end_offset)
                            .await;
                        info!(
                            topic = %cfg.topic,
                            partition = cfg.partition.get(),
                            end_offset,
                            "replicator: truncated to diverging_epoch (KIP-320 in-band)"
                        );
                    }
                    Err(e) => warn!(
                        topic = %cfg.topic,
                        partition = cfg.partition.get(),
                        end_offset,
                        error = %e,
                        "replicator: truncate_to(diverging_epoch) failed"
                    ),
                }
            }
            LoopAction::Continue
        }

        ReplicaFetchMutation::Append => {
            let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: local partition vanished between fetches");
                return LoopAction::Continue;
            };
            let _target_guard = match part
                .lock_replication_target(task_replication_target(cfg))
                .await
            {
                Ok(guard) => guard,
                Err(error) => {
                    warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
                        "replicator: discarding response for stale local target");
                    return LoopAction::StopNotLeader;
                }
            };
            // Move the parsed v2 batches out of the owned response so each
            // batch can be handed to the writer BY VALUE — no per-batch deep
            // clone. `take()` leaves `None` behind; the response is dropped at
            // the end of this call so nothing is read from `records` again.
            // `Raw`/`Legacy` payloads were never processed here (the old
            // `as_v2()` returned `None` for them), so they are ignored.
            if let Some(RecordsPayload::V2(batches)) = part_resp.records.take() {
                for batch in batches {
                    if replication_target_changed(cfg) {
                        return LoopAction::StopNotLeader;
                    }
                    // Capture byte count before the move into replicate_batch
                    // so the metrics update only fires on a successful append.
                    // PERF: `encoded_len()` is computed here for the metric and
                    // again inside the append path; threading a single
                    // computation through would save the re-walk, but that
                    // touches the writer API (cross-file) so it's left as-is.
                    let batch_bytes = batch.encoded_len();
                    if let Err(e) = part.replicate_batch(batch).await {
                        warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
                            "replicator: replicate_batch failed");
                        break;
                    }
                    cfg.metrics.record_replication_in(
                        &cfg.topic,
                        cfg.partition.get(),
                        u64::try_from(batch_bytes).unwrap_or(0),
                    );
                }
            }
            // KIP-392: record the leader's high watermark so consumer reads
            // served from this follower are bounded correctly. Done on every
            // successful response, including empty ones. Wrap the wire `i64`.
            if replication_target_changed(cfg) {
                return LoopAction::StopNotLeader;
            }
            part.set_follower_hw(Offset(part_resp.high_watermark)).await;
            LoopAction::Continue
        }

        ReplicaFetchMutation::Retry => match part_resp.error_code {
            codes::OFFSET_OUT_OF_RANGE => handle_offset_out_of_range(part_resp, cfg).await,
            codes::UNKNOWN_TOPIC_OR_PARTITION => {
                // Leader hasn't materialized its side yet
                // (CreateTopics-vs-replicator race).
                tokio::time::sleep(cfg.replication.unknown_topic_retry_delay.to_std()).await;
                LoopAction::Continue
            }
            codes::NOT_LEADER_OR_FOLLOWER => LoopAction::StopNotLeader,
            codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH => {
                // Stale-response guard: if the target changed, this
                // follower-replicator is stale — STOP it. Without this it neither
                // truncates (the `replication_target_changed` guard in
                // `handle_epoch_fence` skips that) nor stops, so it hot-loops the
                // Fetch at ~full CPU, starving metadata propagation and the
                // cooperative cancellation that would otherwise retire it — the
                // broker then never becomes ready and crashloops.
                if replication_target_changed(cfg) {
                    warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: stopping on fenced epoch from stale target");
                    return LoopAction::StopNotLeader;
                }
                if part_resp.error_code == codes::FENCED_LEADER_EPOCH {
                    warn!(
                        topic = %cfg.topic,
                        partition = cfg.partition.get(),
                        "replicator: fenced leader epoch; calling OffsetForLeaderEpoch"
                    );
                    if let Err(error) = handle_epoch_fence(cfg).await {
                        warn!(
                            topic = %cfg.topic,
                            partition = cfg.partition.get(),
                            %error,
                            "replicator: OffsetForLeaderEpoch recovery failed"
                        );
                    }
                } else {
                    // The leader is behind our metadata image. It cannot answer an
                    // epoch lookup yet, so leave the log untouched and retry Fetch.
                    warn!(
                        topic = %cfg.topic,
                        partition = cfg.partition.get(),
                        "replicator: leader does not know current epoch; retrying Fetch"
                    );
                }
                // Back off before re-fetching so a persistent fence (e.g. our
                // leader_epoch hasn't caught up to the new leader's yet) doesn't
                // hot-spin the CPU between fetch and fence.
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return LoopAction::StopNotLeader,
                    () = tokio::time::sleep(cfg.replication.epoch_fence_backoff.to_std()) => {}
                }
                LoopAction::Continue
            }
            other => {
                warn!(
                    error_code = other,
                    "replicator: unexpected fetch error_code"
                );
                tokio::time::sleep(cfg.replication.unexpected_error_backoff.to_std()).await;
                LoopAction::Continue
            }
        },

        ReplicaFetchMutation::Reject => unreachable!("rejected responses return above"),
    }
}

fn find_partition_response<'a>(
    response: &'a mut FetchResponse,
    cfg: &Config,
) -> Option<&'a mut PartitionData> {
    let mut found = None;
    for (topic_index, topic) in response.responses.iter().enumerate() {
        if !fetch_topic_identity_matches(topic, cfg) {
            continue;
        }
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            if partition.partition_index == cfg.partition
                && found.replace((topic_index, partition_index)).is_some()
            {
                return None;
            }
        }
    }
    let (topic_index, partition_index) = found?;
    response
        .responses
        .get_mut(topic_index)?
        .partitions
        .get_mut(partition_index)
}

fn fetch_topic_identity_matches(topic: &FetchableTopicResponse, cfg: &Config) -> bool {
    let name_present = !topic.topic.is_empty();
    let id_present = topic.topic_id != WireUuid::ZERO;
    (name_present || id_present)
        && (!name_present || topic.topic == cfg.topic)
        && (!id_present || topic.topic_id == cfg.topic_id)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_protocol::{
        owned::fetch_response::{EpochEndOffset, LeaderIdAndEpoch},
        records::{Record, RecordBatch},
    };
    use krabka_raft::NodeId;
    use krabka_units::secs;

    use super::*;
    use crate::replicator::{
        ensure_local_partition,
        test_support::{
            LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, fetch_response, image_with_leader,
            partition_response, test_config,
        },
    };

    fn one_record_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: 4,
            last_offset_delta: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unexpected_error_uses_configured_backoff() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.replication.unexpected_error_backoff = secs(37);
        let response = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::INVALID_REQUEST),
        );

        let response_task =
            tokio::spawn(async move { handle_response(response, &cfg, cfg.leader_epoch.0).await });
        tokio::task::yield_now().await;
        assert!(!response_task.is_finished());

        tokio::time::advance(Duration::from_secs(36)).await;
        tokio::task::yield_now().await;
        assert!(!response_task.is_finished());

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(response_task.await.unwrap() == LoopAction::Continue);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_leader_epoch_retries_without_epoch_lookup() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        cfg.replication.epoch_fence_backoff = secs(37);
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        cfg.leader_port = listener.local_addr().unwrap().port();
        let response = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::UNKNOWN_LEADER_EPOCH),
        );

        let response_task =
            tokio::spawn(async move { handle_response(response, &cfg, cfg.leader_epoch.0).await });
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(37)).await;
        tokio::task::yield_now().await;

        assert!(
            response_task.is_finished(),
            "UNKNOWN_LEADER_EPOCH attempted an OffsetForLeaderEpoch connection"
        );
        assert!(response_task.await.unwrap() == LoopAction::Continue);
        assert!(listener.accept().is_err(), "unexpected epoch lookup");
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_name() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            TOPIC,
            WireUuid::ZERO,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_topic_id() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            "",
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn handle_response_rejects_contradictory_or_duplicate_identity() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));

        for resp in [
            fetch_response(
                TOPIC,
                WireUuid([8; 16]),
                partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
            ),
            fetch_response(
                "other",
                WIRE_TOPIC_ID,
                partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
            ),
        ] {
            assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::Continue);
        }

        let mut duplicate = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );
        duplicate.responses[0]
            .partitions
            .push(partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER));
        assert!(handle_response(duplicate, &cfg, cfg.leader_epoch.0).await == LoopAction::Continue);
    }

    #[tokio::test]
    async fn handle_response_rejects_mismatched_reported_leader() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        for current_leader in [
            LeaderIdAndEpoch {
                leader_id: 99,
                leader_epoch: cfg.leader_epoch.0,
                ..LeaderIdAndEpoch::default()
            },
            LeaderIdAndEpoch {
                leader_id: -2,
                leader_epoch: -2,
                ..LeaderIdAndEpoch::default()
            },
            LeaderIdAndEpoch {
                leader_id: -1,
                leader_epoch: cfg.leader_epoch.0,
                ..LeaderIdAndEpoch::default()
            },
        ] {
            let resp = fetch_response(
                TOPIC,
                WIRE_TOPIC_ID,
                PartitionData {
                    partition_index: PARTITION,
                    error_code: codes::NONE,
                    current_leader,
                    ..PartitionData::default()
                },
            );

            assert!(
                handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::StopNotLeader
            );
        }
    }

    #[tokio::test]
    async fn stale_request_epoch_cannot_truncate_the_follower_log() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let part = cfg.partitions.get(&cfg.topic, cfg.partition).unwrap();
        part.replicate_batch(one_record_batch(0)).await.unwrap();
        assert!(part.log_end_offset() == Offset(1));

        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                diverging_epoch: EpochEndOffset {
                    epoch: cfg.leader_epoch.0,
                    end_offset: 0,
                    ..EpochEndOffset::default()
                },
                ..PartitionData::default()
            },
        );

        assert!(
            handle_response(resp, &cfg, cfg.leader_epoch.0 - 1).await == LoopAction::StopNotLeader
        );
        assert!(part.log_end_offset() == Offset(1));
    }

    #[tokio::test]
    async fn failed_append_leaves_log_retryable() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let part = cfg.partitions.get(&cfg.topic, cfg.partition).unwrap();

        let bad = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                records: Some(RecordsPayload::V2(vec![one_record_batch(2)])),
                ..PartitionData::default()
            },
        );
        assert!(handle_response(bad, &cfg, cfg.leader_epoch.0).await == LoopAction::Continue);
        assert!(part.log_end_offset() == Offset(0));

        let retry = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                records: Some(RecordsPayload::V2(vec![one_record_batch(0)])),
                ..PartitionData::default()
            },
        );
        assert!(handle_response(retry, &cfg, cfg.leader_epoch.0).await == LoopAction::Continue);
        assert!(part.log_end_offset() == Offset(1));
    }

    #[tokio::test]
    async fn handle_response_ignores_other_partition_index() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION + 1, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::Continue);
    }

    #[tokio::test]
    async fn handle_response_stops_on_diverging_epoch_after_local_promotion() {
        let (cfg, _log_dir) = test_config(image_with_leader(NODE_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                diverging_epoch: EpochEndOffset {
                    epoch: 4,
                    end_offset: 0,
                    ..EpochEndOffset::default()
                },
                ..PartitionData::default()
            },
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::StopNotLeader);
    }

    #[tokio::test]
    async fn handle_response_stops_on_out_of_range_after_remote_target_change() {
        let (cfg, _log_dir) = test_config(image_with_leader(NodeId(99)));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::OFFSET_OUT_OF_RANGE),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == LoopAction::StopNotLeader);
    }
}
