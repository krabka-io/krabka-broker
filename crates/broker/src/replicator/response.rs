//! Interpretation of one partition row of a `Fetch` response.
//!
//! One request now covers every partition a fetcher follows on one leader, so
//! the response carries a row per partition rather than exactly one row. This
//! module handles one row: it applies the identity, epoch and target checks
//! that the single-row path always applied, appends the records the row
//! carries, and maps every Kafka error code to what the fetcher should do with
//! that partition -- including the KIP-320 in-band divergence signal.
//!
//! A row never sleeps. A backoff is the fetcher's, not one partition's: a
//! sleep taken here would stall every other partition sharing the round, which
//! is the cost the batching exists to remove. A row that wants one says so
//! with [`RowAction::Backoff`] and the loop applies the longest of them once.

use krabka_log::Offset;
use krabka_protocol::{
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};
use krabka_units::Time;
use krabka_verified::ReplicaFetchMutation;
use tracing::{info, warn};

use super::{
    Config, replication_target_changed, task_replication_target,
    truncation::{handle_epoch_fence, handle_offset_out_of_range},
};
use crate::codes;

/// What the fetcher should do with one partition after reading its row.
///
/// It is not `Eq`: [`RowAction::Backoff`] carries a [`Time`], whose `f64`
/// storage is only `PartialEq`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RowAction {
    /// Keep following the partition and fetch it again next round.
    Continue,
    /// Stop following the partition. The leader disowned it, or the response
    /// was fenced against a target this follower no longer has. The next
    /// supervisor reconcile decides where it belongs.
    Drop,
    /// Keep following the partition, but hold the whole round back first, so a
    /// persistent error does not hot-spin the fetch loop.
    Backoff(Time),
}

/// Applies one partition row of a response against the partition it names.
///
/// `part_resp` is taken by *mutable* reference so the record batches can be
/// moved out of it (via `records.take()`) and handed to the writer without a
/// deep clone per batch.
///
/// `request_leader_epoch` is the epoch this follower sent for *this* partition
/// in the request the row answers. The metadata image may advance while a
/// request is in flight, and a batched request carries a different epoch per
/// row, so the fence is per row and not per response.
// KIP-320 in-band truncation + KIP-101 epoch fence add match arms
pub(super) async fn handle_partition_response(
    part_resp: &mut PartitionData,
    cfg: &Config,
    request_leader_epoch: i32,
) -> RowAction {
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
        return RowAction::Drop;
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
                return RowAction::Drop;
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
                        return RowAction::Drop;
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
            RowAction::Continue
        }

        ReplicaFetchMutation::Append => {
            let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: local partition vanished between fetches");
                return RowAction::Continue;
            };
            let _target_guard = match part
                .lock_replication_target(task_replication_target(cfg))
                .await
            {
                Ok(guard) => guard,
                Err(error) => {
                    warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
                        "replicator: discarding response for stale local target");
                    return RowAction::Drop;
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
                        return RowAction::Drop;
                    }
                    // Capture byte count before the move into replicate_batch
                    // so the metrics update only fires on a successful append.
                    // PERF — measured; decision: KEEP. `encoded_len()` is
                    // computed here for the metric and again inside the append
                    // path; threading a single computation through would save
                    // the re-walk, but that changes the writer API
                    // (cross-file). `benches/perf_deferrals.rs` times the walk
                    // against the `replicate_batch` it precedes, over the
                    // batch shapes a producer actually writes: 0.01% of the
                    // append for one 100 KiB record, ~1% for 100 x 1 KiB, ~4%
                    // for 1000 x 100 B. Even that worst shape's ~4% sits
                    // inside the append's own run-to-run spread, so the
                    // re-walk is not separable from noise in production.
                    // Those three figures are what the `bench` job of the `ci`
                    // workflow measures on the nightly schedule and prints in
                    // its job summary; the latest scheduled `ci` run in the
                    // Actions tab is the reproducible reading, and its
                    // `criterion-baseline` artifact holds the samples.
                    // Revisit only if the replicator's shape mix moves to very
                    // wide batches of tiny records, where the walk is a
                    // per-record cost and the append is not.
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
                return RowAction::Drop;
            }
            part.set_follower_hw(Offset(part_resp.high_watermark)).await;
            RowAction::Continue
        }

        ReplicaFetchMutation::Retry => match part_resp.error_code {
            codes::OFFSET_OUT_OF_RANGE => handle_offset_out_of_range(part_resp, cfg).await,
            codes::UNKNOWN_TOPIC_OR_PARTITION => {
                // Leader hasn't materialized its side yet
                // (CreateTopics-vs-replicator race).
                RowAction::Backoff(cfg.replication.unknown_topic_retry_delay)
            }
            codes::NOT_LEADER_OR_FOLLOWER => RowAction::Drop,
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
                    return RowAction::Drop;
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
                RowAction::Backoff(cfg.replication.epoch_fence_backoff)
            }
            other => {
                warn!(
                    error_code = other,
                    "replicator: unexpected fetch error_code"
                );
                RowAction::Backoff(cfg.replication.unexpected_error_backoff)
            }
        },

        ReplicaFetchMutation::Reject => unreachable!("rejected responses return above"),
    }
}

/// One partition this round asked about, and the epoch it asked under.
pub(super) struct RoundEntry<'round> {
    pub(super) cfg: &'round Config,
    /// The `current_leader_epoch` this follower sent for this partition. The
    /// metadata image can advance while the request is in flight, so the fence
    /// is against what was sent and not against what is current.
    pub(super) request_leader_epoch: i32,
}

/// Applies one response to every partition of the round that asked for it.
///
/// Returns one action per entry of `round`, in the same order.
///
/// A partition the response does not mention gets [`RowAction::Continue`]: on
/// a KIP-227 incremental fetch the leader answers only with the partitions
/// whose state changed, so silence is the normal answer for a caught-up
/// partition and not an error.
///
/// The response is indexed once, so a round covering ten thousand partitions
/// costs one pass over the rows rather than one scan per partition. A
/// partition named by two rows resolves to neither, which is what the
/// single-row path did: a contradictory response is side-effect free.
pub(super) async fn apply_response(
    response: &mut FetchResponse,
    round: &[RoundEntry<'_>],
) -> Vec<RowAction> {
    let index = ResponseIndex::of(response);
    let mut actions = Vec::with_capacity(round.len());
    for entry in round {
        let Some(location) = index.locate(response, entry.cfg) else {
            actions.push(RowAction::Continue);
            continue;
        };
        let Some(part_resp) = response
            .responses
            .get_mut(location.0)
            .and_then(|topic| topic.partitions.get_mut(location.1))
        else {
            actions.push(RowAction::Continue);
            continue;
        };
        actions.push(
            handle_partition_response(part_resp, entry.cfg, entry.request_leader_epoch).await,
        );
    }
    actions
}

/// Where each `(topic identity, partition)` sits in one response.
///
/// A key that two rows claim maps to `None`, so the partition it names is left
/// untouched.
#[derive(Default)]
struct ResponseIndex {
    by_id: std::collections::HashMap<(WireUuid, i32), Option<(usize, usize)>>,
    by_name: std::collections::HashMap<(String, i32), Option<(usize, usize)>>,
}

impl ResponseIndex {
    fn of(response: &FetchResponse) -> Self {
        let mut index = Self::default();
        for (topic_index, topic) in response.responses.iter().enumerate() {
            for (partition_index, partition) in topic.partitions.iter().enumerate() {
                let at = (topic_index, partition_index);
                if topic.topic_id != WireUuid::ZERO {
                    index
                        .by_id
                        .entry((topic.topic_id, partition.partition_index))
                        .and_modify(|slot| *slot = None)
                        .or_insert(Some(at));
                }
                if !topic.topic.is_empty() {
                    index
                        .by_name
                        .entry((topic.topic.clone(), partition.partition_index))
                        .and_modify(|slot| *slot = None)
                        .or_insert(Some(at));
                }
            }
        }
        index
    }

    /// The row that answers `cfg`, if exactly one does and its identity
    /// matches on every field the negotiated version populated.
    fn locate(&self, response: &FetchResponse, cfg: &Config) -> Option<(usize, usize)> {
        let by_id = (cfg.topic_id != WireUuid::ZERO)
            .then(|| self.by_id.get(&(cfg.topic_id, cfg.partition.get())))
            .flatten();
        let by_name = self
            .by_name
            .get(&(cfg.topic.to_string(), cfg.partition.get()));
        // A key claimed twice under either identity is ambiguous under both:
        // the row it would resolve to is one of a contradictory pair.
        let at = match (by_id, by_name) {
            (Some(Some(at)), None) | (None, Some(Some(at))) => *at,
            (Some(Some(id_at)), Some(Some(name_at))) if id_at == name_at => *id_at,
            // Either identity claimed twice, the two identities disagreeing
            // about which row answers, or no row at all: none of them names
            // one unambiguous row, and none of them may mutate anything.
            _ => return None,
        };
        let topic = response.responses.get(at.0)?;
        fetch_topic_identity_matches(topic, cfg).then_some(at)
    }
}

fn fetch_topic_identity_matches(topic: &FetchableTopicResponse, cfg: &Config) -> bool {
    let name_present = !topic.topic.is_empty();
    let id_present = topic.topic_id != WireUuid::ZERO;
    (name_present || id_present)
        && (!name_present || topic.topic.as_str() == &*cfg.topic)
        && (!id_present || topic.topic_id == cfg.topic_id)
}

/// Applies one response to one partition, as the single-partition path used
/// to. It is the batched path with a round of one, so the tests below still
/// exercise the identity, epoch and target checks exactly as they run in
/// production.
#[cfg(test)]
pub(super) async fn handle_response(
    mut resp: FetchResponse,
    cfg: &Config,
    request_leader_epoch: i32,
) -> RowAction {
    let round = [RoundEntry {
        cfg,
        request_leader_epoch,
    }];
    apply_response(&mut resp, &round)
        .await
        .into_iter()
        .next()
        .expect("a round of one answers one action")
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

    /// A row that wants a backoff says so and returns; the sleep is the
    /// fetcher's, because a sleep taken here would hold back every other
    /// partition sharing the round.
    #[tokio::test(start_paused = true)]
    async fn unexpected_error_asks_for_the_configured_backoff() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.replication.unexpected_error_backoff = secs(37);
        let response = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::INVALID_REQUEST),
        );

        let started = tokio::time::Instant::now();
        let action = handle_response(response, &cfg, cfg.leader_epoch.0).await;

        assert!(action == RowAction::Backoff(secs(37)));
        assert!(
            started.elapsed() == Duration::ZERO,
            "the row must not sleep on behalf of the round"
        );
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

        let action = handle_response(response, &cfg, cfg.leader_epoch.0).await;

        assert!(action == RowAction::Backoff(secs(37)));
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

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_topic_id() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            "",
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
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
            assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
        }

        let mut duplicate = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );
        duplicate.responses[0]
            .partitions
            .push(partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER));
        assert!(handle_response(duplicate, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
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

            assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
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

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0 - 1).await == RowAction::Drop);
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
        assert!(handle_response(bad, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
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
        assert!(handle_response(retry, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
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

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
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

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }

    #[tokio::test]
    async fn handle_response_stops_on_out_of_range_after_remote_target_change() {
        let (cfg, _log_dir) = test_config(image_with_leader(NodeId(99)));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::OFFSET_OUT_OF_RANGE),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }
}
