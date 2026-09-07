//! Recovery paths that rewrite the local log after the follower and the leader
//! disagree.
//!
//! `OFFSET_OUT_OF_RANGE` resets the log to the leader's log start.
//! `FENCED_LEADER_EPOCH` runs the KIP-101 `OffsetForLeaderEpoch` lookup and
//! truncates to the epoch boundary the leader reports.
//! `OFFSET_MOVED_TO_TIERED_STORAGE` runs the KIP-405
//! `ListOffsets(EARLIEST_LOCAL_TIMESTAMP)` lookup and restarts the log at the
//! leader's local log start.

use krabka_log::Offset;
use krabka_protocol::owned::{
    fetch_response::PartitionData,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
    offset_for_leader_epoch_request::{
        OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
    },
};
use tracing::{info, warn};

use super::{
    Config, connection::connection_options, replication_target_changed, response::RowAction,
    task_replication_target,
};
use crate::codes;

pub(super) async fn handle_offset_out_of_range(
    partition_response: &PartitionData,
    cfg: &Config,
) -> RowAction {
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping out_of_range reset from stale target");
        return RowAction::Drop;
    }
    let leader_log_start = partition_response.log_start_offset;
    if let Some(partition) = cfg.partitions.get(&cfg.topic, cfg.partition) {
        // Kafka's `fetchOffsetAndTruncate`: a full reset is for a follower
        // that has fallen off the bottom of the leader's log, which is
        // `leaderStartOffset > replicaEndOffset` and nothing else.
        //
        // On a tiered leader (KIP-405) the local log can start above the
        // global one, and a fetch into that band is answered
        // `OFFSET_OUT_OF_RANGE` so the remote tier can serve it. When the
        // remote tier is momentarily unreachable the same code comes back with
        // the true, lower `log_start_offset`. Resetting on that would delete a
        // follower's good local log for a transient object-store failure, and
        // do it again on every retry. Every non-tiered `OFFSET_OUT_OF_RANGE`
        // still passes this test: the leader raises it precisely when the
        // fetch offset is below its log start.
        let local_log_end = partition.log_end_offset();
        if leader_log_start <= local_log_end.0 {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                leader_log_start,
                local_log_end = local_log_end.0,
                "replicator.out_of_range above the leader's log start; retrying without a reset"
            );
            return RowAction::Continue;
        }
        warn!(
            topic = %cfg.topic,
            partition = cfg.partition.get(),
            leader_log_start,
            "replicator.out_of_range; resetting local log to leader log_start"
        );
        let _target_guard = match partition
            .lock_replication_target(task_replication_target(cfg))
            .await
        {
            Ok(guard) => guard,
            Err(error) => {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
                    "replicator: skipping out_of_range reset from stale local target");
                return RowAction::Drop;
            }
        };
        match partition.reset_to(Offset(leader_log_start)).await {
            Ok(()) => {
                cfg.producer_state
                    .truncate(&cfg.topic, cfg.partition, leader_log_start)
                    .await;
            }
            Err(error) => {
                warn!(error = %error, "replicator: reset_to(leader_log_start) failed");
            }
        }
    }
    RowAction::Continue
}

/// Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP`: the sentinel
/// timestamp whose answer is the first offset the leader still holds on local
/// disk. It is the one question that separates a tiered leader's two floors
/// over the wire -- `EARLIEST` answers the global one, which on a tiered
/// partition names an offset that lives only in the archive.
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

/// KIP-405: restarts this follower's log at the leader's local log start after
/// the leader answered `OFFSET_MOVED_TO_TIERED_STORAGE`.
///
/// The leader raises that code for a fetch offset in
/// `[log_start, local_log_start)`: the offset exists, but only in the archive,
/// and copying the archive back down the replication path is exactly what
/// tiered storage exists to avoid. The `Fetch` response cannot carry the
/// leader's *local* log start -- its `log_start_offset` field is the global
/// one, and Kafka adds no field for the other -- so the follower asks for it,
/// with `ListOffsets(EARLIEST_LOCAL_TIMESTAMP)`, exactly as
/// `ReplicaFetcherThread.fetchEarliestLocalOffset` does before
/// `truncateFullyAndStartAt`.
///
/// This is a reset and not a truncation: every offset below the answer is in
/// the archive and none of it belongs on this replica's disk. It is safe where
/// [`handle_offset_out_of_range`]'s reset is not, because the leader named this
/// band itself rather than the follower inferring one from a log start that a
/// transient tier failure could have made look low.
///
/// The lookup failing is not evidence about the log, so nothing is deleted:
/// the row backs off and fetches again.
// cargo-mutants: an IO-only wrapper. Every step is inter-broker IO -- connect,
// send `ListOffsets`, then `reset_to` -- so no in-process seam distinguishes a
// mutant. The request it sends and the answer it reads are computed by
// `build_earliest_local_offsets_request` and `leader_local_log_start`, which
// are unit-tested below.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_offset_moved_to_tiered_storage(cfg: &Config) -> RowAction {
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping tiered-storage restart from stale target");
        return RowAction::Drop;
    }
    let Some(partition) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return RowAction::Continue;
    };
    let our_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    drop(partition);

    let local_log_start = match leader_local_log_start_offset(cfg, our_epoch).await {
        Ok(offset) => offset,
        Err(error) => {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                %error,
                "replicator: could not read the leader's local log start; retrying"
            );
            return RowAction::Backoff(cfg.replication.unexpected_error_backoff);
        }
    };

    let Some(partition) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return RowAction::Continue;
    };
    // Stale-response guard, as on every other recovery path: the metadata
    // image can have chosen another target while the lookup was in flight.
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping tiered-storage restart from stale target");
        return RowAction::Drop;
    }
    let _target_guard = match partition
        .lock_replication_target(task_replication_target(cfg))
        .await
    {
        Ok(guard) => guard,
        Err(error) => {
            warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
                "replicator: skipping tiered-storage restart from stale local target");
            return RowAction::Drop;
        }
    };
    match partition.reset_to(Offset(local_log_start)).await {
        Ok(()) => {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, local_log_start)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                local_log_start,
                "replicator: offset moved to tiered storage; restarting at the leader's \
                 local log start"
            );
        }
        Err(error) => {
            warn!(error = %error, "replicator: reset_to(leader local log start) failed");
        }
    }
    RowAction::Continue
}

/// Asks the leader for the first offset it still holds locally.
async fn leader_local_log_start_offset(cfg: &Config, our_epoch: i32) -> Result<i64, String> {
    let opts = connection_options(&cfg.client_id);
    let client = cfg
        .inter_broker_client
        .connect_as_connection(
            &cfg.leader_host,
            cfg.leader_port,
            cfg.inter_broker_listener_protocol,
            &cfg.inter_broker_server_name,
            opts,
        )
        .await
        .map_err(|e| format!("earliest_local: connect: {e}"))?;
    let response = client
        .send(build_earliest_local_offsets_request(cfg, our_epoch))
        .await
        .map_err(|e| format!("earliest_local: send: {e}"))?;
    leader_local_log_start(&response, &cfg.topic, cfg.partition.get())
}

/// The `ListOffsets` a follower sends to learn the leader's local log start.
///
/// `replica_id` carries this broker's id, as every inter-broker `ListOffsets`
/// does, and `current_leader_epoch` carries the epoch this follower is
/// replicating under so the leader can fence a stale question (KIP-320).
fn build_earliest_local_offsets_request(cfg: &Config, our_epoch: i32) -> ListOffsetsRequest {
    ListOffsetsRequest {
        replica_id: i32::try_from(cfg.node_id.0).unwrap_or(-1),
        topics: vec![ListOffsetsTopic {
            name: cfg.topic.to_string(),
            partitions: vec![ListOffsetsPartition {
                partition_index: cfg.partition.get(),
                current_leader_epoch: our_epoch,
                timestamp: EARLIEST_LOCAL_TIMESTAMP,
                ..ListOffsetsPartition::default()
            }],
            ..ListOffsetsTopic::default()
        }],
        timeout_ms: 0,
        ..ListOffsetsRequest::default()
    }
}

/// Reads the answer for one partition out of a `ListOffsets` response.
///
/// A row the leader answered with an error, a row it did not answer at all,
/// and a negative offset are all failures rather than a truncation point: none
/// of them says where this follower's log should begin, and acting on one
/// would delete a log on no evidence.
fn leader_local_log_start(
    response: &ListOffsetsResponse,
    topic: &str,
    partition: i32,
) -> Result<i64, String> {
    let Some(row) = response
        .topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
    else {
        return Err("earliest_local: the leader answered no row for this partition".into());
    };
    if row.error_code != codes::NONE {
        return Err(format!(
            "earliest_local: ListOffsets error {}",
            row.error_code
        ));
    }
    if row.offset < 0 {
        return Err(format!(
            "earliest_local: the leader reported offset {}",
            row.offset
        ));
    }
    Ok(row.offset)
}

/// Aligns the local log with the epoch history of the leader after an epoch
/// fence.
///
/// On `FENCED_LEADER_EPOCH`, this function calls `OffsetForLeaderEpoch`
/// against the leader to find the truncation point. It then truncates the local
/// log to that point.
///
/// KIP-101: the follower sends its current `leader_epoch`. The leader replies
/// with `end_offset`, the first offset of the next epoch, which is the safe
/// truncation point.
// cargo-mutants: an I/O-only wrapper with no in-process signal. Every step is
// inter-broker IO -- connect, send `OffsetForLeaderEpoch`, then `part.truncate_to`
// / `part.reset_to` -- and the `end_offset >= 0` truncate-vs-reset branch is only
// reachable once a live leader connection has answered, so no in-process seam
// distinguishes a mutant. The truncation point itself is computed by
// `truncation_offset`, which is mutation-tested.
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    name = "replicator_handle_epoch_fence",
    level = "info",
    skip_all,
    fields(topic = %cfg.topic, partition = cfg.partition.get()),
    err,
)]
pub(super) async fn handle_epoch_fence(cfg: &Config) -> Result<(), String> {
    let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return Ok(());
    };
    let our_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    drop(part);

    let opts = connection_options(&cfg.client_id);
    let client = cfg
        .inter_broker_client
        .connect_as_connection(
            &cfg.leader_host,
            cfg.leader_port,
            cfg.inter_broker_listener_protocol,
            &cfg.inter_broker_server_name,
            opts,
        )
        .await
        .map_err(|e| format!("handle_epoch_fence: connect: {e}"))?;

    let req = build_offset_for_leader_epoch_request(cfg, our_epoch);

    let resp = client
        .send(req)
        .await
        .map_err(|e| format!("handle_epoch_fence: send: {e}"))?;

    // Find our (topic, partition) in the response.
    let Some(epoch_result) = resp
        .topics
        .iter()
        .find(|t| t.topic.as_str() == &*cfg.topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == cfg.partition))
    else {
        return Ok(());
    };
    if epoch_result.error_code != codes::NONE {
        return Err(format!(
            "handle_epoch_fence: OffsetForLeaderEpoch error {}",
            epoch_result.error_code
        ));
    }
    let end_offset = epoch_result.end_offset;

    let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return Ok(());
    };

    // Stale-response guard: never truncate/reset from an OffsetForLeaderEpoch
    // response if metadata has since selected another target (see
    // `replication_target_changed`).
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping epoch-fence truncation from stale target");
        return Ok(());
    }

    let _target_guard = part
        .lock_replication_target(task_replication_target(cfg))
        .await
        .map_err(|error| format!("handle_epoch_fence: stale local target: {error}"))?;

    if end_offset >= 0 {
        // Truncate to the epoch boundary. Wrap the wire `i64` into `Offset`.
        if let Err(e) = part.truncate_to(Offset(end_offset)).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                end_offset,
                error = %e,
                "handle_epoch_fence: truncate_to failed"
            );
        } else {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, end_offset)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                end_offset,
                "handle_epoch_fence: truncated to epoch boundary"
            );
        }
    } else {
        // end_offset == -1 (UNDEFINED_OFFSET): no epoch info available;
        // reset to 0 as a safe fallback.
        if let Err(e) = part.reset_to(Offset(0)).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                error = %e,
                "handle_epoch_fence: reset_to(0) failed"
            );
        } else {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, 0)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                "handle_epoch_fence: reset to 0 (undefined epoch boundary)"
            );
        }
    }

    Ok(())
}

fn build_offset_for_leader_epoch_request(
    cfg: &Config,
    our_epoch: i32,
) -> OffsetForLeaderEpochRequest {
    OffsetForLeaderEpochRequest {
        replica_id: i32::try_from(cfg.node_id.0).unwrap_or(-1),
        topics: vec![OffsetForLeaderTopic {
            topic: cfg.topic.to_string(),
            partitions: vec![OffsetForLeaderPartition {
                partition: cfg.partition.get(),
                current_leader_epoch: our_epoch,
                leader_epoch: our_epoch,
                ..OffsetForLeaderPartition::default()
            }],
            ..OffsetForLeaderTopic::default()
        }],
        ..OffsetForLeaderEpochRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_raft::NodeId;

    use super::*;
    use crate::replicator::{
        ensure_local_partition,
        test_support::{LEADER_ID, NODE_ID, PARTITION, TOPIC, image_with_leader, test_config},
    };

    #[test]
    fn offset_epoch_request_and_connection_options_preserve_identity_fields() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let opts = connection_options(&cfg.client_id);
        assert!(opts.client_id == "replica-test");

        let req = build_offset_for_leader_epoch_request(&cfg, 7);
        let expected = OffsetForLeaderEpochRequest {
            replica_id: i32::try_from(NODE_ID.0).unwrap(),
            topics: vec![OffsetForLeaderTopic {
                topic: TOPIC.into(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: PARTITION,
                    current_leader_epoch: 7,
                    leader_epoch: 7,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(req == expected);
    }

    #[test]
    fn offset_epoch_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_offset_for_leader_epoch_request(&cfg, 7);

        assert!(req.replica_id == -1);
    }

    /// The one question that separates a tiered leader's two floors over the
    /// wire. `EARLIEST` would answer the global log start, which on a tiered
    /// partition names an offset that lives only in the archive -- restarting
    /// there is what this whole path exists to stop.
    #[test]
    fn earliest_local_request_asks_the_leader_for_its_local_floor() {
        use krabka_protocol::owned::list_offsets_request::{
            ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
        };

        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));

        let req = build_earliest_local_offsets_request(&cfg, 7);

        let expected = ListOffsetsRequest {
            replica_id: i32::try_from(NODE_ID.0).unwrap(),
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: TOPIC.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: PARTITION,
                    current_leader_epoch: 7,
                    timestamp: -4,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            timeout_ms: 0,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(req == expected);
    }

    #[test]
    fn earliest_local_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_earliest_local_offsets_request(&cfg, 7);

        assert!(req.replica_id == -1);
    }

    /// Only a row this leader answered, for this partition, without an error
    /// and with a real offset, says where the log should begin. Everything
    /// else is a failure: acting on one would delete a follower's log on no
    /// evidence at all.
    #[test]
    fn only_a_clean_row_for_this_partition_names_a_restart_point() {
        use krabka_protocol::owned::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        };

        let row = |partition_index, error_code, offset| ListOffsetsPartitionResponse {
            partition_index,
            error_code,
            timestamp: -1,
            offset,
            leader_epoch: 4,
            ..ListOffsetsPartitionResponse::default()
        };
        let response = |name: &str, partitions| ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: name.into(),
                partitions,
                ..ListOffsetsTopicResponse::default()
            }],
            ..ListOffsetsResponse::default()
        };

        let answered = response(TOPIC, vec![row(PARTITION, codes::NONE, 4_096)]);
        assert!(leader_local_log_start(&answered, TOPIC, PARTITION) == Ok(4_096));

        for (what, resp) in [
            (
                "another topic",
                response("other", vec![row(PARTITION, codes::NONE, 4_096)]),
            ),
            (
                "another partition",
                response(TOPIC, vec![row(PARTITION + 1, codes::NONE, 4_096)]),
            ),
            ("no row at all", response(TOPIC, Vec::new())),
            (
                "an error row",
                response(
                    TOPIC,
                    vec![row(PARTITION, codes::UNKNOWN_LEADER_EPOCH, 4_096)],
                ),
            ),
            (
                "an undefined offset",
                response(TOPIC, vec![row(PARTITION, codes::NONE, -1)]),
            ),
        ] {
            assert!(
                leader_local_log_start(&resp, TOPIC, PARTITION).is_err(),
                "{what} named a restart point"
            );
        }
    }

    /// A lookup that never reached the leader says nothing about the log, so
    /// nothing is deleted: the row backs off and asks again. This is the case
    /// that separates the tiered restart from a reset on a transient failure.
    #[tokio::test]
    async fn a_failed_earliest_local_lookup_backs_off_without_touching_the_log() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        cfg.leader_port = listener.local_addr().unwrap().port();
        drop(listener);
        let before = cfg
            .partitions
            .get(&cfg.topic, cfg.partition)
            .expect("local partition")
            .log_end_offset();

        let action = handle_offset_moved_to_tiered_storage(&cfg).await;

        assert!(action == RowAction::Backoff(cfg.replication.unexpected_error_backoff));
        let after = cfg
            .partitions
            .get(&cfg.topic, cfg.partition)
            .expect("local partition")
            .log_end_offset();
        assert!(after == before);
    }

    #[tokio::test]
    async fn handle_epoch_fence_surfaces_connection_failure_for_local_partition() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        cfg.leader_port = listener.local_addr().unwrap().port();
        drop(listener);

        let err = handle_epoch_fence(&cfg).await.unwrap_err();

        assert!(
            err.contains("handle_epoch_fence: connect"),
            "unexpected error: {err}"
        );
    }
}
