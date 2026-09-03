//! Recovery paths that rewrite the local log after the follower and the leader
//! disagree.
//!
//! `OFFSET_OUT_OF_RANGE` resets the log to the leader's log start.
//! `FENCED_LEADER_EPOCH` runs the KIP-101 `OffsetForLeaderEpoch` lookup and
//! truncates to the epoch boundary the leader reports.

use krabka_log::Offset;
use krabka_protocol::owned::{
    fetch_response::PartitionData,
    offset_for_leader_epoch_request::{
        OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
    },
};
use tracing::{info, warn};

use super::{
    Config, connection::connection_options, replication_target_changed, response::LoopAction,
    task_replication_target,
};
use crate::codes;

pub(super) async fn handle_offset_out_of_range(
    partition_response: &PartitionData,
    cfg: &Config,
) -> LoopAction {
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping out_of_range reset from stale target");
        return LoopAction::StopNotLeader;
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
            return LoopAction::Continue;
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
                return LoopAction::StopNotLeader;
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
    LoopAction::Continue
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
// The `end_offset >= 0` truncate-vs-reset branch is only reachable after a live
// leader connection returns an `OffsetForLeaderEpoch` response; the whole
// function is inter-broker IO (connect, send, then `part.truncate_to` /
// `part.reset_to`) with no pure seam. Exercised by the live-replication suite.
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
