//! The controlled-shutdown drain: it moves leadership off a broker that asks
//! to shut down and reports when nothing transferable is left.
//!
//! The heartbeat client retries on every tick, so the drain is idempotent and
//! reports `false` while leadership is still moving.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::NodeId;

use crate::{
    error::BrokerError, heartbeat::controller_state::ControllerLivenessState,
    leader_election::select_replacement_leader_for_shutdown,
};

/// Scan partitions where `shutting_down` is currently leader, submit a
/// replacement-leader record for each one where a live ISR alternative
/// exists, and return `true` once every *transferable* partition has a new
/// leader, which means the broker is safe to shut down. A partition with no
/// other live replica, such as a single-replica internal topic like
/// `__consumer_offsets` or `__krabka_audit`, cannot transfer leadership
/// anywhere, so this function does not count it. A count of those partitions
/// would block controlled shutdown forever. The function returns
/// `false` while transferable leadership is still moving, and the client
/// retries on the next heartbeat tick.
///
/// The function is pure by construction. `MetadataImage` is read-only, and the
/// controller is the only side-effect channel. On a submit failure it logs and
/// returns `Ok(false)`, so the client retries rather than crashes.
pub(super) async fn drain_leaderships_for_shutdown(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: &Arc<ControllerLivenessState>,
    shutting_down: NodeId,
) -> Result<bool, BrokerError> {
    let image: Arc<MetadataImage> = controller.current_image();

    let mut leader_count: usize = 0;
    let mut changes: Vec<MetadataRecord> = Vec::new();
    // Witness nodes serve no client, so leadership never drains to one. Build
    // the set once per tick, not once per partition.
    let witnesses = crate::config_keys::witness_node_ids(&image);
    // Single O(P) walk over every partition — this runs on every heartbeat
    // tick during a controlled shutdown.
    for pr in image.all_partitions() {
        if pr.leader != shutting_down {
            continue;
        }
        if let Ok(new_pr) = select_replacement_leader_for_shutdown(
            &image,
            liveness,
            &witnesses,
            &pr.topic,
            pr.partition,
            shutting_down,
        )
        .await
        {
            // A live replica can take over: transfer leadership and keep
            // the broker waiting until the new leadership is visible.
            leader_count += 1;
            changes.push(MetadataRecord::V1Partition(new_pr));
        }
        // Else: no live alternative ISR member to transfer to — e.g. the
        // single-replica internal topics (__consumer_offsets,
        // __transaction_state, __krabka_audit), of which every broker
        // leads its own copy, or an ISR whose only survivors are witnesses.
        // Leadership cannot move anywhere, so counting
        // it would block controlled shutdown forever; and the broker is
        // stopping regardless (the partition has no other replica to serve
        // it either way). Do NOT count it toward the drain gate.
    }

    // KIP-966: a drained leadership can shrink the ISR below min ISR, which
    // leaves the replicas it dropped eligible to lead.
    crate::elr::ElrPublisher::new(&image).extend(&mut changes);

    if !changes.is_empty()
        && let Err(e) = controller.submit_change(changes).await
    {
        tracing::warn!(error = %e, "controlled shutdown: submit_change failed");
        return Ok(false);
    }

    // `leader_count` was computed against the pre-submit image and counts
    // only transferable partitions. The submit above (if any) only takes
    // effect on a subsequent heartbeat once the new image is visible — so we
    // report `should_shut_down=true` only when this broker was already not
    // leading any transferable partition.
    Ok(leader_count == 0)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;
    use crate::handlers::broker_heartbeat::test_support::{
        MockSource, image_with_dir_partition, liveness_with,
    };

    #[tokio::test]
    async fn single_replica_partition_does_not_block_controlled_shutdown() {
        // Broker 1 leads an RF=1 partition (replicas=[1], isr=[1]) — exactly
        // the shape of the broker-affinity internal topics __consumer_offsets
        // / __krabka_audit. There is nowhere to transfer leadership, so the
        // drain gate must still report "safe to shut down" (regression: this
        // used to count the partition forever and time out controlled
        // shutdown at 30s).
        let img = image_with_dir_partition(
            krabka_audit::NodeId(1),
            &[krabka_audit::NodeId(1)],
            &[krabka_audit::NodeId(1)],
            &[Uuid::nil()],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[krabka_audit::NodeId(1)]).await;

        let drained =
            drain_leaderships_for_shutdown(&controller, &liveness, krabka_audit::NodeId(1))
                .await
                .unwrap();

        assert!(drained); // untransferable partition is not counted
        assert!(captured.lock().unwrap().is_empty()); // nothing to transfer
    }

    #[tokio::test]
    async fn transferable_partition_blocks_until_leadership_moves() {
        // Broker 1 leads an RF=2 partition with broker 2 alive in ISR: it can
        // and must transfer, so the broker is not yet safe to shut down.
        let img = image_with_dir_partition(
            krabka_audit::NodeId(1),
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[Uuid::nil(), Uuid::nil()],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[krabka_audit::NodeId(1), krabka_audit::NodeId(2)]).await;

        let drained =
            drain_leaderships_for_shutdown(&controller, &liveness, krabka_audit::NodeId(1))
                .await
                .unwrap();

        assert!(!drained); // still leading a transferable partition pre-submit
        let changes = captured.lock().unwrap();
        assert!(changes.len() == 1);
        let MetadataRecord::V1Partition(pr) = &changes[0] else {
            panic!("expected V1Partition change")
        };
        assert!(pr.leader == krabka_audit::NodeId(2)); // leadership handed to the live ISR replica
    }
}
