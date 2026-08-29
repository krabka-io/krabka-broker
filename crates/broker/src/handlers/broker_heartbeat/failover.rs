//! The KIP-112 offline-log-dir failover that a heartbeat can trigger.
//!
//! A broker that reports offline log dirs is still alive, so the liveness
//! `alive` to `dead` failover never fires for it. This module maps the
//! reported dirs to the partitions they hold and moves leadership off them.

/// Run the KIP-112 offline-dir failover for `broker`'s reported offline dirs.
/// It computes the partition changes, submits them, and returns any
/// offset-aware recovery jobs (KIP-966) for the caller to enqueue. It runs on
/// the controller leader only, and the caller gates on leadership. A submit
/// failure is logged and does not propagate.
pub(crate) async fn failover_offline_dirs(
    controller: &std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
    broker: krabka_raft::NodeId,
    offline: &std::collections::HashSet<uuid::Uuid>,
    liveness: &crate::heartbeat::controller_state::ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> Vec<(String, i32, crate::config_keys::RecoveryStrategy)> {
    let image = controller.current_image();
    let plan = crate::leader_election::compute_offline_dir_failover_changes(
        &image, broker, offline, liveness, metrics,
    )
    .await;
    if !plan.changes.is_empty()
        && let Err(e) = controller.submit_change(plan.changes).await
    {
        tracing::warn!(error = %e, "offline-dir failover submit_change failed");
    }
    plan.recoveries
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, PartitionRecord};
    use uuid::Uuid;

    use super::*;
    use crate::handlers::broker_heartbeat::test_support::{
        MockSource, image_with_dir_partition, liveness_with,
    };

    #[tokio::test]
    async fn failover_offline_dirs_submits_change_for_offline_leader() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // leader=1, replicas=[1,2], isr=[1,2]; broker 1's dir is `bad`.
        let img = image_with_dir_partition(
            krabka_audit::NodeId(1),
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[bad, good],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[krabka_audit::NodeId(1), krabka_audit::NodeId(2)]).await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let offline: std::collections::HashSet<Uuid> = maplit::hashset! {bad};

        let recoveries = failover_offline_dirs(
            &controller,
            krabka_audit::NodeId(1),
            &offline,
            &liveness,
            &metrics,
        )
        .await;

        // Exactly one change must have been submitted (the new leader record):
        // broker 2 is elected (broker 1's dir is offline), the offline replica
        // is dropped from the ISR, and both epochs are bumped.
        let changes = captured.lock().unwrap();
        let expected_changes = vec![MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: krabka_audit::NodeId(2),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![bad, good],
            partition_epoch: 1,
        })];
        assert!(*changes == expected_changes);
        // No unclean recovery needed (broker 2 is alive and in ISR).
        assert!(recoveries == vec![]);
    }

    #[tokio::test]
    async fn failover_offline_dirs_no_change_when_dir_healthy() {
        let bad = Uuid::from_u128(0xBAD);
        let good = Uuid::from_u128(0x600D);
        // Both replicas are on `good` dir; reporting `bad` as offline is a no-op.
        let img = image_with_dir_partition(
            krabka_audit::NodeId(1),
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            &[good, good],
        );
        let (source, captured) = MockSource::new(img);
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(source);
        let liveness = liveness_with(&[krabka_audit::NodeId(1), krabka_audit::NodeId(2)]).await;
        let metrics = crate::metrics::BrokerMetrics::new();
        let offline: std::collections::HashSet<Uuid> = maplit::hashset! {bad};

        let recoveries = failover_offline_dirs(
            &controller,
            krabka_audit::NodeId(1),
            &offline,
            &liveness,
            &metrics,
        )
        .await;

        // No change submitted and no recovery needed.
        let changes = captured.lock().unwrap();
        assert!(changes.is_empty());
        assert!(recoveries.is_empty());
    }
}
