//! Thin adapters that let the broker's [`crate::metadata_source::MetadataSource`]
//! satisfy the narrower controller traits the auto-rebalance, reassignment,
//! delegation-token, and break-glass sweeps depend on. They are grouped here
//! because each is a mechanical forward of the same handle.

use std::sync::Arc;

/// Wraps a real [`krabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::leader_rebalance::ControllerLike`] trait required by the
/// auto-rebalance background task.
pub(super) struct ControllerAdapter {
    pub(super) handle: Arc<dyn crate::metadata_source::MetadataSource>,
    pub(super) node_id: krabka_raft::NodeId,
}

#[async_trait::async_trait]
impl crate::leader_rebalance::ControllerLike for ControllerAdapter {
    fn is_leader(&self) -> bool {
        *self.handle.watch_leader().borrow() == Some(self.node_id)
    }

    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    async fn submit_change(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Wraps a real [`krabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::reassignment::ReassignmentController`] trait required by the
/// reassignment-completion background task.
pub(super) struct ReassignmentControllerAdapter {
    pub(super) handle: Arc<dyn crate::metadata_source::MetadataSource>,
    pub(super) node_id: krabka_raft::NodeId,
}

#[async_trait::async_trait]
impl crate::reassignment::ReassignmentController for ReassignmentControllerAdapter {
    fn is_leader(&self) -> bool {
        *self.handle.watch_leader().borrow() == Some(self.node_id)
    }

    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>> {
        self.handle.watch_image()
    }

    async fn submit_change(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// KIP-48: wraps a real [`krabka_raft::ControllerHandle`] so it
/// can satisfy the [`crate::delegation_token_cleanup::DelegationTokenController`]
/// trait required by the delegation-token expiry sweep. Every broker runs
/// the sweep. Raft serializes duplicate tombstones, so each becomes a no-op.
pub(super) struct DelegationTokenCleanupControllerAdapter {
    pub(super) handle: Arc<dyn crate::metadata_source::MetadataSource>,
}

#[async_trait::async_trait]
impl crate::delegation_token_cleanup::DelegationTokenController
    for DelegationTokenCleanupControllerAdapter
{
    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    async fn submit_mutations(
        &self,
        mutations: Vec<krabka_raft::DelegationTokenMutation>,
    ) -> Result<(), String> {
        self.handle
            .submit_delegation_token_mutations(mutations)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// KFC-9: wraps a real [`krabka_raft::ControllerHandle`] so it can satisfy the
/// [`crate::break_glass::sweep::BreakGlassController`] trait that the
/// break-glass expiry sweep needs. Every broker runs the sweep, the way every
/// broker runs the delegation-token sweep. Raft serializes duplicate
/// tombstones, so each one after the first is a no-op on the apply path.
pub(super) struct BreakGlassSweepControllerAdapter {
    pub(super) handle: Arc<dyn crate::metadata_source::MetadataSource>,
}

#[async_trait::async_trait]
impl crate::break_glass::sweep::BreakGlassController for BreakGlassSweepControllerAdapter {
    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
        self.handle.current_image()
    }

    async fn submit_change(
        &self,
        records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::broker::test_support::{fake_source, metadata_topic_record};

    #[test]
    fn controller_adapters_report_leadership_from_leader_watch() {
        let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(1)),
            Some(krabka_raft::NodeId(7)),
        ));

        let leader_adapter = ControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(7),
        };
        let follower_adapter = ControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(8),
        };
        assert!(crate::leader_rebalance::ControllerLike::is_leader(
            &leader_adapter
        ));
        assert!(!crate::leader_rebalance::ControllerLike::is_leader(
            &follower_adapter
        ));

        let leader_adapter = ReassignmentControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(7),
        };
        let follower_adapter = ReassignmentControllerAdapter {
            handle: source,
            node_id: krabka_raft::NodeId(8),
        };
        assert!(crate::reassignment::ReassignmentController::is_leader(
            &leader_adapter
        ));
        assert!(!crate::reassignment::ReassignmentController::is_leader(
            &follower_adapter
        ));
    }

    #[test]
    fn image_watcher_adapters_forward_current_image() {
        let cluster_id = uuid::Uuid::from_u128(0x5150);
        let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(fake_source(
            krabka_metadata::MetadataImage::new(cluster_id),
            Some(krabka_raft::NodeId(1)),
        ));

        let leader = ControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(1),
        };
        assert!(
            crate::leader_rebalance::ControllerLike::current_image(&leader).cluster_id()
                == cluster_id
        );

        let reassignment = ReassignmentControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(1),
        };
        assert!(
            crate::reassignment::ReassignmentController::current_image(&reassignment).cluster_id()
                == cluster_id
        );
        let reassignment_rx =
            crate::reassignment::ReassignmentController::watch_image(&reassignment);
        assert!(reassignment_rx.borrow().cluster_id() == cluster_id);

        let cleanup = DelegationTokenCleanupControllerAdapter { handle: source };
        assert!(
            crate::delegation_token_cleanup::DelegationTokenController::current_image(&cleanup)
                .cluster_id()
                == cluster_id
        );
    }

    #[tokio::test]
    async fn controller_adapters_forward_submit_errors() {
        // The only site that needs a rejecting write: each adapter must
        // surface the controller's error rather than swallow it into `Ok`.
        let source: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
            crate::test_support::FakeMetadataSource::builder()
                .image(krabka_metadata::MetadataImage::new(uuid::Uuid::from_u128(
                    1,
                )))
                .leader(Some(krabka_raft::NodeId(1)))
                .on_submit(|_| Err(krabka_raft::RaftError::Unsupported("adapter test")))
                .build(),
        );
        let record = metadata_topic_record("adapter-submit-mutant-topic", 0xADAD);

        let leader = ControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(1),
        };
        assert!(
            crate::leader_rebalance::ControllerLike::submit_change(&leader, vec![record.clone()])
                .await
                .is_err()
        );

        let reassignment = ReassignmentControllerAdapter {
            handle: source.clone(),
            node_id: krabka_raft::NodeId(1),
        };
        assert!(
            crate::reassignment::ReassignmentController::submit_change(
                &reassignment,
                vec![record.clone()],
            )
            .await
            .is_err()
        );

        let cleanup = DelegationTokenCleanupControllerAdapter { handle: source };
        assert!(
            crate::delegation_token_cleanup::DelegationTokenController::submit_mutations(
                &cleanup,
                Vec::new(),
            )
            .await
            .is_err()
        );
    }
}
