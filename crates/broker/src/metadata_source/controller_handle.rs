//! The [`MetadataSource`] implementation for a live `ControllerHandle`, which
//! is what combined and controller nodes run. Every method delegates straight
//! to the openraft voter, so it lives apart from the broker-only observer path.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_raft::{
    AddVoter, ControllerHandle, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter,
    SnapshotRange, SubmitChangeResult, UpdateVoter,
};
use tokio::sync::watch;

use super::MetadataSource;

#[async_trait::async_trait]
impl MetadataSource for ControllerHandle {
    fn current_image(&self) -> Arc<MetadataImage> {
        ControllerHandle::current_image(self)
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        ControllerHandle::watch_image(self)
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        ControllerHandle::watch_leader(self)
    }
    fn quorum_state(&self) -> QuorumState {
        ControllerHandle::quorum_state(self)
    }
    fn quorum_committed_offset(&self) -> i64 {
        ControllerHandle::quorum_committed_offset(self)
    }
    fn voted_directory_id(&self) -> Option<uuid::Uuid> {
        ControllerHandle::voted_directory_id(self)
    }
    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        ControllerHandle::submit_change(self, records).await
    }
    async fn change_membership(&self, new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        ControllerHandle::change_membership(self, new_voters).await
    }
    async fn add_learner(&self, node_id: NodeId, node: Node) -> Result<(), RaftError> {
        ControllerHandle::add_learner(self, node_id, node).await
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        ControllerHandle::controller_bound_addr(self)
    }
    fn read_snapshot_range(&self, position: i64, max_bytes: i32) -> SnapshotRange {
        ControllerHandle::read_snapshot_range(self, position, max_bytes)
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        ControllerHandle::trigger_snapshot(self).await
    }
    async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::add_voter(self, req).await
    }
    async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::remove_voter(self, req).await
    }
    async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::update_voter(self, req).await
    }
    async fn finalize_kraft_version(&self, version: u16) -> Result<ReconfigOutcome, RaftError> {
        ControllerHandle::finalize_kraft_version(self, version).await
    }
    async fn cancel(&self) {
        ControllerHandle::cancel(self).await;
    }
}

#[cfg(test)]
mod tests {
    use krabka_raft::{BootstrapMode, Controller, ControllerConfig};
    use tempfile::TempDir;

    use super::*;
    use crate::metadata_source::test_support::{topic_record, wait_for_controller_leader};

    async fn bind_eventually(addr: SocketAddr) -> tokio::net::TcpListener {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => return listener,
                Err(err) if tokio::time::Instant::now() < deadline => {
                    let _ = err;
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(err) => panic!("listener address {addr} was not released: {err}"),
            }
        }
    }

    #[tokio::test]
    async fn controller_handle_metadata_source_forwards_snapshot_reconfig_and_cancel() {
        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("controller");
        wait_for_controller_leader(&ctrl).await;
        let source: &dyn MetadataSource = &ctrl;

        source
            .add_learner(NodeId(2), Node::default())
            .await
            .expect("stage learner identity");
        source
            .submit_change(vec![topic_record("snapshot-topic")])
            .await
            .expect("submit metadata");
        source
            .submit_change(vec![topic_record("snapshot-topic-2")])
            .await
            .expect("submit second metadata record");
        let expected_offset = i64::try_from(source.quorum_state().last_applied_index)
            .unwrap_or(i64::MAX)
            .saturating_sub(1);
        assert2::assert!(
            (expected_offset) != (1),
            "test must distinguish a constant offset"
        );
        assert2::assert!((source.current_metadata_offset()) == (expected_offset));
        // On a leader the quorum's committed offset is this node's own: it is
        // the node that decides what "committed" means, so the readiness probe
        // must measure exactly zero lag here. A follower is where the two part
        // company, which `krabka-raft`'s engine tests cover.
        assert2::assert!((source.quorum_committed_offset()) == (expected_offset));
        source.trigger_snapshot().await.expect("snapshot");
        assert2::assert!(matches!(
            source.read_snapshot_range(0, 1),
            SnapshotRange::Slice(_)
        ));

        let addr = source.controller_bound_addr();
        source.cancel().await;
        let listener = bind_eventually(addr).await;
        drop(listener);
    }
}
