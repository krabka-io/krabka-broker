//! Fixtures shared by the unit tests of the share-partition leader manager.
//!
//! The concern modules each carry their own `#[cfg(test)] mod tests`, and they
//! build their manager from here, so every test runs against the same mock
//! metadata source and the same lock duration.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId};
use krabka_raft::{
    AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    UpdateVoter,
};
use krabka_security::ListenerProtocol;
use tokio::sync::watch;

use super::SharePartitionLeaderManager;
use crate::{
    coordinator::unified::share::config::ShareGroupConfig,
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    share_coordinator::{
        config::ShareCoordinatorConfig, coordinator::ShareCoordinator,
        persister_client::SharePersister,
    },
};

pub(super) const LOCK: Duration = Duration::from_secs(30);

/// Minimal `MetadataSource` over a fixed image that holds no brokers.
///
/// The bootstrap of the share-state topic cannot run against this image,
/// because it has no brokers. The `read_state` of the persister thus stops
/// early with an error, before any routing. This exercises the best-effort
/// empty-window fallback of `get_or_load` without an inter-broker server.
struct MockSource {
    image: Arc<MetadataImage>,
    leader_rx: watch::Receiver<Option<NodeId>>,
    _leader_tx: watch::Sender<Option<NodeId>>,
}

impl MockSource {
    fn new() -> Self {
        Self::with_image(Arc::new(MetadataImage::new(uuid::Uuid::nil())))
    }

    fn with_image(image: Arc<MetadataImage>) -> Self {
        let (tx, rx) = watch::channel(Some(krabka_metadata::NodeId(1)));
        Self {
            image,
            leader_rx: rx,
            _leader_tx: tx,
        }
    }
}

#[async_trait]
impl MetadataSource for MockSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.image.clone()
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        unimplemented!()
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_rx.clone()
    }
    fn quorum_state(&self) -> QuorumState {
        unimplemented!()
    }
    async fn submit_change(
        &self,
        _records: Vec<MetadataRecord>,
    ) -> Result<krabka_raft::SubmitChangeResult, RaftError> {
        Ok(krabka_raft::SubmitChangeResult::default())
    }
    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        unimplemented!()
    }
    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        unimplemented!()
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        unimplemented!()
    }
    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        unimplemented!()
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        unimplemented!()
    }
    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!()
    }
    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!()
    }
    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!()
    }
    async fn cancel(&self) {}
}

pub(super) fn manager() -> Arc<SharePartitionLeaderManager> {
    manager_with_unlimited_fallback(
        crate::config::BrokerConfig::default().share_session_cache_max_when_unlimited,
    )
}

/// A manager whose controller serves `image`.
///
/// `current_leader_of` and the related methods thus resolve real topic and
/// partition leadership.
pub(super) fn manager_with_image(image: Arc<MetadataImage>) -> Arc<SharePartitionLeaderManager> {
    let reg = Arc::new(PartitionRegistry::new());
    let controller: Arc<dyn MetadataSource> = Arc::new(MockSource::with_image(image));
    let coord = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        reg.clone(),
        ShareCoordinatorConfig::default(),
    ));
    let client = Arc::new(InterBrokerClient::new(None, None));
    let persister = Arc::new(SharePersister::new(
        krabka_audit::NodeId(1),
        coord,
        controller.clone(),
        client,
        ListenerProtocol::Plaintext,
        "INTERNAL".to_string(),
    ));
    Arc::new(SharePartitionLeaderManager::new(
        krabka_audit::NodeId(1),
        reg,
        controller,
        persister,
        Arc::new(ShareGroupConfig::default()),
        crate::config::BrokerConfig::default().share_session_cache_max_when_unlimited,
    ))
}

pub(super) fn manager_with_unlimited_fallback(fallback: usize) -> Arc<SharePartitionLeaderManager> {
    let reg = Arc::new(PartitionRegistry::new());
    let controller: Arc<dyn MetadataSource> = Arc::new(MockSource::new());
    let coord = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        reg.clone(),
        ShareCoordinatorConfig::default(),
    ));
    let client = Arc::new(InterBrokerClient::new(None, None));
    let persister = Arc::new(SharePersister::new(
        krabka_audit::NodeId(1),
        coord,
        controller.clone(),
        client,
        ListenerProtocol::Plaintext,
        "INTERNAL".to_string(),
    ));
    Arc::new(SharePartitionLeaderManager::new(
        krabka_audit::NodeId(1),
        reg,
        controller,
        persister,
        Arc::new(ShareGroupConfig::default()),
        fallback,
    ))
}
