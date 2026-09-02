//! Fixtures shared by the unit tests of the share-partition leader manager.
//!
//! The concern modules each carry their own `#[cfg(test)] mod tests`, and they
//! build their manager from here, so every test runs against the same mock
//! metadata source and the same lock duration.

use std::{sync::Arc, time::Duration};

use krabka_metadata::{MetadataImage, NodeId};
use krabka_security::ListenerProtocol;

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
    test_support::FakeMetadataSource,
};

pub(super) const LOCK: Duration = Duration::from_secs(30);

/// A metadata source over `image`, with this node reported as the
/// controller leader.
///
/// An image that holds no brokers is deliberate in the default case: the
/// bootstrap of the share-state topic cannot run against it, so `read_state`
/// on the persister stops early with an error, before any routing. That
/// exercises the best-effort empty-window fallback of `get_or_load` without an
/// inter-broker server.
fn fake_source(image: Arc<MetadataImage>) -> Arc<dyn MetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(Some(NodeId(1)))
            .build(),
    )
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
    let controller = fake_source(image);
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
    let controller = fake_source(Arc::new(MetadataImage::new(uuid::Uuid::nil())));
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
