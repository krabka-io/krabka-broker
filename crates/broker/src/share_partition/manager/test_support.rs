//! Fixtures shared by the unit tests of the share-partition leader manager.
//!
//! The concern modules each carry their own `#[cfg(test)] mod tests`, and they
//! build their manager from here, so every test runs against the same mock
//! metadata source and the same lock duration.

use std::{path::Path, sync::Arc, time::Duration};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, Offset};
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
    manager_with_image_and_partitions(image, Arc::new(PartitionRegistry::new()))
}

/// A manager whose controller serves `image` and whose local partitions are
/// `reg`.
///
/// The share-partition start a fresh cell resolves reads both: the image
/// carries the topic and the group config, and the registry carries the log
/// the strategy resolves against.
pub(super) fn manager_with_image_and_partitions(
    image: Arc<MetadataImage>,
    reg: Arc<PartitionRegistry>,
) -> Arc<SharePartitionLeaderManager> {
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

/// Opens a real data partition under `log_dir`, appends `batches`, publishes
/// `hw` as its high watermark, and registers it in `reg`.
///
/// Each batch is `(timestamp_ms, values)`, and every record in it carries that
/// timestamp, which is what a `by_duration` strategy resolves against.
pub(super) async fn open_data_partition(
    reg: &PartitionRegistry,
    log_dir: &Path,
    topic: &str,
    partition: i32,
    batches: &[(i64, &[&'static [u8]])],
    hw: Offset,
) {
    let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let log = Log::open(&part_dir, LogConfig::default()).expect("open partition log");
    let part = crate::broker::spawn_partition(
        topic.to_string(),
        PartitionIndex(partition),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    for (timestamp_ms, values) in batches {
        let mut batch = krabka_protocol::records::RecordBatch {
            last_offset_delta: i32::try_from(values.len() - 1).expect("record count fits"),
            base_timestamp: *timestamp_ms,
            max_timestamp: *timestamp_ms,
            records: values
                .iter()
                .enumerate()
                .map(|(idx, value)| krabka_protocol::records::Record {
                    offset_delta: i32::try_from(idx).expect("offset delta fits"),
                    value: Some(bytes::Bytes::from_static(value)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        part.log
            .lock()
            .expect("partition log lock")
            .append(&mut batch)
            .expect("append records");
    }
    part.replica_state.lock().await.hw = hw;
    reg.insert(topic.into(), PartitionIndex(partition), part);
}
