//! Fixtures shared by the leader-election test modules. They build metadata
//! images with one partition, seed a controller liveness registry alive or
//! dead, and provide a `MetadataSource` double that records every batch the
//! code under test submits.

use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use krabka_metadata::{
    BrokerConfigRecord, LeaderEpoch, MetadataImage, MetadataRecord, PartitionRecord,
    TopicConfigRecord, TopicRecord,
};
use krabka_raft::NodeId;
use uuid::Uuid;

use crate::{
    heartbeat::controller_state::{ControllerLivenessState, TestClock},
    test_support::FakeMetadataSource,
};

pub fn img_with_partition(
    topic: &str,
    partition: i32,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).unwrap(),
    }));
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.into(),
        partition,
        leader: NodeId(leader),
        replicas: replicas.iter().copied().map(NodeId).collect(),
        isr: isr.iter().copied().map(NodeId).collect(),
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }));
    img
}

/// The witness set for a plain, non-stretch cluster. Every pre-witness
/// behaviour must be unchanged against it.
pub fn no_witnesses() -> std::collections::HashSet<NodeId> {
    std::collections::HashSet::new()
}

/// Mark `ids` as data-bearing witnesses.
pub fn witnesses(ids: &[u64]) -> std::collections::HashSet<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

/// Register `ids` as brokers and publish `broker.witness=true` for each
/// one, which is the path the real broker takes at registration. Use this
/// where the code under test reads the witness set out of the image.
pub fn mark_witnesses_in_image(img: &mut MetadataImage, ids: &[u64]) {
    register_brokers(img, ids);
    for &id in ids {
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(id),
            config_name: crate::config_keys::BROKER_WITNESS.into(),
            config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
        }));
    }
}

/// The alive-broker set the operator elections take, spelled as the ids that
/// are up. Equivalent to [`liveness_with_alive`] followed by `alive_snapshot`,
/// without the registry the selectors no longer read.
pub fn alive_set(alive: &[u64]) -> std::collections::HashSet<u64> {
    alive.iter().copied().collect()
}

pub async fn liveness_with_alive(alive: &[u64]) -> Arc<ControllerLivenessState> {
    let l = ControllerLivenessState::new(krabka_units::secs(10));
    for &n in alive {
        l.record_heartbeat(n).await;
    }
    Arc::new(l)
}

/// A metadata source over `image`, with `leader` as the controller leader. It
/// records every batch the driver submits, which is what these tests assert
/// on.
pub fn fake_source(image: MetadataImage, leader: Option<NodeId>) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(leader)
            .build(),
    )
}

/// Like [`fake_source`], but no `submit_change` ever completes. This models a raft
/// commit that stalls, so the driver's own timeout path runs.
pub fn stalled_fake_source(
    image: MetadataImage,
    leader: Option<NodeId>,
) -> Arc<FakeMetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(leader)
            .stall_submits()
            .build(),
    )
}

pub fn recovery_handle_for_tests() -> crate::unclean_recovery::UncleanRecoveryHandle {
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    crate::unclean_recovery::UncleanRecoveryHandle::for_tests(tx)
}

/// Apply a `V1TopicConfig` override on top of an existing image. This
/// matches the runtime path where `AlterConfigs` writes the record.
pub fn set_topic_config(img: &mut MetadataImage, topic: &str, key: &str, value: &str) {
    set_topic_configs(img, topic, &[(key, value)]);
}

/// [`set_topic_config`] for several keys at once. A `V1TopicConfig` record
/// replaces the topic's whole override map, so a test that needs two keys has
/// to publish them in one record.
pub fn set_topic_configs(img: &mut MetadataImage, topic: &str, entries: &[(&str, &str)]) {
    let overrides: BTreeMap<String, String> = entries
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides,
    }));
}

pub fn set_cluster_default(img: &mut MetadataImage, key: &str, value: &str) {
    img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: key.into(),
        config_value: Some(value.into()),
    }));
}

/// The submitted batches that carry partition changes.
///
/// A liveness tick also publishes the controller's fencing decisions (see
/// [`crate::heartbeat::fencing`]), so a failover test reads the batches it
/// cares about through this filter rather than by position.
pub fn partition_batches(batches: &[Vec<MetadataRecord>]) -> Vec<Vec<MetadataRecord>> {
    batches
        .iter()
        .filter(|batch| {
            batch
                .iter()
                .all(|record| matches!(record, MetadataRecord::V1Partition(_)))
        })
        .cloned()
        .collect()
}

/// The `(broker, fenced)` pairs a liveness tick published, in submission
/// order. `fenced` is `false` where the tick tombstoned the key.
pub fn fencing_updates(batches: &[Vec<MetadataRecord>]) -> Vec<(u64, bool)> {
    batches
        .iter()
        .flatten()
        .filter_map(|record| match record {
            MetadataRecord::V1BrokerConfig(config)
                if config.config_name == crate::config_keys::BROKER_FENCED =>
            {
                Some((config.node_id.0, config.config_value.is_some()))
            }
            _ => None,
        })
        .collect()
}

/// The one `PartitionRecord` in a change list that also carries records of
/// other kinds -- a KIP-966 failover appends the republished ELR beside the
/// election, so the election cannot be read by position. Panics unless
/// exactly one partition change is there.
pub fn elected_partition(changes: &[MetadataRecord]) -> &PartitionRecord {
    let mut partitions = changes.iter().filter_map(|record| match record {
        MetadataRecord::V1Partition(pr) => Some(pr),
        _ => None,
    });
    let first = partitions
        .next()
        .unwrap_or_else(|| panic!("expected a partition change, got {changes:?}"));
    assert!(
        partitions.next().is_none(),
        "expected exactly one partition change, got {changes:?}"
    );
    first
}

/// Extract the single-element `PartitionRecord` from a one-entry change
/// list. Panics if the list is empty or carries a non-partition record.
pub fn one_partition_change(changes: &[MetadataRecord]) -> &PartitionRecord {
    assert!(
        changes.len() == 1,
        "expected exactly one change, got {changes:?}"
    );
    match &changes[0] {
        MetadataRecord::V1Partition(pr) => pr,
        other => panic!("expected V1Partition, got {other:?}"),
    }
}

/// Liveness where every broker in `dead` has an expired session and every
/// broker in `alive` heartbeated inside the current window. The `tick`
/// that flips `dead` to `Dead` runs here, so the caller sees no edge.
pub async fn liveness_with_dead(dead: &[u64], alive: &[u64]) -> Arc<ControllerLivenessState> {
    let clock = TestClock::new();
    let l = ControllerLivenessState::with_test_clock(std::time::Duration::from_millis(10), &clock);
    for &n in dead {
        l.record_heartbeat(n).await;
    }
    clock.advance(std::time::Duration::from_millis(11));
    for &n in alive {
        l.record_heartbeat(n).await;
    }
    let _ = l.tick().await;
    Arc::new(l)
}

pub fn register_brokers(img: &mut MetadataImage, ids: &[u64]) {
    for &id in ids {
        img.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                node_id: NodeId(id),
                broker_epoch: 0,
                incarnation_id: Uuid::from_u128(u128::from(id)),
                host: "127.0.0.1".into(),
                port: 9_092,
                rack: None,
                endpoints: vec![],
                log_dirs: vec![],
                features: BTreeMap::new(),
            },
        ));
    }
}

pub fn img_with_dirs(
    topic: &str,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    dirs: &[uuid::Uuid],
) -> MetadataImage {
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: uuid::Uuid::nil(),
        partitions: 1,
        replication_factor: i16::try_from(replicas.len()).unwrap(),
    }));
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.into(),
        partition: 0,
        leader: NodeId(leader),
        replicas: replicas.iter().copied().map(NodeId).collect(),
        isr: isr.iter().copied().map(NodeId).collect(),
        leader_epoch: LeaderEpoch(5),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: dirs.to_vec(),
        partition_epoch: 0,
    }));
    img
}
