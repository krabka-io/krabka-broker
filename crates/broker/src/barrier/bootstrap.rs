//! Lazy creation of the `__barrier_state` internal topic.
//!
//! The topic holds the group definitions, the injection-start records, and the
//! cuts. It is compacted, so the newest record of every key survives for the
//! life of the log. Old cuts leave through tombstones and not through a log
//! trim, because the group definitions share the same prefix.
//!
//! This mirrors the `__transaction_state` and `__share_group_state`
//! bootstraps.

use std::{collections::BTreeMap, sync::Arc};

use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord};
use krabka_raft::RaftError;
use uuid::Uuid;

use crate::{
    barrier::{STATE_TOPIC, error::BarrierError},
    metadata_source::MetadataSource,
};

/// Make sure `__barrier_state` exists in the controller's metadata.
///
/// The function does nothing when the topic already exists. It tolerates
/// `TopicExists`, because two brokers can reach this path at the same time.
///
/// # Errors
/// Returns [`BarrierError::Bootstrap`] when no broker is registered yet, or
/// when the controller rejects the change for any reason other than
/// `TopicExists`.
pub(crate) async fn ensure_topic(
    controller: &Arc<dyn MetadataSource>,
    num_partitions: i32,
    replication_factor: i16,
) -> Result<(), BarrierError> {
    let image = controller.current_image();
    if image.topic(STATE_TOPIC).is_some() {
        return Ok(());
    }

    let mut sorted: Vec<NodeId> = image.brokers().map(|b| b.node_id).collect();
    if sorted.is_empty() {
        return Err(BarrierError::Bootstrap(
            "no brokers registered; cannot bootstrap __barrier_state".to_owned(),
        ));
    }
    sorted.sort_unstable();

    let records = topic_records(&sorted, num_partitions, replication_factor);
    match controller.submit_change(records).await {
        Ok(_) | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(BarrierError::Bootstrap(format!(
            "submit_change failed: {e}"
        ))),
    }
}

/// Build the metadata records that create `__barrier_state`.
///
/// `brokers` is the sorted set of registered node ids. The replicas of
/// partition `p` start at `brokers[p % brokers.len()]` and walk the list, which
/// is the round-robin assignment the other internal topics use.
fn topic_records(
    brokers: &[NodeId],
    num_partitions: i32,
    replication_factor: i16,
) -> Vec<MetadataRecord> {
    let count = brokers.len();
    let rf_usize = crate::bootstrap::internal_topic_replication_factor(replication_factor, count);
    let rf = i16::try_from(rf_usize).expect("bounded by the configured i16 replication factor");

    let capacity = 2 + usize::try_from(num_partitions).unwrap_or(0);
    let mut records: Vec<MetadataRecord> = Vec::with_capacity(capacity);
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: STATE_TOPIC.to_owned(),
        topic_id: Uuid::new_v4(),
        partitions: num_partitions,
        replication_factor: rf,
    }));

    // Compaction keeps the newest record of every key, which is what makes a
    // group definition and a retained cut survive without a bound on the log.
    let mut overrides = BTreeMap::new();
    overrides.insert(
        crate::config_keys::CLEANUP_POLICY.to_owned(),
        "compact".to_owned(),
    );
    records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: STATE_TOPIC.to_owned(),
        overrides,
    }));

    for p in 0..num_partitions {
        let base = usize::try_from(p).expect("a partition index is not negative");
        let mut replicas = Vec::with_capacity(rf_usize);
        for i in 0..rf_usize {
            replicas.push(brokers[(base + i) % count]);
        }
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: STATE_TOPIC.to_owned(),
            partition: p,
            leader: replicas[0],
            replicas: replicas.clone(),
            isr: replicas,
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }
    records
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataImage;

    use super::*;

    fn image_of(records: &[MetadataRecord]) -> MetadataImage {
        MetadataImage::from_records(Uuid::nil(), records)
    }

    #[test]
    fn the_topic_is_compacted() {
        let image = image_of(&topic_records(&[NodeId(1)], 3, 1));
        let mut expected = BTreeMap::new();
        expected.insert("cleanup.policy".to_owned(), "compact".to_owned());
        assert!(image.topic_config(STATE_TOPIC) == Some(&expected));
    }

    #[test]
    fn every_partition_gets_a_leader_and_an_isr() {
        let brokers = [NodeId(1), NodeId(2), NodeId(3)];
        let image = image_of(&topic_records(&brokers, 4, 3));
        let topic = image.topic(STATE_TOPIC).expect("the topic record is there");
        assert!(topic.partitions == 4);
        assert!(topic.replication_factor == 3);
        for p in image.partitions_of(STATE_TOPIC) {
            assert!(p.replicas.len() == 3);
            assert!(p.isr == p.replicas);
            assert!(p.leader == p.replicas[0]);
        }
    }

    #[test]
    fn the_broker_count_caps_the_replication_factor() {
        let image = image_of(&topic_records(&[NodeId(1)], 2, 3));
        let topic = image.topic(STATE_TOPIC).expect("the topic record is there");
        assert!(topic.replication_factor == 1);
        for p in image.partitions_of(STATE_TOPIC) {
            assert!(p.replicas == vec![NodeId(1)]);
        }
    }

    #[test]
    fn the_leaders_walk_the_broker_list() {
        let brokers = [NodeId(1), NodeId(2), NodeId(3)];
        let image = image_of(&topic_records(&brokers, 4, 1));
        let leaders: Vec<NodeId> = image.partitions_of(STATE_TOPIC).map(|p| p.leader).collect();
        assert!(leaders == vec![NodeId(1), NodeId(2), NodeId(3), NodeId(1)]);
    }
}
