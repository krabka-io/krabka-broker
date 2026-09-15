//! Lazy creation of the `__share_group_state` internal topic (KIP-932).
//! Mirrors the `__transaction_state` bootstrap.

use std::{collections::BTreeMap, sync::Arc};

use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord};
use krabka_raft::RaftError;
use krabka_units::convert::ByteSizeExt as _;
use uuid::Uuid;

pub const TOPIC: &str = "__share_group_state";

/// The topic configs Kafka writes when it creates `__share_group_state`
/// (`ShareCoordinatorService.shareGroupStateTopicConfigs`, as KIP-932
/// defines them).
pub(crate) fn topic_configs(
    config: &crate::share_coordinator::config::ShareCoordinatorConfig,
) -> BTreeMap<String, String> {
    use crate::config_keys::{
        CLEANUP_POLICY, COMPRESSION_TYPE, MIN_INSYNC_REPLICAS, RETENTION_MS, SEGMENT_BYTES,
    };
    BTreeMap::from([
        (CLEANUP_POLICY.to_owned(), "delete".to_owned()),
        (COMPRESSION_TYPE.to_owned(), "producer".to_owned()),
        (
            SEGMENT_BYTES.to_owned(),
            config.state_topic_segment_bytes.bytes_u64().to_string(),
        ),
        (
            MIN_INSYNC_REPLICAS.to_owned(),
            config.state_topic_min_isr.to_string(),
        ),
        (RETENTION_MS.to_owned(), "-1".to_owned()),
    ])
}

/// Make sure `__share_group_state` exists in the controller's metadata.
/// This is a no-op if the topic already exists. It tolerates `TopicExists`,
/// because a concurrent `FindCoordinator(SHARE)` can create the topic first.
pub(crate) async fn ensure_topic(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    num_partitions: i32,
    replication_factor: i16,
    topic_configs: &BTreeMap<String, String>,
) -> Result<(), crate::error::BrokerError> {
    let image = controller.current_image();
    if image.topic(TOPIC).is_some() {
        return Ok(());
    }

    // Collect registered brokers for round-robin replica assignment.
    let mut sorted: Vec<NodeId> = image.brokers().map(|b| b.node_id).collect();
    if sorted.is_empty() {
        return Err(crate::error::BrokerError::Share(
            "no brokers registered; cannot bootstrap __share_group_state".into(),
        ));
    }
    sorted.sort_unstable();

    let k = sorted.len();
    let rf_usize = crate::bootstrap::internal_topic_replication_factor(replication_factor, k);
    let rf = i16::try_from(rf_usize).expect("bounded by configured i16 replication factor");

    let mut records: Vec<MetadataRecord> = Vec::new();
    let topic_id = Uuid::new_v4();
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_string(),
        topic_id,
        partitions: num_partitions,
        replication_factor: rf,
    }));

    for p in 0..num_partitions {
        let mut replicas = Vec::with_capacity(rf_usize);
        // p >= 0 (i32 literal range), k >= 1; safe to cast.
        let base = usize::try_from(p).expect("partition index fits in usize");
        for i in 0..rf_usize {
            replicas.push(sorted[(base + i) % k]);
        }
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: TOPIC.to_string(),
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

    records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: TOPIC.to_string(),
        overrides: topic_configs.clone(),
    }));

    match controller.submit_change(records).await {
        Ok(_) | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(crate::error::BrokerError::Share(format!(
            "submit_change failed: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;
    use crate::{broker::Broker, config::BrokerConfig};

    #[tokio::test]
    async fn nondefault_partition_count_controls_created_topic() {
        let dir = tempdir().unwrap();
        let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .expect("start broker");
        let broker = handle.broker_arc_for_test();

        ensure_topic(
            &broker.controller,
            7,
            3,
            &topic_configs(&broker.config.share_coordinator),
        )
        .await
        .expect("create share-state topic");

        let image = handle.controller_image_for_test();
        let topic = image.topic(TOPIC).expect("share-state topic");
        assert!(topic.partitions == 7);
        assert!(topic.replication_factor == 1);
        assert!(image.partitions_of(TOPIC).count() == 7);
        handle.shutdown().await;
    }
}
