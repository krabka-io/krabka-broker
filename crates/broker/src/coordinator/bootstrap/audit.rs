//! Creation of the `__krabka_audit` internal topic.
//!
//! The audit topic is bootstrapped on its own schedule, separate from
//! `__consumer_offsets`, because it is optional and it places one partition on
//! every registered broker instead of spreading a fixed partition count.

use std::sync::Arc;

use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use krabka_raft::RaftError;

use crate::{config::BrokerConfig, error::BrokerError};

/// Create `__krabka_audit` with one partition per registered broker at RF=1.
///
/// Broker-affinity: the i-th broker in ascending node-id order leads partition
/// `i`. Each broker therefore leads exactly one audit partition and writes to
/// it locally.
///
/// The function is idempotent. It treats a `TopicExists` error from the
/// controller as a success, because another broker or a restart already
/// created the topic.
///
/// Only the quorum leader submits the metadata records. This matches the
/// `__consumer_offsets` bootstrap path and prevents TOCTOU duplicate-id races.
/// Followers submit nothing. The leader's records replicate into their image
/// through the normal raft log.
///
/// The function returns `Ok(())` at once when `config.audit_enabled` is
/// `false`.
pub async fn bootstrap_audit_topic(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) -> Result<(), BrokerError> {
    if !config.audit_enabled {
        return Ok(());
    }

    // Only the quorum leader submits; copy out the leader id before any `.await`
    // so we don't hold the `Ref` across an await point.
    let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
    if !am_leader {
        return Ok(());
    }

    // Already created (idempotent restart or another leader beat us).
    if controller
        .current_image()
        .topic(&config.audit_topic)
        .is_some()
    {
        return Ok(());
    }

    let image = controller.current_image();
    let mut brokers: Vec<krabka_raft::NodeId> = image.brokers().map(|b| b.node_id).collect();
    drop(image);
    if brokers.is_empty() {
        brokers.push(config.node_id);
    }
    brokers.sort_unstable();

    let num_partitions = i32::try_from(brokers.len()).unwrap_or(1);
    // RF=1: partition i → brokers[i % len] as sole replica/leader.
    // Use the crate-internal round_robin helper; falls back to explicit
    // per-broker assignment when brokers.len() == num_partitions (the common
    // single-broker test case also satisfies this).
    let assignments =
        crate::handlers::create_topics::round_robin_replicas(&brokers, num_partitions, 1);

    let mut records = Vec::with_capacity(1 + usize::try_from(num_partitions).unwrap_or(0));
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: config.audit_topic.clone(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: num_partitions,
        replication_factor: 1,
    }));
    for (p, replicas) in assignments.iter().enumerate() {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: config.audit_topic.clone(),
            partition: i32::try_from(p).expect("audit partition index overflows i32"),
            leader: replicas[0],
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }

    match controller.submit_change(records).await {
        // Idempotent: another broker / a restart already created it.
        Ok(_) | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(BrokerError::Startup(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    use krabka_raft::NodeId;

    use super::*;
    use crate::{
        config::BrokerConfig, metadata_source::MetadataSource, test_support::FakeMetadataSource,
    };

    fn broker_registration(node_id: u64) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(node_id),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
            host: "127.0.0.1".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
            log_dirs: vec![],
            features: std::collections::BTreeMap::new(),
        })
    }

    #[tokio::test]
    async fn audit_topic_skipped_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.audit_enabled = false;
        config.node_id = NodeId(1);
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .leader(Some(NodeId(1)))
                .commit_submits()
                .build(),
        );
        let controller: Arc<dyn MetadataSource> = fake.clone();

        let res = bootstrap_audit_topic(&config, &controller).await;
        check!(res.is_ok());
        check!(fake.submitted().is_empty());
        check!(
            controller
                .current_image()
                .topic(&config.audit_topic)
                .is_none()
        );
    }

    #[tokio::test]
    async fn audit_topic_skipped_when_not_leader() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.audit_enabled = true;
        config.node_id = NodeId(2);
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .leader(Some(NodeId(1)))
                .commit_submits()
                .build(),
        );
        let controller: Arc<dyn MetadataSource> = fake.clone();

        let res = bootstrap_audit_topic(&config, &controller).await;
        check!(res.is_ok());
        check!(fake.submitted().is_empty());
        check!(
            controller
                .current_image()
                .topic(&config.audit_topic)
                .is_none()
        );
    }

    #[tokio::test]
    async fn audit_topic_created_when_leader_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.audit_enabled = true;
        config.node_id = NodeId(1);
        let fake = Arc::new(
            FakeMetadataSource::builder()
                .leader(Some(NodeId(1)))
                .commit_submits()
                .build(),
        );
        let controller: Arc<dyn MetadataSource> = fake.clone();

        let res = bootstrap_audit_topic(&config, &controller).await;
        check!(res.is_ok());
        check!(fake.submitted().len() == 1);

        let image = controller.current_image();
        let topic = image.topic(&config.audit_topic);
        assert!(topic.is_some());
        let t = topic.unwrap();
        check!(t.partitions == 1);
        check!(t.replication_factor == 1);
        check!(image.partition(&config.audit_topic, 0).is_some());

        // Second call is idempotent: topic already exists in image, so nothing is submitted
        let res2 = bootstrap_audit_topic(&config, &controller).await;
        check!(res2.is_ok());
        check!(fake.submitted().len() == 1);
    }

    #[tokio::test]
    async fn audit_topic_scales_partitions_to_all_registered_brokers() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.audit_enabled = true;
        config.node_id = NodeId(1);

        let fake = Arc::new(
            FakeMetadataSource::builder()
                .records(&[
                    broker_registration(1),
                    broker_registration(2),
                    broker_registration(3),
                ])
                .leader(Some(NodeId(1)))
                .commit_submits()
                .build(),
        );
        let controller: Arc<dyn MetadataSource> = fake.clone();

        let res = bootstrap_audit_topic(&config, &controller).await;
        check!(res.is_ok());
        check!(fake.submitted().len() == 1);

        let image = controller.current_image();
        let topic = image.topic(&config.audit_topic);
        assert!(topic.is_some());
        let t = topic.unwrap();
        check!(t.partitions == 3);
        check!(t.replication_factor == 1);
        for p in 0..3i32 {
            let part = image.partition(&config.audit_topic, p);
            assert!(part.is_some());
            let node = u64::try_from(p).unwrap_or(0) + 1;
            check!(part.unwrap().leader == NodeId(node));
        }
    }

    #[tokio::test]
    async fn audit_topic_handles_topic_exists_error_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let audit_topic = config.audit_topic.clone();
        config.audit_enabled = true;
        config.node_id = NodeId(1);

        let fake = Arc::new(
            FakeMetadataSource::builder()
                .leader(Some(NodeId(1)))
                .on_submit(move |_| {
                    Err(krabka_raft::RaftError::Metadata(
                        krabka_metadata::MetadataError::TopicExists(audit_topic.clone()),
                    ))
                })
                .build(),
        );
        let controller: Arc<dyn MetadataSource> = fake.clone();

        let res = bootstrap_audit_topic(&config, &controller).await;
        check!(res.is_ok());
    }

    #[tokio::test]
    async fn audit_topic_fails_on_raft_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.audit_enabled = true;
        config.node_id = NodeId(1);

        let fake = Arc::new(
            FakeMetadataSource::builder()
                .leader(Some(NodeId(1)))
                .on_submit(|_| {
                    Err(krabka_raft::RaftError::NotLeader {
                        current_leader: None,
                    })
                })
                .build(),
        );
        let controller: Arc<dyn MetadataSource> = fake.clone();

        let res = bootstrap_audit_topic(&config, &controller).await;
        check!(res.is_err());
    }
}
