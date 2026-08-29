//! Fixtures shared by the unit tests of several `broker` submodules: a stub
//! [`crate::metadata_source::MetadataSource`], a locally spawned partition
//! seeded with records, and the metadata records that tests submit. They live
//! in one module so no submodule owns a helper its siblings also need.

use std::sync::Arc;

use assert2::assert;
use krabka_ids::PartitionIndex;

use crate::{
    broker::{BrokerHandle, partition_spawn::spawn_partition},
    partition::Partition,
};

pub(super) struct MockMetadataSource {
    pub(super) image: Arc<krabka_metadata::MetadataImage>,
    pub(super) leader_tx: tokio::sync::watch::Sender<Option<krabka_raft::NodeId>>,
}

impl MockMetadataSource {
    pub(super) fn new(
        image: krabka_metadata::MetadataImage,
        leader: Option<krabka_raft::NodeId>,
    ) -> Self {
        let (leader_tx, _) = tokio::sync::watch::channel(leader);
        Self {
            image: Arc::new(image),
            leader_tx,
        }
    }
}

#[async_trait::async_trait]
impl crate::metadata_source::MetadataSource for MockMetadataSource {
    fn current_image(&self) -> Arc<krabka_metadata::MetadataImage> {
        self.image.clone()
    }

    fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>> {
        let (_, rx) = tokio::sync::watch::channel(self.image.clone());
        rx
    }

    fn watch_leader(&self) -> tokio::sync::watch::Receiver<Option<krabka_raft::NodeId>> {
        self.leader_tx.subscribe()
    }

    fn quorum_state(&self) -> krabka_raft::QuorumState {
        krabka_raft::QuorumState {
            current_term: 0,
            last_applied_index: 0,
            current_leader: *self.leader_tx.borrow(),
            voters: Vec::new(),
            voter_nodes: std::collections::BTreeMap::new(),
            per_voter_matched_index: std::collections::BTreeMap::new(),
        }
    }

    async fn submit_change(
        &self,
        _records: Vec<krabka_metadata::MetadataRecord>,
    ) -> Result<krabka_raft::SubmitChangeResult, krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    async fn change_membership(
        &self,
        _new_voters: std::collections::BTreeSet<krabka_raft::NodeId>,
    ) -> Result<(), krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    async fn add_learner(
        &self,
        _node_id: krabka_raft::NodeId,
        _node: krabka_raft::Node,
    ) -> Result<(), krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    fn controller_bound_addr(&self) -> std::net::SocketAddr {
        "127.0.0.1:9093".parse().unwrap()
    }

    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> krabka_raft::SnapshotRange {
        krabka_raft::SnapshotRange::NoSnapshot
    }

    async fn trigger_snapshot(&self) -> Result<(), krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    async fn add_voter(
        &self,
        _req: krabka_raft::AddVoter,
    ) -> Result<krabka_raft::ReconfigOutcome, krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    async fn remove_voter(
        &self,
        _req: krabka_raft::RemoveVoter,
    ) -> Result<krabka_raft::ReconfigOutcome, krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    async fn update_voter(
        &self,
        _req: krabka_raft::UpdateVoter,
    ) -> Result<krabka_raft::ReconfigOutcome, krabka_raft::RaftError> {
        Err(krabka_raft::RaftError::Unsupported("mock metadata source"))
    }

    async fn cancel(&self) {}
}

pub(super) fn local_partition_with_records(
    log_dir: &std::path::Path,
    topic: &str,
    partition: i32,
    values: &[&'static [u8]],
) -> Arc<Partition> {
    let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default())
        .expect("open partition log");
    let part = spawn_partition(
        topic.to_string(),
        PartitionIndex(partition),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    if !values.is_empty() {
        let mut batch = krabka_protocol::records::RecordBatch {
            last_offset_delta: i32::try_from(values.len() - 1).expect("record count fits"),
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
    part
}

pub(super) fn metadata_topic_record(
    topic: &str,
    topic_id: u128,
) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: topic.to_string(),
        topic_id: uuid::Uuid::from_u128(topic_id),
        partitions: 1,
        replication_factor: 1,
    })
}

pub(super) fn metadata_partition_record(
    topic: &str,
    partition: i32,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    leader_epoch: i32,
) -> krabka_metadata::PartitionRecord {
    krabka_metadata::PartitionRecord {
        topic: topic.to_string(),
        partition,
        leader: krabka_audit::NodeId(leader),
        replicas: replicas.iter().copied().map(krabka_audit::NodeId).collect(),
        isr: isr.iter().copied().map(krabka_audit::NodeId).collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: vec![uuid::Uuid::nil(); replicas.len()],
        partition_epoch: 0,
    }
}

pub(super) async fn submit_metadata_topic_partition(
    handle: &BrokerHandle,
    topic_spec: (&str, u128),
    partition: i32,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    leader_epoch: i32,
) {
    let (topic, topic_id) = topic_spec;
    handle
        .submit_metadata_record_for_test(metadata_topic_record(topic, topic_id))
        .await
        .expect("submit topic record");
    let partition_record =
        metadata_partition_record(topic, partition, leader, replicas, isr, leader_epoch);
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(
            partition_record.clone(),
        ))
        .await
        .expect("submit partition record");

    let image = handle.controller_image_for_test();
    assert!(image.topic(topic).is_some());
    assert!(image.partition(topic, partition) == Some(&partition_record));
}
