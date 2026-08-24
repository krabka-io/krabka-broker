//! Test doubles that the barrier unit tests share.
//!
//! The module holds a [`MetadataSource`] that hands out one image, and the
//! helpers that open a real partition with a live writer. Both the fan-out
//! tests and the coordinator tests need them.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::Path,
    sync::{Arc, RwLock},
};

use crabka_ids::PartitionIndex;
use crabka_log::{Log, LogConfig};
use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
use crabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    SubmitChangeResult, UpdateVoter,
};
use tokio::sync::watch;
use uuid::Uuid;

use crate::{metadata_source::MetadataSource, partition_registry::PartitionRegistry};

/// A [`MetadataSource`] that serves one image and accepts a replacement.
pub(crate) struct StaticSource {
    image: RwLock<Arc<MetadataImage>>,
}

impl StaticSource {
    /// A source over the image that `records` build.
    pub(crate) fn new(records: &[MetadataRecord]) -> Self {
        Self {
            image: RwLock::new(Arc::new(MetadataImage::from_records(Uuid::nil(), records))),
        }
    }

    /// Replace the image, as a metadata change does.
    pub(crate) fn set_records(&self, records: &[MetadataRecord]) {
        *self.image.write().expect("the image lock is not poisoned") =
            Arc::new(MetadataImage::from_records(Uuid::nil(), records));
    }
}

#[async_trait::async_trait]
impl MetadataSource for StaticSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        Arc::clone(&self.image.read().expect("the image lock is not poisoned"))
    }
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        unimplemented!("no barrier test watches the image")
    }
    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        unimplemented!("no barrier test watches the leader")
    }
    fn quorum_state(&self) -> QuorumState {
        unimplemented!("no barrier test reads the quorum state")
    }
    async fn submit_change(
        &self,
        _records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        unimplemented!("the coordinator submits no metadata change")
    }
    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        unimplemented!("no barrier test changes the membership")
    }
    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        unimplemented!("no barrier test adds a learner")
    }
    fn controller_bound_addr(&self) -> SocketAddr {
        unimplemented!("no barrier test reads the controller address")
    }
    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        unimplemented!("no barrier test reads a snapshot")
    }
    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        unimplemented!("no barrier test triggers a snapshot")
    }
    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!("no barrier test adds a voter")
    }
    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!("no barrier test removes a voter")
    }
    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        unimplemented!("no barrier test updates a voter")
    }
    async fn cancel(&self) {}
}

/// The topic and partition records of one topic, with one leader for every
/// partition and a leader epoch of 3.
pub(crate) fn topic_records(topic: &str, partitions: i32, leader: NodeId) -> Vec<MetadataRecord> {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: topic.to_owned(),
        topic_id: Uuid::new_v4(),
        partitions,
        replication_factor: 1,
    })];
    for p in 0..partitions {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: p,
            leader,
            replicas: vec![leader],
            isr: vec![leader],
            leader_epoch: crabka_metadata::LeaderEpoch(3),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }
    records
}

/// Open a real partition with a live writer, and register it.
pub(crate) fn open_partition(registry: &PartitionRegistry, dir: &Path, topic: &str, index: i32) {
    let partition_dir = crate::log_dir::partition_dir(dir, topic, index);
    std::fs::create_dir_all(&partition_dir).expect("create the partition directory");
    let log = Log::open(&partition_dir, LogConfig::default()).expect("open the log");
    let partition = crate::broker::spawn_partition(
        topic.to_owned(),
        PartitionIndex(index),
        dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    registry.insert(topic.to_owned(), PartitionIndex(index), partition);
}
