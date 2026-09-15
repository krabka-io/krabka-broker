//! Per-broker `TxnCoordinator`.
//!
//! The coordinator owns the in-memory state map of every `transactional_id`
//! whose `__transaction_state` partition this broker leads. It persists every
//! state change as a record in the matching `__transaction_state` partition.
//! On `Broker::start` it recovers the state by replaying those partitions.

use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_metadata::MetadataImage;
use krabka_security::ListenerProtocol;
use krabka_units::ByteSize;
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    partition_registry::PartitionRegistry,
    txn::{partitioner::partition_for_tid, state::TxnEntry},
};

/// KIP-98 transactional-id expiry: the decision core and the sweep over the
/// tids this broker coordinates. [`crate::txn::id_expiration`] ticks it.
/// Completion of transactions whose `Prepare*` record is durable.
pub(crate) mod completion;
pub(crate) mod expiry;
#[cfg(any(test, feature = "test-helpers"))]
pub(crate) mod fanout_gate;
mod leadership;
mod markers;
mod persistence;
mod pid_index;
pub(crate) mod produce_verification;
mod reaper;
mod registration;

#[cfg(test)]
mod test_support;

/// Per-broker transaction coordinator. `Broker::start` constructs it and
/// shares it with the transaction wire handlers through an `Arc`.
pub(crate) struct TxnCoordinator {
    pub(crate) node_id: krabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    num_partitions: i32,
    recovery_read_max: ByteSize,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Serializes durable state writes by `__transaction_state` partition.
    /// The reaper holds the matching lock across its post-marker recheck and
    /// completion append so no staged writer can slip between them.
    state_partition_writes: Vec<Mutex<()>>,
    /// The newest known leadership of each `__transaction_state` partition.
    /// [`leadership::apply_image`] orders updates by leader epoch.
    leader_partitions: RwLock<leadership::StatePartitionLeaders>,
    /// Reverse lookup: `producer_id` → `transactional_id`. The Produce
    /// handler reads it to verify transactional batches (KIP-1319 v2).
    pid_to_tid: DashMap<ProducerId, String>,
    /// Serializes the multi-key PID ownership check and publication after a
    /// durable transaction-state append.
    pid_install: StdMutex<()>,
    /// Latches a recovery failure so later metadata refreshes cannot restore
    /// coordinator ownership over a partial or rejected replay image.
    recovery_valid: AtomicBool,
    /// Transactional ids whose `Prepare*` record is durable, queued for
    /// [`crate::txn::completion`] by recovery or by a failed request.
    pending_completions: StdMutex<BTreeSet<String>>,
    /// Wakes [`crate::txn::completion`] when an id joins
    /// `pending_completions`.
    completion_requested: Notify,
    marker_transport: Option<MarkerTransport>,
    group_coordinator: Option<Arc<crate::coordinator::GroupCoordinator>>,
    /// Test gate in front of every transaction-marker fan-out.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) marker_fanout_gate: fanout_gate::MarkerFanoutGate,
}

struct MarkerTransport {
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
    protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: krabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
        num_partitions: i32,
        recovery_read_max: ByteSize,
    ) -> Self {
        let state_partition_count = usize::try_from(num_partitions)
            .expect("transaction state partition count must be nonnegative");
        Self {
            node_id,
            partitions,
            producer_ids,
            num_partitions,
            recovery_read_max,
            state: DashMap::new(),
            state_partition_writes: (0..state_partition_count).map(|_| Mutex::new(())).collect(),
            leader_partitions: RwLock::new(leadership::StatePartitionLeaders::new()),
            pid_to_tid: DashMap::new(),
            pid_install: StdMutex::new(()),
            recovery_valid: AtomicBool::new(true),
            pending_completions: StdMutex::new(BTreeSet::new()),
            completion_requested: Notify::new(),
            marker_transport: None,
            group_coordinator: None,
            #[cfg(any(test, feature = "test-helpers"))]
            marker_fanout_gate: fanout_gate::MarkerFanoutGate::default(),
        }
    }

    pub(crate) fn configure_marker_transport(
        &mut self,
        controller: Arc<dyn crate::metadata_source::MetadataSource>,
        inter_broker_client: Arc<crate::network::client::InterBrokerClient>,
        protocol: ListenerProtocol,
        listener_name: String,
        server_name: String,
        group_coordinator: Arc<crate::coordinator::GroupCoordinator>,
    ) {
        self.marker_transport = Some(MarkerTransport {
            controller,
            inter_broker_client,
            protocol,
            listener_name,
            server_name,
        });
        self.group_coordinator = Some(group_coordinator);
    }

    /// Applies the `__transaction_state` leadership of `image`. `recover` calls
    /// it, and so does every metadata change and every transaction handler.
    /// An image that is older than one already applied changes nothing.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        if !self.recovery_valid.load(Ordering::Acquire) {
            self.leader_partitions.write().await.clear();
            return;
        }
        self.install_leader_partitions(image).await;
    }

    async fn install_leader_partitions(&self, image: &MetadataImage) {
        leadership::apply_image(
            &mut *self.leader_partitions.write().await,
            self.node_id,
            image,
        );
    }

    /// Test-only: makes this broker the leader of `partition` without an image.
    #[cfg(test)]
    pub(crate) async fn lead_state_partition_for_test(&self, partition: PartitionIndex) {
        self.leader_partitions.write().await.insert(
            partition,
            leadership::StatePartitionLeadership {
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                leads: true,
            },
        );
    }

    /// Returns the `__transaction_state` partition index responsible for `tid`.
    pub(crate) fn partition_for(&self, tid: &str) -> PartitionIndex {
        PartitionIndex(partition_for_tid(tid, self.num_partitions))
    }

    /// Returns `true` if this broker is the transaction coordinator for `tid`.
    pub(crate) async fn is_coordinator_for(&self, tid: &str) -> bool {
        leadership::leads(
            &*self.leader_partitions.read().await,
            self.partition_for(tid),
        )
    }

    /// Returns the locked `TxnEntry` for `tid`, or `None` if `tid` is
    /// unknown.
    pub(crate) fn get(&self, tid: &str) -> Option<Arc<Mutex<TxnEntry>>> {
        self.state.get(tid).map(|e| e.value().clone())
    }

    /// Returns `true` if `handle` is still the entry registered for `tid`.
    ///
    /// Every durable append publishes a new handle, so a caller that read a
    /// handle before it waited on a lock checks this before it acts.
    pub(crate) fn is_current_entry(&self, tid: &str, handle: &Arc<Mutex<TxnEntry>>) -> bool {
        self.get(tid)
            .is_some_and(|current| Arc::ptr_eq(&current, handle))
    }

    /// Returns the `transactional_id` that `producer_id` was registered
    /// under, or `None` if the pid is unknown.
    #[cfg(test)]
    pub(crate) fn tid_for_pid(&self, pid: ProducerId) -> Option<String> {
        self.pid_to_tid.get(&pid).map(|e| e.value().clone())
    }

    /// Snapshots every locally-coordinated `TxnEntry`.
    ///
    /// The KIP-664 admin handlers `ListTransactions` and
    /// `DescribeTransactions` call this to expose the in-memory txn-state map.
    /// The method locks and clones each entry in turn, so the snapshot is
    /// consistent for one tid but not across the whole batch. That is
    /// acceptable for an admin introspection API, and Apache Kafka's JVM
    /// coordinator has the same property.
    pub(crate) async fn snapshot(&self) -> Vec<TxnEntry> {
        // Collect the `Arc<Mutex<_>>` handles first so we don't hold the
        // DashMap shard locks while taking the inner async mutex.
        let handles: Vec<Arc<Mutex<TxnEntry>>> =
            self.state.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let entry = h.lock().await;
            out.push(entry.clone());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::txn::coordinator::test_support::{
        test_coordinator, test_coordinator_with_partitions,
    };

    #[test]
    fn partition_for_maps_tid_via_java_hash_over_num_partitions() {
        // Canonical JVM String.hashCode vectors (see `partitioner` tests) with N=50.
        // Pins the real mapping so a
        // constant `PartitionIndex(0)` (the Default) is caught: none of these
        // hash to 0.
        let coordinator = test_coordinator();
        check!(coordinator.partition_for("my-tid") == PartitionIndex(20));
        check!(coordinator.partition_for("producer-1") == PartitionIndex(30));
        check!(coordinator.partition_for("tx-orders-prod") == PartitionIndex(16));
    }

    /// An image with no `__transaction_state` topic, as the image before the
    /// topic was created is.
    fn image_without_state_topic() -> MetadataImage {
        MetadataImage::new(uuid::Uuid::nil())
    }

    fn image_with_leader(leader: krabka_metadata::NodeId, leader_epoch: i32) -> MetadataImage {
        use krabka_metadata::{LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord};

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: crate::txn::bootstrap::TOPIC.to_string(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: crate::txn::bootstrap::TOPIC.to_string(),
            partition: 0,
            leader,
            replicas: vec![leader],
            isr: vec![leader],
            leader_epoch: LeaderEpoch(leader_epoch),
            ..Default::default()
        }));
        image
    }

    /// Many tasks apply the image that each one read, so an older image can
    /// arrive after a newer one. Before the fix, the reconcile loop applied
    /// the image from before `__transaction_state` existed after
    /// `InitProducerId` had applied a newer one, and a transactional Produce
    /// got `NOT_COORDINATOR` (#975).
    #[tokio::test]
    async fn only_a_higher_leader_epoch_changes_the_coordinator_leadership() {
        const THIS_BROKER: krabka_metadata::NodeId = krabka_metadata::NodeId(1);
        const OTHER_BROKER: krabka_metadata::NodeId = krabka_metadata::NodeId(2);
        struct Step {
            name: &'static str,
            image: MetadataImage,
            coordinates: bool,
        }
        let steps = [
            Step {
                name: "an image before the topic exists",
                image: image_without_state_topic(),
                coordinates: false,
            },
            Step {
                name: "this broker is elected at epoch 0",
                image: image_with_leader(THIS_BROKER, 0),
                coordinates: true,
            },
            Step {
                name: "a stale image from before the topic existed",
                image: image_without_state_topic(),
                coordinates: true,
            },
            Step {
                name: "another broker is elected at epoch 1",
                image: image_with_leader(OTHER_BROKER, 1),
                coordinates: false,
            },
            Step {
                name: "a stale image of epoch 0",
                image: image_with_leader(THIS_BROKER, 0),
                coordinates: false,
            },
            Step {
                name: "a stale image from before the topic existed, after a resignation",
                image: image_without_state_topic(),
                coordinates: false,
            },
            Step {
                name: "this broker is elected again at epoch 2",
                image: image_with_leader(THIS_BROKER, 2),
                coordinates: true,
            },
        ];
        let coordinator = test_coordinator_with_partitions(1);
        for step in steps {
            coordinator.refresh_leader_partitions(&step.image).await;
            check!(
                coordinator.is_coordinator_for("any-tid").await == step.coordinates,
                "{}",
                step.name
            );
        }
    }

    #[test]
    fn nondefault_partition_count_changes_coordinator_routing() {
        let coordinator = test_coordinator_with_partitions(7);
        check!(
            coordinator.partition_for("my-tid") == PartitionIndex(partition_for_tid("my-tid", 7))
        );
    }
}
