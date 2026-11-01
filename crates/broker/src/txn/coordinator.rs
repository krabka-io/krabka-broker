//! Per-broker `TxnCoordinator`.
//!
//! The coordinator owns the in-memory state map of every `transactional_id`
//! whose `__transaction_state` partition this broker leads. It persists every
//! state change as a record in the matching `__transaction_state` partition.
//! It loads a partition when this broker becomes its leader, and it unloads
//! the partition when this broker stops leading it (see `leadership`).

#[cfg(test)]
use std::sync::atomic::Ordering;
use {
    self::verification_client::VerificationClient,
    crate::{
        codes::{COORDINATOR_NOT_AVAILABLE, INVALID_PRODUCER_ID_MAPPING, UNKNOWN_SERVER_ERROR},
        coordinator::GroupCoordinator,
        error::BrokerError,
        metadata_source::MetadataSource,
        network::client::InterBrokerClient,
        partition_registry::PartitionRegistry,
        producer_id_manager::ProducerIdManager,
        task_util::cloned_registry_values,
        txn::{partitioner::partition_for_tid, state::TxnEntry},
    },
    dashmap::DashMap,
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
    krabka_security::ListenerProtocol,
    krabka_units::ByteSize,
    std::{
        collections::BTreeSet,
        iter,
        sync::{Arc, Mutex as StdMutex, atomic::AtomicU64},
    },
    tokio::sync::{Mutex, Notify, RwLock},
};

/// The commit rule of a `__transaction_state` write.
mod commit;
/// KIP-98 transactional-id expiry: the decision core and the sweep over the
/// tids this broker coordinates. [`crate::txn::id_expiration`] ticks it.
/// Completion of transactions whose `Prepare*` record is durable.
pub mod completion;
pub mod expiry;
#[cfg(any(test, feature = "test-helpers"))]
pub mod fanout_gate;
pub mod leadership;
mod load;
mod markers;
mod persistence;
mod pid_index;
pub mod produce_verification;
mod reaper;
mod registration;
mod verification_client;

// Only the #[cfg(test)] stateright models in txn/ reach these.
#[cfg(test)]
pub use self::{
    markers::{remove_marked_partitions, remove_marked_partitions_from},
    reaper::{ReaperStep, apply_prepare_abort, reaper_step},
    registration::{registration_decision, stage_registration},
};

#[cfg(test)]
mod test_support;

/// Per-broker transaction coordinator. `Broker::start` constructs it and
/// shares it with the transaction wire handlers through an `Arc`.
pub struct TxnCoordinator {
    pub(crate) node_id: krabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) producer_ids: Arc<ProducerIdManager>,
    num_partitions: i32,
    recovery_read_max: ByteSize,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Serializes durable state writes by `__transaction_state` partition.
    /// The reaper holds the matching lock across its post-marker recheck and
    /// completion append so no staged writer can slip between them.
    state_partition_writes: StatePartitionWrites,
    /// The newest known leadership of each `__transaction_state` partition.
    /// [`leadership::apply_image`] orders updates by leader epoch.
    leader_partitions: RwLock<leadership::StatePartitionLeaders>,
    /// Reverse lookup: `producer_id` → `transactional_id`. The Produce
    /// handler reads it to verify transactional batches (KIP-1319 v2).
    pid_to_tid: DashMap<ProducerId, String>,
    /// Serializes the multi-key PID ownership check and publication after a
    /// durable transaction-state append.
    pid_install: StdMutex<()>,
    /// Source of [`leadership::LeaderTerm::generation`].
    next_generation: AtomicU64,
    /// Wakes every `__transaction_state` write that waits to commit when a
    /// term starts or ends, so a write whose term ended stops waiting.
    leadership_changed: Notify,
    /// The metadata that names the leader of each `__transaction_state`
    /// partition. A write waits to commit only while it names this broker at
    /// the epoch of the write's term. `None` in tests that build no
    /// controller; such a write follows the coordinator's own terms only.
    metadata: Option<Arc<dyn MetadataSource>>,
    /// Wakes the callers of [`Self::wait_for_load`] when a load ends.
    load_finished: Notify,
    /// Transactional ids whose `Prepare*` record is durable, queued for
    /// [`crate::txn::completion`] by recovery or by a failed request.
    pending_completions: StdMutex<BTreeSet<String>>,
    /// Wakes [`crate::txn::completion`] when an id joins
    /// `pending_completions`.
    completion_requested: Notify,
    marker_transport: Option<MarkerTransport>,
    group_coordinator: Option<Arc<GroupCoordinator>>,
    /// Whether `__transaction_state` values carry `LastProducerEpoch` (tag 4).
    /// Kafka trunk persists it (KAFKA-20357); 4.3.1 keeps it in memory only, so
    /// this is on only under `unstable.api.versions.enable`.
    persist_last_producer_epoch: bool,
    /// Test gate in front of every transaction-marker fan-out.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) marker_fanout_gate: fanout_gate::MarkerFanoutGate,
}

/// One write lock for each `__transaction_state` partition.
#[derive(Debug)]
struct StatePartitionWrites {
    per_partition: Vec<Mutex<()>>,
    /// The lock of a partition index outside `0..num_partitions`, and of an
    /// id with no partition, which `partition_for_tid` yields only for a
    /// non-positive partition count.
    out_of_range: Mutex<()>,
}

impl StatePartitionWrites {
    fn new(num_partitions: i32) -> Self {
        let count = usize::try_from(num_partitions).unwrap_or(0);
        Self {
            per_partition: iter::repeat_with(|| Mutex::new(())).take(count).collect(),
            out_of_range: Mutex::new(()),
        }
    }

    /// The write lock of `partition`.
    fn lock_for(&self, partition: Option<PartitionIndex>) -> &Mutex<()> {
        partition
            .and_then(|partition| usize::try_from(partition.get()).ok())
            .and_then(|index| self.per_partition.get(index))
            .unwrap_or(&self.out_of_range)
    }
}

struct MarkerTransport {
    verification_client: VerificationClient,
    controller: Arc<dyn MetadataSource>,
    inter_broker_client: Arc<InterBrokerClient>,
    protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: krabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        producer_ids: Arc<ProducerIdManager>,
        num_partitions: i32,
        recovery_read_max: ByteSize,
    ) -> Self {
        Self {
            node_id,
            partitions,
            producer_ids,
            num_partitions,
            recovery_read_max,
            state: DashMap::new(),
            state_partition_writes: StatePartitionWrites::new(num_partitions),
            leader_partitions: RwLock::new(leadership::StatePartitionLeaders::new()),
            pid_to_tid: DashMap::new(),
            pid_install: StdMutex::new(()),
            next_generation: AtomicU64::new(0),
            leadership_changed: Notify::new(),
            metadata: None,
            load_finished: Notify::new(),
            pending_completions: StdMutex::new(BTreeSet::new()),
            completion_requested: Notify::new(),
            marker_transport: None,
            group_coordinator: None,
            persist_last_producer_epoch: false,
            #[cfg(any(test, feature = "test-helpers"))]
            marker_fanout_gate: fanout_gate::MarkerFanoutGate::default(),
        }
    }

    /// Persist and reload `LastProducerEpoch` (tag 4 of `TransactionLogValue`),
    /// as Kafka trunk does. `Broker::start` turns it on under
    /// `unstable.api.versions.enable`.
    pub(crate) fn set_persist_last_producer_epoch(&mut self, enabled: bool) {
        self.persist_last_producer_epoch = enabled;
    }

    pub(crate) fn configure_marker_transport(
        &mut self,
        controller: Arc<dyn MetadataSource>,
        inter_broker_client: Arc<InterBrokerClient>,
        protocol: ListenerProtocol,
        listener_name: String,
        server_name: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) {
        self.metadata = Some(Arc::clone(&controller));
        self.marker_transport = Some(MarkerTransport {
            verification_client: VerificationClient::new(
                self.node_id,
                Arc::clone(&inter_broker_client),
                protocol,
                listener_name.clone(),
                server_name.clone(),
            ),
            controller,
            inter_broker_client,
            protocol,
            listener_name,
            server_name,
        });
        self.group_coordinator = Some(group_coordinator);
    }

    /// Test-only: the metadata a `__transaction_state` write checks its term
    /// against, without the marker transport that sets it in a broker.
    #[cfg(test)]
    pub(crate) fn set_metadata_source_for_test(&mut self, metadata: Arc<dyn MetadataSource>) {
        self.metadata = Some(metadata);
    }

    /// Test-only: makes this broker the loaded leader of `partition` without
    /// an image.
    #[cfg(test)]
    pub(crate) async fn lead_state_partition_for_test(&self, partition: PartitionIndex) {
        self.leader_partitions.write().await.insert(
            partition,
            leadership::StatePartitionLeadership {
                topic_id: uuid::Uuid::nil(),
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                term: Some(leadership::LeaderTerm {
                    generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
                    status: leadership::LoadStatus::Loaded,
                }),
            },
        );
    }

    /// Returns the `__transaction_state` partition index responsible for `tid`,
    /// or `None` when the configured partition count is not positive.
    pub(crate) fn partition_for(&self, tid: &str) -> Option<PartitionIndex> {
        partition_for_tid(tid, self.num_partitions).map(PartitionIndex)
    }

    /// [`Self::partition_for`], or an error naming `tid` when the configured
    /// partition count is not positive.
    pub(crate) fn require_partition_for(&self, tid: &str) -> Result<PartitionIndex, BrokerError> {
        self.partition_for(tid).ok_or_else(|| {
            BrokerError::Txn(format!(
                "no __transaction_state partition for {tid}: partition count {} is not positive",
                self.num_partitions
            ))
        })
    }

    /// Returns `true` if this broker is the transaction coordinator for `tid`
    /// and its `__transaction_state` partition is loaded.
    pub(crate) async fn is_transaction_coordinator_for(&self, tid: &str) -> bool {
        self.coordinator_error(tid).await.is_none()
    }

    /// The Kafka error code for a request about `tid`, or `None` when this
    /// broker coordinates `tid` and its partition is loaded.
    ///
    /// The code is `COORDINATOR_LOAD_IN_PROGRESS` while the partition loads,
    /// and `NOT_COORDINATOR` when this broker does not lead the partition or
    /// its load failed.
    pub(crate) async fn coordinator_error(&self, tid: &str) -> Option<i16> {
        let Some(partition) = self.partition_for(tid) else {
            return Some(COORDINATOR_NOT_AVAILABLE);
        };
        leadership::coordinator_error(self.load_status(partition).await)
    }

    /// The error code for a failed append about `tid`.
    ///
    /// A write that did not commit answers the coordinator error Kafka's
    /// `appendTransactionToLog` answers for it. Any other failure answers the
    /// coordinator error when the coordinator term changed, and
    /// `UNKNOWN_SERVER_ERROR` otherwise.
    pub(crate) async fn append_error_code(&self, tid: &str, error: &BrokerError) -> i16 {
        if let BrokerError::TransactionStateWriteUncommitted { code, .. } = error {
            return *code;
        }
        self.coordinator_error(tid)
            .await
            .unwrap_or(UNKNOWN_SERVER_ERROR)
    }

    /// The error code for a request about `tid` whose entry lookup missed.
    ///
    /// A leadership change can unload the coordinator partition, and so evict
    /// its entries, between a caller's `coordinator_error` check and its
    /// lookup. This reads the coordinator status again after the miss: when
    /// it names an error, the miss is the unload, and the caller answers the
    /// retriable `COORDINATOR_LOAD_IN_PROGRESS` or `NOT_COORDINATOR`.
    /// Otherwise the id is unknown, which is `INVALID_PRODUCER_ID_MAPPING`.
    pub(crate) async fn missing_entry_error(&self, tid: &str) -> i16 {
        self.coordinator_error(tid)
            .await
            .unwrap_or(INVALID_PRODUCER_ID_MAPPING)
    }

    /// The load status of `partition`, or `None` when this broker does not
    /// lead it.
    pub(crate) async fn load_status(
        &self,
        partition: PartitionIndex,
    ) -> Option<leadership::LoadStatus> {
        leadership::status(&*self.leader_partitions.read().await, partition)
    }

    /// Returns the locked `TxnEntry` for `tid`, or `None` if `tid` is
    /// unknown.
    pub(crate) fn get(&self, tid: &str) -> Option<Arc<Mutex<TxnEntry>>> {
        self.state.get(tid).map(|e| Arc::clone(e.value()))
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
        let handles = cloned_registry_values(&self.state);
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
    use {
        super::*,
        crate::{
            codes,
            test_support::PartitionCount,
            txn::coordinator::test_support::{test_coordinator, test_coordinator_with_partitions},
        },
        assert2::check,
    };

    #[test]
    fn partition_for_maps_tid_via_java_hash_over_num_partitions() {
        // Canonical JVM String.hashCode vectors (see `partitioner` tests) with N=50.
        // Pins the real mapping so a
        // constant `PartitionIndex(0)` (the Default) is caught: none of these
        // hash to 0.
        let coordinator = test_coordinator();
        check!(coordinator.partition_for("my-tid") == Some(PartitionIndex(20)));
        check!(coordinator.partition_for("producer-1") == Some(PartitionIndex(30)));
        check!(coordinator.partition_for("tx-orders-prod") == Some(PartitionIndex(16)));
    }

    /// Regression: a leadership change can unload the coordinator partition
    /// between a caller's `coordinator_error` check and its entry lookup. A
    /// miss answers the fresh coordinator error, and only a coordinator that
    /// still owns the loaded partition answers `INVALID_PRODUCER_ID_MAPPING`.
    #[tokio::test]
    async fn a_missing_entry_answers_the_fresh_coordinator_error() {
        let coordinator = test_coordinator();
        let tid = "tid-missing";
        let unloaded = coordinator.missing_entry_error(tid).await;
        coordinator
            .lead_state_partition_for_test(coordinator.partition_for(tid).unwrap())
            .await;
        let loaded = coordinator.missing_entry_error(tid).await;
        check!((unloaded, loaded) == (codes::NOT_COORDINATOR, INVALID_PRODUCER_ID_MAPPING));
    }

    #[test]
    fn nondefault_partition_count_changes_coordinator_routing() {
        let coordinator = test_coordinator_with_partitions(PartitionCount(7));
        check!(
            coordinator.partition_for("my-tid")
                == partition_for_tid("my-tid", 7).map(PartitionIndex)
        );
    }
}
