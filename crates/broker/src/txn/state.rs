use {krabka_ids::PartitionIndex, krabka_log::ProducerId, std::collections::HashSet};

/// Transaction state machine. It mirrors Apache Kafka's classic transaction
/// states (KIP-98), extended for KIP-1319 v2.
///
/// `as_str` is the state's name as Kafka's `TransactionState` spells it on the
/// wire, the string `DescribeTransactions` and `ListTransactions` report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::EnumStr)]
#[cfg_attr(test, derive(krabka_bdd::StepParam))]
pub enum TxnState {
    #[cfg_attr(test, step_param("Empty"))]
    Empty,
    #[cfg_attr(test, step_param("Ongoing"))]
    Ongoing,
    #[cfg_attr(test, step_param("PrepareCommit"))]
    PrepareCommit,
    #[cfg_attr(test, step_param("PrepareAbort"))]
    PrepareAbort,
    #[cfg_attr(test, step_param("CompleteCommit"))]
    CompleteCommit,
    #[cfg_attr(test, step_param("CompleteAbort"))]
    CompleteAbort,
    #[cfg_attr(test, step_param("Dead"))]
    Dead,
}

impl TxnState {
    /// Can the machine move from `self` to `other`?
    pub fn can_transition_to(self, other: Self) -> bool {
        use TxnState::{
            CompleteAbort, CompleteCommit, Dead, Empty, Ongoing, PrepareAbort, PrepareCommit,
        };
        matches!(
            (self, other),
            // re-init: empty → empty, or after a completed txn
            (Empty | CompleteCommit | CompleteAbort, Empty)
            // add partitions: first or subsequent, or starting a new txn after
            // a completed one (CompleteCommit/CompleteAbort → Ongoing is the
            // implicit "re-use without init" path that Apache Kafka supports)
            | (Empty | Ongoing | CompleteCommit | CompleteAbort, Ongoing)
            // begin end-of-txn
            | (Ongoing, PrepareCommit | PrepareAbort)
            // finalise
            | (PrepareCommit, CompleteCommit)
            | (PrepareAbort, CompleteAbort)
            // expire / delete
            | (CompleteCommit | CompleteAbort, Dead)
        )
    }

    /// Kafka `TransactionState` byte id, that is
    /// `TransactionLogValue.TransactionStatus` as an int8.
    ///
    /// It matches `org.apache.kafka.coordinator.transaction.TransactionState`
    /// exactly: Empty=0, Ongoing=1, PrepareCommit=2, PrepareAbort=3,
    /// CompleteCommit=4, CompleteAbort=5, and Dead=6. Kafka also has
    /// `PrepareEpochFence`=7, a transient fencing state that Krabka does not
    /// model.
    #[must_use]
    pub fn to_kafka_status(self) -> i8 {
        match self {
            Self::Empty => 0,
            Self::Ongoing => 1,
            Self::PrepareCommit => 2,
            Self::PrepareAbort => 3,
            Self::CompleteCommit => 4,
            Self::CompleteAbort => 5,
            Self::Dead => 6,
        }
    }

    /// The inverse of [`Self::to_kafka_status`]. It returns `None` for an id
    /// that Krabka does not model, such as 7 for `PrepareEpochFence`, and for
    /// an id outside the range.
    #[must_use]
    pub fn from_kafka_status(id: i8) -> Option<Self> {
        Some(match id {
            0 => Self::Empty,
            1 => Self::Ongoing,
            2 => Self::PrepareCommit,
            3 => Self::PrepareAbort,
            4 => Self::CompleteCommit,
            5 => Self::CompleteAbort,
            6 => Self::Dead,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: PartitionIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnEntry {
    pub transactional_id: String,
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    pub state: TxnState,
    pub txn_timeout_ms: i32,
    pub partitions: HashSet<TopicPartition>,
    /// KIP-890/KIP-939 producer identity bookkeeping. `next_producer_id` and
    /// `next_producer_epoch` hold the fenced identity returned by
    /// `InitProducerId(keepPreparedTxn=true)` until the older ongoing
    /// transaction completes.
    pub prev_producer_id: ProducerId,
    pub next_producer_id: ProducerId,
    pub next_producer_epoch: i16,
    /// KIP-360 fencing state. `last_producer_epoch` is the epoch the entry held
    /// before its last bump or before an epoch fence was prepared for an abort,
    /// which is how a retry that names it is recognised. Kafka keeps it in
    /// memory only, and a reload sees `-1`. Kafka trunk persists it as
    /// `TransactionLogValue`'s `LastProducerEpoch` (tag 4, KAFKA-20357), and so
    /// does this broker under `unstable.api.versions.enable`, so it survives a
    /// coordinator restart or a partition move there (#892).
    /// `has_failed_epoch_fence` says that abort
    /// then failed, so the producer that still holds `last_producer_epoch`
    /// may retry its `InitProducerId`; it stays in-memory only, since a
    /// reload always sees the fence as not yet attempted.
    pub last_producer_epoch: i16,
    pub has_failed_epoch_fence: bool,
    pub last_update_ms: i64,
    pub start_ms: i64,
    /// `TransactionLogValue.ClientTransactionVersion`: the transaction
    /// version of the request that made the latest transition, not the
    /// cluster's live transaction.version level. Kafka derives it from the
    /// request's API version: `2` for `EndTxn` v5 and `AddPartitionsToTxn` v4
    /// and later (completion bumps the epoch under KIP-890's version-2 rule),
    /// `0` for every earlier version and for `AddOffsetsToTxn`. A server-side
    /// abort (the timeout reaper, the `InitProducerId` fence) stamps `2` on a
    /// `transaction.version` 2 cluster and `0` below it, since
    /// `endTransactionWithTV1` stamps `TV_0`. The transition that completes a
    /// transaction keeps the stamp of the one that prepared it, and the
    /// `WriteTxnMarkers` `transaction_version` reads it.
    pub client_transaction_version: i16,
}

impl TxnEntry {
    pub fn has_staged_producer_identity(&self) -> bool { self.next_producer_epoch >= 0 }

    /// Fresh entry for a tid that's never been seen, or the entry after an
    /// epoch bump.
    ///
    /// The transaction start time is `-1` until a partition joins the
    /// transaction, as Kafka's `TransactionMetadata` keeps
    /// `txnStartTimestamp` (`prepareIncrementProducerEpoch` and
    /// `prepareProducerIdRotation` reset it to `-1`).
    pub fn new_empty(
        transactional_id: String,
        producer_id: ProducerId,
        producer_epoch: i16,
        txn_timeout_ms: i32,
        now_ms: i64,
    ) -> Self {
        Self {
            transactional_id,
            producer_id,
            producer_epoch,
            state: TxnState::Empty,
            txn_timeout_ms,
            partitions: HashSet::new(),
            prev_producer_id: ProducerId(-1),
            next_producer_id: ProducerId(-1),
            next_producer_epoch: -1,
            last_producer_epoch: -1,
            has_failed_epoch_fence: false,
            last_update_ms: now_ms,
            start_ms: -1,
            client_transaction_version: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// One transaction state and what a scenario did with it.
    struct TransactionStateScenario {
        state: krabka_bdd::Given<TxnState>,
        status: krabka_bdd::Given<i8>,
        recovered: krabka_bdd::Given<Option<TxnState>>,
        move_allowed: krabka_bdd::Given<bool>,
    }

    #[rstest::fixture]
    fn transaction_state() -> TransactionStateScenario {
        TransactionStateScenario {
            state: krabka_bdd::Given::new("state", "Given a transaction is in state <state>"),
            status: krabka_bdd::Given::new(
                "status",
                "Given a transaction log record contains Kafka status byte <status>",
            ),
            recovered: krabka_bdd::Given::new(
                "recovered",
                "When the coordinator round-trips the transaction through the transaction log",
            ),
            move_allowed: krabka_bdd::Given::new(
                "move_allowed",
                "When the coordinator checks a move to state <to>",
            ),
        }
    }

    #[krabka_bdd::given("a transaction is in state {state}")]
    fn in_state(transaction_state: &mut TransactionStateScenario, state: TxnState) {
        transaction_state.state.set(state);
    }

    #[krabka_bdd::given("a transaction log record contains Kafka status byte {status}")]
    fn record_status(transaction_state: &mut TransactionStateScenario, status: i8) {
        transaction_state.status.set(status);
    }

    #[krabka_bdd::when("the coordinator round-trips the transaction through the transaction log")]
    fn persist_and_recover(transaction_state: &mut TransactionStateScenario) {
        let status = transaction_state.state.get().to_kafka_status();
        transaction_state.status.set(status);
        transaction_state
            .recovered
            .set(TxnState::from_kafka_status(status));
    }

    #[krabka_bdd::when("the Kafka transaction status byte is recovered")]
    fn recover(transaction_state: &mut TransactionStateScenario) {
        let recovered = TxnState::from_kafka_status(*transaction_state.status.get());
        transaction_state.recovered.set(recovered);
    }

    #[krabka_bdd::when("the coordinator checks a move to state {to}")]
    fn check_move(transaction_state: &mut TransactionStateScenario, to: TxnState) {
        let allowed = transaction_state.state.get().can_transition_to(to);
        transaction_state.move_allowed.set(allowed);
    }

    #[krabka_bdd::then("the transaction log holds status byte {status}")]
    fn holds_status(transaction_state: &TransactionStateScenario, status: i8) {
        assert!(*transaction_state.status.get() == status);
    }

    #[krabka_bdd::then("the coordinator reads back state {state}")]
    fn reads_back(transaction_state: &TransactionStateScenario, state: TxnState) {
        assert!(*transaction_state.recovered.get() == Some(state));
    }

    #[krabka_bdd::then("the status byte has no modeled transaction state")]
    fn no_modeled_state(transaction_state: &TransactionStateScenario) {
        assert!(*transaction_state.recovered.get() == None);
    }

    #[krabka_bdd::then("the move is allowed")]
    fn move_allowed(transaction_state: &TransactionStateScenario) {
        assert!(*transaction_state.move_allowed.get());
    }

    #[krabka_bdd::then("the move is refused")]
    fn move_refused(transaction_state: &TransactionStateScenario) {
        assert!(!*transaction_state.move_allowed.get());
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/txn/state.feature",
        fixtures = [transaction_state: TransactionStateScenario],
    );
}
