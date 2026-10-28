use {
    super::*,
    crate::{
        partition::Partition,
        partition_registry::PartitionRegistry,
        producer_id_manager::ProducerIdManager,
        test_support::open_partition,
        txn::{bootstrap, two_pc::should_abort_idle_txn},
    },
    assert2::{assert, check},
    std::{path::Path, sync::Arc},
};

const TID: &str = "tid-offsets";
const NOW: i64 = 1_000_000;

fn producer(id: i64, epoch: i16) -> ProducerIdentity {
    ProducerIdentity {
        producer_id: ProducerId(id),
        producer_epoch: epoch,
    }
}

fn offsets_partition(index: i32) -> TopicPartition {
    TopicPartition {
        topic: OFFSETS_TOPIC.to_owned(),
        partition: PartitionIndex(index),
    }
}

fn entry(state: TxnState, partitions: &[TopicPartition]) -> TxnEntry {
    let mut entry = TxnEntry::new_empty(TID.into(), ProducerId(7), 3, 60_000, 0);
    entry.state = state;
    // Every append stamps the client's transaction version on the record.
    entry.client_transaction_version = 2;
    entry.partitions.extend(partitions.iter().cloned());
    entry
}

#[test]
fn the_response_carries_the_error_code() {
    for code in [codes::NONE, codes::NOT_COORDINATOR] {
        check!(
            AddOffsetsToTxnResponse::error(code)
                == AddOffsetsToTxnResponse {
                    error_code: code,
                    ..Default::default()
                }
        );
    }
}

/// Kafka `TransactionMetadata.prepareAddPartitions` sets the start time only
/// when a transaction starts (#850).
#[test]
fn a_transaction_that_starts_gets_the_start_time() {
    let ongoing_since = |start_ms, partitions: &[TopicPartition]| {
        let mut entry = entry(TxnState::Ongoing, partitions);
        entry.start_ms = start_ms;
        entry.last_update_ms = NOW;
        entry
    };
    let cases = [
        (
            "an empty transaction starts",
            entry(TxnState::Empty, &[]),
            ongoing_since(NOW, &[offsets_partition(4)]),
        ),
        (
            "a completed transaction starts again with only the new partition",
            {
                let mut completed = entry(TxnState::CompleteCommit, &[offsets_partition(1)]);
                completed.start_ms = 5;
                completed
            },
            ongoing_since(NOW, &[offsets_partition(4)]),
        ),
        (
            "an ongoing transaction keeps its start time",
            ongoing_since(5, &[offsets_partition(1)]),
            ongoing_since(5, &[offsets_partition(1), offsets_partition(4)]),
        ),
    ];
    for (name, mut before, expected) in cases {
        add_partition(&mut before, offsets_partition(4), NOW);
        check!(before == expected, "{name}");
    }
}

#[test]
fn a_new_entry_has_no_start_time() {
    check!(TxnEntry::new_empty(TID.into(), ProducerId(7), 0, 60_000, NOW).start_ms == -1);
}

/// A coordinator and the state partition whose log it writes.
struct LoggedCoordinator {
    coordinator: Arc<TxnCoordinator>,
    partition: Arc<Partition>,
}

/// A coordinator over a real log that already leads state partition 0.
async fn leading_coordinator(directory: &Path) -> Arc<TxnCoordinator> {
    let LoggedCoordinator { coordinator, .. } = coordinator_with_log(directory);
    coordinator
        .lead_state_partition_for_test(PartitionIndex(0))
        .await;
    coordinator
}

fn coordinator_with_log(directory: &Path) -> LoggedCoordinator {
    let coordinator = Arc::new(TxnCoordinator::new(
        krabka_metadata::NodeId(1),
        Arc::new(PartitionRegistry::new()),
        Arc::new(ProducerIdManager::new()),
        1,
        krabka_units::mebibytes(1),
    ));
    let partition = open_partition()
        .log_dir(directory)
        .topic(bootstrap::TOPIC)
        .call();
    coordinator.partitions.insert(
        bootstrap::TOPIC.into(),
        PartitionIndex(0),
        Arc::clone(&partition),
    );
    LoggedCoordinator {
        coordinator,
        partition,
    }
}

/// Regression: a caller that already holds the entry handle from before this
/// append, such as `AddPartitionsToTxn`'s `register_partitions` queued on the
/// same mutex, must see the durable post-append state once it gets the lock,
/// not the pre-append snapshot. The append publishes a new handle in the
/// coordinator map, but an already-captured handle stays the old object
/// unless this function writes the staged value back into it too.
#[tokio::test]
async fn a_caller_already_holding_the_handle_sees_the_durable_state() {
    let directory = tempfile::tempdir().expect("tempdir");
    let coordinator = leading_coordinator(directory.path()).await;
    coordinator
        .put(entry(TxnState::Ongoing, &[]), TxnVersion::Verified)
        .await
        .expect("seed the transaction");

    // Capture the handle the way a concurrent register_partitions call would,
    // before this append runs.
    let pre_append_handle = coordinator.get(TID).expect("entry");

    let code = add_offsets_partition(
        &coordinator,
        TID,
        producer(7, 3),
        offsets_partition(4),
        TxnVersion::Verified,
    )
    .await;
    assert!(code == codes::NONE);

    let via_old_handle = pre_append_handle.lock().await.clone();
    assert!(via_old_handle.partitions.contains(&offsets_partition(4)));
    assert!(via_old_handle.state == TxnState::Ongoing);
}

/// A transaction that `AddOffsetsToTxn` opens after a long idle period is not
/// reapable at once (#850).
#[tokio::test]
async fn the_reaper_does_not_abort_a_transaction_that_add_offsets_just_opened() {
    let directory = tempfile::tempdir().expect("tempdir");
    let coordinator = leading_coordinator(directory.path()).await;
    // The previous transaction started and completed long ago.
    let mut completed = entry(TxnState::CompleteCommit, &[]);
    completed.start_ms = 0;
    completed.last_update_ms = 0;
    coordinator
        .put(completed.clone(), TxnVersion::Verified)
        .await
        .expect("seed the transaction");

    let code = add_offsets_partition(
        &coordinator,
        TID,
        producer(7, 3),
        offsets_partition(4),
        TxnVersion::Verified,
    )
    .await;
    check!(code == codes::NONE);

    let stored = coordinator.get(TID).expect("entry").lock().await.clone();
    let now = now_millis();
    check!(
        !should_abort_idle_txn(stored.state, stored.txn_timeout_ms, stored.start_ms, now),
        "start_ms={} now={now}",
        stored.start_ms
    );
}

#[derive(serde::Deserialize)]
struct OffsetsInput {
    state: String,
    partitions: Vec<i32>,
    held_id: i64,
    next_id: i64,
    requested_id: i64,
    requested_epoch: i16,
    transactional_id: String,
}

fn offsets_state(value: &str) -> TxnState {
    match value {
        "Empty" => TxnState::Empty,
        "Ongoing" => TxnState::Ongoing,
        "PrepareCommit" => TxnState::PrepareCommit,
        "PrepareAbort" => TxnState::PrepareAbort,
        "CompleteCommit" => TxnState::CompleteCommit,
        "CompleteAbort" => TxnState::CompleteAbort,
        _ => panic!("unknown transaction state {value}"),
    }
}

impl OffsetsInput {
    fn seeded(&self) -> TxnEntry {
        let partitions = self
            .partitions
            .iter()
            .copied()
            .map(offsets_partition)
            .collect::<Vec<_>>();
        let mut seeded = entry(offsets_state(&self.state), &partitions);
        seeded.producer_id = ProducerId(self.held_id);
        if self.next_id >= 0 {
            seeded.next_producer_id = ProducerId(self.next_id);
            seeded.next_producer_epoch = 0;
        }
        seeded
    }

    fn requested(&self) -> ProducerIdentity { producer(self.requested_id, self.requested_epoch) }
}

#[derive(Default)]
struct OffsetsPolicy {
    input: Option<OffsetsInput>,
    decision: Option<AddOffsetsDecision>,
}

#[rstest::fixture]
fn offsets_policy() -> OffsetsPolicy {
    // Each decision starts with independent transaction metadata.
    OffsetsPolicy::default()
}

#[rstest_bdd_macros::given("transaction metadata and AddOffsets request {input}")]
fn offsets_policy_input(offsets_policy: &mut OffsetsPolicy, input: String) {
    offsets_policy.input = Some(serde_json::from_str(&input).expect("AddOffsets input"));
}

#[rstest_bdd_macros::when("the AddOffsets policy checks offsets partition 4")]
fn decide_offsets(offsets_policy: &mut OffsetsPolicy) {
    let input = offsets_policy.input.as_ref().expect("transaction metadata");
    offsets_policy.decision = Some(decide(
        &input.seeded(),
        input.requested(),
        &offsets_partition(4),
    ));
}

#[rstest_bdd_macros::then("the AddOffsets decision is {expected}")]
fn offsets_decision(offsets_policy: &OffsetsPolicy, expected: String) {
    let expected = if expected == "append" {
        AddOffsetsDecision::Append
    } else {
        AddOffsetsDecision::Answer(expected.parse().expect("Kafka error code"))
    };
    assert!(offsets_policy.decision.as_ref().expect("policy evaluated") == &expected);
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/txn/handlers/add_offset_commits_to_txn/tests/policy.feature",
    fixtures = [offsets_policy: OffsetsPolicy],
);

#[derive(Default)]
struct LoggedOffsets {
    input: Option<OffsetsInput>,
    seeded: Option<TxnEntry>,
    log: Option<LoggedCoordinator>,
    before: i64,
    code: Option<i16>,
    // The log must close before its temporary directory is removed.
    directory_guard: Option<tempfile::TempDir>,
}

#[rstest::fixture]
fn logged_offsets() -> LoggedOffsets {
    // Each request gets a fresh coordinator and a real transaction-state log.
    LoggedOffsets::default()
}

#[rstest_bdd_macros::given("a leading transaction log seeded for AddOffsets {input}")]
async fn seed_offsets_log(logged_offsets: &mut LoggedOffsets, input: String) {
    let input: OffsetsInput = serde_json::from_str(&input).expect("AddOffsets request");
    let seeded = input.seeded();
    let directory = tempfile::tempdir().expect("transaction directory");
    let log = coordinator_with_log(directory.path());
    log.coordinator
        .lead_state_partition_for_test(PartitionIndex(0))
        .await;
    log.coordinator
        .put(seeded.clone(), TxnVersion::Verified)
        .await
        .expect("seed transaction");
    logged_offsets.before = log.partition.log_end_offset().0;
    logged_offsets.seeded = Some(seeded);
    logged_offsets.input = Some(input);
    logged_offsets.log = Some(log);
    logged_offsets.directory_guard = Some(directory);
}

#[rstest_bdd_macros::when("AddOffsets registers offsets partition 4 through the coordinator")]
async fn append_offsets(logged_offsets: &mut LoggedOffsets) {
    let input = logged_offsets.input.as_ref().expect("request");
    let log = logged_offsets.log.as_ref().expect("leading coordinator");
    logged_offsets.code = Some(
        add_offsets_partition(
            &log.coordinator,
            &input.transactional_id,
            input.requested(),
            offsets_partition(4),
            TxnVersion::Verified,
        )
        .await,
    );
}

#[rstest_bdd_macros::then("AddOffsets returns code {code} and log mutation {mutation}")]
async fn offsets_log_result(logged_offsets: &LoggedOffsets, code: i16, mutation: String) {
    assert!(logged_offsets.code.expect("AddOffsets executed") == code);
    let log = logged_offsets.log.as_ref().expect("leading coordinator");
    let stored = log
        .coordinator
        .get(TID)
        .expect("transaction entry")
        .lock()
        .await
        .clone();
    let appended = log.partition.log_end_offset().0 > logged_offsets.before;
    match mutation.as_str() {
        "Ongoing-with-start-time" => {
            assert!(appended);
            assert!(stored.state == TxnState::Ongoing);
            assert!(stored.start_ms >= 0);
        }
        "unchanged-entry" => {
            assert!(!appended);
            assert!(&stored == logged_offsets.seeded.as_ref().expect("seeded entry"));
        }
        _ => panic!("unknown log mutation {mutation}"),
    }
}

/// The coordinator answers with Kafka's code and appends only a state change.
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/src/txn/handlers/add_offset_commits_to_txn/tests/log.feature",
    name = "Durable AddOffsets decisions"
)]
#[tokio::test]
async fn add_offsets_answers_kafka_codes_and_appends_only_a_change(
    logged_offsets: LoggedOffsets,
    input: String,
    code: i16,
    mutation: String,
) {
    let _ = (logged_offsets, input, code, mutation);
}
