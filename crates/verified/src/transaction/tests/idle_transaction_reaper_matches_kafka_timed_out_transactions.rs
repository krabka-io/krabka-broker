use {
    super::*,
    assert2::assert,
    rstest::fixture,
    rstest_bdd::Slot,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::vec,
};

#[derive(Default)]
struct CoordinatorDecisions {
    idle: Option<(IdleTransactionState, i32, i64)>,
    abort_idle: Option<bool>,
    allocation: Option<(bool, bool, i64, i16, Option<i64>)>,
    next_identity: Slot<Option<(i64, i16)>>,
    initialization: Option<(i64, i16, i16, i64)>,
    initialized: Option<InitProducerIdIdentityDecision>,
    completion: Option<(
        TransactionSnapshot,
        TransactionIdentity,
        TransactionIdentity,
    )>,
    completed: Option<TransactionCompletionDecision>,
}

#[fixture]
fn coordinator() -> CoordinatorDecisions {
    // Start this scenario with fresh inputs and observations.

    CoordinatorDecisions {
        idle: None,
        ..CoordinatorDecisions::default()
    }
}

#[given("a transaction in state {state} with timeout {timeout} and start time {start} ms")]
fn idle_transaction(
    coordinator: &mut CoordinatorDecisions,
    state: String,
    timeout: String,
    start: i64,
) {
    let state = match state.as_str() {
        "ongoing" => IdleTransactionState::Ongoing,
        "not-ongoing" => IdleTransactionState::Other,
        _ => panic!("unknown idle transaction state: {state}"),
    };
    let timeout = if timeout == "external-2pc" {
        NO_TRANSACTION_TIMEOUT_MS
    } else {
        timeout
            .parse()
            .expect("transaction timeout is a signed millisecond count")
    };
    coordinator.idle = Some((state, timeout, start));
}

#[when("the transaction timeout reaper runs at {now} ms")]
fn reap_idle_transaction(coordinator: &mut CoordinatorDecisions, now: i64) {
    let (state, timeout, start) = coordinator.idle.expect("a transaction was given");
    coordinator.abort_idle = Some(should_abort_idle_transaction(state, timeout, start, now));
}

#[then("the timeout reaper aborts the transaction: {abort}")]
fn reaped_transaction(coordinator: &CoordinatorDecisions, abort: bool) {
    assert!(coordinator.abort_idle == Some(abort));
}

#[given(
    "producer {pid} at epoch {epoch} has verified identity {verified}, recovery mode {recovery}, and replacement producer ID {fresh}"
)]
fn existing_identity(
    coordinator: &mut CoordinatorDecisions,
    pid: i64,
    epoch: i16,
    verified: bool,
    recovery: bool,
    fresh: String,
) {
    let fresh = if fresh == "none" {
        None
    } else {
        Some(
            fresh
                .parse()
                .expect("replacement producer ID is an integer"),
        )
    };
    coordinator.allocation = Some((verified, recovery, pid, epoch, fresh));
}

#[when("the coordinator allocates the next producer identity")]
fn allocate_identity(coordinator: &mut CoordinatorDecisions) {
    let (verified, recovery, pid, epoch, fresh) = coordinator
        .allocation
        .expect("producer identity allocation facts were given");
    coordinator.next_identity.set(next_producer_identity(
        verified, recovery, pid, epoch, fresh,
    ));
}

fn identity(text: &str) -> TransactionIdentity {
    let (pid, epoch) = text
        .split_once(':')
        .expect("producer identity uses producer-id:epoch notation");
    TransactionIdentity {
        pid: pid.parse().expect("producer ID is an integer"),
        epoch: epoch.parse().expect("producer epoch is an integer"),
    }
}

#[then("the allocated producer identity is {allocated}")]
fn allocated_identity(coordinator: &CoordinatorDecisions, allocated: String) {
    let expected = if allocated == "unavailable" {
        None
    } else {
        let value = identity(&allocated);
        Some((value.pid, value.epoch))
    };
    assert!(coordinator.next_identity.get() == Some(expected));
}

#[given(
    "a producer entry with ID {pid}, epoch {epoch}, last epoch {last_epoch}, and previous ID {previous_pid}"
)]
fn initialization_entry(
    coordinator: &mut CoordinatorDecisions,
    pid: i64,
    epoch: i16,
    last_epoch: i16,
    previous_pid: i64,
) {
    coordinator.initialization = Some((pid, epoch, last_epoch, previous_pid));
}

#[when("InitProducerId requests producer ID {pid} and epoch {epoch}")]
fn initialize_identity(coordinator: &mut CoordinatorDecisions, pid: i64, epoch: i16) {
    let (entry_pid, entry_epoch, last_epoch, previous_pid) = coordinator
        .initialization
        .expect("a producer entry was given");
    coordinator.initialized = Some(init_producer_id_identity_decision(
        entry_pid,
        entry_epoch,
        last_epoch,
        previous_pid,
        pid,
        epoch,
    ));
}

#[then("InitProducerId identity handling is {outcome}")]
fn initialized_identity(coordinator: &CoordinatorDecisions, outcome: String) {
    let expected = match outcome.as_str() {
        "bump-without-expected-identity" => InitProducerIdIdentityDecision::BumpWithoutIdentity,
        "bump-epoch" => InitProducerIdIdentityDecision::Bump,
        "retry-current-identity" => InitProducerIdIdentityDecision::Retry,
        "fence-requester" => InitProducerIdIdentityDecision::Fenced,
        _ => panic!("unknown InitProducerId identity outcome: {outcome}"),
    };
    assert!(coordinator.initialized == Some(expected));
}

#[given(
    "a current transaction identity {current} in state {state}, prepared identity {prepared}, and completion identity {completion}"
)]
fn prepared_transaction(
    coordinator: &mut CoordinatorDecisions,
    current: String,
    state: String,
    prepared: String,
    completion: String,
) {
    let current = identity(&current);
    let state = match state.as_str() {
        "ongoing" => ONGOING,
        "prepare-commit" => PREPARE_COMMIT,
        "complete-commit" => COMPLETE_COMMIT,
        _ => panic!("unknown transaction completion state: {state}"),
    };
    coordinator.completion = Some((
        TransactionSnapshot {
            pid: current.pid,
            epoch: current.epoch,
            state,
        },
        identity(&prepared),
        identity(&completion),
    ));
}

#[when("the coordinator completes the prepared commit")]
fn complete_commit(coordinator: &mut CoordinatorDecisions) {
    let (current, prepared, completion) = coordinator
        .completion
        .expect("a prepared transaction was given");
    coordinator.completed = Some(transaction_completion_decision(
        current,
        prepared,
        completion,
        PREPARE_COMMIT,
        COMPLETE_COMMIT,
    ));
}

#[then("transaction completion is {outcome}")]
fn completed_transaction(coordinator: &CoordinatorDecisions, outcome: String) {
    let expected = match outcome.as_str() {
        "proceed" => TransactionCompletionDecision::Proceed,
        "already-complete" => TransactionCompletionDecision::AlreadyComplete,
        "reject-stale-identity" => TransactionCompletionDecision::RejectStaleIdentity,
        "reject-state" => TransactionCompletionDecision::RejectState,
        _ => panic!("unknown transaction completion outcome: {outcome}"),
    };
    assert!(coordinator.completed == Some(expected));
}

scenarios!("tests/features/migrated/src/transaction/tests/idle_transaction_reaper_matches_kafka_timed_out_transactions.feature", fixtures = [coordinator: CoordinatorDecisions]);
