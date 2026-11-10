//! KIP-939 two-phase-commit (2PC) participation: pure decision cores.
//!
//! In a 2PC transaction, an *external* transaction coordinator owns the
//! commit/abort decision. That coordinator is, for example, an XA resource
//! manager or Apache Flink's sink. Kafka acts as a 2PC participant. After the
//! producer has prepared, the coordinator MUST keep the transaction completable
//! indefinitely, and it MUST NOT proactively abort the transaction on the
//! transaction timeout. Only the external coordinator, through `EndTxn`, or an
//! explicit `InitProducerId` may end it.
//!
//! ## How "this is a 2PC transaction" is encoded
//!
//! Krabka follows Apache Kafka exactly, so 2PC is NOT a new persisted field.
//! The already-persisted `TransactionTimeoutMs` encodes it as the sentinel
//! `i32::MAX`. Kafka's
//! `TransactionMetadata.isDistributedTwoPhaseCommitTxn()` is literally
//! `txnTimeoutMs == Integer.MAX_VALUE`, and `InitProducerId` resolves the
//! stored timeout to `Int.MaxValue` when `enable2Pc` is set. Because the
//! timeout round-trips through `TransactionLogValue`, the property survives
//! coordinator failover and log replay without any schema change.
//!
//! These functions are pure, and [`super::two_pc_model`] model-checks them
//! exhaustively. The live coordinator calls the same functions, so the model's
//! guarantees bind production behaviour. The callers are the idle-transaction
//! reaper in [`super::expiration`] and the `InitProducerId` handler.

use {
    super::state::TxnState,
    crate::codes::INVALID_TRANSACTION_TIMEOUT,
    krabka_verified::transaction::{
        IdleTransactionState, NO_TRANSACTION_TIMEOUT_MS, should_abort_idle_transaction,
    },
};

/// Persisted 2PC timeout sentinel.
pub const NO_TIMEOUT_MS: i32 = NO_TRANSACTION_TIMEOUT_MS;

/// The `TransactionTimeoutMs` to persist for an `InitProducerId`, or
/// `INVALID_TRANSACTION_TIMEOUT` when the request asks for a timeout that
/// Kafka refuses.
///
/// Kafka's `TransactionStateManager.validateTransactionTimeoutMs` is
/// `enableTwoPC || (txnTimeoutMs <= config.transactionMaxTimeoutMs &&
/// txnTimeoutMs > 0)`, and `TransactionCoordinator.handleInitProducerId`
/// answers `INVALID_TRANSACTION_TIMEOUT` when it fails. A valid timeout is
/// stored as the client sent it.
///
/// * `enable_2pc` gives `i32::MAX`: the external transaction manager owns the
///   commit decision, so the broker never times the transaction out. The
///   requested timeout is not read, and `transaction.max.timeout.ms` does not
///   apply (KIP-939).
///
/// # Errors
///
/// Returns `INVALID_TRANSACTION_TIMEOUT` (50) for a timeout that is not
/// positive, or that is above `max_timeout_ms`.
pub fn resolve_txn_timeout(
    enable_2pc: bool,
    requested_ms: i32,
    max_timeout_ms: i32,
) -> Result<i32, i16> {
    if enable_2pc {
        return Ok(NO_TIMEOUT_MS);
    }
    if requested_ms <= 0 || requested_ms > max_timeout_ms {
        return Err(INVALID_TRANSACTION_TIMEOUT);
    }
    Ok(requested_ms)
}

/// Reports whether this persisted timeout marks a 2PC (externally-coordinated)
/// transaction. The `i32::MAX` sentinel identifies it, exactly like
/// Kafka's `isDistributedTwoPhaseCommitTxn`.
#[must_use]
#[cfg(test)]
pub fn is_two_phase_commit(txn_timeout_ms: i32) -> bool { txn_timeout_ms == NO_TIMEOUT_MS }

/// THE safety-critical decision: should the idle-transaction reaper abort the
/// transaction in `state` as of `now_ms`? The transaction carries the persisted
/// `txn_timeout_ms` and became `Ongoing` at `start_ms`.
///
/// Returns `true` iff ALL of the following hold:
///  - the transaction is [`TxnState::Ongoing`]. Only an *open* transaction can
///    time out. The reaper never reaps `Empty` / `Prepare*` / `Complete*` /
///    `Dead`. Prepare\* is a transient commit/abort that the coordinator is
///    already driving, and a separate, much longer transactional-id expiry,
///    [`crate::txn::coordinator::expiry`], reclaims the terminal and idle
///    states, not this reaper. That sweep refuses every `Prepare*` state in
///    turn, so neither reaper takes a prepared 2PC decision away from the
///    transaction manager that owns it.
///  - it is NOT a 2PC transaction (`!is_two_phase_commit`). This is **the
///    KIP-939 guarantee**: the reaper never unilaterally aborts a prepared 2PC
///    transaction.
///  - it has been open longer than `txn_timeout_ms`
///    (`start_ms + txn_timeout_ms < now_ms`), Kafka's strict comparison in
///    `TransactionStateManager.timedOutTransactions`.
///
/// Pure and total over every persisted timeout, as Kafka is: the transaction
/// log accepts any `TransactionTimeoutMs`, and a zero or negative one is
/// compared arithmetically. A backwards clock (`now_ms <= start_ms`) never
/// aborts a transaction with a nonnegative timeout, which is every timeout
/// [`resolve_txn_timeout`] persists.
#[must_use]
pub fn should_abort_idle_txn(
    state: TxnState,
    txn_timeout_ms: i32,
    start_ms: i64,
    now_ms: i64,
) -> bool {
    let state = if state == TxnState::Ongoing {
        IdleTransactionState::Ongoing
    } else {
        IdleTransactionState::Other
    };
    should_abort_idle_transaction(state, txn_timeout_ms, start_ms, now_ms)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// The transaction timeout a scenario's producer asks for, and what the
    /// broker made of it.
    struct TransactionTimeoutScenario {
        maximum_ms: krabka_bdd::Given<i32>,
        requested_ms: krabka_bdd::Given<i32>,
        resolved: krabka_bdd::Given<Result<i32, i16>>,
        state: krabka_bdd::Given<TxnState>,
        aborted: krabka_bdd::Given<bool>,
    }

    #[rstest::fixture]
    fn transaction_timeout() -> TransactionTimeoutScenario {
        TransactionTimeoutScenario {
            maximum_ms: krabka_bdd::Given::new(
                "maximum_ms",
                "Given the maximum transaction timeout is <maximum> milliseconds",
            ),
            requested_ms: krabka_bdd::Given::new(
                "requested_ms",
                "Given the producer requests transaction timeout <requested> milliseconds",
            ),
            resolved: krabka_bdd::Given::new(
                "resolved",
                "When the broker resolves the ordinary transaction timeout",
            ),
            state: krabka_bdd::Given::new(
                "state",
                "Given a transaction in state <state> has been open longer than its timeout",
            ),
            aborted: krabka_bdd::Given::new(
                "aborted",
                "When the timeout reaper checks the transaction",
            ),
        }
    }

    /// The timeout the transaction of a reaper scenario was opened with.
    const OPEN_TIMEOUT_MS: i32 = 60_000;

    #[krabka_bdd::given("the maximum transaction timeout is {maximum} milliseconds")]
    fn maximum_timeout(transaction_timeout: &mut TransactionTimeoutScenario, maximum: i32) {
        transaction_timeout.maximum_ms.set(maximum);
    }

    #[krabka_bdd::given("the producer requests transaction timeout {requested} milliseconds")]
    fn requested_timeout(transaction_timeout: &mut TransactionTimeoutScenario, requested: i32) {
        transaction_timeout.requested_ms.set(requested);
    }

    #[krabka_bdd::given("a transaction in state {state} has been open longer than its timeout")]
    fn open_past_its_timeout(
        transaction_timeout: &mut TransactionTimeoutScenario,
        state: TxnState,
    ) {
        transaction_timeout.state.set(state);
    }

    fn resolve(transaction_timeout: &mut TransactionTimeoutScenario, enable_2pc: bool) {
        let resolved = resolve_txn_timeout(
            enable_2pc,
            *transaction_timeout.requested_ms.get(),
            *transaction_timeout.maximum_ms.get(),
        );
        transaction_timeout.resolved.set(resolved);
    }

    #[krabka_bdd::when("the broker resolves the ordinary transaction timeout")]
    fn resolve_ordinary(transaction_timeout: &mut TransactionTimeoutScenario) {
        resolve(transaction_timeout, false);
    }

    #[krabka_bdd::when("the broker resolves the two-phase-commit transaction timeout")]
    fn resolve_two_phase_commit(transaction_timeout: &mut TransactionTimeoutScenario) {
        resolve(transaction_timeout, true);
    }

    #[krabka_bdd::when("the timeout reaper checks the transaction")]
    fn reaper_checks(transaction_timeout: &mut TransactionTimeoutScenario) {
        // Opened at 0 and checked one millisecond past its timeout.
        let aborted = should_abort_idle_txn(
            *transaction_timeout.state.get(),
            OPEN_TIMEOUT_MS,
            0,
            i64::from(OPEN_TIMEOUT_MS) + 1,
        );
        transaction_timeout.aborted.set(aborted);
    }

    #[krabka_bdd::then("the transaction timeout is {stored} milliseconds")]
    fn stored_timeout(transaction_timeout: &TransactionTimeoutScenario, stored: i32) {
        assert!(*transaction_timeout.resolved.get() == Ok(stored));
    }

    #[krabka_bdd::then("the transaction timeout is refused with INVALID_TRANSACTION_TIMEOUT")]
    fn refused_timeout(transaction_timeout: &TransactionTimeoutScenario) {
        assert!(*transaction_timeout.resolved.get() == Err(INVALID_TRANSACTION_TIMEOUT));
    }

    #[krabka_bdd::then("the transaction is stored with the no-timeout sentinel")]
    fn stored_without_timeout(transaction_timeout: &TransactionTimeoutScenario) {
        assert!(*transaction_timeout.resolved.get() == Ok(i32::MAX));
    }

    #[krabka_bdd::then("the reaper aborts the transaction")]
    fn reaper_aborts(transaction_timeout: &TransactionTimeoutScenario) {
        assert!(*transaction_timeout.aborted.get());
    }

    #[krabka_bdd::then("the reaper leaves the transaction alone")]
    fn reaper_leaves_it(transaction_timeout: &TransactionTimeoutScenario) {
        assert!(!*transaction_timeout.aborted.get());
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/txn/two_pc.feature",
        fixtures = [transaction_timeout: TransactionTimeoutScenario],
    );

    #[test]
    fn non_2pc_resolution_never_collides_with_the_sentinel() {
        use assert2::check;
        // The ceiling is far below i32::MAX, so a non-2PC transaction can
        // never accidentally look like a 2PC one.
        check!(!is_two_phase_commit(
            resolve_txn_timeout(false, 8_000, 8_000).expect("a valid timeout")
        ));
        check!(is_two_phase_commit(
            resolve_txn_timeout(true, 1, 8_000).expect("2PC")
        ));
    }
}
