use super::*;

#[test]
fn txn_basic() {
    // One tid, epoch 0..=3: every interleaving of Init / BeginTxn / EndTxn
    // Phase1 / Phase3 / Complete, including an overtaken EndTxn in the window.
    run(
        TxnModel {
            max_epoch: 3,
            fence_version: TxnVersion::Verified,
        },
        "txn_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn txn_wide() {
    // More producer-epoch generations → deeper commit/abort/fence interleavings.
    run(
        TxnModel {
            max_epoch: 6,
            fence_version: TxnVersion::Verified,
        },
        "txn_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

#[test]
fn txn_basic_below_tv2_fence() {
    // The same interleavings with a cluster below `TV_2`: the fence of an
    // `Ongoing` transaction raises the epoch itself.
    run(
        TxnModel {
            max_epoch: 3,
            fence_version: TxnVersion::Classic,
        },
        "txn_basic_below_tv2_fence",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn txn_wide_below_tv2_fence() {
    run(
        TxnModel {
            max_epoch: 6,
            fence_version: TxnVersion::Classic,
        },
        "txn_wide_below_tv2_fence",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

/// `InitProducerId` against each state of the live entry: the `Prepare*`
/// states refuse (Kafka's `CONCURRENT_TRANSACTIONS`), `Ongoing` is fenced and
/// prepared for abort, and the rest bump the epoch into `Empty`. The fence
/// raises the epoch once, at every cluster version.
#[test]
fn init_producer_id_by_state() {
    let at = |state: TxnState| TxnProj {
        pid: PID.get(),
        epoch: 2,
        state: state.to_kafka_status(),
        generation: 2,
        pending: None,
        finalized: vec![],
        violations: BTreeSet::new(),
    };
    let rows = [
        (TxnState::PrepareCommit, None),
        (TxnState::PrepareAbort, None),
        (
            TxnState::Ongoing,
            Some(TxnProj {
                // Kafka bumps the epoch of a fenced producer once: 2 → 3, in
                // the completion at `TV_2` and in the fence below it.
                epoch: 3,
                state: TxnState::PrepareAbort.to_kafka_status(),
                ..at(TxnState::Ongoing)
            }),
        ),
        (
            TxnState::Empty,
            Some(TxnProj {
                epoch: 3,
                ..at(TxnState::Empty)
            }),
        ),
        (
            TxnState::CompleteCommit,
            Some(TxnProj {
                epoch: 3,
                state: TxnState::Empty.to_kafka_status(),
                ..at(TxnState::CompleteCommit)
            }),
        ),
        (
            TxnState::CompleteAbort,
            Some(TxnProj {
                epoch: 3,
                state: TxnState::Empty.to_kafka_status(),
                ..at(TxnState::CompleteAbort)
            }),
        ),
    ];
    for fence_version in [TxnVersion::Verified, TxnVersion::Classic] {
        let model = TxnModel {
            max_epoch: 6,
            fence_version,
        };
        for (state, expected) in &rows {
            assert2::assert!(
                model.next_state(&at(*state), TxnAction::Init) == *expected,
                "{fence_version:?} {state:?}"
            );
        }
    }
}
