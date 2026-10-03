use super::*;

#[test]
fn txn_basic() {
    run(
        EosModel {
            producers: 2,
            max_gen: 1,
            max_data_per_txn: 2,
            max_log: 5,
        },
        "txn_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn txn_wide() {
    // Deeper interleaving: a second transaction generation per producer + a
    // longer log, so a producer's committed txn can be held back by another
    // producer's later open txn across more offset orderings.
    run(
        EosModel {
            producers: 2,
            max_gen: 2,
            max_data_per_txn: 2,
            max_log: 7,
        },
        "txn_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
