use super::*;

#[test]
fn two_pc_basic() {
    run(
        TwoPcModel {
            max_epoch: 3,
            fence_version: TxnVersion::Verified,
        },
        "two_pc_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn two_pc_wide() {
    // More generations → deeper classic↔2PC alternations and reaper interleaves.
    run(
        TwoPcModel {
            max_epoch: 5,
            fence_version: TxnVersion::Verified,
        },
        "two_pc_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

#[test]
fn two_pc_basic_below_tv2_fence() {
    // A cluster below `TV_2`: the reaper and the `InitProducerId` fence raise
    // the epoch themselves, and completion does not.
    run(
        TwoPcModel {
            max_epoch: 3,
            fence_version: TxnVersion::Classic,
        },
        "two_pc_basic_below_tv2_fence",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn two_pc_wide_below_tv2_fence() {
    run(
        TwoPcModel {
            max_epoch: 5,
            fence_version: TxnVersion::Classic,
        },
        "two_pc_wide_below_tv2_fence",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
