use assert2::assert;

use super::*;

#[test]
fn fetch_visibility_matches_kafka_fetch_scenarios() {
    let open_txn = OPEN_TXN;
    // Every row reports the partition's own HW (8) and LSO (6), follower
    // or consumer: `Partition.readRecords` reads both whoever fetches.
    for (scenario, is_follower, read_committed, w, fetch_offset, expected) in [
        (
            "read_uncommitted consumer reads to the high watermark",
            false,
            false,
            open_txn,
            3,
            FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 8,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "read_committed consumer stops at the open transaction",
            false,
            true,
            open_txn,
            3,
            FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 6,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "follower reads to the log end but learns the committed bounds",
            true,
            false,
            open_txn,
            8,
            FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 10,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "caught-up follower has nothing to read",
            true,
            false,
            open_txn,
            10,
            FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 10,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "a fetch below the log start is OFFSET_OUT_OF_RANGE",
            false,
            false,
            open_txn,
            1,
            FetchVisibility {
                out_of_range: true,
                empty: false,
                limit_offset: 8,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
    ] {
        assert!(
            fetch_visibility(is_follower, read_committed, w, fetch_offset) == expected,
            "{scenario}"
        );
    }

    // An LSO that has run ahead of the high watermark is capped at it, in
    // the report as well as the read_committed bound, as
    // `UnifiedLog.lastStableOffset` caps it.
    assert!(
        fetch_visibility(false, true, FetchWatermarks { lso: 9, ..open_txn }, 3)
            == FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 8,
                effective_lso: 8,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 8,
            }
    );
}

#[test]
fn fetch_visibility_caps_only_a_consumer_at_the_delivery_watermark() {
    let open_txn = OPEN_TXN;
    // As above, every row reports the partition's own HW (8) and LSO (6).
    for (scenario, is_follower, read_committed, w, fetch_offset, expected) in [
        (
            "a follower is not gated by a delivery watermark at the log start",
            true,
            false,
            FetchWatermarks {
                deliverable: 2,
                ..open_txn
            },
            3,
            FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 10,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "a consumer is held below a batch that is not due yet",
            false,
            false,
            FetchWatermarks {
                deliverable: 5,
                ..open_txn
            },
            3,
            FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 5,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "a consumer parked at the delivery watermark reads nothing",
            false,
            false,
            FetchWatermarks {
                deliverable: 5,
                ..open_txn
            },
            5,
            FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 5,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "read_committed takes the delivery watermark where it is lowest",
            false,
            true,
            FetchWatermarks {
                deliverable: 4,
                ..open_txn
            },
            3,
            FetchVisibility {
                out_of_range: false,
                empty: false,
                limit_offset: 4,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "a delivery watermark above the high watermark exposes nothing uncommitted",
            false,
            false,
            FetchWatermarks {
                deliverable: 10,
                ..open_txn
            },
            8,
            FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 8,
                effective_lso: 6,
                read_committed_aborts: false,
                response_hw: 8,
                response_lso: 6,
            },
        ),
        (
            "a delivery watermark below the log start leaves an empty window",
            false,
            true,
            FetchWatermarks {
                deliverable: 0,
                ..open_txn
            },
            2,
            FetchVisibility {
                out_of_range: false,
                empty: true,
                limit_offset: 0,
                effective_lso: 6,
                read_committed_aborts: true,
                response_hw: 8,
                response_lso: 6,
            },
        ),
    ] {
        assert!(
            fetch_visibility(is_follower, read_committed, w, fetch_offset) == expected,
            "{scenario}"
        );
    }
}
