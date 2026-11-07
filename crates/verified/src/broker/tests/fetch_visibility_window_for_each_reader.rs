use super::*;

/// Every watermark distinct, so each reader's window names the bound that
/// stopped it: `lso < deliverable < hw < log_end`.
const SPREAD: FetchWatermarks = FetchWatermarks {
    log_start: 2,
    lso: 4,
    deliverable: 6,
    hw: 8,
    log_end: 10,
};

/// The window a follower is given: it reads to the log end.
const FOLLOWER_WINDOW: FetchVisibility = FetchVisibility {
    out_of_range: false,
    empty: false,
    limit_offset: 10,
    effective_lso: 4,
    read_committed_aborts: false,
    response_hw: 8,
    response_lso: 4,
};

#[yare::parameterized(
    follower = { FetchReader::Follower, FOLLOWER_WINDOW },
    consumer_read_uncommitted = { FetchReader::Consumer(Isolation::ReadUncommitted), FetchVisibility {
        limit_offset: 6,
        ..FOLLOWER_WINDOW
    } },
    consumer_read_committed = { FetchReader::Consumer(Isolation::ReadCommitted), FetchVisibility {
        limit_offset: 4,
        read_committed_aborts: true,
        ..FOLLOWER_WINDOW
    } },
)]
fn fetch_visibility_window_for_each_reader(reader: FetchReader, expected: FetchVisibility) {
    assert2::assert!(fetch_visibility(reader, SPREAD, 3) == expected);
}
