use super::*;

#[test]
fn replay_starts_at_the_loaded_snapshot_or_the_log_start() {
    let tiered = ProducerReloadRange {
        log_start: 0,
        local_start: 5,
        log_end: 10,
    };
    let trimmed_below_local = ProducerReloadRange {
        log_start: 7,
        local_start: 5,
        log_end: 10,
    };
    for (range, snapshot, expected) in [
        (
            ProducerReloadRange {
                log_start: 0,
                local_start: 0,
                log_end: 0,
            },
            None,
            Some(0),
        ),
        (RANGE, None, Some(5)),
        (RANGE, Some(7), Some(7)),
        (RANGE, Some(10), Some(10)),
        // Snapshots the reload deletes cannot be loaded.
        (RANGE, Some(5), None),
        (RANGE, Some(3), None),
        (RANGE, Some(11), None),
        (RANGE, Some(-1), None),
        // Remote storage: no replay below the first local segment.
        (tiered, None, Some(5)),
        (tiered, Some(3), Some(5)),
        (tiered, Some(8), Some(8)),
        (trimmed_below_local, None, Some(7)),
        // Malformed ranges.
        (
            ProducerReloadRange {
                log_start: 5,
                local_start: 5,
                log_end: 4,
            },
            None,
            None,
        ),
        (
            ProducerReloadRange {
                log_start: -1,
                local_start: 0,
                log_end: 10,
            },
            None,
            None,
        ),
        (
            ProducerReloadRange {
                log_start: 0,
                local_start: 11,
                log_end: 10,
            },
            None,
            None,
        ),
        (
            ProducerReloadRange {
                log_start: 0,
                local_start: -1,
                log_end: 10,
            },
            None,
            None,
        ),
    ] {
        assert2::check!(
            producer_snapshot_replay_start(range, snapshot) == expected,
            "{range:?} {snapshot:?}"
        );
    }
}
