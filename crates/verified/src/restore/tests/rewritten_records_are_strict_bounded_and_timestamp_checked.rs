use super::*;

#[test]
fn rewritten_records_are_strict_bounded_and_timestamp_checked() {
    use RestoreTimestampType::{CreateTime, LogAppendTime};
    let create = frame(CreateTime, 100, 110);
    // A LogAppendTime batch whose producer clock ran ahead of the broker:
    // every base + delta exceeds max_timestamp, which Kafka never reads.
    let append = frame(LogAppendTime, 2_000, 1_000);
    for (name, previous, frame, record, expected) in [
        ("first record", None, create, deltas(1, 5), Some((11, 105))),
        (
            "last record at the max",
            Some(1),
            create,
            deltas(4, 10),
            Some((14, 110)),
        ),
        ("repeated delta", Some(1), create, deltas(1, 5), None),
        ("regressing delta", Some(3), create, deltas(2, 5), None),
        ("past the span", None, create, deltas(5, 5), None),
        (
            "create time above the max",
            None,
            frame(CreateTime, 100, 104),
            deltas(1, 5),
            None,
        ),
        (
            "create time overflow",
            None,
            frame(CreateTime, i64::MAX, i64::MAX),
            deltas(1, 1),
            None,
        ),
        (
            "log append time with producer clock ahead",
            None,
            append,
            deltas(0, 0),
            Some((10, 1_000)),
        ),
        (
            "log append time later record",
            Some(0),
            append,
            deltas(3, 900),
            Some((13, 1_000)),
        ),
        (
            "log append time still strict",
            Some(3),
            append,
            deltas(3, 0),
            None,
        ),
    ] {
        check!(
            restore_rewritten_record(previous, frame, record) == expected,
            "{name}"
        );
    }
}

#[test]
fn record_selection_uses_inclusive_offset_and_exclusive_time_bounds() {
    for (name, offset, timestamp, exclusions, expected) in [
        ("inside both bounds", 10, 99, NONE, true),
        ("past the offset bound", 11, 99, NONE, false),
        ("at the timestamp bound", 10, 100, NONE, false),
        (
            "producer excluded",
            10,
            99,
            RestoreExclusions {
                producer: true,
                ..NONE
            },
            false,
        ),
        (
            "offset excluded",
            10,
            99,
            RestoreExclusions {
                offset: true,
                ..NONE
            },
            false,
        ),
        (
            "key excluded",
            10,
            99,
            RestoreExclusions {
                content: RestoreContentExclusions {
                    key: true,
                    ..NO_CONTENT
                },
                ..NONE
            },
            false,
        ),
        (
            "header excluded",
            10,
            99,
            RestoreExclusions {
                content: RestoreContentExclusions {
                    header: true,
                    ..NO_CONTENT
                },
                ..NONE
            },
            false,
        ),
    ] {
        check!(
            restore_record_selected(offset, Some(10), timestamp, Some(100), exclusions) == expected,
            "{name}"
        );
    }
    check!(restore_record_selected(
        i64::MIN,
        None,
        i64::MAX,
        None,
        NONE
    ));
}

#[test]
fn batch_fold_and_skip_are_exact() {
    check!(restore_batch_filter_decision(false, false) == RestoreFilterDecision::Keep);
    check!(restore_batch_filter_decision(true, false) == RestoreFilterDecision::Keep);
    check!(restore_batch_filter_decision(false, true) == RestoreFilterDecision::Empty);
    check!(restore_batch_filter_decision(true, true) == RestoreFilterDecision::Filter);
    check!(!restore_batch_past_offset_bound(10, Some(10)));
    check!(restore_batch_past_offset_bound(11, Some(10)));
    check!(!restore_batch_past_offset_bound(i64::MAX, None));
}
