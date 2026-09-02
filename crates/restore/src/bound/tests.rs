//! Unit tests for the decisions `Predicates` makes about one archived batch
//! and the records inside it, driven through the shared `decide` harness.

use assert2::check;
use bytes::Bytes;
use krabka_protocol::records::{Record, RecordsError};

use super::{
    test_support::{
        BASE_TIMESTAMP, batch, decide, header, partition, predicates, record, try_decide,
    },
    *,
};
use crate::error::RestoreError;

#[test]
fn no_predicates_keep_everything() {
    let predicates = predicates(&[]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                key: Some(Bytes::from_static(b"k0")),
                ..record(0)
            },
            Record {
                key: None,
                ..record(1)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
}

#[test]
fn to_offset_bound_is_inclusive_at_the_named_offset() {
    let predicates = predicates(&["--to-offset", "orders:0=42"]);

    check!(predicates.offset_bound(&partition("orders", 0)) == Some(Offset(42)));
    check!(predicates.offset_bound(&partition("orders", 1)).is_none());
    check!(predicates.offset_bound(&partition("other", 0)).is_none());
    check!(!predicates.batch_past_offset_bound(&partition("orders", 0), Offset(42)));
    check!(predicates.batch_past_offset_bound(&partition("orders", 0), Offset(43)));
    check!(!predicates.batch_past_offset_bound(&partition("orders", 1), Offset(i64::MAX)));
}

#[test]
fn to_offset_filters_a_batch_that_straddles_the_inclusive_bound() {
    let predicates = predicates(&["--to-offset", "orders:0=1001"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(1, vec![record(0), record(1), record(2)]);

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Filter);
    check!(
        records
            == [
                RecordDecision::Keep,
                RecordDecision::Keep,
                RecordDecision::Drop,
            ]
    );

    let (other_decision, other_records) = decide(&predicates, &partition("orders", 1), &owned);
    check!(other_decision == BatchDecision::Keep);
    check!(other_records == [RecordDecision::Keep; 3]);
}

#[test]
fn exclude_key_filters_only_matching_records() {
    let predicates = predicates(&["--exclude-key", "^alpha"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                key: Some(Bytes::from_static(b"alpha-1")),
                ..record(0)
            },
            Record {
                key: Some(Bytes::from_static(b"beta-1")),
                ..record(1)
            },
            Record {
                key: Some(Bytes::from_static(b"alpha-2")),
                ..record(2)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Filter);
    check!(
        records
            == [
                RecordDecision::Drop,
                RecordDecision::Keep,
                RecordDecision::Drop,
            ]
    );
}

#[test]
fn exclude_key_matching_every_record_empties_the_batch() {
    let predicates = predicates(&["--exclude-key", "^k"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                key: Some(Bytes::from_static(b"k1")),
                ..record(0)
            },
            Record {
                key: Some(Bytes::from_static(b"k2")),
                ..record(1)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Empty);
    check!(records == [RecordDecision::Drop, RecordDecision::Drop]);
}

#[test]
fn exclude_key_matching_nothing_keeps_the_batch() {
    let predicates = predicates(&["--exclude-key", "^zzz"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                key: Some(Bytes::from_static(b"k1")),
                ..record(0)
            },
            Record {
                key: Some(Bytes::from_static(b"k2")),
                ..record(1)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
}

#[test]
fn a_keyless_record_never_matches_an_exclude_key_pattern_even_dot_star() {
    let predicates = predicates(&["--exclude-key", ".*"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                key: None,
                ..record(0)
            },
            Record {
                key: Some(Bytes::from_static(b"anything")),
                ..record(1)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Filter);
    check!(records == [RecordDecision::Keep, RecordDecision::Drop]);
}

#[test]
fn exclude_header_matches_on_name_and_value_not_name_alone() {
    let predicates = predicates(&["--exclude-header", "trace=^bad"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                headers: vec![header("trace", b"bad-1")],
                ..record(0)
            },
            Record {
                headers: vec![header("trace", b"good-1")],
                ..record(1)
            },
            Record {
                headers: vec![header("other", b"bad-1")],
                ..record(2)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Filter);
    check!(
        records
            == [
                RecordDecision::Drop,
                RecordDecision::Keep,
                RecordDecision::Keep,
            ]
    );
}

#[test]
fn exclude_producer_id_drops_every_record_from_that_producer_and_no_other() {
    let predicates = predicates(&["--exclude-producer-id", "7"]);
    let orders_0 = partition("orders", 0);

    let named = batch(7, vec![record(0), record(1)]);
    let (batch_decision, records) = decide(&predicates, &orders_0, &named);
    check!(batch_decision == BatchDecision::Empty);
    check!(records == [RecordDecision::Drop, RecordDecision::Drop]);

    let other = batch(8, vec![record(0), record(1)]);
    let (batch_decision, records) = decide(&predicates, &orders_0, &other);
    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
}

#[test]
fn exclude_offset_range_is_half_open() {
    // BASE_OFFSET is 1_000, so offset_delta N is absolute offset 1_000+N.
    // The range 1001..1003 must drop 1001 (inclusive start) and 1002, and
    // keep 1000 and 1003 (exclusive end).
    let predicates = predicates(&["--exclude-offset", "orders:0=1001..1003"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(1, vec![record(0), record(1), record(2), record(3)]);

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Filter);
    check!(
        records
            == [
                RecordDecision::Keep,
                RecordDecision::Drop,
                RecordDecision::Drop,
                RecordDecision::Keep,
            ]
    );
}

#[test]
fn exclude_offset_only_applies_to_its_named_partition() {
    let predicates = predicates(&["--exclude-offset", "orders:0=1001..1003"]);
    let orders_1 = partition("orders", 1);
    let owned = batch(1, vec![record(1)]);

    let (batch_decision, records) = decide(&predicates, &orders_1, &owned);

    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep]);
}

#[test]
fn to_timestamp_entirely_before_the_bound_keeps_the_batch() {
    let bound = BASE_TIMESTAMP + 100;
    let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                timestamp_delta: 0,
                ..record(0)
            },
            Record {
                timestamp_delta: 50,
                ..record(1)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep, RecordDecision::Keep]);
}

#[test]
fn to_timestamp_entirely_at_or_after_the_bound_empties_the_batch() {
    let bound = BASE_TIMESTAMP + 100;
    let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                timestamp_delta: 100,
                ..record(0)
            },
            Record {
                timestamp_delta: 200,
                ..record(1)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Empty);
    check!(records == [RecordDecision::Drop, RecordDecision::Drop]);
}

#[test]
fn to_timestamp_straddling_the_bound_filters_the_right_split() {
    let bound = BASE_TIMESTAMP + 100;
    let predicates = predicates(&["--to-timestamp", &bound.to_string()]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        1,
        vec![
            Record {
                timestamp_delta: 0,
                ..record(0)
            },
            Record {
                timestamp_delta: 100,
                ..record(1)
            },
            Record {
                timestamp_delta: 150,
                ..record(2)
            },
        ],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Filter);
    check!(
        records
            == [
                RecordDecision::Keep,
                RecordDecision::Drop,
                RecordDecision::Drop,
            ]
    );
}

#[test]
fn predicates_that_both_match_one_record_still_drop_it_once() {
    let predicates = predicates(&["--exclude-key", "^bad", "--exclude-producer-id", "9"]);
    let orders_0 = partition("orders", 0);
    let owned = batch(
        9,
        vec![Record {
            key: Some(Bytes::from_static(b"bad-1")),
            ..record(0)
        }],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Empty);
    check!(records == [RecordDecision::Drop]);
}

#[test]
fn non_utf8_key_bytes_never_match_and_do_not_panic() {
    let predicates = predicates(&["--exclude-key", ".*"]);
    let orders_0 = partition("orders", 0);
    let invalid_utf8: &[u8] = &[0xFF, 0xFE, 0xFD];
    let owned = batch(
        1,
        vec![Record {
            key: Some(Bytes::copy_from_slice(invalid_utf8)),
            ..record(0)
        }],
    );

    let (batch_decision, records) = decide(&predicates, &orders_0, &owned);

    check!(batch_decision == BatchDecision::Keep);
    check!(records == [RecordDecision::Keep]);
}

#[test]
fn record_offset_outside_the_declared_batch_is_an_integrity_error() {
    let predicates = predicates(&["--exclude-key", "never"]);
    let orders_0 = partition("orders", 0);
    let mut owned = batch(1, vec![record(1)]);
    owned.last_offset_delta = 0;

    let error = try_decide(&predicates, &orders_0, &owned).unwrap_err();

    check!(matches!(
        error,
        RestoreError::Records(RecordsError::RecordParse(_))
    ));
}

#[test]
fn record_offset_overflow_is_an_integrity_error() {
    let predicates = predicates(&["--exclude-key", "never"]);
    let orders_0 = partition("orders", 0);
    let mut owned = batch(1, vec![record(1)]);
    owned.base_offset = i64::MAX;

    let error = try_decide(&predicates, &orders_0, &owned).unwrap_err();

    check!(matches!(
        error,
        RestoreError::Records(RecordsError::RecordParse(_))
    ));
}

#[test]
fn record_timestamp_overflow_is_an_integrity_error() {
    let predicates = predicates(&["--to-timestamp", "0"]);
    let orders_0 = partition("orders", 0);
    let mut owned = batch(
        1,
        vec![Record {
            timestamp_delta: 1,
            ..record(0)
        }],
    );
    owned.base_timestamp = i64::MAX;
    owned.max_timestamp = i64::MAX;

    let error = try_decide(&predicates, &orders_0, &owned).unwrap_err();

    check!(matches!(
        error,
        RestoreError::Records(RecordsError::RecordParse(_))
    ));
}
