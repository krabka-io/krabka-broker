use super::*;

fn data(key: u8, value: Option<u8>, horizon: Option<i64>) -> Entry {
    Entry {
        key: Some(key),
        kind: EntryKind::Data { value },
        horizon,
    }
}

fn marker(producer_id: u8, horizon: Option<i64>) -> Entry {
    Entry {
        key: None,
        kind: EntryKind::Marker {
            producer_id,
            commit: true,
        },
        horizon,
    }
}

/// Each row is a hand-written pass that breaks exactly the rules listed,
/// or a lawful pass that breaks none. The rules are the specification, so
/// they are pinned against scenarios rather than against the model's own
/// pass.
#[test]
fn each_rule_rejects_the_pass_that_breaks_it() {
    let clock = 4;
    let stamp = Some(clock + DELETE_RETENTION_MS);
    for (name, input, output, want) in [
        (
            "lawful: superseded value dropped, tombstone stamped, marker kept",
            vec![data(0, Some(0), None), data(0, None, None), marker(1, None)],
            vec![data(0, None, stamp), marker(1, stamp)],
            vec![],
        ),
        (
            "lawful: aged tombstone and aged marker without data leave",
            vec![data(0, None, Some(clock)), marker(1, Some(clock - 1))],
            vec![],
            vec![],
        ),
        (
            "lawful: an aged marker leaves although its producer has newer live data",
            vec![marker(0, Some(clock - 1)), data(0, Some(0), None)],
            vec![data(0, Some(0), None)],
            vec![],
        ),
        (
            "legacy dedup: older marker dropped against the newer one",
            vec![
                data(0, Some(0), None),
                marker(0, None),
                data(1, Some(0), None),
                marker(1, None),
            ],
            vec![
                data(0, Some(0), None),
                data(1, Some(0), None),
                marker(1, None),
            ],
            vec![
                Invariant::ControlNotDeduped,
                Invariant::MarkerDataPrecedence,
            ],
        ),
        (
            "aged marker dropped while data of its transaction is in front of it",
            vec![data(0, Some(0), None), marker(0, Some(clock))],
            vec![data(0, Some(0), None)],
            vec![Invariant::MarkerDataPrecedence],
        ),
        (
            "unexpired newest tombstone dropped",
            vec![data(0, None, Some(clock + 1))],
            vec![],
            vec![Invariant::TombstoneAging],
        ),
        (
            "aged tombstone kept",
            vec![data(0, None, Some(clock))],
            vec![data(0, None, Some(clock))],
            vec![Invariant::TombstoneAging],
        ),
        (
            "existing horizon re-stamped",
            vec![marker(1, Some(clock + 1))],
            vec![marker(1, stamp)],
            vec![Invariant::IdempotentStamp],
        ),
        (
            "newest live value dropped",
            vec![data(0, Some(0), None)],
            vec![],
            vec![Invariant::NoDataLoss],
        ),
        (
            "tombstone dropped ahead of the value it superseded",
            vec![data(0, Some(0), None), data(0, None, Some(clock))],
            vec![data(0, Some(0), None)],
            vec![Invariant::NoDataLoss],
        ),
    ] {
        assert2::assert!(
            Invariant::violations(&input, &output, clock) == want,
            "case {name}"
        );
    }
}
