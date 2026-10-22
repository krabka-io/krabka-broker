use {super::*, crate::list_offsets::ListOffsetsSelectionDecision, std::collections::BTreeMap};

/// One tier's segments: each its largest timestamp and its records by offset.
type Segments = Vec<(i64, BTreeMap<i64, i64>)>;

/// Kafka's answer for one tier, computed directly: the first matching record of
/// the first segment whose largest timestamp reaches the target.
fn tier_oracle(segments: &Segments, target: i64, minimum: i64) -> Option<(i64, i64)> {
    let (_, records) = segments.iter().find(|(largest, _)| *largest >= target)?;
    records
        .iter()
        .find(|(offset, timestamp)| **offset >= minimum && **timestamp >= target)
        .map(|(offset, timestamp)| (*offset, *timestamp))
}

fn decision(found: Option<(i64, i64)>, bound: i64, epoch: i32) -> ListOffsetsSelectionDecision {
    found.filter(|(offset, _)| *offset < bound).map_or(
        ListOffsetsSelectionDecision::Unknown,
        |(offset, timestamp)| ListOffsetsSelectionDecision::Resolved {
            offset,
            timestamp,
            leader_epoch: epoch,
        },
    )
}

/// Runs the composition over two tiers given as [`Segments`].
fn lookup(
    remote: &Segments,
    local: &Segments,
    request: (i64, i64, i64), // target timestamp, logical floor, exclusive bound
    epoch: i32,
) -> ListOffsetsSelectionDecision {
    let (target, minimum, bound) = request;
    let records = |segments: &Segments| -> Vec<Vec<(i64, i64)>> {
        segments
            .iter()
            .map(|(_, records)| records.iter().map(|(o, t)| (*o, *t)).collect())
            .collect()
    };
    let largest = |segments: &Segments| -> Vec<i64> { segments.iter().map(|(l, _)| *l).collect() };
    let (remote_records, local_records) = (records(remote), records(local));
    let remote_slices: Vec<&[(i64, i64)]> = remote_records.iter().map(Vec::as_slice).collect();
    let local_slices: Vec<&[(i64, i64)]> = local_records.iter().map(Vec::as_slice).collect();
    tiered_timestamp_lookup_answers_first_candidate(
        (&remote_slices, &largest(remote)),
        (&local_slices, &largest(local)),
        target,
        minimum,
        bound,
        epoch,
    )
}

fn segments_strategy() -> impl Strategy<Value = Segments> {
    proptest::collection::vec(
        (
            any::<i64>(),
            proptest::collection::btree_map(0_i64..=i64::MAX, any::<i64>(), 0..6),
        ),
        0..5,
    )
}

proptest! {
    #[test]
    fn tier_union_matches_a_first_candidate_oracle(
        remote in segments_strategy(),
        local in segments_strategy(),
        target in 0_i64..=i64::MAX, minimum in 0_i64..=i64::MAX, bound in 0_i64..=i64::MAX, epoch in -1_i32..=i32::MAX,
    ) {
        let found = match (tier_oracle(&remote, target, minimum), tier_oracle(&local, target, minimum)) {
            (Some(r), Some(l)) => Some(if l.0 < r.0 { l } else { r }),
            (r, l) => r.or(l),
        };
        prop_assert_eq!(lookup(&remote, &local, (target, minimum, bound), epoch), decision(found, bound, epoch));
    }
}

#[yare::parameterized(
    earlier_local_match = { (7, 7_000), (2, 1_600), 6, Some((2, 1_600)) },
    remote_match_is_earlier = { (2, 1_600), (7, 7_000), 6, Some((2, 1_600)) },
    exclusive_bound = { (7, 7_000), (2, 1_600), 2, None },
    same_offset = { (2, 1_600), (2, 1_600), 6, Some((2, 1_600)) },
    maximum_offset_is_exclusive = {
        (i64::MAX, i64::MAX),
        (i64::MAX - 1, i64::MAX),
        i64::MAX,
        Some((i64::MAX - 1, i64::MAX)),
    },
)]
fn a_later_remote_hit_cannot_hide_a_visible_local_match(
    remote: (i64, i64),
    local: (i64, i64),
    bound: i64,
    expected: Option<(i64, i64)>,
) {
    let segment = |(offset, timestamp): (i64, i64)| {
        Vec::from([(i64::MAX, BTreeMap::from([(offset, timestamp)]))])
    };
    assert2::assert!(
        lookup(&segment(remote), &segment(local), (1_500, 0, bound), 0)
            == decision(expected, bound, 0)
    );
}

/// #1388: each tier answers from its first segment whose largest timestamp
/// reaches the target, as Kafka 4.3.1 does, and a later segment's match
/// never stands in for a first candidate with none above the floor.
#[test]
fn each_tier_answers_from_its_first_candidate_segment_only() {
    let first = (500, BTreeMap::from([(0, 100), (1, 500), (2, 150)]));
    let later = (600, BTreeMap::from([(3, 600)]));
    let below_target = (300, BTreeMap::from([(0, 100), (1, 300)]));
    let cases = [
        (
            "the first candidate holds the match",
            Vec::from([first.clone(), later.clone()]),
            0,
            Some((1, 500)),
        ),
        (
            "its only match is below the floor",
            Vec::from([first.clone(), later.clone()]),
            2,
            None,
        ),
        (
            "a segment below the target is no candidate",
            Vec::from([below_target, later.clone()]),
            0,
            Some((3, 600)),
        ),
        (
            "a largest timestamp no record reaches still makes the candidate",
            Vec::from([(400, BTreeMap::from([(0, 100)])), later]),
            0,
            None,
        ),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, segments, minimum, want) in cases {
        actual.push((name, lookup(&segments, &Vec::new(), (400, minimum, 10), 0)));
        expected.push((name, decision(want, 10, 0)));
        actual.push((name, lookup(&Vec::new(), &segments, (400, minimum, 10), 0)));
        expected.push((name, decision(want, 10, 0)));
    }
    assert2::assert!(actual == expected);
}

#[test]
fn empty_tiered_lookup_is_unknown() {
    assert2::assert!(
        lookup(
            &Vec::from([(7_000, BTreeMap::from([(7, 7_000)]))]),
            &Vec::from([(2_000, BTreeMap::from([(2, 1_600), (4, 2_000)]))]),
            (1_500, 4, 8),
            0
        ) == ListOffsetsSelectionDecision::Resolved {
            offset: 4,
            timestamp: 2_000,
            leader_epoch: 0
        }
    );
    assert2::assert!(
        lookup(&Vec::new(), &Vec::new(), (0, 0, 0), -1) == ListOffsetsSelectionDecision::Unknown
    );
}
