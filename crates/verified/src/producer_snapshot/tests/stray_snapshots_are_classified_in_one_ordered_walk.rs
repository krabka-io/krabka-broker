use {
    super::{producer_snapshot_stray, producer_snapshot_strays, take_classifier_visits},
    proptest::prelude::*,
};

#[yare::parameterized(
    empty_bases = { &[1, 4, 9][..], &[][..], &[true, true, false][..] },
    exact_matches = { &[1, 4, 9][..], &[1, 4, 9][..], &[false, false, false][..] },
    below_and_between = { &[1, 4, 9][..], &[2, 8, 10][..], &[true, true, true][..] },
    newest_above_all = { &[1, 4, 12][..], &[2, 8, 10][..], &[true, true, false][..] },
    match_and_newest_exception = { &[1, 4, 12][..], &[1, 8, 10][..], &[false, true, false][..] },
    duplicate_offsets = { &[1, 4, 12, 12][..], &[1, 8, 10, 10][..], &[false, true, false, false][..] },
)]
fn ordered_examples(snapshots: &[i64], bases: &[i64], expected: &[bool]) {
    assert2::assert!(producer_snapshot_strays(snapshots, bases).as_slice() == expected);
}

#[test]
fn sorted_unordered_fixtures_match_the_order_independent_oracle() {
    let mut snapshots = [12, 1, 9, 4];
    let mut bases = [10, 2, 9, 6];
    snapshots.sort_unstable();
    bases.sort_unstable();

    let expected: Vec<_> = (0..snapshots.len())
        .map(|index| producer_snapshot_stray(&snapshots, index, &bases))
        .collect();
    assert2::assert!(producer_snapshot_strays(&snapshots, &bases) == expected);
}

proptest! {
    #[test]
    fn bulk_classifier_is_equivalent_to_the_order_independent_oracle(
        mut snapshots in proptest::collection::vec(any::<i64>(), 0..128),
        mut bases in proptest::collection::vec(any::<i64>(), 0..128),
    ) {
        snapshots.sort_unstable();
        bases.sort_unstable();
        let expected: Vec<_> = (0..snapshots.len())
            .map(|index| producer_snapshot_stray(&snapshots, index, &bases))
            .collect();

        assert2::assert!(producer_snapshot_strays(&snapshots, &bases) == expected);
    }
}

#[test]
fn classification_visits_are_bounded_by_both_input_lengths() {
    for size in [100, 1_000, 10_000] {
        let offsets: Vec<_> = (0..size).collect();
        let _ = take_classifier_visits();
        assert2::assert!(
            producer_snapshot_strays(&offsets, &offsets) == vec![false; offsets.len()]
        );
        assert2::assert!(take_classifier_visits() <= offsets.len() * 2);
    }
}
