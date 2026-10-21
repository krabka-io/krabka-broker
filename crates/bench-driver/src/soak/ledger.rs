//! Sequence sets: the compact form every soak ledger keeps its records in.
//!
//! A sustained-load run acknowledges hundreds of millions of records. Holding
//! each one as its own entry would make the ledger the largest thing on the
//! load host, so each producer numbers its records `0, 1, 2, ...` and the
//! ledgers store those numbers as sorted, disjoint, half-open runs. A producer
//! that acks in order costs one run however long it runs, and a reader that
//! sees partitions interleaved costs a run per gap until the gaps close.

use serde::{Deserialize, Serialize};

/// A set of `u64` sequence numbers, held as sorted, disjoint, half-open runs.
///
/// Adjacent runs are always merged, so two sets with the same members have the
/// same runs and compare equal. The JSON form is the run list itself:
/// `[[0, 10], [12, 13]]` is `{0, ..., 9, 12}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<[u64; 2]>", into = "Vec<[u64; 2]>")]
pub struct SeqSet {
    runs: Vec<[u64; 2]>,
}

impl SeqSet {
    /// The empty set.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// The set `start..end`.
    #[must_use]
    pub fn range(start: u64, end: u64) -> Self {
        let mut set = Self::new();
        set.insert_range(start, end);
        set
    }

    /// Add `seq`, and say whether it was new.
    ///
    /// A reader uses the answer to spot a record it has already seen.
    pub fn insert(&mut self, seq: u64) -> bool { self.insert_range(seq, seq.saturating_add(1)) > 0 }

    /// Add every member of `start..end`, and return how many were new.
    pub fn insert_range(&mut self, start: u64, end: u64) -> u64 {
        if start >= end {
            return 0;
        }
        // The runs that overlap or touch `start..end`: every run from the
        // first that does not end before `start` to the last that does not
        // begin after `end`.
        let low = self.runs.partition_point(|run| run[1] < start);
        let high = self.runs.partition_point(|run| run[0] <= end);
        let already: u64 = self.runs[low..high]
            .iter()
            .map(|run| run[1].min(end).saturating_sub(run[0].max(start)))
            .sum();
        let merged = match self.runs.get(low..high) {
            Some([first, .., last]) => [first[0].min(start), last[1].max(end)],
            Some([only]) => [only[0].min(start), only[1].max(end)],
            _ => [start, end],
        };
        self.runs.splice(low..high, [merged]);
        (end - start) - already
    }

    /// Remove `seq`, and say whether it was a member.
    pub fn remove(&mut self, seq: u64) -> bool {
        let index = self.runs.partition_point(|run| run[1] <= seq);
        let Some(run) = self.runs.get(index).copied() else {
            return false;
        };
        if seq < run[0] {
            return false;
        }
        let pieces = [[run[0], seq], [seq + 1, run[1]]];
        self.runs.splice(
            index..=index,
            pieces.into_iter().filter(|piece| piece[0] < piece[1]),
        );
        true
    }

    /// Whether `seq` is a member.
    #[must_use]
    pub fn contains(&self, seq: u64) -> bool {
        let index = self.runs.partition_point(|run| run[1] <= seq);
        self.runs.get(index).is_some_and(|run| run[0] <= seq)
    }

    /// How many members the set has.
    #[must_use]
    pub fn len(&self) -> u64 { self.runs.iter().map(|run| run[1] - run[0]).sum() }

    /// Whether the set has no members.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.runs.is_empty() }

    /// The largest member.
    #[must_use]
    pub fn last(&self) -> Option<u64> { self.runs.last().map(|run| run[1] - 1) }

    /// Every member, in ascending order. Reverse it to walk down from the
    /// largest member.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = u64> + '_ {
        self.runs.iter().flat_map(|run| run[0]..run[1])
    }

    /// Every member of `self` or `other`.
    ///
    /// One merge over the two run lists, so the cost is linear in their runs.
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        let mut out = Self::new();
        let (mut left, mut right) = (self.runs.iter().peekable(), other.runs.iter().peekable());
        loop {
            let next = match (left.peek(), right.peek()) {
                (Some(a), Some(b)) if a[0] <= b[0] => left.next(),
                (_, Some(_)) => right.next(),
                (Some(_), None) => left.next(),
                (None, None) => break,
            };
            if let Some(&run) = next {
                out.push_run(run);
            }
        }
        out
    }

    /// Every member of any of `sets`.
    ///
    /// It collects every run, sorts them once and merges them, so it costs
    /// `O(R log R)` in the total run count `R`. Folding [`Self::union`] over
    /// many sets costs a merge per set instead.
    #[must_use]
    pub fn union_all<'a>(sets: impl IntoIterator<Item = &'a Self>) -> Self {
        sets.into_iter()
            .flat_map(|set| set.runs.iter().copied())
            .collect::<Vec<_>>()
            .into()
    }

    /// Every member of `self` that is not a member of `other`.
    ///
    /// One merge over the two run lists, so the cost is linear in their runs.
    #[must_use]
    pub fn difference(&self, other: &Self) -> Self {
        let mut out = Self::new();
        // `first` is the first run of `other` that ends after the current run
        // of `self` starts. It only moves forward, and each run of `other`
        // past it is visited for at most two runs of `self`: the runs of
        // `self` are disjoint, so a cut that reaches past one run is the last
        // cut that run sees.
        let mut first = 0;
        for &[start, end] in &self.runs {
            while other.runs.get(first).is_some_and(|cut| cut[1] <= start) {
                first += 1;
            }
            let mut cursor = start;
            for cut in other.runs[first..].iter().take_while(|cut| cut[0] < end) {
                if cut[0] > cursor {
                    out.push_run([cursor, cut[0]]);
                }
                cursor = cursor.max(cut[1]);
            }
            if cursor < end {
                out.push_run([cursor, end]);
            }
        }
        out
    }

    /// Every member of both `self` and `other`.
    ///
    /// One merge over the two run lists, with at most one binary search per
    /// run.
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        let mut out = Self::new();
        let (mut left, mut right) = (0, 0);
        while let (Some(a), Some(b)) = (self.runs.get(left), other.runs.get(right)) {
            let overlap = [a[0].max(b[0]), a[1].min(b[1])];
            if overlap[0] < overlap[1] {
                out.push_run(overlap);
            }
            // Drop the run that ends first: nothing after it can overlap it.
            // A side whose runs all end before the other's current run starts
            // skips them in one binary search, so a small set intersected
            // with a large one costs a search per small run, not a step per
            // large run.
            if a[1] <= b[0] {
                left += self.runs[left..].partition_point(|run| run[1] <= b[0]);
            } else if b[1] <= a[0] {
                right += other.runs[right..].partition_point(|run| run[1] <= a[0]);
            } else if a[1] <= b[1] {
                left += 1;
            } else {
                right += 1;
            }
        }
        out
    }

    /// Append `run`, which must start no earlier than the last run starts,
    /// and merge it into the last run if the two overlap or touch.
    ///
    /// Every merge above feeds runs in start order, so this keeps the runs
    /// sorted, disjoint and merged in constant time per run.
    fn push_run(&mut self, run: [u64; 2]) {
        if run[0] >= run[1] {
            return;
        }
        match self.runs.last_mut() {
            Some(last) if run[0] <= last[1] => last[1] = last[1].max(run[1]),
            _ => self.runs.push(run),
        }
    }

    /// The first `count` members, for a failure message that names examples
    /// without printing a whole ledger.
    #[must_use]
    pub fn sample(&self, count: usize) -> Vec<u64> { self.iter().take(count).collect() }
}

/// Reading a run list back canonicalizes it: overlapping, touching or unsorted
/// runs merge exactly as inserting them would.
impl From<Vec<[u64; 2]>> for SeqSet {
    fn from(mut runs: Vec<[u64; 2]>) -> Self {
        runs.sort_unstable();
        let mut set = Self::new();
        for run in runs {
            set.push_run(run);
        }
        set
    }
}

impl From<SeqSet> for Vec<[u64; 2]> {
    fn from(set: SeqSet) -> Self { set.runs }
}

impl FromIterator<u64> for SeqSet {
    fn from_iter<I: IntoIterator<Item = u64>>(iter: I) -> Self {
        let mut set = Self::new();
        for seq in iter {
            set.insert(seq);
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use {
        super::SeqSet,
        assert2::{assert, check},
        std::collections::BTreeSet,
    };

    /// The same set, one member at a time: the model the run arithmetic is
    /// checked against.
    fn members(set: &SeqSet) -> BTreeSet<u64> { set.iter().collect() }

    fn runs(list: &[[u64; 2]]) -> SeqSet { SeqSet::from(list.to_vec()) }

    #[yare::parameterized(
        ascending = { &[0, 1, 2, 3], &[[0, 4]] },
        descending = { &[3, 2, 1, 0], &[[0, 4]] },
        gap_then_fill = { &[0, 2, 1], &[[0, 3]] },
        two_runs = { &[0, 1, 5, 6], &[[0, 2], [5, 7]] },
        interleaved = { &[10, 0, 11, 1, 12], &[[0, 2], [10, 13]] },
        repeats = { &[4, 4, 4], &[[4, 5]] },
    )]
    fn inserts_merge_into_canonical_runs(inserted: &[u64], runs: &[[u64; 2]]) {
        let set: SeqSet = inserted.iter().copied().collect();
        check!(set.runs == runs);
    }

    #[test]
    fn insert_reports_whether_a_member_was_new() {
        let mut set = SeqSet::new();
        check!(set.insert(7));
        check!(!set.insert(7));
        check!(set.insert(8));
        check!(set.insert_range(0, 10) == 8);
        check!(set.insert_range(5, 5) == 0);
        check!(set.len() == 10);
    }

    #[yare::parameterized(
        middle = { 5, true, &[[0, 5], [6, 10]] },
        first = { 0, true, &[[1, 10]] },
        last = { 9, true, &[[0, 9]] },
        absent = { 12, false, &[[0, 10]] },
    )]
    fn remove_splits_a_run(seq: u64, was_member: bool, runs: &[[u64; 2]]) {
        let mut set = SeqSet::range(0, 10);
        check!(set.remove(seq) == was_member);
        check!(set.runs == runs);
        check!(!set.contains(seq));
    }

    #[yare::parameterized(
        disjoint = { SeqSet::range(0, 5), SeqSet::range(10, 15), SeqSet::range(0, 5) },
        covering = { SeqSet::range(0, 5), SeqSet::range(0, 10), SeqSet::new() },
        hole = { SeqSet::range(0, 10), [4, 5].into_iter().collect(), [0, 1, 2, 3, 6, 7, 8, 9].into_iter().collect() },
        overlapping_tail = { SeqSet::range(0, 10), SeqSet::range(8, 20), SeqSet::range(0, 8) },
    )]
    fn difference_keeps_only_the_left_side(left: SeqSet, right: SeqSet, expected: SeqSet) {
        check!(left.difference(&right) == expected);
    }

    #[test]
    fn union_and_intersection_agree_with_membership() {
        let left: SeqSet = [1, 2, 3, 10].into_iter().collect();
        let right: SeqSet = [3, 4, 10, 11].into_iter().collect();
        check!(left.union(&right) == [1, 2, 3, 4, 10, 11].into_iter().collect());
        check!(left.intersection(&right) == [3, 10].into_iter().collect());
    }

    #[test]
    fn the_json_form_is_the_run_list() {
        let set: SeqSet = [0, 1, 2, 9].into_iter().collect();
        let json = serde_json::to_string(&set).unwrap();
        assert!(json == "[[0,3],[9,10]]");
        check!(serde_json::from_str::<SeqSet>(&json).unwrap() == set);
        check!(set.last() == Some(9));
        check!(set.sample(2) == vec![0, 1]);
    }

    #[test]
    fn a_non_canonical_run_list_reads_back_canonical() {
        let set: SeqSet = serde_json::from_str("[[5,8],[0,2],[2,5],[7,9]]").unwrap();
        check!(set == SeqSet::range(0, 9));
    }

    #[yare::parameterized(
        both_empty = { &[], &[] },
        left_empty = { &[], &[[3, 8]] },
        right_empty = { &[[3, 8]], &[] },
        equal = { &[[3, 8], [10, 12]], &[[3, 8], [10, 12]] },
        disjoint = { &[[0, 5]], &[[10, 15]] },
        touching = { &[[0, 5]], &[[5, 10]] },
        touching_both_sides = { &[[0, 5], [10, 15]], &[[5, 10]] },
        overlapping = { &[[0, 8]], &[[5, 12]] },
        nested = { &[[0, 20]], &[[5, 8], [10, 12]] },
        nested_reversed = { &[[5, 8], [10, 12]], &[[0, 20]] },
        cut_spans_two_runs = { &[[0, 4], [6, 10], [12, 14]], &[[2, 13]] },
        many_runs_interleaved = { &[[0, 2], [4, 6], [8, 10], [12, 14], [16, 18]], &[[1, 5], [7, 8], [9, 13], [17, 30]] },
        single_members = { &[[0, 1], [2, 3], [4, 5]], &[[1, 2], [3, 4], [5, 6]] },
        far_apart = { &[[0, 1], [u64::MAX - 2, u64::MAX]], &[[1, 2], [u64::MAX - 1, u64::MAX]] },
    )]
    fn set_operations_match_a_member_by_member_model(left: &[[u64; 2]], right: &[[u64; 2]]) {
        let (left, right) = (runs(left), runs(right));
        let (model_left, model_right) = (members(&left), members(&right));
        for (name, got, expected) in [
            ("union", left.union(&right), &model_left | &model_right),
            (
                "difference",
                left.difference(&right),
                &model_left - &model_right,
            ),
            (
                "intersection",
                left.intersection(&right),
                &model_left & &model_right,
            ),
            (
                "union_all",
                SeqSet::union_all([&left, &right]),
                &model_left | &model_right,
            ),
        ] {
            check!(members(&got) == expected, "{name}");
            // Equal members and equal runs: the result is canonical.
            check!(
                got == expected.iter().copied().collect::<SeqSet>(),
                "{name}"
            );
        }
    }

    /// Two sets of 200 000 runs each, every run of one between two runs of the
    /// other. A quadratic union, difference or intersection would take
    /// minutes here; the linear merges take milliseconds.
    #[test]
    fn set_operations_stay_linear_on_many_runs() {
        const RUNS: u64 = 200_000;
        let left = SeqSet::from((0..RUNS).map(|n| [4 * n, 4 * n + 2]).collect::<Vec<_>>());
        let right = SeqSet::from(
            (0..RUNS)
                .map(|n| [4 * n + 1, 4 * n + 3])
                .collect::<Vec<_>>(),
        );
        check!(
            left.union(&right)
                == SeqSet::from((0..RUNS).map(|n| [4 * n, 4 * n + 3]).collect::<Vec<_>>())
        );
        check!(left.union(&right).len() == 3 * RUNS);
        check!(left.difference(&right).len() == RUNS);
        check!(left.intersection(&right).len() == RUNS);
        let many: Vec<SeqSet> = (0..RUNS).map(|n| SeqSet::range(2 * n, 2 * n + 1)).collect();
        check!(SeqSet::union_all(&many).len() == RUNS);
    }

    #[test]
    fn iter_walks_down_from_the_largest_member_in_reverse() {
        let set = runs(&[[0, 2], [5, 7]]);
        check!(set.iter().rev().collect::<Vec<_>>() == [6, 5, 1, 0]);
    }
}
