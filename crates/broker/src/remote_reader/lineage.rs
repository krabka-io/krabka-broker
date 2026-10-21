//! Whether a remote segment belongs to the partition's leader-epoch lineage.
//!
//! An unclean leader election can leave the tier holding segments that a
//! deposed leader copied for offsets the current lineage assigns to a
//! different history. Kafka's `RemoteLogManager.findOffsetByTimestamp` passes
//! every candidate segment through `isRemoteSegmentWithinLeaderEpochs` before
//! it reads it, so a by-timestamp `ListOffsets` never answers with an offset
//! from a divergent copy. This module is that check and the epoch map it
//! reads.

use {
    krabka_ids::LeaderEpoch,
    krabka_log::Log,
    krabka_remote_storage::{LogOffset, RemoteLogSegmentMetadata},
    std::{collections::BTreeMap, ops::Bound},
};

/// Kafka's `RemoteLogManager.buildFilteredLeaderEpochMap`: the epochs of
/// `leader_epochs` that own at least one offset.
///
/// An epoch whose start offset equals the next epoch's start offset wrote
/// nothing, so it is dropped. The last epoch is always kept.
pub(crate) fn filtered_leader_epochs(
    leader_epochs: &BTreeMap<LeaderEpoch, LogOffset>,
) -> BTreeMap<LeaderEpoch, LogOffset> {
    let mut filtered = BTreeMap::new();
    let mut previous: Option<(LeaderEpoch, LogOffset)> = None;
    for (&epoch, &start) in leader_epochs {
        if let Some((previous_epoch, previous_start)) = previous
            && previous_start != start
        {
            filtered.insert(previous_epoch, previous_start);
        }
        previous = Some((epoch, start));
    }
    if let Some((epoch, start)) = previous {
        filtered.insert(epoch, start);
    }
    filtered
}

/// The first entry of `map` strictly above `epoch`: Java's
/// `NavigableMap.higherEntry`.
fn higher_entry(
    map: &BTreeMap<LeaderEpoch, LogOffset>,
    epoch: LeaderEpoch,
) -> Option<(&LeaderEpoch, &LogOffset)> {
    map.range((Bound::Excluded(epoch), Bound::Unbounded)).next()
}

/// Kafka's `RemoteLogManager.isRemoteSegmentWithinLeaderEpochs`, line for
/// line.
///
/// `segment_epochs` and `segment_end` are the segment's
/// `segmentLeaderEpochs` and `endOffset`. `leader_epochs` is the partition's
/// lineage, already filtered by [`filtered_leader_epochs`] as Kafka's caller
/// filters it. The segment belongs to the lineage when:
/// - its last epoch lies within the lineage's epochs;
/// - from the first of its epochs the lineage still holds, every epoch is one
///   the lineage holds, with the same next epoch and start offset;
/// - its first such epoch does not start before the lineage's start for that
///   epoch, unless that epoch is the lineage's first;
/// - it ends before the lineage's next epoch begins, and before
///   `log_end_offset`.
///
/// Kafka throws on an empty segment or lineage map; this answers `false`.
pub(crate) fn segment_within_leader_epochs(
    segment_epochs: &BTreeMap<LeaderEpoch, LogOffset>,
    segment_end: LogOffset,
    log_end_offset: LogOffset,
    leader_epochs: &BTreeMap<LeaderEpoch, LogOffset>,
) -> bool {
    let segment = filtered_leader_epochs(segment_epochs);
    let (Some((&segment_last, _)), Some((&leader_first, _)), Some((&leader_last, _))) = (
        segment.last_key_value(),
        leader_epochs.first_key_value(),
        leader_epochs.last_key_value(),
    ) else {
        return false;
    };
    if segment_last < leader_first || segment_last > leader_last {
        return false;
    }
    // Overlapping copies: a segment that starts in an epoch the lineage has
    // since cut off is judged from the first epoch the lineage still holds.
    let Some((&segment_first, _)) = segment.range(leader_first..).next() else {
        return false;
    };
    for (&epoch, &offset) in segment.range(segment_first..) {
        let Some(&leader_start) = leader_epochs.get(&epoch) else {
            return false;
        };
        if epoch == segment_first
            && leader_epochs.range(..epoch).next_back().is_some()
            && offset < leader_start
        {
            return false;
        }
        if epoch == segment_last
            && let Some((_, &next_start)) = higher_entry(leader_epochs, epoch)
            && segment_end > next_start.saturating_sub(1)
        {
            return false;
        }
        if epoch != segment_last
            && higher_entry(leader_epochs, epoch) != higher_entry(&segment, epoch)
        {
            return false;
        }
    }
    segment_end < log_end_offset
}

/// The partition's leader-epoch lineage as a remote timestamp lookup reads
/// it: the epoch checkpoint, filtered as Kafka filters it, and the log end
/// offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EpochLineage {
    epochs: BTreeMap<LeaderEpoch, LogOffset>,
    log_end_offset: LogOffset,
}

impl EpochLineage {
    /// The lineage of `log` as it stands now.
    pub(crate) fn of(log: &Log) -> Self {
        let checkpoint: BTreeMap<LeaderEpoch, LogOffset> = log
            .epoch_checkpoint()
            .entries()
            .iter()
            .map(|entry| (entry.epoch, entry.start_offset.0))
            .collect();
        Self {
            epochs: filtered_leader_epochs(&checkpoint),
            log_end_offset: log.log_end_offset().0,
        }
    }

    /// Whether a remote timestamp lookup may read `segment`.
    ///
    /// This is [`segment_within_leader_epochs`], except where the lineage says
    /// nothing about the segment, which happens only in states Kafka never
    /// reaches and the remote `Fetch` already serves:
    /// - an empty checkpoint, where `Fetch` reads under the current leader
    ///   epoch;
    /// - a segment that ends below the checkpoint's first entry, which a
    ///   floor established too high cut out of the checkpoint and a follower
    ///   later took back from its leader. `Fetch` asks the archive which epoch
    ///   owned such an offset (`RemoteReader::archived_epoch_for_offset`).
    ///
    /// Kafka cuts its cache only to the log start, so there the second case
    /// is a segment below the log start, which the lookup's `minimum` offset
    /// already excludes.
    pub(crate) fn admits(&self, segment: &RemoteLogSegmentMetadata) -> bool {
        let Some((_, &first_start)) = self.epochs.first_key_value() else {
            return true;
        };
        segment.end_offset() < first_start
            || segment_within_leader_epochs(
                segment.segment_leader_epochs(),
                segment.end_offset(),
                self.log_end_offset,
                &self.epochs,
            )
    }

    /// A lineage that admits every segment: an empty checkpoint.
    #[cfg(test)]
    pub(crate) fn unknown() -> Self {
        Self {
            epochs: BTreeMap::new(),
            log_end_offset: LogOffset::MAX,
        }
    }

    /// A lineage of `epochs` (epoch, start offset) and `log_end_offset`.
    #[cfg(test)]
    pub(crate) fn from_entries(epochs: &[(i32, LogOffset)], log_end_offset: LogOffset) -> Self {
        Self {
            epochs: filtered_leader_epochs(&epoch_map(epochs)),
            log_end_offset,
        }
    }
}

/// An epoch map from `(epoch, start offset)` pairs.
#[cfg(test)]
fn epoch_map(entries: &[(i32, LogOffset)]) -> BTreeMap<LeaderEpoch, LogOffset> {
    entries
        .iter()
        .map(|&(epoch, start)| (LeaderEpoch(epoch), start))
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// Kafka's `RemoteLogManagerTest.testBuildFilteredLeaderEpochMap`.
    #[test]
    fn the_filtered_map_drops_epochs_that_wrote_nothing() {
        let cases = [
            (
                epoch_map(&[
                    (0, 0),
                    (1, 0),
                    (2, 0),
                    (3, 30),
                    (4, 40),
                    (5, 60),
                    (6, 60),
                    (7, 70),
                    (8, 70),
                ]),
                epoch_map(&[(2, 0), (3, 30), (4, 40), (6, 60), (8, 70)]),
            ),
            (epoch_map(&[(0, 0), (1, 0), (2, 0)]), epoch_map(&[(2, 0)])),
        ];
        for (leader_epochs, want) in cases {
            check!(filtered_leader_epochs(&leader_epochs) == want);
        }
    }

    /// One `isRemoteSegmentWithinLeaderEpochs` case: the segment's epochs and
    /// end offset, the log end offset, and the answer.
    struct Case {
        label: &'static str,
        segment_epochs: &'static [(i32, LogOffset)],
        segment_end: LogOffset,
        log_end_offset: LogOffset,
        want: bool,
    }

    fn run(leader_epochs: &[(i32, LogOffset)], cases: &[Case]) {
        let leader_epochs = epoch_map(leader_epochs);
        let actual: Vec<(&str, bool)> = cases
            .iter()
            .map(|case| {
                (
                    case.label,
                    segment_within_leader_epochs(
                        &epoch_map(case.segment_epochs),
                        case.segment_end,
                        case.log_end_offset,
                        &leader_epochs,
                    ),
                )
            })
            .collect();
        let expected: Vec<(&str, bool)> =
            cases.iter().map(|case| (case.label, case.want)).collect();
        check!(actual == expected);
    }

    /// Kafka's `RemoteLogManagerTest.testRemoteSegmentWithinLeaderEpochs`.
    #[test]
    fn a_segment_is_within_the_lineage_as_kafka_decides() {
        const LEADER: &[(i32, LogOffset)] =
            &[(0, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (7, 70)];
        run(LEADER, &[
            Case {
                label: "several epochs within the lineage",
                segment_epochs: &[(1, 15), (2, 20), (3, 30)],
                segment_end: 35,
                log_end_offset: 90,
                want: true,
            },
            Case {
                label: "one epoch within the lineage",
                segment_epochs: &[(1, 15)],
                segment_end: 19,
                log_end_offset: 90,
                want: true,
            },
            Case {
                label: "starts at the lineage's first epoch start",
                segment_epochs: &[(0, 0)],
                segment_end: 5,
                log_end_offset: 90,
                want: true,
            },
            Case {
                label: "starts at the lineage's last epoch start",
                segment_epochs: &[(7, 70)],
                segment_end: 75,
                log_end_offset: 90,
                want: true,
            },
            Case {
                label: "ends at the last offset of its last epoch",
                segment_epochs: &[(1, 15), (2, 20)],
                segment_end: 29,
                log_end_offset: 90,
                want: true,
            },
            Case {
                label: "an epoch the lineage does not hold",
                segment_epochs: &[(5, 55), (6, 60), (7, 70)],
                segment_end: 85,
                log_end_offset: 90,
                want: false,
            },
            Case {
                label: "skips an epoch the lineage holds",
                segment_epochs: &[(1, 15), (2, 20), (4, 40)],
                segment_end: 45,
                log_end_offset: 90,
                want: false,
            },
            Case {
                label: "ends past the log end offset",
                segment_epochs: LEADER,
                segment_end: 95,
                log_end_offset: 90,
                want: false,
            },
            Case {
                label: "ends at the log end offset",
                segment_epochs: LEADER,
                segment_end: 90,
                log_end_offset: 90,
                want: false,
            },
            Case {
                label: "starts before its first epoch's start",
                segment_epochs: &[(1, 5), (2, 20)],
                segment_end: 25,
                log_end_offset: 90,
                want: false,
            },
            Case {
                label: "ends past its last epoch's end",
                segment_epochs: &[(1, 15), (2, 20)],
                segment_end: 35,
                log_end_offset: 90,
                want: false,
            },
        ]);
    }

    /// Kafka's
    /// `RemoteLogManagerTest.testRemoteSegmentWithinLeaderEpochsForOverlappingSegments`.
    #[test]
    fn an_overlapping_segment_is_judged_from_the_first_epoch_the_lineage_holds() {
        run(&[(7, 51), (9, 100)], &[
            Case {
                label: "starts in an epoch the lineage cut off, then follows it",
                segment_epochs: &[(5, 14), (7, 15), (9, 100)],
                segment_end: 150,
                log_end_offset: 210,
                want: true,
            },
            Case {
                label: "every epoch is below the lineage",
                segment_epochs: &[(2, 5), (3, 6)],
                segment_end: 7,
                log_end_offset: 210,
                want: false,
            },
            Case {
                label: "an epoch past the lineage's last",
                segment_epochs: &[(7, 15), (9, 100), (10, 200)],
                segment_end: 250,
                log_end_offset: 210,
                want: false,
            },
            Case {
                label: "an epoch the lineage does not hold",
                segment_epochs: &[(8, 75)],
                segment_end: 100,
                log_end_offset: 210,
                want: false,
            },
            Case {
                label: "a next epoch that starts elsewhere",
                segment_epochs: &[(7, 15), (9, 101)],
                segment_end: 150,
                log_end_offset: 210,
                want: false,
            },
            Case {
                label: "starts before its epoch's start",
                segment_epochs: &[(9, 99)],
                segment_end: 150,
                log_end_offset: 210,
                want: false,
            },
        ]);
    }

    /// The two states where krabka's lineage says nothing about a segment
    /// that `Fetch` serves, beside the plain port.
    #[test]
    fn a_lookup_reads_what_the_lineage_cannot_judge() {
        let segment = |start, end, epochs: &[(i32, LogOffset)]| {
            RemoteLogSegmentMetadata::new(
                krabka_remote_storage::RemoteLogSegmentId::new(
                    krabka_remote_storage::TopicIdPartition::new(uuid::Uuid::nil(), "t", 0),
                    uuid::Uuid::nil(),
                ),
                start,
                end,
                0,
                1,
                0,
                krabka_remote_storage::RemoteLogSegmentDetails::new(
                    1,
                    krabka_remote_storage::RemoteLogSegmentState::CopySegmentFinished,
                    epoch_map(epochs),
                ),
            )
            .unwrap()
        };
        let cut = EpochLineage::from_entries(&[(7, 51), (9, 100)], 210);
        let cases = [
            (
                "an empty checkpoint judges nothing",
                EpochLineage::unknown(),
                segment(0, 9, &[(3, 0)]),
                true,
            ),
            (
                "a segment below a cut checkpoint is read, as Fetch reads it",
                cut.clone(),
                segment(2, 7, &[(2, 5), (3, 6)]),
                true,
            ),
            (
                "a segment that reaches the checkpoint is judged by it",
                cut.clone(),
                segment(15, 150, &[(7, 15), (9, 101)]),
                false,
            ),
            (
                "and admitted when it follows it",
                cut,
                segment(14, 150, &[(5, 14), (7, 15), (9, 100)]),
                true,
            ),
        ];
        for (label, lineage, segment, want) in cases {
            check!(lineage.admits(&segment) == want, "{label}");
        }
    }
}
