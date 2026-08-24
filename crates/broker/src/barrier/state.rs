//! The in-memory state of one barrier group, and the pure decisions over it.
//!
//! The coordinator holds one [`GroupEntry`] per group behind a mutex. Recovery
//! folds the `__barrier_state` records of a partition into these entries with
//! [`apply_record`], and an injection turns its frozen target set and its
//! collected offsets into a cut with [`build_cut`].
//!
//! Every function here is pure, so a unit test drives it without a log, a
//! metadata image, or a partition.

use std::collections::BTreeMap;

use crabka_ids::PartitionIndex;
use crabka_log::Offset;
use crabka_units::{Time, convert::TimeExt as _};

use crate::barrier::persistence::{
    CutStatus, CutValue, GroupValue, InjectionStartValue, MissingPartition, PartitionOffset,
    TopicOffsets, TopicTarget,
};

/// The epoch of a group that has never injected.
///
/// The first injection allocates epoch 1.
pub(crate) const NO_EPOCH_YET: i64 = 0;

/// One partition of a frozen target set.
///
/// The type is ordered, so a placement map holds the partitions of a cut in a
/// stable order and the cut record is byte-identical across two runs of the
/// same injection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TargetPartition {
    pub(crate) topic: String,
    pub(crate) partition: PartitionIndex,
}

/// The definition a caller supplies for a barrier group.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`Time`] is backed by a
/// float.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupSpec {
    /// The topics the group cuts across.
    pub(crate) topics: Vec<String>,
    /// How often the coordinator injects without a trigger request. `None`
    /// turns periodic injection off.
    pub(crate) interval: Option<Time>,
    /// How many cuts the group keeps.
    pub(crate) retained_cuts: i32,
}

/// The injection-start record of an epoch that carries no cut record yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingInjection {
    pub(crate) epoch: i64,
    pub(crate) start: InjectionStartValue,
}

/// The live state of one barrier group.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`GroupValue`] carries a
/// [`Time`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupEntry {
    /// The last definition that a group record carried.
    pub(crate) definition: GroupValue,
    /// The cuts the group retains, keyed by epoch and ordered by it.
    pub(crate) cuts: BTreeMap<i64, CutValue>,
    /// The injection that started and published no cut.
    pub(crate) pending: Option<PendingInjection>,
    /// When the scheduler should inject next, in milliseconds since the Unix
    /// epoch. It is `None` for a group that injects only on demand.
    pub(crate) next_due_ms: Option<i64>,
}

impl Default for GroupEntry {
    fn default() -> Self {
        Self {
            definition: GroupValue {
                topics: Vec::new(),
                interval: None,
                retained_cuts: 0,
                last_epoch: NO_EPOCH_YET,
            },
            cuts: BTreeMap::new(),
            pending: None,
            next_due_ms: None,
        }
    }
}

impl GroupEntry {
    /// The entry of a group the caller just defined.
    ///
    /// The coordinator folds an entry out of the state topic instead, so only
    /// the tests here build one straight from a spec.
    #[cfg(test)]
    pub(crate) fn from_spec(spec: GroupSpec, last_epoch: i64) -> Self {
        Self {
            definition: GroupValue {
                topics: spec.topics,
                interval: spec.interval,
                retained_cuts: spec.retained_cuts,
                last_epoch,
            },
            cuts: BTreeMap::new(),
            pending: None,
            next_due_ms: None,
        }
    }

    /// The highest epoch this group has allocated.
    ///
    /// The group record can carry an older value than the log holds, because a
    /// coordinator writes the injection-start record before it rewrites the
    /// group record. The maximum over all three sources is what makes an epoch
    /// impossible to reuse.
    pub(crate) fn last_epoch(&self) -> i64 {
        let from_pending = self.pending.as_ref().map_or(NO_EPOCH_YET, |p| p.epoch);
        let from_cuts = self.cuts.keys().copied().max().unwrap_or(NO_EPOCH_YET);
        self.definition.last_epoch.max(from_pending).max(from_cuts)
    }

    /// Whether a group record ever defined this entry.
    ///
    /// A cut record of a retained epoch can sit before the newest group record
    /// in the log, so the fold creates an entry for it. A group needs at least
    /// one topic, so an entry with no topic saw no group record.
    pub(crate) fn is_defined(&self) -> bool {
        !self.definition.topics.is_empty()
    }
}

/// One decoded `__barrier_state` record. A `None` value is a tombstone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StateRecord {
    /// A group definition. A tombstone deletes the group.
    Group {
        group: String,
        value: Option<GroupValue>,
    },
    /// The frozen target set of one epoch. A tombstone drops the record of an
    /// epoch that left the retention window.
    InjectionStart {
        group: String,
        epoch: i64,
        value: Option<InjectionStartValue>,
    },
    /// The published offsets of one epoch. A tombstone drops the cut of an
    /// epoch that left the retention window.
    Cut {
        group: String,
        epoch: i64,
        value: Option<CutValue>,
    },
}

/// Fold one replayed record into the state map.
///
/// A group tombstone removes the whole entry. An injection-start tombstone or
/// a cut tombstone removes only what it names.
pub(crate) fn apply_record(state: &mut BTreeMap<String, GroupEntry>, record: StateRecord) {
    match record {
        StateRecord::Group { group, value } => match value {
            Some(definition) => state.entry(group).or_default().definition = definition,
            None => {
                state.remove(&group);
            }
        },
        StateRecord::InjectionStart {
            group,
            epoch,
            value,
        } => {
            let entry = state.entry(group).or_default();
            match value {
                Some(start) => entry.pending = Some(PendingInjection { epoch, start }),
                None => {
                    if entry.pending.as_ref().is_some_and(|p| p.epoch == epoch) {
                        entry.pending = None;
                    }
                }
            }
        }
        StateRecord::Cut {
            group,
            epoch,
            value,
        } => {
            let entry = state.entry(group).or_default();
            match value {
                Some(cut) => {
                    entry.cuts.insert(epoch, cut);
                    // The cut of an epoch retires its injection-start record.
                    if entry.pending.as_ref().is_some_and(|p| p.epoch == epoch) {
                        entry.pending = None;
                    }
                }
                None => {
                    entry.cuts.remove(&epoch);
                }
            }
        }
    }
}

/// The next epoch of a group.
pub(crate) const fn next_epoch(last_epoch: i64) -> i64 {
    last_epoch + 1
}

/// Expand a frozen target set into the partitions the injection marks.
///
/// A topic with a partition count of zero or below contributes nothing.
pub(crate) fn expand_targets(targets: &[TopicTarget]) -> Vec<TargetPartition> {
    let mut out = Vec::new();
    for target in targets {
        for partition in 0..target.partition_count.max(0) {
            out.push(TargetPartition {
                topic: target.topic.clone(),
                partition: PartitionIndex(partition),
            });
        }
    }
    out
}

/// Build the cut record of one injection.
///
/// `placed` holds the offset that each marker took. A target partition that is
/// absent from `placed` carries no marker, so the cut names it in `missing`
/// and its status is [`CutStatus::Partial`]. The epoch is consumed either way.
pub(crate) fn build_cut(
    triggered_at: i64,
    completed_at: i64,
    targets: &[TopicTarget],
    placed: &BTreeMap<TargetPartition, Offset>,
) -> CutValue {
    let mut topics = Vec::with_capacity(targets.len());
    let mut missing = Vec::new();

    for target in targets {
        let mut partitions = Vec::new();
        for partition in 0..target.partition_count.max(0) {
            let key = TargetPartition {
                topic: target.topic.clone(),
                partition: PartitionIndex(partition),
            };
            match placed.get(&key) {
                Some(offset) => partitions.push(PartitionOffset {
                    partition: key.partition,
                    offset: *offset,
                }),
                None => missing.push(MissingPartition {
                    topic: key.topic,
                    partition: key.partition,
                }),
            }
        }
        topics.push(TopicOffsets {
            topic: target.topic.clone(),
            partitions,
        });
    }

    let status = if missing.is_empty() {
        CutStatus::Complete
    } else {
        CutStatus::Partial
    };
    CutValue {
        triggered_at,
        completed_at,
        status,
        topics,
        missing,
    }
}

/// Set when the scheduler should inject next.
///
/// A group with no interval injects only on demand, so it gets no due time.
pub(crate) fn schedule_next(entry: &mut GroupEntry, now_ms: i64) {
    entry.next_due_ms = entry
        .definition
        .interval
        .map(|interval| now_ms.saturating_add(interval.millis_i64().max(0)));
}

/// Whether the scheduler should inject this group now.
pub(crate) fn is_due(entry: &GroupEntry, now_ms: i64) -> bool {
    entry.next_due_ms.is_some_and(|due| now_ms >= due)
}

/// The epochs that leave the retention window when `published` is published.
///
/// The group keeps its last `retained_cuts` cuts, so every held epoch at or
/// below `published - retained_cuts` falls off. `held` is the set of epochs
/// the group still carries, and it does not hold `published` yet. A group edit
/// that reduced `retained_cuts` drops more than one epoch at once. A
/// `retained_cuts` that is not positive drops nothing, and
/// [`crate::barrier::coordinator::validate_spec`] rejects such a value.
pub(crate) fn expired_cut_epochs(published: i64, retained_cuts: i32, held: &[i64]) -> Vec<i64> {
    if retained_cuts <= 0 {
        return Vec::new();
    }
    let cutoff = published - i64::from(retained_cuts);
    held.iter()
        .copied()
        .filter(|epoch| *epoch <= cutoff)
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::millis;

    use super::*;

    fn target(topic: &str, count: i32) -> TopicTarget {
        TopicTarget {
            topic: topic.to_owned(),
            partition_count: count,
        }
    }

    fn at(topic: &str, partition: i32) -> TargetPartition {
        TargetPartition {
            topic: topic.to_owned(),
            partition: PartitionIndex(partition),
        }
    }

    fn group_value(last_epoch: i64) -> GroupValue {
        GroupValue {
            topics: vec!["orders".to_owned()],
            interval: Some(millis(60_000)),
            retained_cuts: 4,
            last_epoch,
        }
    }

    fn cut_value(status: CutStatus) -> CutValue {
        CutValue {
            triggered_at: 10,
            completed_at: 20,
            status,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: vec![PartitionOffset {
                    partition: PartitionIndex(0),
                    offset: Offset(7),
                }],
            }],
            missing: Vec::new(),
        }
    }

    fn start_value(coordinator_epoch: i32) -> InjectionStartValue {
        InjectionStartValue {
            coordinator_epoch,
            triggered_at: 10,
            targets: vec![target("orders", 1)],
        }
    }

    #[test]
    fn a_target_set_expands_to_one_entry_per_partition() {
        let expanded = expand_targets(&[target("orders", 3), target("payments", 1)]);
        assert!(
            expanded
                == vec![
                    at("orders", 0),
                    at("orders", 1),
                    at("orders", 2),
                    at("payments", 0),
                ]
        );
    }

    #[test]
    fn a_topic_with_no_partition_expands_to_nothing() {
        for count in [0, -1] {
            check!(
                expand_targets(&[target("orders", count)]).is_empty(),
                "{count}"
            );
        }
    }

    #[test]
    fn a_cut_that_reached_every_partition_is_complete() {
        let targets = vec![target("orders", 2), target("payments", 1)];
        let placed = BTreeMap::from([
            (at("orders", 0), Offset(10)),
            (at("orders", 1), Offset(11)),
            (at("payments", 0), Offset(5)),
        ]);
        let expected = CutValue {
            triggered_at: 100,
            completed_at: 140,
            status: CutStatus::Complete,
            topics: vec![
                TopicOffsets {
                    topic: "orders".to_owned(),
                    partitions: vec![
                        PartitionOffset {
                            partition: PartitionIndex(0),
                            offset: Offset(10),
                        },
                        PartitionOffset {
                            partition: PartitionIndex(1),
                            offset: Offset(11),
                        },
                    ],
                },
                TopicOffsets {
                    topic: "payments".to_owned(),
                    partitions: vec![PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(5),
                    }],
                },
            ],
            missing: Vec::new(),
        };
        assert!(build_cut(100, 140, &targets, &placed) == expected);
    }

    #[test]
    fn a_cut_that_missed_a_partition_is_partial_and_names_it() {
        let targets = vec![target("orders", 2)];
        let placed = BTreeMap::from([(at("orders", 1), Offset(11))]);
        let expected = CutValue {
            triggered_at: 100,
            completed_at: 140,
            status: CutStatus::Partial,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: vec![PartitionOffset {
                    partition: PartitionIndex(1),
                    offset: Offset(11),
                }],
            }],
            missing: vec![MissingPartition {
                topic: "orders".to_owned(),
                partition: PartitionIndex(0),
            }],
        };
        assert!(build_cut(100, 140, &targets, &placed) == expected);
    }

    #[test]
    fn a_cut_that_reached_nothing_names_every_target() {
        let targets = vec![target("orders", 2)];
        let cut = build_cut(1, 2, &targets, &BTreeMap::new());
        let expected = CutValue {
            triggered_at: 1,
            completed_at: 2,
            status: CutStatus::Partial,
            topics: vec![TopicOffsets {
                topic: "orders".to_owned(),
                partitions: Vec::new(),
            }],
            missing: vec![
                MissingPartition {
                    topic: "orders".to_owned(),
                    partition: PartitionIndex(0),
                },
                MissingPartition {
                    topic: "orders".to_owned(),
                    partition: PartitionIndex(1),
                },
            ],
        };
        assert!(cut == expected);
    }

    #[test]
    fn a_cut_ignores_an_offset_that_no_target_names() {
        let targets = vec![target("orders", 1)];
        let placed = BTreeMap::from([(at("orders", 0), Offset(4)), (at("dropped", 0), Offset(9))]);
        let cut = build_cut(1, 2, &targets, &placed);
        assert!(cut.status == CutStatus::Complete);
        assert!(cut.topics.len() == 1);
        assert!(cut.topics[0].topic == "orders");
    }

    struct RetentionCase {
        name: &'static str,
        published: i64,
        retained_cuts: i32,
        held: &'static [i64],
        expired: &'static [i64],
    }

    #[test]
    fn the_retention_window_drops_the_epochs_below_it() {
        const ALL: &[i64] = &[1, 2, 3, 4, 5, 6, 7, 8];
        let cases = [
            RetentionCase {
                name: "nothing held yet",
                published: 1,
                retained_cuts: 1,
                held: &[],
                expired: &[],
            },
            RetentionCase {
                name: "one cut retained",
                published: 2,
                retained_cuts: 1,
                held: &[1],
                expired: &[1],
            },
            RetentionCase {
                name: "three cuts retained",
                published: 4,
                retained_cuts: 3,
                held: &[1, 2, 3],
                expired: &[1],
            },
            RetentionCase {
                name: "window not full",
                published: 3,
                retained_cuts: 3,
                held: &[1, 2],
                expired: &[],
            },
            RetentionCase {
                name: "default window",
                published: 10,
                retained_cuts: 32,
                held: ALL,
                expired: &[],
            },
            RetentionCase {
                name: "a reduced window drops several",
                published: 9,
                retained_cuts: 2,
                held: ALL,
                expired: &[1, 2, 3, 4, 5, 6, 7],
            },
            RetentionCase {
                name: "no retention drops nothing",
                published: 9,
                retained_cuts: 0,
                held: ALL,
                expired: &[],
            },
            RetentionCase {
                name: "negative retention drops nothing",
                published: 9,
                retained_cuts: -1,
                held: ALL,
                expired: &[],
            },
        ];
        for case in cases {
            check!(
                expired_cut_epochs(case.published, case.retained_cuts, case.held) == case.expired,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn the_next_epoch_follows_the_last_one() {
        assert!(next_epoch(NO_EPOCH_YET) == 1);
        assert!(next_epoch(41) == 42);
    }

    #[test]
    fn a_group_record_defines_the_entry() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: Some(group_value(7)),
            },
        );
        let entry = state.get("orders-cut").expect("the group is there");
        assert!(entry.definition == group_value(7));
        assert!(entry.is_defined());
        assert!(entry.last_epoch() == 7);
    }

    #[test]
    fn a_group_tombstone_removes_the_whole_entry() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: Some(group_value(7)),
            },
        );
        apply_record(
            &mut state,
            StateRecord::Cut {
                group: "orders-cut".to_owned(),
                epoch: 7,
                value: Some(cut_value(CutStatus::Complete)),
            },
        );
        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: None,
            },
        );
        assert!(state.is_empty());
    }

    #[test]
    fn a_cut_record_retires_the_injection_start_of_its_epoch() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: Some(group_value(0)),
            },
        );
        apply_record(
            &mut state,
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 1,
                value: Some(start_value(3)),
            },
        );
        assert!(state["orders-cut"].pending.is_some());

        apply_record(
            &mut state,
            StateRecord::Cut {
                group: "orders-cut".to_owned(),
                epoch: 1,
                value: Some(cut_value(CutStatus::Complete)),
            },
        );
        let entry = &state["orders-cut"];
        assert!(entry.pending.is_none());
        assert!(entry.cuts.keys().copied().collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn an_injection_start_with_no_cut_stays_pending() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: Some(group_value(1)),
            },
        );
        apply_record(
            &mut state,
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 2,
                value: Some(start_value(4)),
            },
        );
        let entry = &state["orders-cut"];
        assert!(
            entry.pending
                == Some(PendingInjection {
                    epoch: 2,
                    start: start_value(4),
                })
        );
        assert!(entry.last_epoch() == 2);
    }

    #[test]
    fn a_tombstone_drops_only_the_epoch_it_names() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: Some(group_value(0)),
            },
        );
        for epoch in [1, 2, 3] {
            apply_record(
                &mut state,
                StateRecord::Cut {
                    group: "orders-cut".to_owned(),
                    epoch,
                    value: Some(cut_value(CutStatus::Complete)),
                },
            );
        }
        apply_record(
            &mut state,
            StateRecord::Cut {
                group: "orders-cut".to_owned(),
                epoch: 1,
                value: None,
            },
        );
        assert!(state["orders-cut"].cuts.keys().copied().collect::<Vec<_>>() == vec![2, 3]);
    }

    #[test]
    fn an_injection_start_tombstone_of_another_epoch_keeps_the_pending_one() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 9,
                value: Some(start_value(2)),
            },
        );
        apply_record(
            &mut state,
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 4,
                value: None,
            },
        );
        assert!(state["orders-cut"].pending.as_ref().map(|p| p.epoch) == Some(9));

        apply_record(
            &mut state,
            StateRecord::InjectionStart {
                group: "orders-cut".to_owned(),
                epoch: 9,
                value: None,
            },
        );
        assert!(state["orders-cut"].pending.is_none());
    }

    // The newest group record sits after the cut records of older epochs in
    // the log, so the fold creates the entry for a cut it sees first.
    #[test]
    fn a_cut_that_precedes_the_group_record_survives_the_fold() {
        let mut state = BTreeMap::new();
        apply_record(
            &mut state,
            StateRecord::Cut {
                group: "orders-cut".to_owned(),
                epoch: 6,
                value: Some(cut_value(CutStatus::Partial)),
            },
        );
        assert!(!state["orders-cut"].is_defined());

        apply_record(
            &mut state,
            StateRecord::Group {
                group: "orders-cut".to_owned(),
                value: Some(group_value(6)),
            },
        );
        let entry = &state["orders-cut"];
        assert!(entry.is_defined());
        assert!(entry.cuts.contains_key(&6));
    }

    #[test]
    fn a_group_with_an_interval_gets_a_due_time() {
        let mut entry = GroupEntry::from_spec(
            GroupSpec {
                topics: vec!["orders".to_owned()],
                interval: Some(millis(5_000)),
                retained_cuts: 4,
            },
            0,
        );
        assert!(!is_due(&entry, 1_000));

        schedule_next(&mut entry, 1_000);
        assert!(entry.next_due_ms == Some(6_000));
        check!(!is_due(&entry, 5_999));
        check!(is_due(&entry, 6_000));
        check!(is_due(&entry, 9_999));
    }

    #[test]
    fn a_group_with_no_interval_is_never_due() {
        let mut entry = GroupEntry::from_spec(
            GroupSpec {
                topics: vec!["orders".to_owned()],
                interval: None,
                retained_cuts: 4,
            },
            0,
        );
        schedule_next(&mut entry, 1_000);
        assert!(entry.next_due_ms.is_none());
        assert!(!is_due(&entry, i64::MAX));
    }

    #[test]
    fn the_last_epoch_is_the_highest_of_every_source() {
        let mut entry = GroupEntry::from_spec(
            GroupSpec {
                topics: vec!["orders".to_owned()],
                interval: None,
                retained_cuts: 4,
            },
            3,
        );
        assert!(entry.last_epoch() == 3);

        entry.cuts.insert(5, cut_value(CutStatus::Complete));
        assert!(entry.last_epoch() == 5);

        entry.pending = Some(PendingInjection {
            epoch: 6,
            start: start_value(1),
        });
        assert!(entry.last_epoch() == 6);
    }
}
