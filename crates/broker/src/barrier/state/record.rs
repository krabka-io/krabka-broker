//! The decoded `__barrier_state` records and the fold that replays them.
//!
//! Recovery reads the state partition it leads and folds every record into the
//! group map. The fold is the one place that decides what a tombstone removes,
//! so it stays apart from the entry it writes into.

use std::collections::BTreeMap;

use super::{GroupEntry, PendingInjection};
use crate::barrier::persistence::{CutValue, GroupValue, InjectionStartValue};

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

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::barrier::{
        persistence::CutStatus,
        state::test_support::{cut_value, group_value, start_value},
    };

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
}
