//! In-memory per-`(group, topicId, partition)` share-delivery state.
//!
//! The broker builds this state as Kafka's `ShareCoordinatorShard.replay`
//! does: a `ShareSnapshot` replaces the state, and a `ShareUpdate` merges
//! into it (`ShareCoordinatorShard.merge`). The live write path appends a
//! record and then applies it through the same functions, so a replay and the
//! live state never differ.
//!
//! Batches merge through [`combine_state_batches`], a port of Kafka's
//! `PersisterStateBatchCombiner`.

use std::{cmp::Reverse, collections::BTreeMap};

use krabka_log::Offset;

use crate::share_coordinator::persistence::{ShareSnapshotValue, ShareUpdateValue, StateBatch};

/// The state of one share key, with the bookkeeping that Kafka's shard keeps
/// beside it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharePartitionState {
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<StateBatch>,
    pub snapshot_epoch: i32,
    /// The time the state was created, from the latest snapshot.
    pub create_timestamp: i64,
    /// The time the latest snapshot was written. The cold-partition snapshot
    /// compares it with its interval.
    pub write_timestamp: i64,
    /// The log offset of the latest snapshot record of the key.
    pub last_snapshot_offset: Offset,
    /// Kafka's `snapshotUpdateCount`: the updates since the count last
    /// reached the snapshot threshold.
    pub updates_since_snapshot: u32,
    /// Kafka's `leaderEpochMap`: the highest leader epoch any record of the
    /// key carried. Reads and writes are fenced against it.
    pub fence_leader_epoch: i32,
    /// Kafka's `stateEpochMap`: the highest state epoch any snapshot of the
    /// key carried. Initializes and writes are fenced against it.
    pub fence_state_epoch: i32,
}

impl SharePartitionState {
    /// The state of a key whose first record is the snapshot `v` at `offset`.
    #[must_use]
    pub fn from_snapshot(v: &ShareSnapshotValue, offset: Offset) -> Self {
        let mut state = Self {
            fence_leader_epoch: v.leader_epoch,
            fence_state_epoch: v.state_epoch,
            ..Self::default()
        };
        state.apply_snapshot(v, offset, u32::MAX);
        state
    }

    /// The state of a key whose first record is the update `v`, as Kafka's
    /// `ShareGroupOffset.fromRecord(ShareUpdateValue)` builds it: state epoch
    /// 0 and no timestamps.
    #[must_use]
    pub fn from_update(v: &ShareUpdateValue) -> Self {
        Self {
            state_epoch: 0,
            leader_epoch: v.leader_epoch,
            start_offset: v.start_offset,
            delivery_complete_count: v.delivery_complete_count,
            state_batches: v.state_batches.clone(),
            snapshot_epoch: v.snapshot_epoch,
            updates_since_snapshot: 1,
            fence_leader_epoch: v.leader_epoch,
            ..Self::default()
        }
    }

    /// Replaces the state with the snapshot `v` at `offset`, as Kafka's
    /// `handleShareSnapshot` does. The update count restarts only when it
    /// has reached `updates_per_snapshot`.
    pub fn apply_snapshot(
        &mut self,
        v: &ShareSnapshotValue,
        offset: Offset,
        updates_per_snapshot: u32,
    ) {
        self.fence_leader_epoch = self.fence_leader_epoch.max(v.leader_epoch);
        self.fence_state_epoch = self.fence_state_epoch.max(v.state_epoch);
        self.snapshot_epoch = v.snapshot_epoch;
        self.state_epoch = v.state_epoch;
        self.leader_epoch = v.leader_epoch;
        self.start_offset = v.start_offset;
        self.delivery_complete_count = v.delivery_complete_count;
        self.state_batches.clone_from(&v.state_batches);
        self.create_timestamp = v.create_timestamp;
        self.write_timestamp = v.write_timestamp;
        self.last_snapshot_offset = offset;
        if self.updates_since_snapshot >= updates_per_snapshot {
            self.updates_since_snapshot = 0;
        }
    }

    /// Merges the update `v`, as Kafka's `ShareCoordinatorShard.merge` does.
    ///
    /// A start offset or leader epoch of `-1` keeps the stored value. The
    /// batches combine with the stored ones and are clipped at the new start
    /// offset. The snapshot epoch, state epoch and timestamps stay.
    pub fn apply_update(&mut self, v: &ShareUpdateValue) {
        self.fence_leader_epoch = self.fence_leader_epoch.max(v.leader_epoch);
        if v.start_offset.0 != -1 {
            self.start_offset = v.start_offset;
        }
        if v.leader_epoch != -1 {
            self.leader_epoch = v.leader_epoch;
        }
        self.delivery_complete_count = v.delivery_complete_count;
        self.state_batches =
            combine_state_batches(&self.state_batches, &v.state_batches, self.start_offset);
        self.updates_since_snapshot = self.updates_since_snapshot.saturating_add(1);
    }

    /// The snapshot of this state with `snapshot_epoch` and `write_timestamp`
    /// replaced, as Kafka's cold-partition snapshot writes it.
    #[must_use]
    pub fn to_snapshot(&self, snapshot_epoch: i32, write_timestamp: i64) -> ShareSnapshotValue {
        ShareSnapshotValue {
            snapshot_epoch,
            state_epoch: self.state_epoch,
            leader_epoch: self.leader_epoch,
            start_offset: self.start_offset,
            delivery_complete_count: self.delivery_complete_count,
            create_timestamp: self.create_timestamp,
            write_timestamp,
            state_batches: self.state_batches.clone(),
        }
    }
}

/// Combines `so_far` with `new_batches` into the shortest non-overlapping
/// cover of their union, clipped at `start_offset`.
///
/// This is Kafka's `PersisterStateBatchCombiner.combineStateBatches`. Where
/// batches overlap, the one with the higher delivery count wins, then the one
/// with the higher delivery state. Adjacent ranges with the same state and
/// count merge. A `start_offset` of `-1` clips nothing.
#[must_use]
pub fn combine_state_batches(
    so_far: &[StateBatch],
    new_batches: &[StateBatch],
    start_offset: Offset,
) -> Vec<StateBatch> {
    let pruned: Vec<StateBatch> = so_far
        .iter()
        .chain(new_batches)
        .filter(|b| start_offset.0 == -1 || b.last_offset >= start_offset)
        .map(|b| {
            if start_offset.0 == -1 || b.first_offset >= start_offset {
                b.clone()
            } else {
                StateBatch {
                    first_offset: start_offset,
                    ..b.clone()
                }
            }
        })
        .collect();
    if pruned.len() <= 1 {
        return pruned;
    }
    sweep_merge(&pruned)
}

/// The priority of a batch: the higher delivery count wins, then the higher
/// delivery state. `Reverse` makes the winner the first key of a map.
type Priority = Reverse<(i16, i8)>;

/// Kafka's event-driven sweep over the pruned batches.
fn sweep_merge(batches: &[StateBatch]) -> Vec<StateBatch> {
    // (offset, is_begin, priority). `false` sorts before `true`, so an END
    // comes before a BEGIN at the same offset.
    let mut events: Vec<(i64, bool, Priority)> = batches
        .iter()
        .flat_map(|b| {
            let priority = Reverse((b.delivery_count, b.delivery_state));
            [
                (b.first_offset.0, true, priority),
                (b.last_offset.0.saturating_add(1), false, priority),
            ]
        })
        .collect();
    events.sort_by_key(|&(offset, is_begin, _)| (offset, is_begin));

    let mut active: BTreeMap<Priority, usize> = BTreeMap::new();
    let mut out: Vec<StateBatch> = Vec::new();
    let mut open_from = -1;
    let mut open_winner: Option<Priority> = None;
    let mut i = 0;
    while i < events.len() {
        let offset = events[i].0;
        if let Some(winner) = open_winner
            && offset > open_from
        {
            append_coalesced(&mut out, open_from, offset - 1, winner);
        }
        while i < events.len() && events[i].0 == offset {
            let (_, is_begin, priority) = events[i];
            i += 1;
            if is_begin {
                *active.entry(priority).or_insert(0) += 1;
            } else if let Some(count) = active.get_mut(&priority) {
                *count -= 1;
                if *count == 0 {
                    active.remove(&priority);
                }
            }
        }
        open_winner = active.keys().next().copied();
        open_from = offset;
    }
    out
}

fn append_coalesced(out: &mut Vec<StateBatch>, from: i64, to: i64, winner: Priority) {
    let Reverse((delivery_count, delivery_state)) = winner;
    if let Some(tail) = out.last_mut()
        && tail.last_offset.0 + 1 == from
        && tail.delivery_state == delivery_state
        && tail.delivery_count == delivery_count
    {
        tail.last_offset = Offset(to);
        return;
    }
    out.push(StateBatch {
        first_offset: Offset(from),
        last_offset: Offset(to),
        delivery_state,
        delivery_count,
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::share_coordinator::coordinator::test_support::state_batch as batch;

    /// Cases of Kafka's `PersisterStateBatchCombinerTest`: the combined
    /// batches of `so_far` and `new` at a start offset.
    #[test]
    fn combine_matches_kafka_combiner() {
        type Row = (
            &'static str,
            Vec<StateBatch>,
            Vec<StateBatch>,
            i64,
            Vec<StateBatch>,
        );
        let rows: Vec<Row> = vec![
            ("both empty", vec![], vec![], 0, vec![]),
            (
                "one batch passes through",
                vec![],
                vec![batch(100, 109, 0, 1)],
                -1,
                vec![batch(100, 109, 0, 1)],
            ),
            (
                "disjoint batches stay apart and sort",
                vec![batch(110, 119, 0, 1)],
                vec![batch(100, 104, 2, 1)],
                -1,
                vec![batch(100, 104, 2, 1), batch(110, 119, 0, 1)],
            ),
            (
                "adjacent equal batches coalesce",
                vec![batch(100, 104, 0, 1)],
                vec![batch(105, 109, 0, 1)],
                -1,
                vec![batch(100, 109, 0, 1)],
            ),
            (
                "the higher delivery count wins an overlap",
                vec![batch(100, 109, 0, 1)],
                vec![batch(103, 105, 0, 2)],
                -1,
                vec![
                    batch(100, 102, 0, 1),
                    batch(103, 105, 0, 2),
                    batch(106, 109, 0, 1),
                ],
            ),
            (
                "the higher state wins at an equal count",
                vec![batch(100, 109, 2, 1)],
                vec![batch(100, 109, 0, 1)],
                -1,
                vec![batch(100, 109, 2, 1)],
            ),
            (
                "the start offset drops and clips",
                vec![batch(90, 99, 0, 1), batch(100, 109, 0, 1)],
                vec![batch(95, 104, 2, 1)],
                103,
                vec![batch(103, 104, 2, 1), batch(105, 109, 0, 1)],
            ),
            // The rows of issue #935.
            (
                "a later batch splits the stored one",
                vec![batch(0, 9, 0, 1)],
                vec![batch(5, 9, 2, 1)],
                0,
                vec![batch(0, 4, 0, 1), batch(5, 9, 2, 1)],
            ),
            (
                "a lower delivery count loses",
                vec![batch(0, 9, 0, 2)],
                vec![batch(0, 9, 2, 1)],
                0,
                vec![batch(0, 9, 0, 2)],
            ),
            (
                "a higher state wins over the stored batch",
                vec![batch(0, 9, 0, 1)],
                vec![batch(0, 9, 2, 1)],
                0,
                vec![batch(0, 9, 2, 1)],
            ),
            (
                "a lone batch is clipped at the start offset",
                vec![batch(0, 9, 0, 1)],
                vec![],
                5,
                vec![batch(5, 9, 0, 1)],
            ),
            (
                "adjacent equal batches coalesce at start offset 0",
                vec![batch(0, 4, 2, 1)],
                vec![batch(5, 9, 2, 1)],
                0,
                vec![batch(0, 9, 2, 1)],
            ),
            (
                "an inner batch splits the stored one in three",
                vec![batch(0, 9, 0, 1)],
                vec![batch(3, 5, 4, 3)],
                -1,
                vec![batch(0, 2, 0, 1), batch(3, 5, 4, 3), batch(6, 9, 0, 1)],
            ),
            (
                "a batch wholly below the start offset is dropped",
                vec![],
                vec![batch(0, 9, 0, 1), batch(20, 29, 0, 1)],
                20,
                vec![batch(20, 29, 0, 1)],
            ),
        ];
        for (name, so_far, new, start, expected) in rows {
            check!(
                combine_state_batches(&so_far, &new, Offset(start)) == expected,
                "{name}"
            );
        }
    }

    fn snapshot(start: i64, batches: Vec<StateBatch>) -> ShareSnapshotValue {
        ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 2,
            leader_epoch: 3,
            start_offset: Offset(start),
            delivery_complete_count: 0,
            create_timestamp: 10,
            write_timestamp: 20,
            state_batches: batches,
        }
    }

    #[test]
    fn snapshot_then_update_merges_as_kafka() {
        let mut s = SharePartitionState::from_snapshot(
            &snapshot(
                0,
                vec![batch(0, 9, 0, 1), batch(10, 19, 0, 1), batch(20, 29, 0, 1)],
            ),
            Offset(7),
        );
        s.apply_update(&ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: 4,
            start_offset: Offset(20),
            delivery_complete_count: 7,
            state_batches: vec![batch(30, 39, 0, 1)],
        });

        let expected = SharePartitionState {
            state_epoch: 2,
            leader_epoch: 4,
            start_offset: Offset(20),
            delivery_complete_count: 7,
            state_batches: vec![batch(20, 39, 0, 1)],
            snapshot_epoch: 1,
            create_timestamp: 10,
            write_timestamp: 20,
            last_snapshot_offset: Offset(7),
            updates_since_snapshot: 1,
            fence_leader_epoch: 4,
            fence_state_epoch: 2,
        };
        assert!(s == expected);
    }

    /// An update with `-1` keeps the stored start offset and leader epoch.
    #[test]
    fn update_with_minus_one_keeps_stored_values() {
        let mut s = SharePartitionState::from_snapshot(&snapshot(50, vec![]), Offset(0));
        s.apply_update(&ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: -1,
            start_offset: Offset(-1),
            delivery_complete_count: 3,
            state_batches: vec![batch(40, 55, 0, 1)],
        });
        check!(s.start_offset == Offset(50));
        check!(s.leader_epoch == 3);
        check!(s.state_batches == vec![batch(50, 55, 0, 1)]);
    }

    /// The update count restarts on a snapshot only once it has reached the
    /// threshold, and the fence epochs never go down.
    #[test]
    fn snapshot_resets_count_at_threshold_and_keeps_fences() {
        let rows = [(1, 2, 1), (2, 2, 0), (3, 2, 0)];
        for (updates, threshold, expected) in rows {
            let mut s = SharePartitionState {
                updates_since_snapshot: updates,
                fence_leader_epoch: 9,
                fence_state_epoch: 9,
                ..SharePartitionState::default()
            };
            s.apply_snapshot(&snapshot(0, vec![]), Offset(1), threshold);
            check!(
                s.updates_since_snapshot == expected,
                "{updates}/{threshold}"
            );
            check!((s.fence_leader_epoch, s.fence_state_epoch) == (9, 9));
        }
    }
}
