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

use {
    crate::share_coordinator::persistence::{ShareSnapshotValue, ShareUpdateValue, StateBatch},
    krabka_log::Offset,
    std::{cmp::Reverse, collections::BTreeMap},
};

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
type Priority = Reverse<BatchPriority>;

/// A batch's delivery count and delivery state, in the order that decides
/// which batch wins an overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BatchPriority {
    delivery_count: i16,
    delivery_state: i8,
}

/// A batch boundary of the sweep: an offset where a batch of some priority
/// begins or, one past its last offset, ends.
#[derive(Debug, Clone, Copy)]
struct SweepEvent {
    offset: i64,
    is_begin: bool,
    priority: Priority,
}

/// Kafka's event-driven sweep over the pruned batches.
fn sweep_merge(batches: &[StateBatch]) -> Vec<StateBatch> {
    // `false` sorts before `true`, so an END comes before a BEGIN at the same
    // offset.
    let mut events: Vec<SweepEvent> = batches
        .iter()
        .flat_map(|b| {
            let priority = Reverse(BatchPriority {
                delivery_count: b.delivery_count,
                delivery_state: b.delivery_state,
            });
            [
                SweepEvent {
                    offset: b.first_offset.0,
                    is_begin: true,
                    priority,
                },
                SweepEvent {
                    offset: b.last_offset.0.saturating_add(1),
                    is_begin: false,
                    priority,
                },
            ]
        })
        .collect();
    events.sort_by_key(|event| (event.offset, event.is_begin));

    let mut active: BTreeMap<Priority, usize> = BTreeMap::new();
    let mut out: Vec<StateBatch> = Vec::new();
    let mut open_from = -1;
    let mut open_winner: Option<Priority> = None;
    for same_offset in events.chunk_by(|a, b| a.offset == b.offset) {
        let Some(&SweepEvent { offset, .. }) = same_offset.first() else {
            continue;
        };
        if let Some(winner) = open_winner
            && offset > open_from
        {
            append_coalesced(&mut out, open_from, offset - 1, winner);
        }
        for &SweepEvent {
            is_begin, priority, ..
        } in same_offset
        {
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
    let Reverse(BatchPriority {
        delivery_count,
        delivery_state,
    }) = winner;
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
    use {
        super::*,
        crate::share_coordinator::coordinator::test_support::{
            DeliveryAttemptCount, FixtureDeliveryState, state_batch as batch,
        },
        assert2::{assert, check},
        rstest_bdd::datatable::Rows,
        std::{fmt::Debug, str::FromStr},
    };

    #[derive(Default)]
    struct ShareBatchCombination {
        stored: Vec<StateBatch>,
        received: Vec<StateBatch>,
        start: Offset,
        combined: Option<Vec<StateBatch>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct DeliveryRange {
        first: i64,
        last: i64,
        delivery: String,
        attempts: i16,
    }

    fn delivery_range(row: &DeliveryRange) -> StateBatch {
        let delivery = match row.delivery.as_str() {
            "available" => FixtureDeliveryState::Available,
            "acknowledged" => FixtureDeliveryState::Acknowledged,
            "archived" => FixtureDeliveryState::Archived,
            other => panic!("unknown delivery state: {other}"),
        };
        batch()
            .bounds(Offset(row.first)..=Offset(row.last))
            .delivery(delivery)
            .attempts(DeliveryAttemptCount(row.attempts))
            .call()
    }

    fn delivery_ranges(rows: Rows<DeliveryRange>) -> Vec<StateBatch> {
        rows.into_iter().map(|row| delivery_range(&row)).collect()
    }

    #[rstest::fixture]
    fn share_batches() -> ShareBatchCombination {
        // Keep share batches inputs and observations fresh for each scenario.
        ShareBatchCombination::default()
    }

    #[rstest_bdd_macros::given("the stored share-delivery ranges are:")]
    fn stored_delivery_ranges(
        share_batches: &mut ShareBatchCombination,
        #[datatable] rows: Rows<DeliveryRange>,
    ) {
        share_batches.stored = delivery_ranges(rows);
    }

    #[rstest_bdd_macros::given("the incoming share-delivery ranges are:")]
    fn incoming_delivery_ranges(
        share_batches: &mut ShareBatchCombination,
        #[datatable] rows: Rows<DeliveryRange>,
    ) {
        share_batches.received = delivery_ranges(rows);
    }

    #[rstest_bdd_macros::given("the retained share-delivery log starts at offset {start}")]
    fn retained_delivery_start(share_batches: &mut ShareBatchCombination, start: i64) {
        share_batches.start = Offset(start);
    }

    #[rstest_bdd_macros::when(
        "the share coordinator combines the stored and incoming delivery ranges"
    )]
    fn combine_delivery_ranges(share_batches: &mut ShareBatchCombination) {
        share_batches.combined = Some(combine_state_batches(
            &share_batches.stored,
            &share_batches.received,
            share_batches.start,
        ));
    }

    #[rstest_bdd_macros::then("the resulting share-delivery ranges are:")]
    fn combined_delivery_ranges(
        share_batches: &ShareBatchCombination,
        #[datatable] rows: Rows<DeliveryRange>,
    ) {
        let expected = delivery_ranges(rows);
        check!(share_batches.combined.as_ref() == Some(&expected));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/share_coordinator/state/combiner.feature",
        fixtures = [share_batches: ShareBatchCombination]
    );

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

    #[derive(Default)]
    struct ShareReplayScenario {
        snapshot: Option<ShareSnapshotValue>,
        record_offset: Offset,
        update: Option<ShareUpdateValue>,
        replayed: Option<SharePartitionState>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ReplayField {
        field: String,
        value: String,
    }

    #[rstest::fixture]
    fn share_replay() -> ShareReplayScenario {
        // Keep share replay inputs and observations fresh for each scenario.
        ShareReplayScenario::default()
    }

    #[rstest_bdd_macros::given(
        "a replay snapshot has snapshot epoch {snapshot}, state epoch {state}, leader epoch {leader}, and record offset {offset}"
    )]
    fn replay_snapshot_epochs(
        share_replay: &mut ShareReplayScenario,
        snapshot: i32,
        state: i32,
        leader: i32,
        offset: i64,
    ) {
        share_replay.snapshot = Some(ShareSnapshotValue {
            snapshot_epoch: snapshot,
            state_epoch: state,
            leader_epoch: leader,
            start_offset: Offset(0),
            delivery_complete_count: 0,
            create_timestamp: 0,
            write_timestamp: 0,
            state_batches: Vec::new(),
        });
        share_replay.record_offset = Offset(offset);
    }

    #[rstest_bdd_macros::given(
        "the replay snapshot starts at {start} with completed count {complete}, creation time {created}, and write time {written}"
    )]
    fn replay_snapshot_metadata(
        share_replay: &mut ShareReplayScenario,
        start: i64,
        complete: i32,
        created: i64,
        written: i64,
    ) {
        let snapshot = share_replay
            .snapshot
            .as_mut()
            .expect("a replay snapshot was given");
        snapshot.start_offset = Offset(start);
        snapshot.delivery_complete_count = complete;
        snapshot.create_timestamp = created;
        snapshot.write_timestamp = written;
    }

    #[rstest_bdd_macros::given("the replay snapshot delivery ranges are:")]
    fn replay_snapshot_ranges(
        share_replay: &mut ShareReplayScenario,
        #[datatable] rows: Rows<DeliveryRange>,
    ) {
        share_replay
            .snapshot
            .as_mut()
            .expect("a replay snapshot was given")
            .state_batches = delivery_ranges(rows);
    }

    #[rstest_bdd_macros::given(
        "the replay update has snapshot epoch {snapshot}, leader epoch {leader}, start offset {start}, and completed count {complete}"
    )]
    fn replay_update_metadata(
        share_replay: &mut ShareReplayScenario,
        snapshot: i32,
        leader: i32,
        start: i64,
        complete: i32,
    ) {
        share_replay.update = Some(ShareUpdateValue {
            snapshot_epoch: snapshot,
            leader_epoch: leader,
            start_offset: Offset(start),
            delivery_complete_count: complete,
            state_batches: Vec::new(),
        });
    }

    #[rstest_bdd_macros::given("the replay update delivery ranges are:")]
    fn replay_update_ranges(
        share_replay: &mut ShareReplayScenario,
        #[datatable] rows: Rows<DeliveryRange>,
    ) {
        share_replay
            .update
            .as_mut()
            .expect("a replay update was given")
            .state_batches = delivery_ranges(rows);
    }

    #[rstest_bdd_macros::when("the coordinator replays the snapshot followed by the update")]
    fn replay_snapshot_and_update(share_replay: &mut ShareReplayScenario) {
        let snapshot = share_replay
            .snapshot
            .as_ref()
            .expect("a replay snapshot was given");
        let update = share_replay
            .update
            .as_ref()
            .expect("a replay update was given");
        let mut state = SharePartitionState::from_snapshot(snapshot, share_replay.record_offset);
        state.apply_update(update);
        share_replay.replayed = Some(state);
    }

    fn replay_field<T: FromStr>(fields: &BTreeMap<String, String>, name: &str) -> T
    where
        T::Err: Debug,
    {
        fields
            .get(name)
            .unwrap_or_else(|| panic!("missing replay-state field {name}"))
            .parse()
            .unwrap_or_else(|error| panic!("invalid replay-state field {name}: {error:?}"))
    }

    #[rstest_bdd_macros::then("the complete replayed share state is:")]
    fn complete_replayed_state(
        share_replay: &ShareReplayScenario,
        #[datatable] rows: Rows<ReplayField>,
    ) {
        let fields: BTreeMap<_, _> = rows.into_iter().map(|row| (row.field, row.value)).collect();
        let raw_batches: Vec<(i64, i64, String, i16)> = serde_json::from_str(
            fields
                .get("state_batches")
                .expect("expected delivery ranges were specified"),
        )
        .expect("delivery ranges are first/last/state/attempt tuples");
        let expected = SharePartitionState {
            state_epoch: replay_field(&fields, "state_epoch"),
            leader_epoch: replay_field(&fields, "leader_epoch"),
            start_offset: Offset(replay_field(&fields, "start_offset")),
            delivery_complete_count: replay_field(&fields, "delivery_complete_count"),
            state_batches: raw_batches
                .into_iter()
                .map(|(first, last, delivery, attempts)| {
                    delivery_range(&DeliveryRange {
                        first,
                        last,
                        delivery,
                        attempts,
                    })
                })
                .collect(),
            snapshot_epoch: replay_field(&fields, "snapshot_epoch"),
            create_timestamp: replay_field(&fields, "create_timestamp"),
            write_timestamp: replay_field(&fields, "write_timestamp"),
            last_snapshot_offset: Offset(replay_field(&fields, "last_snapshot_offset")),
            updates_since_snapshot: replay_field(&fields, "updates_since_snapshot"),
            fence_leader_epoch: replay_field(&fields, "fence_leader_epoch"),
            fence_state_epoch: replay_field(&fields, "fence_state_epoch"),
        };
        assert!(share_replay.replayed.as_ref() == Some(&expected));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/share_coordinator/state/replay.feature",
        fixtures = [share_replay: ShareReplayScenario]
    );

    /// An update with `-1` keeps the stored start offset and leader epoch.
    #[test]
    fn update_with_minus_one_keeps_stored_values() {
        let mut s = SharePartitionState::from_snapshot(&snapshot(50, vec![]), Offset(0));
        s.apply_update(&ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: -1,
            start_offset: Offset(-1),
            delivery_complete_count: 3,
            state_batches: vec![batch().bounds(Offset(40)..=Offset(55)).call()],
        });
        check!(s.start_offset == Offset(50));
        check!(s.leader_epoch == 3);
        check!(s.state_batches == vec![batch().bounds(Offset(50)..=Offset(55)).call()]);
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
