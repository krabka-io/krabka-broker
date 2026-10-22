//! `RemoteLogMetadataCache` holds per-partition metadata state with the
//! lifecycle state machine and an epoch-indexed offset lookup.
//!
//! Mirrors Kafka's `RemoteLogMetadataCache`. The cache holds segments in an
//! id-keyed map. When a segment reaches
//! [`CopySegmentFinished`](RemoteLogSegmentState::CopySegmentFinished), the
//! cache also indexes it per leader epoch by the offset at which that epoch
//! starts to contribute, so an `(epoch, offset)` query is a navigable-map
//! floor lookup. When a segment leaves the readable set at
//! [`DeleteSegmentStarted`](RemoteLogSegmentState::DeleteSegmentStarted), the
//! cache removes it from the epoch index.
//! [`DeleteSegmentFinished`](RemoteLogSegmentState::DeleteSegmentFinished)
//! drops it entirely. Each transition touches only the index entries of the
//! segment it names.

use {
    crate::{
        error::RemoteStorageError,
        metadata::{
            RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
            RemotePartitionDeleteState,
        },
    },
    krabka_ids::LeaderEpoch,
    krabka_verified::{
        remote_read_relative_offset,
        storage::{RemoteCacheAction, remote_cache_action},
    },
    std::collections::{BTreeSet, HashMap, HashSet, hash_map::Entry},
    uuid::Uuid,
};

/// The derived per-epoch index over finished segments.
///
/// For each leader epoch it holds every finished segment that carries the
/// epoch, as `(offset at which the epoch starts to contribute, segment id)`.
/// A lifecycle transition inserts or removes only the entries of the segment
/// it touches. When two finished segments share an epoch and a start offset,
/// which only malformed metadata produces, the lowest UUID owns the slot; the
/// other entries stay indexed, so when the owner leaves readable state the
/// next lowest UUID takes the slot over.
#[derive(Debug, Default)]
struct EpochIndex(HashMap<LeaderEpoch, BTreeSet<(i64, Uuid)>>);

impl EpochIndex {
    /// Indexes `metadata` under each of its epochs if it is finished.
    fn insert(&mut self, metadata: &RemoteLogSegmentMetadata) {
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return;
        }
        let id = metadata.remote_log_segment_id().id;
        for (&epoch, &start) in metadata.segment_leader_epochs() {
            self.0.entry(epoch).or_default().insert((start, id));
        }
    }

    /// Removes the entries [`Self::insert`] added for `metadata`, dropping an
    /// epoch once it has no entry left.
    fn remove(&mut self, metadata: &RemoteLogSegmentMetadata) {
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return;
        }
        let id = metadata.remote_log_segment_id().id;
        for (&epoch, &start) in metadata.segment_leader_epochs() {
            if let Entry::Occupied(mut entries) = self.0.entry(epoch) {
                entries.get_mut().remove(&(start, id));
                if entries.get().is_empty() {
                    entries.remove();
                }
            }
        }
    }

    /// Replaces the index with one over the finished segments of `segments`.
    /// It sorts every entry once, so each epoch's set is built in bulk from
    /// an ordered run.
    fn rebuild<'a>(&mut self, segments: impl IntoIterator<Item = &'a RemoteLogSegmentMetadata>) {
        let mut entries = segments
            .into_iter()
            .filter(|metadata| metadata.state() == RemoteLogSegmentState::CopySegmentFinished)
            .flat_map(|metadata| {
                let id = metadata.remote_log_segment_id().id;
                metadata
                    .segment_leader_epochs()
                    .iter()
                    .map(move |(&epoch, &start)| (epoch, start, id))
            })
            .collect::<Vec<_>>();
        entries.sort_unstable();
        self.0 = entries
            .chunk_by(|a, b| a.0 == b.0)
            .map(|run| {
                (
                    run[0].0,
                    run.iter().map(|&(_, start, id)| (start, id)).collect(),
                )
            })
            .collect();
    }

    /// The owner of the slot with the greatest start at or below `offset`.
    ///
    /// The walk goes backwards from the floor entry, so it ends on the lowest
    /// UUID that shares the floor's start.
    fn floor(&self, leader_epoch: LeaderEpoch, offset: i64) -> Option<Uuid> {
        let mut below = self
            .0
            .get(&leader_epoch)?
            .range(..=(offset, Uuid::max()))
            .rev();
        let &(start, highest) = below.next()?;
        Some(
            below
                .take_while(|&&(other, _)| other == start)
                .last()
                .map_or(highest, |&(_, id)| id),
        )
    }

    /// The owner of every slot of `leader_epoch`, in start order.
    fn owners(&self, leader_epoch: LeaderEpoch) -> impl Iterator<Item = (i64, Uuid)> + '_ {
        let mut previous_start = None;
        self.0
            .get(&leader_epoch)
            .into_iter()
            .flatten()
            .filter(move |&&(start, _)| previous_start.replace(start) != Some(start))
            .copied()
    }
}

#[derive(Debug, Default)]
pub(crate) struct RemoteLogMetadataCache {
    /// Every known segment, keyed by its per-segment UUID.
    id_to_metadata: HashMap<Uuid, RemoteLogSegmentMetadata>,
    /// The per-epoch offset index. Only finished, readable segments appear
    /// here.
    epoch_index: EpochIndex,
    /// Partition-delete lifecycle state, once marked.
    delete_state: Option<RemotePartitionDeleteState>,
}

impl RemoteLogMetadataCache {
    pub(crate) fn add(
        &mut self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let id = metadata.remote_log_segment_id().clone();
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd {
                id,
                reason: format!(
                    "starting state must be CopySegmentStarted, got {:?}",
                    metadata.state()
                ),
            });
        }
        if self.id_to_metadata.contains_key(&id.id) {
            return Err(RemoteStorageError::InvalidAdd {
                id,
                reason: "segment id already exists".into(),
            });
        }
        self.id_to_metadata.insert(id.id, metadata);
        Ok(())
    }

    /// Applies a lifecycle transition. The epoch index changes only for the
    /// segment the update names: its old entries leave if it was finished,
    /// and its new ones enter if it is finished now.
    pub(crate) fn update(
        &mut self,
        update: &RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let id = &update.remote_log_segment_id;
        let Some(existing) = self.id_to_metadata.get(&id.id) else {
            return match remote_cache_action(None, update.state.lifecycle(), false) {
                RemoteCacheAction::Noop => Ok(()),
                _ => Err(RemoteStorageError::SegmentNotFound(id.clone())),
            };
        };
        let exact_retry = update.state == existing.state()
            && update.event_timestamp_ms == existing.event_timestamp_ms()
            && update.broker_id == existing.broker_id()
            && update
                .custom_metadata
                .as_ref()
                .is_none_or(|custom| existing.custom_metadata() == Some(custom));

        match remote_cache_action(
            Some(existing.state().lifecycle()),
            update.state.lifecycle(),
            exact_retry,
        ) {
            RemoteCacheAction::Reject => Err(RemoteStorageError::InvalidSegmentTransition {
                id: id.clone(),
                from: existing.state(),
                to: update.state,
            }),
            RemoteCacheAction::Noop => Ok(()),
            RemoteCacheAction::StoreFinished | RemoteCacheAction::StoreHidden => {
                let updated = existing.with_update(update)?;
                self.epoch_index.remove(existing);
                self.epoch_index.insert(&updated);
                self.id_to_metadata.insert(id.id, updated);
                Ok(())
            }
            RemoteCacheAction::Remove => {
                existing.with_update(update)?;
                self.epoch_index.remove(existing);
                self.id_to_metadata.remove(&id.id);
                Ok(())
            }
        }
    }

    pub(crate) fn segment_for(
        &self,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Option<RemoteLogSegmentMetadata> {
        let id = self.epoch_index.floor(leader_epoch, offset)?;
        let md = self.id_to_metadata.get(&id)?;
        let (epoch_start, next_epoch_start) = md.epoch_bounds(leader_epoch);
        if remote_read_relative_offset(
            md.start_offset(),
            md.end_offset(),
            offset,
            md.state() == RemoteLogSegmentState::CopySegmentFinished,
            epoch_start,
            next_epoch_start,
        )
        .is_some()
        {
            Some(md.clone())
        } else {
            None
        }
    }

    pub(crate) fn highest_offset_for_epoch(&self, leader_epoch: LeaderEpoch) -> Option<i64> {
        self.epoch_index
            .owners(leader_epoch)
            .filter_map(|(_, id)| self.id_to_metadata.get(&id))
            .filter(|metadata| {
                metadata.state() == RemoteLogSegmentState::CopySegmentFinished
                    && metadata.segment_leader_epochs().contains_key(&leader_epoch)
            })
            .map(RemoteLogSegmentMetadata::end_offset)
            .max()
    }

    pub(crate) fn list(&self) -> Vec<RemoteLogSegmentMetadata> {
        let mut out: Vec<RemoteLogSegmentMetadata> =
            self.id_to_metadata.values().cloned().collect();
        sort_by_start_offset(&mut out);
        out
    }

    pub(crate) fn list_by_epoch(&self, leader_epoch: LeaderEpoch) -> Vec<RemoteLogSegmentMetadata> {
        let mut out: Vec<RemoteLogSegmentMetadata> = self
            .id_to_metadata
            .values()
            .filter(|metadata| {
                metadata.state() == RemoteLogSegmentState::CopySegmentFinished
                    && metadata.segment_leader_epochs().contains_key(&leader_epoch)
            })
            .cloned()
            .collect();
        sort_by_start_offset(&mut out);
        out
    }

    /// Every tracked segment, in all states, unordered. The owning manager
    /// pairs this with [`Self::delete_state`] to dump the partition for a
    /// snapshot.
    pub(crate) fn dump_segments(&self) -> Vec<RemoteLogSegmentMetadata> {
        self.id_to_metadata.values().cloned().collect()
    }

    /// Replace this cache from a dump without replaying lifecycle transitions.
    ///
    /// Duplicate UUIDs fold toward the most advanced lifecycle state, and a
    /// `delete_finished` UUID remains tombstoned for the whole pass so later
    /// stale rows cannot resurrect it. The seed then builds the per-epoch
    /// index from finished primary entries, the entries the live path would
    /// have indexed.
    /// Repeating the same seed is idempotent. The partition-delete state is
    /// replaced verbatim.
    pub(crate) fn seed(
        &mut self,
        segments: Vec<RemoteLogSegmentMetadata>,
        delete_state: Option<RemotePartitionDeleteState>,
    ) {
        self.id_to_metadata.clear();
        let mut tombstones = HashSet::new();
        for md in segments {
            let id = md.remote_log_segment_id().id;
            if md.state() == RemoteLogSegmentState::DeleteSegmentFinished {
                self.id_to_metadata.remove(&id);
                tombstones.insert(id);
            } else if !tombstones.contains(&id) {
                match self.id_to_metadata.entry(id) {
                    Entry::Vacant(entry) => {
                        entry.insert(md);
                    }
                    Entry::Occupied(mut entry) => {
                        if md.state().lifecycle() > entry.get().state().lifecycle() {
                            entry.insert(md);
                        }
                    }
                }
            }
        }
        self.epoch_index.rebuild(self.id_to_metadata.values());
        self.delete_state = delete_state;
    }

    pub(crate) fn delete_state(&self) -> Option<RemotePartitionDeleteState> { self.delete_state }

    pub(crate) fn set_delete_state(&mut self, state: RemotePartitionDeleteState) {
        self.delete_state = Some(state);
    }
}

pub(crate) fn sort_by_start_offset(segments: &mut [RemoteLogSegmentMetadata]) {
    segments.sort_by(|a, b| {
        a.start_offset().cmp(&b.start_offset()).then_with(|| {
            a.remote_log_segment_id()
                .id
                .cmp(&b.remote_log_segment_id().id)
        })
    });
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::metadata::{
            CustomMetadata, RemoteLogSegmentDetails, RemoteLogSegmentId, TopicIdPartition,
        },
        assert2::{assert, check},
        proptest::prelude::*,
        std::collections::BTreeMap,
    };

    fn tp() -> TopicIdPartition { TopicIdPartition::new(Uuid::from_u128(1), "t", 0) }

    use krabka_ids::Offset;

    /// The first offset a leader epoch contributes to a segment.
    #[derive(Clone, Copy)]
    struct EpochStart {
        epoch: LeaderEpoch,
        start: Offset,
    }

    const fn epoch_start(epoch: LeaderEpoch, start: Offset) -> EpochStart {
        EpochStart { epoch, start }
    }

    const FIRST_SEGMENT_EPOCHS: &[EpochStart] = &[epoch_start(LeaderEpoch(0), Offset(0))];
    const FOLLOWING_SEGMENT_EPOCHS: &[EpochStart] = &[epoch_start(LeaderEpoch(0), Offset(100))];

    #[bon::builder]
    fn seg(
        #[builder(default = Uuid::from_u128(10))] id: Uuid,
        #[builder(default = FIRST_SEGMENT_EPOCHS)] epochs: &[EpochStart],
        #[builder(into, default = Offset(0))] start: Offset,
        #[builder(into, default = Offset(99))] end: Offset,
    ) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), id),
            start.0,
            end.0,
            end.0,
            1,
            100,
            RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                epochs
                    .iter()
                    .map(|&EpochStart { epoch, start }| (epoch, start.0))
                    .collect(),
            ),
        )
        .unwrap()
    }

    fn following_segment() -> RemoteLogSegmentMetadata {
        following_segment_for_epochs(FOLLOWING_SEGMENT_EPOCHS)
    }

    fn following_segment_for_epochs(epochs: &[EpochStart]) -> RemoteLogSegmentMetadata {
        seg()
            .id(Uuid::from_u128(11))
            .epochs(epochs)
            .start(Offset(100))
            .end(Offset(199))
            .call()
    }

    fn finish(id: Uuid) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), id),
            event_timestamp_ms: 200,
            custom_metadata: Some(CustomMetadata(vec![1])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        }
    }

    fn transition(id: Uuid, state: RemoteLogSegmentState) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), id),
            event_timestamp_ms: 300,
            custom_metadata: None,
            state,
            broker_id: 1,
        }
    }

    #[test]
    fn started_segment_is_invisible_until_finished() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().call()).unwrap();
        assert!(
            c.segment_for(LeaderEpoch(0), 50).is_none(),
            "started not yet readable"
        );
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        let got = c
            .segment_for(LeaderEpoch(0), 50)
            .expect("finished is readable");
        assert!(got.remote_log_segment_id().id == Uuid::from_u128(10));
    }

    fn one_finished_segment() -> RemoteLogMetadataCache {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().call()).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        c
    }

    fn two_finished_segments() -> RemoteLogMetadataCache {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().call()).unwrap();
        c.add(following_segment()).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();
        c
    }

    #[yare::parameterized(
        first_start = { 0, Some(Uuid::from_u128(10)) },
        first_end = { 99, Some(Uuid::from_u128(10)) },
        second_start = { 100, Some(Uuid::from_u128(11)) },
        second_end = { 199, Some(Uuid::from_u128(11)) },
        past_end = { 200, None },
    )]
    fn offset_lookup_across_segments_one_epoch(offset: i64, want: Option<Uuid>) {
        let c = two_finished_segments();
        check!(
            c.segment_for(LeaderEpoch(0), offset)
                .map(|s| s.remote_log_segment_id().id)
                == want,
            "offset={offset}"
        );
    }

    #[test]
    fn offset_lookup_rejects_offsets_below_segment_start_and_extreme_spans() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().start(100).end(199).call()).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        check!(c.segment_for(LeaderEpoch(0), 50).is_none());

        c.add(
            seg()
                .id(Uuid::from_u128(11))
                .epochs(&[epoch_start(LeaderEpoch(1), Offset(i64::MIN))])
                .start(Offset(i64::MIN))
                .end(Offset(i64::MAX))
                .call(),
        )
        .unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();
        check!(c.segment_for(LeaderEpoch(1), i64::MAX).is_none());
    }

    #[test]
    fn list_by_epoch_returns_matching_segments() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().call()).unwrap();
        assert!(
            c.list_by_epoch(LeaderEpoch(0)).is_empty(),
            "copy-started is not epoch-readable"
        );
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        let listed_ids: Vec<Uuid> = c
            .list_by_epoch(LeaderEpoch(0))
            .iter()
            .map(|s| s.remote_log_segment_id().id)
            .collect();
        assert!(listed_ids == [Uuid::from_u128(10)]);
        assert!(
            c.list_by_epoch(LeaderEpoch(7)).is_empty(),
            "unknown epoch -> empty"
        );
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        assert!(
            c.list_by_epoch(LeaderEpoch(0)).is_empty(),
            "delete-started is not epoch-readable"
        );
    }

    #[test]
    fn deindex_removes_epoch_slot() {
        let mut c = one_finished_segment();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(99));
        // DeleteSegmentStarted deindexes the epoch slot (but the metadata is
        // still present until DeleteSegmentFinished). highest_offset_for_epoch
        // reads the epoch index directly, so it must now miss.
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[yare::parameterized(
        epoch_zero_inside = { LeaderEpoch(0), 10, Some(Uuid::from_u128(10)) },
        epoch_zero_end = { LeaderEpoch(0), 49, Some(Uuid::from_u128(10)) },
        epoch_zero_after = { LeaderEpoch(0), 50, None },
        epoch_one_before_start = { LeaderEpoch(1), 49, None },
        epoch_one_at_start = { LeaderEpoch(1), 50, Some(Uuid::from_u128(10)) },
        epoch_zero_missing_segment = { LeaderEpoch(0), 150, None },
        epoch_one_first_segment = { LeaderEpoch(1), 60, Some(Uuid::from_u128(10)) },
        epoch_one_second_segment = { LeaderEpoch(1), 150, Some(Uuid::from_u128(11)) },
    )]
    fn offset_lookup_respects_epoch(epoch: LeaderEpoch, offset: i64, want: Option<Uuid>) {
        let mut c = RemoteLogMetadataCache::default();
        // One segment spanning two epochs: epoch 0 owns [0,49], epoch 1 owns [50,99].
        c.add(
            seg()
                .epochs(&[
                    epoch_start(LeaderEpoch(0), Offset(0)),
                    epoch_start(LeaderEpoch(1), Offset(50)),
                ])
                .call(),
        )
        .unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        // A second segment, epoch 1 only.
        c.add(following_segment_for_epochs(&[epoch_start(
            LeaderEpoch(1),
            Offset(100),
        )]))
        .unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();

        check!(
            c.segment_for(epoch, offset)
                .map(|s| s.remote_log_segment_id().id)
                == want,
            "epoch={epoch:?} offset={offset}"
        );
    }

    #[test]
    fn highest_offset_for_epoch_is_max_end() {
        let c = two_finished_segments();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(199));
        assert!(c.highest_offset_for_epoch(LeaderEpoch(7)) == None);
    }

    #[test]
    fn delete_started_hides_segment_delete_finished_drops_it() {
        let mut c = one_finished_segment();
        assert!(c.segment_for(LeaderEpoch(0), 50).is_some());

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        assert!(
            c.segment_for(LeaderEpoch(0), 50).is_none(),
            "delete-started hides it"
        );
        assert!(
            c.list().len() == 1,
            "still tracked while delete in progress"
        );

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .unwrap();
        assert!(c.list().is_empty(), "delete-finished drops it entirely");
    }

    #[test]
    fn update_unknown_segment_errors() {
        let mut c = RemoteLogMetadataCache::default();
        let err = c.update(&finish(Uuid::from_u128(404))).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn exact_update_and_absent_tombstone_retries_are_idempotent() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().call()).unwrap();
        let finished = finish(Uuid::from_u128(10));
        c.update(&finished).unwrap();
        c.update(&finished).expect("exact retry is a no-op");
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(99));

        let conflicting = transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::CopySegmentFinished,
        );
        let error = c.update(&conflicting).unwrap_err();
        check!(matches!(
            error,
            RemoteStorageError::InvalidSegmentTransition { .. }
        ));

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .unwrap();
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .expect("absent delete-finished tombstone is a no-op");
        check!(c.list().is_empty());
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn stale_update_cannot_reindex_a_deleting_segment() {
        let mut c = one_finished_segment();
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(c.update(&finish(Uuid::from_u128(10))).is_err());
        check!(c.segment_for(LeaderEpoch(0), 50).is_none());
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn add_with_wrong_state_errors() {
        let mut c = RemoteLogMetadataCache::default();
        let mut s = seg().call();
        s = s
            .with_update(&finish(Uuid::from_u128(10)))
            .expect("force to finished for the test");
        let err = c.add(s).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
    }

    #[test]
    fn duplicate_add_errors() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().call()).unwrap();
        let err = c.add(seg().call()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
    }

    #[test]
    fn dump_then_seed_rebuilds_epoch_index() {
        let mut c = two_finished_segments();
        c.update(&transition(
            Uuid::from_u128(11),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        c.set_delete_state(RemotePartitionDeleteState::DeletePartitionMarked);

        let segments = c.dump_segments();
        let delete_state = c.delete_state();

        let mut seeded = RemoteLogMetadataCache::default();
        seeded.seed(segments, delete_state);

        // Finished seg 10 is queryable; delete-started seg 11 is hidden
        // but still listed; delete_state survives.
        check!(
            seeded
                .segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(10)
        );
        check!(seeded.segment_for(LeaderEpoch(0), 150).is_none());
        check!(seeded.list().len() == 2);
        check!(seeded.delete_state() == Some(RemotePartitionDeleteState::DeletePartitionMarked));
    }

    #[test]
    fn repeated_seed_and_tombstone_duplicates_do_not_resurrect() {
        let started = seg().call();
        let finished = started.with_update(&finish(Uuid::from_u128(10))).unwrap();
        let deleting = finished
            .with_update(&transition(
                Uuid::from_u128(10),
                RemoteLogSegmentState::DeleteSegmentStarted,
            ))
            .unwrap();
        let deleted = deleting
            .with_update(&transition(
                Uuid::from_u128(10),
                RemoteLogSegmentState::DeleteSegmentFinished,
            ))
            .unwrap();
        let dump = vec![finished, deleted, started];

        let mut c = RemoteLogMetadataCache::default();
        c.seed(dump.clone(), None);
        check!(c.list().is_empty());
        check!(c.segment_for(LeaderEpoch(0), 50).is_none());
        c.seed(dump, None);
        check!(c.list().is_empty());
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn index_collision_is_deterministic_and_reveals_the_remaining_finished_segment() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg().id(Uuid::from_u128(11)).call()).unwrap();
        c.add(seg().call()).unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        check!(
            c.segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(10)
        );

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(
            c.segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(11)
        );
    }

    /// Every `(epoch, start, id)` entry of finished primary state: what the
    /// index held when each transition rebuilt it from scratch.
    fn rebuilt_entries(c: &RemoteLogMetadataCache) -> BTreeSet<(LeaderEpoch, i64, Uuid)> {
        c.id_to_metadata
            .values()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .flat_map(|md| {
                let id = md.remote_log_segment_id().id;
                md.segment_leader_epochs()
                    .iter()
                    .map(move |(&epoch, &start)| (epoch, start, id))
            })
            .collect()
    }

    /// The slot owners of a full rebuild: the lowest UUID per
    /// `(epoch, start)`.
    fn rebuilt_owners(c: &RemoteLogMetadataCache) -> BTreeMap<(LeaderEpoch, i64), Uuid> {
        let mut owners = BTreeMap::new();
        for (epoch, start, id) in rebuilt_entries(c) {
            owners.entry((epoch, start)).or_insert(id);
        }
        owners
    }

    fn indexed_entries(c: &RemoteLogMetadataCache) -> BTreeSet<(LeaderEpoch, i64, Uuid)> {
        c.epoch_index
            .0
            .iter()
            .flat_map(|(&epoch, entries)| {
                entries.iter().map(move |&(start, id)| (epoch, start, id))
            })
            .collect()
    }

    fn indexed_owners(c: &RemoteLogMetadataCache) -> BTreeMap<(LeaderEpoch, i64), Uuid> {
        c.epoch_index
            .0
            .keys()
            .flat_map(|&epoch| {
                c.epoch_index
                    .owners(epoch)
                    .map(move |(start, id)| ((epoch, start), id))
            })
            .collect()
    }

    #[test]
    fn deleting_a_collision_owner_hands_the_slot_to_the_next_lowest_uuid() {
        let mut c = RemoteLogMetadataCache::default();
        for id in [12, 10, 11] {
            c.add(seg().id(Uuid::from_u128(id)).call()).unwrap();
            c.update(&finish(Uuid::from_u128(id))).unwrap();
        }
        let owner = |c: &RemoteLogMetadataCache| {
            c.segment_for(LeaderEpoch(0), 50)
                .map(|md| md.remote_log_segment_id().id)
        };
        check!(owner(&c) == Some(Uuid::from_u128(10)));

        for state in [
            RemoteLogSegmentState::DeleteSegmentStarted,
            RemoteLogSegmentState::DeleteSegmentFinished,
        ] {
            c.update(&transition(Uuid::from_u128(10), state)).unwrap();
            check!(owner(&c) == Some(Uuid::from_u128(11)), "{state:?}");
        }
        c.update(&transition(
            Uuid::from_u128(11),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(owner(&c) == Some(Uuid::from_u128(12)));
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(99));

        c.update(&transition(
            Uuid::from_u128(12),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(owner(&c) == None);
        check!(indexed_entries(&c).is_empty());
        check!(c.epoch_index.0.is_empty(), "an emptied epoch is dropped");
    }

    #[test]
    fn a_transition_leaves_other_segments_entries_in_place() {
        let mut c = two_finished_segments();
        c.add(
            seg()
                .id(Uuid::from_u128(12))
                .epochs(&[epoch_start(LeaderEpoch(1), Offset(200))])
                .start(Offset(200))
                .end(Offset(299))
                .call(),
        )
        .unwrap();
        c.update(&finish(Uuid::from_u128(12))).unwrap();
        c.update(&transition(
            Uuid::from_u128(11),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(
            indexed_entries(&c)
                == BTreeSet::from([
                    (LeaderEpoch(0), 0, Uuid::from_u128(10)),
                    (LeaderEpoch(1), 200, Uuid::from_u128(12)),
                ])
        );
    }

    /// One step of a lifecycle history over a small pool of segments.
    #[derive(Debug, Clone)]
    enum Step {
        Add(usize),
        Update {
            slot: usize,
            state: RemoteLogSegmentState,
            event_timestamp_ms: i64,
        },
        Reseed,
    }

    const POOL: usize = 5;

    fn pool_segment(slot: usize, epochs: &BTreeMap<LeaderEpoch, i64>) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(10 + slot as u128)),
            0,
            199,
            199,
            1,
            100,
            RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                epochs.clone(),
            ),
        )
        .unwrap()
    }

    fn any_step() -> impl Strategy<Value = Step> {
        prop_oneof![
            (0..POOL).prop_map(Step::Add),
            (
                0..POOL,
                prop_oneof![
                    Just(RemoteLogSegmentState::CopySegmentStarted),
                    Just(RemoteLogSegmentState::CopySegmentFinished),
                    Just(RemoteLogSegmentState::DeleteSegmentStarted),
                    Just(RemoteLogSegmentState::DeleteSegmentFinished),
                ],
                prop_oneof![Just(200_i64), Just(300_i64)],
            )
                .prop_map(|(slot, state, event_timestamp_ms)| Step::Update {
                    slot,
                    state,
                    event_timestamp_ms,
                }),
            Just(Step::Reseed),
        ]
    }

    /// Few epochs and starts, so pool segments collide on slots often.
    fn any_epochs() -> impl Strategy<Value = BTreeMap<LeaderEpoch, i64>> {
        prop::collection::btree_map(
            (0..3_i32).prop_map(LeaderEpoch),
            prop_oneof![Just(0_i64), Just(50_i64), Just(100_i64)],
            1..=3,
        )
    }

    proptest! {
        /// Whatever history of adds, accepted and rejected transitions and
        /// reseeds runs, the incrementally kept index holds exactly the
        /// entries, and the slot owners, that a full rebuild from primary
        /// state produces, and lookups answer from those owners.
        #[test]
        fn incremental_index_matches_a_full_rebuild(
            pool in prop::collection::vec(any_epochs(), POOL),
            steps in prop::collection::vec(any_step(), 0..40),
        ) {
            let mut c = RemoteLogMetadataCache::default();
            for step in steps {
                match step {
                    Step::Add(slot) => {
                        let _ = c.add(pool_segment(slot, &pool[slot]));
                    }
                    Step::Update { slot, state, event_timestamp_ms } => {
                        let _ = c.update(&RemoteLogSegmentMetadataUpdate {
                            remote_log_segment_id: RemoteLogSegmentId::new(
                                tp(),
                                Uuid::from_u128(10 + slot as u128),
                            ),
                            event_timestamp_ms,
                            custom_metadata: None,
                            state,
                            broker_id: 1,
                        });
                    }
                    Step::Reseed => {
                        let segments = c.dump_segments();
                        c.seed(segments, None);
                    }
                }
                prop_assert!(indexed_entries(&c) == rebuilt_entries(&c));
                let owners = rebuilt_owners(&c);
                prop_assert!(indexed_owners(&c) == owners);
                prop_assert!(c.epoch_index.0.values().all(|entries| !entries.is_empty()));
                for epoch in (0..3).map(LeaderEpoch) {
                    for offset in [0, 49, 50, 99, 100, 199] {
                        let want = owners
                            .range((epoch, i64::MIN)..=(epoch, offset))
                            .next_back()
                            .map(|(_, &id)| id)
                            .filter(|id| c.id_to_metadata[id].epoch_bounds(epoch).1.is_none_or(|next| offset < next));
                        prop_assert!(
                            c.segment_for(epoch, offset).map(|md| md.remote_log_segment_id().id) == want,
                            "epoch={epoch:?} offset={offset}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn list_is_ordered_by_start_offset() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(following_segment()).unwrap();
        c.add(seg().call()).unwrap();
        let listed = c.list();
        assert!(listed[0].start_offset() == 0);
        assert!(listed[1].start_offset() == 100);
    }
}
