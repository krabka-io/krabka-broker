//! The actor registry: the lookups that hand out a group's actor handle, the
//! spawn-on-miss and respawn-on-death paths behind them, and the bootstrap
//! finalize pass that turns replayed seeds into live actors.
//!
//! One registry exists per group protocol, and all of them share the same
//! dead-actor detection and hydrate-from-cache rules, so they are defined
//! together.

use {
    super::{
        actor::{self, GroupActorHandle, GroupActorMessage, GroupKindTag},
        group::CoordinatorGroup,
        group_coordinator::{GroupCoordinator, GroupType},
        seeds::{GroupSeed, ShareGroupSeed, StreamsGroupSeed},
        share::actor::{ShareGroupActorHandle, ShareGroupActorMessage},
        streams::actor::{StreamsGroupActorHandle, StreamsGroupActorMessage},
    },
    dashmap::{DashMap, Entry},
    std::sync::Arc,
    tokio::sync::mpsc::error::TrySendError,
};

/// What a registry needs from an actor handle: whether the actor has exited,
/// and how to hydrate a freshly-spawned one.
trait RegisteredActor {
    type Seed;

    /// `true` once the actor has exited and its mailbox is closed.
    fn is_closed(&self) -> bool;

    /// Hands `seed` to the actor without waiting.
    ///
    /// # Errors
    ///
    /// Fails, dropping `seed`, when the mailbox is full or closed.
    fn try_seed(&self, seed: Self::Seed) -> Result<(), TrySendError<()>>;
}

/// `error` without the message it could not deliver.
fn undelivered<T>(error: &TrySendError<T>) -> TrySendError<()> {
    match error {
        TrySendError::Full(_) => TrySendError::Full(()),
        TrySendError::Closed(_) => TrySendError::Closed(()),
    }
}

impl RegisteredActor for GroupActorHandle {
    type Seed = GroupSeed;

    fn is_closed(&self) -> bool { self.tx.is_closed() }

    fn try_seed(&self, seed: GroupSeed) -> Result<(), TrySendError<()>> {
        self.tx
            .try_send(GroupActorMessage::Seed(seed))
            .map_err(|error| undelivered(&error))
    }
}

impl RegisteredActor for ShareGroupActorHandle {
    type Seed = ShareGroupSeed;

    fn is_closed(&self) -> bool { self.tx.is_closed() }

    fn try_seed(&self, seed: ShareGroupSeed) -> Result<(), TrySendError<()>> {
        self.tx
            .try_send(ShareGroupActorMessage::Seed(seed))
            .map_err(|error| undelivered(&error))
    }
}

impl RegisteredActor for StreamsGroupActorHandle {
    type Seed = StreamsGroupSeed;

    fn is_closed(&self) -> bool { self.tx.is_closed() }

    fn try_seed(&self, seed: StreamsGroupSeed) -> Result<(), TrySendError<()>> {
        self.tx
            .try_send(StreamsGroupActorMessage::Seed(seed))
            .map_err(|error| undelivered(&error))
    }
}

/// The live actor that `map` holds for `group_id`, or a fresh one from
/// `spawn` when the entry is absent or its actor has exited.
///
/// A fresh actor is hydrated with what `seed` returns before it is
/// registered, so the seed is the first message it reads. When another caller
/// registers a live actor first, that actor is returned untouched and the
/// fresh one is dropped: one group never has two actors, and a live actor is
/// never seeded twice.
fn get_or_spawn<H: RegisteredActor>(
    map: &DashMap<String, Arc<H>>,
    group_id: &str,
    spawn: impl FnOnce() -> H,
    seed: impl FnOnce() -> Option<H::Seed>,
) -> Arc<H> {
    // Dead-actor detection: a closed mpsc sender means the actor exited
    // (typically after a log-write failure), and the entry is replaced.
    if let Some(h) = map.get(group_id)
        && !h.value().is_closed()
    {
        return Arc::clone(h.value());
    }
    let spawned = Arc::new(spawn());
    if let Some(seed) = seed()
        && let Err(error) = spawned.try_seed(seed)
    {
        tracing::warn!(group_id, %error, "dropped the seed of a respawned group actor");
    }
    match map.entry(group_id.into()) {
        Entry::Occupied(entry) if !entry.get().is_closed() => Arc::clone(entry.get()),
        Entry::Occupied(mut entry) => {
            entry.insert(Arc::clone(&spawned));
            spawned
        }
        Entry::Vacant(entry) => {
            entry.insert(Arc::clone(&spawned));
            spawned
        }
    }
}

/// The `GROUP_ID_NOT_FOUND` message of a `ConsumerGroupHeartbeat` that names a
/// group that does not exist: Kafka's `getOrMaybeCreateConsumerGroup` says
/// `Consumer group <id> not found.`, and `consumerGroupLeave` reaches the
/// generic `Group <id> not found.` through `consumerGroup`.
pub fn consumer_group_not_found(group_id: &str, member_epoch: i32) -> String {
    if member_epoch < 0 {
        format!("Group {group_id} not found.")
    } else {
        format!("Consumer group {group_id} not found.")
    }
}

impl GroupCoordinator {
    /// Get the one actor for `group_id`, and spawn it with `initial_kind` when
    /// it is absent.
    ///
    /// The kind argument only decides the spawn kind for a brand-new group.
    /// Both families route to one actor; the actor rejects the family it does
    /// not currently serve and can change kind in place, so a group is not
    /// pinned to its spawn kind. Keeps the dead-actor
    /// (closed tx) respawn and the consumer re-hydrate-from-seed paths.
    #[must_use]
    pub fn get_or_create_group(
        self: &Arc<Self>,
        group_id: &str,
        initial_kind: GroupKindTag,
    ) -> Arc<GroupActorHandle> {
        get_or_spawn(
            &self.groups,
            group_id,
            || {
                GroupActorHandle::spawn(
                    group_id.into(),
                    initial_kind,
                    Arc::clone(&self.config),
                    Arc::clone(&self.metadata),
                    Arc::clone(&self.offsets_log),
                    Arc::clone(self),
                )
            },
            // Re-hydrate a respawned consumer actor from its last-known-good
            // state.
            || {
                if initial_kind == GroupKindTag::Consumer {
                    self.cached_seed(group_id)
                } else {
                    None
                }
            },
        )
    }

    /// Get or create a classic-protocol actor.
    ///
    /// This method spawns a classic actor for a brand-new id. For an id that
    /// exists, it returns the one actor whatever its kind. The actor then
    /// serves or rejects the request per its live kind.
    #[must_use]
    pub fn get_or_create_classic(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Classic)
    }

    /// Get or create a next-gen consumer-protocol actor.
    ///
    /// This method spawns a consumer actor for a brand-new id. For an id that
    /// exists, it returns the one actor whatever its kind. The actor then
    /// serves or rejects the request per its live kind.
    #[must_use]
    pub fn get_or_create_consumer(self: &Arc<Self>, group_id: &str) -> Arc<GroupActorHandle> {
        self.get_or_create_group(group_id, GroupKindTag::Consumer)
    }

    #[must_use]
    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| Arc::clone(e.value()))
    }

    #[must_use]
    pub fn get_or_create_share(self: &Arc<Self>, group_id: &str) -> Arc<ShareGroupActorHandle> {
        get_or_spawn(
            &self.share_groups,
            group_id,
            || {
                ShareGroupActorHandle::spawn(
                    group_id.into(),
                    Arc::clone(&self.share_config),
                    Arc::clone(&self.metadata),
                    Arc::clone(self),
                )
            },
            || self.cached_share_seed(group_id),
        )
    }

    #[must_use]
    pub fn find_share(&self, group_id: &str) -> Option<Arc<ShareGroupActorHandle>> {
        self.share_groups
            .get(group_id)
            .map(|e| Arc::clone(e.value()))
    }

    /// Snapshot the ids of every live share group, per KIP-932.
    ///
    /// The call is synchronous and cheap. It reads the registry keys and makes
    /// no actor round-trip. `ListGroups`, `api_key` 16, can therefore include
    /// share groups together with classic ones without the per-group
    /// `Describe` mpsc hop.
    #[must_use]
    pub fn share_group_ids(&self) -> Vec<String> {
        self.share_groups.iter().map(|e| e.key().clone()).collect()
    }

    // ── KIP-1071 streams-group registry ──────────────────────────────────

    #[must_use]
    pub fn get_or_create_streams(self: &Arc<Self>, group_id: &str) -> Arc<StreamsGroupActorHandle> {
        get_or_spawn(
            &self.streams_groups,
            group_id,
            || {
                StreamsGroupActorHandle::spawn(
                    group_id.into(),
                    Arc::clone(&self.streams_config),
                    Arc::clone(self),
                )
            },
            || self.cached_streams_seed(group_id),
        )
    }

    #[must_use]
    pub fn find_streams(&self, group_id: &str) -> Option<Arc<StreamsGroupActorHandle>> {
        self.streams_groups
            .get(group_id)
            .map(|e| Arc::clone(e.value()))
    }

    /// The actor of the streams group `group_id`, when the group exists.
    ///
    /// Unlike [`find_streams`](Self::find_streams), it never returns the
    /// closed handle of an actor that exited after a failed write: it
    /// respawns that actor from the group's replayed state, as
    /// [`get_or_create_streams`](Self::get_or_create_streams) does. It never
    /// creates a group: one deleted or converted after the lookup stays gone.
    #[must_use]
    pub fn find_live_streams(
        self: &Arc<Self>,
        group_id: &str,
    ) -> Option<Arc<StreamsGroupActorHandle>> {
        let found = self.find_streams(group_id)?;
        if found.tx.is_closed() {
            self.respawn_closed_streams(group_id, &found)
        } else {
            Some(found)
        }
    }

    /// Replaces the closed actor `closed` of `group_id` with a fresh one,
    /// seeded from the group's replayed state, only while the registry still
    /// holds `closed`. A group removed since answers `None`, and an actor
    /// that replaced `closed` since is returned as it is.
    fn respawn_closed_streams(
        self: &Arc<Self>,
        group_id: &str,
        closed: &Arc<StreamsGroupActorHandle>,
    ) -> Option<Arc<StreamsGroupActorHandle>> {
        let respawned = match self.streams_groups.entry(group_id.into()) {
            Entry::Vacant(_) => return None,
            Entry::Occupied(entry) if !Arc::ptr_eq(entry.get(), closed) => {
                return Some(Arc::clone(entry.get()));
            }
            Entry::Occupied(mut entry) => {
                let respawned = Arc::new(StreamsGroupActorHandle::spawn(
                    group_id.into(),
                    Arc::clone(&self.streams_config),
                    Arc::clone(self),
                ));
                entry.insert(Arc::clone(&respawned));
                respawned
            }
        };
        if let Some(seed) = self.cached_streams_seed(group_id) {
            let _ = respawned.tx.try_send(StreamsGroupActorMessage::Seed(seed));
        }
        Some(respawned)
    }

    /// Snapshot the ids of every live streams group, per KIP-1071.
    ///
    /// The method is the counterpart of
    /// [`share_group_ids`](Self::share_group_ids). `ListGroups` uses it to
    /// emit `group_type="streams"` entries without a per-group `Describe` hop.
    #[must_use]
    pub fn streams_group_ids(&self) -> Vec<String> {
        self.streams_groups
            .iter()
            .map(|e| e.key().clone())
            .collect()
    }

    /// Ids of every live next-gen KIP-848 consumer group actor.
    ///
    /// The method is the counterpart of
    /// [`share_group_ids`](Self::share_group_ids). `ListGroups` uses it to
    /// emit `group_type="consumer"` entries without an actor round-trip.
    ///
    /// Note: this method returns all group ids from the shared `groups` map,
    /// classic groups included. The `emitted` dedup set in the `ListGroups`
    /// handler prevents a double wire emission. A classic group therefore goes
    /// out once as `group_type="classic"` and not again here.
    pub fn consumer_group_ids(&self) -> Vec<String> {
        self.groups.iter().map(|e| e.key().clone()).collect()
    }

    /// `true` when `group_id` names a share or streams group, which is never a
    /// consumer group.
    #[must_use]
    pub fn is_share_or_streams_group(&self, group_id: &str) -> bool {
        matches!(
            self.group_type(group_id),
            Some(GroupType::Share | GroupType::Streams)
        ) || self.find_share(group_id).is_some()
            || self.find_streams(group_id).is_some()
    }

    /// The `error_message` of the `GROUP_ID_NOT_FOUND` answer to a
    /// `ConsumerGroupHeartbeat` for `group_id`, or `None` when the heartbeat
    /// may go to the consumer-group actor.
    ///
    /// It follows Kafka's `GroupMetadataManager`: `getOrMaybeCreateConsumerGroup`
    /// creates a missing group only when `member_epoch == 0` and refuses a
    /// share or streams group, and `consumerGroupLeave` looks the group up
    /// through `consumerGroup`, which refuses a missing group with the generic
    /// message. A classic group is the actor's to judge, because a join may
    /// convert an empty one.
    #[must_use]
    pub fn consumer_group_lookup_error(&self, group_id: &str, member_epoch: i32) -> Option<String> {
        if self.is_share_or_streams_group(group_id) {
            return Some(format!("Group {group_id} is not a consumer group."));
        }
        (member_epoch != 0 && self.find(group_id).is_none())
            .then(|| consumer_group_not_found(group_id, member_epoch))
    }

    /// Spawn a classic actor seeded with a fully-replayed `Group` at
    /// bootstrap.
    pub fn seed_classic(self: &Arc<Self>, group_id: &str, group: Box<CoordinatorGroup>) {
        let handle = self.get_or_create_classic(group_id);
        let _ = handle.tx.try_send(GroupActorMessage::ClassicSeed(group));
    }

    pub fn finalize_bootstrap(self: &Arc<Self>) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid) {
                let handle = self.get_or_create_consumer(&gid);
                let _ = handle.tx.try_send(actor::GroupActorMessage::Seed(seed));
            }
        }
        let share_ids: Vec<String> = self.share_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in share_ids {
            if let Some((_, seed)) = self.share_seeds.remove(&gid) {
                let handle = self.get_or_create_share(&gid);
                let _ = handle.tx.try_send(ShareGroupActorMessage::Seed(seed));
            }
        }
        let streams_ids: Vec<String> = self.streams_seeds.iter().map(|e| e.key().clone()).collect();
        for gid in streams_ids {
            if let Some((_, seed)) = self.streams_seeds.remove(&gid) {
                let handle = self.get_or_create_streams(&gid);
                let _ = handle.tx.try_send(StreamsGroupActorMessage::Seed(seed));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            actor::MetadataProvider,
            config::NextGenConfig,
            seeds::{ShareGroupSeed, StreamsGroupSeed},
            share::config::ShareGroupConfig,
            streams::config::StreamsGroupConfig,
            test_support::{ImageMetadatalessProvider, make_coord, stop_streams_actor},
        },
        assert2::{assert, check},
        std::{
            iter,
            sync::{
                Barrier, Mutex,
                atomic::{AtomicBool, Ordering},
            },
            thread,
        },
        tokio::sync::{mpsc, oneshot},
    };

    #[tokio::test]
    async fn actor_mailboxes_use_component_configuration() {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;

        let metadata: Arc<dyn MetadataProvider> = Arc::new(ImageMetadatalessProvider);
        let coord = Arc::new(
            GroupCoordinator::new(
                NextGenConfig {
                    actor_mailbox_capacity: 3,
                    ..NextGenConfig::default()
                },
                ShareGroupConfig {
                    actor_mailbox_capacity: 5,
                    ..ShareGroupConfig::default()
                },
                metadata,
                Arc::new(InMemoryOffsetsLog::default()),
                StreamsGroupConfig {
                    actor_mailbox_capacity: 7,
                    ..StreamsGroupConfig::default()
                },
            )
            .expect("valid test assignor configuration"),
        );

        assert!(coord.get_or_create_classic("classic").tx.max_capacity() == 3);
        assert!(coord.get_or_create_share("share").tx.max_capacity() == 5);
        assert!(coord.get_or_create_streams("streams").tx.max_capacity() == 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_group_returns_the_one_actor_regardless_of_kind() {
        // KIP-848 live migration: BOTH RPC families route to the one actor.
        // The kind argument only decides the spawn kind for a brand-new group;
        // a later request of the other kind returns the SAME actor (the kind
        // lock now lives in the actor's message arms, not in this registry).
        let coord = make_coord();
        let a = coord.get_or_create_group("g", GroupKindTag::Classic);
        let b = coord.get_or_create_group("g", GroupKindTag::Consumer);
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_or_create_share_is_idempotent() {
        let coord = make_coord();
        let a = coord.get_or_create_share("sg");
        let b = coord.get_or_create_share("sg");
        assert!(Arc::ptr_eq(&a, &b));
        assert!(coord.find_share("sg").is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn share_and_streams_registries_report_live_ids() {
        let coord = make_coord();
        let share_a = coord.get_or_create_share("share-a");
        let share_b = coord.get_or_create_share("share-b");
        check!(Arc::ptr_eq(&share_a, &coord.get_or_create_share("share-a")));
        check!(coord.find_share("share-b").is_some());
        check!(!Arc::ptr_eq(&share_a, &share_b));

        let streams_a = coord.get_or_create_streams("streams-a");
        assert!(Arc::ptr_eq(
            &streams_a,
            &coord.get_or_create_streams("streams-a")
        ));
        assert!(Arc::ptr_eq(
            &streams_a,
            &coord.find_streams("streams-a").unwrap()
        ));

        let mut share_ids = coord.share_group_ids();
        share_ids.sort();
        assert!(share_ids == vec!["share-a".to_owned(), "share-b".to_owned()]);

        let mut streams_ids = coord.streams_group_ids();
        streams_ids.sort();
        assert!(streams_ids == vec!["streams-a".to_owned()]);

        coord.shutdown_all().await;
    }

    /// Sends `Shutdown` down `tx` and waits until the actor has exited and
    /// closed its mailbox, as an actor does after a failed log write.
    async fn stop_actor<M>(tx: &mpsc::Sender<M>, shutdown: impl FnOnce(oneshot::Sender<()>) -> M) {
        let (reply, stopping) = oneshot::channel();
        tx.send(shutdown(reply)).await.expect("send Shutdown");
        stopping.await.expect("the actor stops");
        tx.closed().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_consumer_actor_respawns_hydrated_from_the_cached_seed() {
        let coord = make_coord();
        let closed = coord.get_or_create_consumer("g");
        stop_actor(&closed.tx, GroupActorMessage::Shutdown).await;
        coord.update_cached_seed("g", |seed| seed.group_epoch = 7);

        let fresh = coord.get_or_create_consumer("g");

        check!(!Arc::ptr_eq(&fresh, &closed));
        check!(!fresh.tx.is_closed());
        check!(coord.find("g").is_some_and(|r| Arc::ptr_eq(&r, &fresh)));
        let (reply, view) = oneshot::channel();
        fresh
            .tx
            .send(GroupActorMessage::Describe { reply })
            .await
            .expect("send Describe");
        assert!(view.await.expect("a describe view").group_epoch == 7);
        coord.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_share_actor_respawns_hydrated_from_the_cached_seed() {
        let coord = make_coord();
        let closed = coord.get_or_create_share("sg");
        stop_actor(&closed.tx, ShareGroupActorMessage::Shutdown).await;
        coord.update_share_cache("sg", ShareGroupSeed {
            group_epoch: 17,
            ..ShareGroupSeed::default()
        });

        let fresh = coord.get_or_create_share("sg");

        check!(!Arc::ptr_eq(&fresh, &closed));
        check!(!fresh.tx.is_closed());
        check!(
            coord
                .find_share("sg")
                .is_some_and(|r| Arc::ptr_eq(&r, &fresh))
        );
        let (reply, view) = oneshot::channel();
        fresh
            .tx
            .send(ShareGroupActorMessage::Describe { reply })
            .await
            .expect("send Describe");
        assert!(view.await.expect("a describe view").group_epoch == 17);
        coord.shutdown_all().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_streams_actor_respawns_hydrated_from_the_cached_seed() {
        let coord = make_coord();
        let closed = coord.get_or_create_streams("st");
        stop_actor(&closed.tx, StreamsGroupActorMessage::Shutdown).await;
        coord.update_streams_cache("st", StreamsGroupSeed {
            group_epoch: 27,
            ..StreamsGroupSeed::default()
        });

        let fresh = coord.get_or_create_streams("st");

        check!(!Arc::ptr_eq(&fresh, &closed));
        check!(!fresh.tx.is_closed());
        check!(
            coord
                .find_streams("st")
                .is_some_and(|r| Arc::ptr_eq(&r, &fresh))
        );
        let (reply, view) = oneshot::channel();
        fresh
            .tx
            .send(StreamsGroupActorMessage::Describe { reply })
            .await
            .expect("send Describe");
        assert!(view.await.expect("a describe view").group_epoch == 27);
        coord.shutdown_all().await;
    }

    /// A registry entry that records every seed it is handed, standing in for
    /// an actor handle so a test can choose when it reads as closed.
    #[derive(Debug, Default)]
    struct FakeActor {
        closed: AtomicBool,
        seeds: Mutex<Vec<u32>>,
    }

    impl FakeActor {
        fn closed() -> Self {
            Self {
                closed: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn seeds(&self) -> Vec<u32> { self.seeds.lock().expect("seeds lock").clone() }
    }

    impl RegisteredActor for FakeActor {
        type Seed = u32;

        fn is_closed(&self) -> bool { self.closed.load(Ordering::SeqCst) }

        fn try_seed(&self, seed: u32) -> Result<(), TrySendError<()>> {
            self.seeds.lock().expect("seeds lock").push(seed);
            Ok(())
        }
    }

    #[test]
    fn a_respawn_keeps_and_does_not_seed_an_actor_another_caller_registered() {
        let map = DashMap::new();
        map.insert("g".to_owned(), Arc::new(FakeActor::closed()));
        let winner = Arc::new(FakeActor::default());

        // Another caller respawns the group while this one spawns its own.
        let answer = get_or_spawn(
            &map,
            "g",
            || {
                map.insert("g".to_owned(), Arc::clone(&winner));
                FakeActor::default()
            },
            || Some(7),
        );

        check!(Arc::ptr_eq(&answer, &winner));
        check!(
            map.get("g")
                .is_some_and(|r| Arc::ptr_eq(r.value(), &winner))
        );
        assert!(winner.seeds() == Vec::<u32>::new());
    }

    #[test]
    fn racing_respawns_of_a_closed_actor_agree_on_one_actor_seeded_once() {
        const CALLERS: usize = 8;
        for _ in 0..200 {
            let map = DashMap::new();
            map.insert("g".to_owned(), Arc::new(FakeActor::closed()));
            let start = Barrier::new(CALLERS);

            let answers: Vec<Arc<FakeActor>> = thread::scope(|scope| {
                let callers: Vec<_> = iter::repeat_with(|| {
                    scope.spawn(|| {
                        start.wait();
                        get_or_spawn(&map, "g", FakeActor::default, || Some(7))
                    })
                })
                .take(CALLERS)
                .collect();
                callers
                    .into_iter()
                    .map(|caller| caller.join().expect("the caller finishes"))
                    .collect()
            });

            let registered = Arc::clone(map.get("g").expect("a registered actor").value());
            check!(!registered.is_closed());
            check!(answers.iter().all(|a| Arc::ptr_eq(a, &registered)));
            assert!(registered.seeds() == vec![7]);
        }
    }

    /// What the registry holds for the group when `respawn_closed_streams`
    /// runs, after a lookup found its closed actor.
    #[derive(Debug, Clone, Copy)]
    enum SinceLookup {
        Unchanged,
        Removed,
        Replaced,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_streams_actor_respawns_only_while_still_registered() {
        for since in [
            SinceLookup::Unchanged,
            SinceLookup::Removed,
            SinceLookup::Replaced,
        ] {
            let coord = make_coord();
            let _ = coord.get_or_create_streams("g");
            stop_streams_actor(&coord, "g").await;
            let closed = coord.find_streams("g").expect("the closed actor");
            let replacement = match since {
                SinceLookup::Unchanged => None,
                SinceLookup::Removed => {
                    coord.streams_groups.remove("g");
                    None
                }
                SinceLookup::Replaced => Some(coord.get_or_create_streams("g")),
            };

            let answer = coord.respawn_closed_streams("g", &closed);

            let registered = coord.find_streams("g");
            match since {
                SinceLookup::Unchanged => {
                    let answer = answer.expect("a respawned actor");
                    check!(!answer.tx.is_closed(), "{since:?}");
                    check!(!Arc::ptr_eq(&answer, &closed), "{since:?}");
                    check!(
                        registered.is_some_and(|r| Arc::ptr_eq(&r, &answer)),
                        "{since:?}"
                    );
                }
                SinceLookup::Removed => {
                    check!(answer.is_none(), "{since:?}");
                    check!(registered.is_none(), "{since:?}");
                }
                SinceLookup::Replaced => {
                    let replacement = replacement.expect("the replacement");
                    check!(
                        answer.is_some_and(|a| Arc::ptr_eq(&a, &replacement)),
                        "{since:?}"
                    );
                    check!(
                        registered.is_some_and(|r| Arc::ptr_eq(&r, &replacement)),
                        "{since:?}"
                    );
                }
            }
            coord.shutdown_all().await;
        }
    }
}
