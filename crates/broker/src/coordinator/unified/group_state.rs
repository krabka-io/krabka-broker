//! Every per-group map of the [`GroupCoordinator`], searched and cleared in
//! one place.
//!
//! A broker that loses a `__consumer_offsets` partition must find each group
//! it holds any state for and drop all of it. Listing the maps here, side by
//! side, keeps a new per-group map from being left out of one of those lists.

use {super::group_coordinator::GroupCoordinator, dashmap::DashMap, std::collections::HashSet};

impl GroupCoordinator {
    /// Every group id that any per-group map names: a live classic, consumer,
    /// share or streams actor, a type lock, a replayed or cached seed, or a
    /// respawn seed.
    ///
    /// No single map is enough. An offset-only group has a classic actor but
    /// no type lock, and a respawn seed can outlive its actor's entry.
    #[must_use]
    pub(crate) fn known_group_ids(&self) -> HashSet<String> {
        let mut group_ids = HashSet::new();
        group_ids.extend(keys(&self.group_types));
        group_ids.extend(keys(&self.groups));
        group_ids.extend(keys(&self.share_groups));
        group_ids.extend(keys(&self.streams_groups));
        group_ids.extend(keys(&self.seeds));
        group_ids.extend(keys(&self.seeds_cache));
        group_ids.extend(keys(&self.respawn_seeds));
        group_ids.extend(keys(&self.share_seeds));
        group_ids.extend(keys(&self.share_seeds_cache));
        group_ids.extend(keys(&self.streams_seeds));
        group_ids.extend(keys(&self.streams_seeds_cache));
        group_ids
    }

    /// Drops the replayed and cached seeds, the respawn seed and the type
    /// lock of `group_id`.
    ///
    /// It leaves the group's actors to the caller, which has to shut each one
    /// down. Dropping the respawn seed detaches the commits it still held, as
    /// [`drop_respawn_seed`](Self::drop_respawn_seed) describes.
    pub(crate) fn forget_group_state(&self, group_id: &str) {
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
        self.drop_respawn_seed(group_id);
        self.share_seeds.remove(group_id);
        self.share_seeds_cache.remove(group_id);
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        self.group_types.remove(group_id);
    }
}

/// The group ids `map` holds an entry for.
fn keys<V>(map: &DashMap<String, V>) -> impl Iterator<Item = String> + '_ {
    map.iter().map(|entry| entry.key().clone())
}
