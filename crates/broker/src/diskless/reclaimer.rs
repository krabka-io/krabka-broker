//! Reclaim unreferenced diskless objects under their index publication fences.
//!
//! A broker decides what to delete from its own projection of
//! `__diskless_wal_index`, and that projection can stall while the index
//! topic keeps moving. So a sweep never trusts the projection as it stands:
//! it first brings the projection up to a fence committed to every index
//! partition, and skips the sweep when it cannot. One sweep:
//!
//! 1. Takes a fresh view, then lists `<prefix>/diskless-wal/`. An object that
//!    view references or reserves is kept, and its grace period restarts.
//! 2. For the objects unreferenced for the whole grace period, takes a second
//!    fresh view after the listing. The listing saw each object's bytes, and a
//!    writer at the reservation level commits its reservation before the
//!    upload, so this view holds that reservation unless the writer already
//!    released it. An object this view references or reserves is kept.
//! 3. Takes a third fresh view before any DELETE, and deletes only what that
//!    view does not reference either. A writer releases its reservation only
//!    after its ranges are committed, so a release that the second view held
//!    means ranges the third view holds, whichever index partitions they
//!    landed on.
//!
//! The final reference check runs under the object's local fence, so a
//! projection addition on this broker cannot interleave with the DELETE.
//! Objects written below the reservation level carry no reservation, so for
//! them the grace period is the only cover for a delayed index publication, as
//! in 1.0.0.

use {
    super::index_log::DisklessIndexLog,
    futures_util::TryStreamExt as _,
    object_store::{ObjectMeta, ObjectStore, ObjectStoreExt as _},
    std::{
        collections::{HashMap, HashSet},
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// Every broker sweeps the shared prefix, including objects from removed brokers.
pub(super) const RECLAIM_INTERVAL: Duration = Duration::from_secs(30);
/// Allow independent index consumers to apply replacements before deleting objects.
pub(super) const RECLAIM_GRACE: Duration = Duration::from_mins(5);
/// How long one fresh view may take, fence publication included, before the
/// sweep gives up and returns to the flusher loop.
const RECLAIM_FRESHNESS_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct Reclaimer {
    index_log: DisklessIndexLog,
    object_store: Arc<dyn ObjectStore>,
    object_prefix: Option<String>,
    first_seen: HashMap<String, Instant>,
    grace: Duration,
    freshness_timeout: Duration,
}

impl Reclaimer {
    pub(super) fn new(
        index_log: DisklessIndexLog,
        object_store: Arc<dyn ObjectStore>,
        object_prefix: Option<String>,
        grace: Duration,
    ) -> Self {
        Self {
            index_log,
            object_store,
            object_prefix,
            first_seen: HashMap::new(),
            grace,
            freshness_timeout: RECLAIM_FRESHNESS_TIMEOUT,
        }
    }

    #[cfg(test)]
    pub(super) fn with_freshness_timeout(mut self, freshness_timeout: Duration) -> Self {
        self.freshness_timeout = freshness_timeout;
        self
    }

    pub(super) async fn sweep(&mut self) { self.sweep_at(Instant::now()).await; }

    pub(super) async fn sweep_at(&mut self, now: Instant) {
        if !self.fresh_view().await {
            return;
        }
        let prefix =
            krabka_object_store::prefixed_key(self.object_prefix.as_deref(), "diskless-wal");
        let objects = match self
            .object_store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
        {
            Ok(objects) => objects,
            Err(error) => {
                tracing::warn!(%error, "diskless WAL reclaim listing failed; retrying");
                return;
            }
        };
        let objects = self.unprotected(objects).await;
        let mut unreferenced = HashSet::new();
        let mut candidates = Vec::new();
        for object in objects {
            let object_key = object.location.to_string();
            unreferenced.insert(object_key.clone());
            let first_seen = *self.first_seen.entry(object_key).or_insert(now);
            let grace_elapsed = now.duration_since(first_seen) >= self.grace;
            if krabka_verified::diskless_object_reclaimable(false, grace_elapsed) {
                candidates.push(object);
            }
        }
        self.first_seen
            .retain(|object_key, _| unreferenced.contains(object_key));
        if candidates.is_empty() || !self.fresh_view().await {
            return;
        }
        let candidates = self.unprotected(candidates).await;
        if candidates.is_empty() || !self.fresh_view().await {
            return;
        }
        for object in candidates {
            self.delete_unreferenced(&object).await;
        }
    }

    /// Bring the projection up to a fence on every index partition, or warn
    /// that the sweep stops here.
    async fn fresh_view(&self) -> bool {
        let fresh = self
            .index_log
            .wait_for_fresh_view(self.freshness_timeout)
            .await;
        if !fresh {
            tracing::warn!("diskless WAL index view is not current; skipping reclaim");
        }
        fresh
    }

    /// The objects the projection neither references nor reserves. A kept
    /// object's grace period starts over.
    async fn unprotected(&mut self, objects: Vec<ObjectMeta>) -> Vec<ObjectMeta> {
        let cache = self.index_log.cache();
        let cache = cache.lock().await;
        objects
            .into_iter()
            .filter(|object| {
                let object_key = object.location.to_string();
                let protected = cache.references_object(&object_key);
                if protected {
                    self.first_seen.remove(&object_key);
                }
                !protected
            })
            .collect()
    }

    async fn delete_unreferenced(&mut self, object: &ObjectMeta) {
        let object_key = object.location.to_string();
        // Additions for this object take the same fence before the cache
        // lock. Keep only the object-local fence over slow DELETE.
        let _object_fence = self.index_log.object_fence(&object_key).await;
        let cache = self.index_log.cache();
        let cache = cache.lock().await;
        if !krabka_verified::diskless_object_reclaimable(cache.references_object(&object_key), true)
        {
            self.first_seen.remove(&object_key);
            return;
        }
        drop(cache);
        match self.object_store.delete(&object.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                self.first_seen.remove(&object_key);
            }
            Err(error) => {
                tracing::warn!(%object_key, %error, "diskless WAL reclaim failed; retrying");
            }
        }
    }
}
