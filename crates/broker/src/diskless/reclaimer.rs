//! Reclaim unreferenced diskless objects under their index publication fences.

use {
    super::index_log::DisklessIndexLog,
    futures_util::TryStreamExt as _,
    object_store::{ObjectStore, ObjectStoreExt as _},
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

pub(super) struct Reclaimer {
    index_log: DisklessIndexLog,
    object_store: Arc<dyn ObjectStore>,
    object_prefix: Option<String>,
    first_seen: HashMap<String, Instant>,
    grace: Duration,
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
        }
    }

    pub(super) async fn sweep(&mut self) { self.sweep_at(Instant::now()).await; }

    pub(super) async fn sweep_at(&mut self, now: Instant) {
        let referenced = self.index_log.cache().lock().await.referenced_objects();
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
        let mut unreferenced = HashSet::new();
        for object in objects {
            let object_key = object.location.to_string();
            if referenced.contains(&object_key) {
                self.first_seen.remove(&object_key);
                continue;
            }
            unreferenced.insert(object_key.clone());
            let first_seen = *self.first_seen.entry(object_key.clone()).or_insert(now);
            let grace_elapsed = now.duration_since(first_seen) >= self.grace;
            if !krabka_verified::diskless_object_reclaimable(false, grace_elapsed) {
                continue;
            }
            // Additions for this object take the same fence before the cache
            // lock. Keep only the object-local fence over slow DELETE.
            let _object_fence = self.index_log.object_fence(&object_key).await;
            let cache = self.index_log.cache();
            let cache = cache.lock().await;
            if !krabka_verified::diskless_object_reclaimable(
                cache.references_object(&object_key),
                true,
            ) {
                self.first_seen.remove(&object_key);
                continue;
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
        self.first_seen
            .retain(|object_key, _| unreferenced.contains(object_key));
    }
}
