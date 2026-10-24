//! Reclaim unreferenced diskless objects under their index publication fences.

use {
    super::{
        DISKLESS_WAL_V1_PREFIX, DISKLESS_WAL_V2_PREFIX, index_log::DisklessIndexLog,
        prefixed_object_key,
    },
    futures_util::TryStreamExt as _,
    object_store::{ObjectStore, ObjectStoreExt as _, path::Path},
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
const RECLAIM_FRESHNESS_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct Reclaimer {
    index_log: DisklessIndexLog,
    object_store: Arc<dyn ObjectStore>,
    first_seen: HashMap<String, Instant>,
    grace: Duration,
    object_prefix: Option<String>,
    pub(super) freshness_timeout: Duration,
}

impl Reclaimer {
    pub(super) fn new(
        index_log: DisklessIndexLog,
        object_store: Arc<dyn ObjectStore>,
        grace: Duration,
        object_prefix: Option<String>,
    ) -> Self {
        Self {
            index_log,
            object_store,
            first_seen: HashMap::new(),
            grace,
            object_prefix,
            freshness_timeout: RECLAIM_FRESHNESS_TIMEOUT,
        }
    }

    pub(super) async fn sweep(&mut self, allow_legacy_namespace: bool) {
        self.sweep_at(Instant::now(), allow_legacy_namespace).await;
    }

    pub(super) async fn sweep_at(&mut self, now: Instant, allow_legacy_namespace: bool) {
        if !self
            .index_log
            .wait_for_fresh_view(self.freshness_timeout)
            .await
        {
            tracing::warn!("diskless WAL index view is not current; skipping destructive reclaim");
            return;
        }
        let (referenced, reservations) = {
            let cache = self.index_log.cache();
            let cache = cache.lock().await;
            (cache.referenced_objects(), cache.reserved_objects())
        };
        let mut objects = Vec::new();
        let prefixes: &[&str] = if allow_legacy_namespace {
            &[DISKLESS_WAL_V1_PREFIX, DISKLESS_WAL_V2_PREFIX]
        } else {
            &[DISKLESS_WAL_V2_PREFIX]
        };
        for prefix in prefixes {
            let prefix = Path::from(prefixed_object_key(self.object_prefix.as_deref(), prefix));
            match self
                .object_store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
            {
                Ok(mut listed) => objects.append(&mut listed),
                Err(error) => {
                    tracing::warn!(%error, "diskless WAL reclaim listing failed; retrying");
                    return;
                }
            }
        }
        let mut unreferenced = HashSet::new();
        for object in objects {
            let object_key = object.location.to_string();
            if referenced.contains(&object_key) || reserved_object(&object_key, &reservations) {
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

fn reserved_object(object_key: &str, reservations: &HashSet<uuid::Uuid>) -> bool {
    let Some(filename) = object_key.rsplit('/').next() else {
        return false;
    };
    filename
        .strip_suffix(".ckwl")
        .and_then(|uuid| uuid::Uuid::parse_str(uuid).ok())
        .is_some_and(|object_id| reservations.contains(&object_id))
}
