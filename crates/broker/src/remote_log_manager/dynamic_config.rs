//! Dynamic tiered-storage configuration for a running remote-log manager.
//!
//! Each metadata image resolves the copy and fetch quota bounds and the index
//! cache size through the broker, cluster and static defaults. Quota updates
//! keep the recorded samples. The fetch path reads `remote.fetch.max.wait.ms`
//! on each request, as Kafka does for each `DelayedRemoteFetch`.

use {
    super::quota::{RemoteLogQuotas, UNLIMITED_BYTES_PER_SECOND},
    crate::{config_keys::node_or_cluster_default, metadata_source::watch_image_loop},
    krabka_metadata::{MetadataImage, NodeId},
    krabka_remote_storage::RemoteIndexCache,
    std::sync::Arc,
    tokio::sync::watch,
    tokio_util::sync::CancellationToken,
    tracing::info,
};

/// Kafka's `remote.log.manager.copy.max.bytes.per.second`.
pub(crate) const COPY_MAX_BYTES_PER_SECOND: &str = "remote.log.manager.copy.max.bytes.per.second";

/// Kafka's `remote.log.manager.fetch.max.bytes.per.second`.
pub(crate) const FETCH_MAX_BYTES_PER_SECOND: &str = "remote.log.manager.fetch.max.bytes.per.second";

/// Kafka's `remote.log.index.file.cache.total.size.bytes`.
pub(crate) const INDEX_CACHE_TOTAL_SIZE_BYTES: &str =
    "remote.log.index.file.cache.total.size.bytes";

/// The value of the positive `Long` broker key `key` for `node`: its own
/// dynamic value, else the cluster default, else `fallback`. A value that
/// does not parse, or that is under one, falls back too: the alter path
/// refuses both, so only a hand-written record could carry one.
fn positive_long(image: &MetadataImage, node: NodeId, key: &str, fallback: i64) -> i64 {
    node_or_cluster_default(image, node, key)
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(fallback)
}

/// What [`apply_image`] changes besides the quotas.
pub(crate) struct DynamicRemoteLogTargets<'a> {
    pub(crate) quotas: &'a RemoteLogQuotas,
    /// The reader's index cache, and the size it has when no dynamic config
    /// sets one: the static `remote.log.index.file.cache.total.size.bytes`.
    pub(crate) index_cache: Option<(&'a RemoteIndexCache, u64)>,
}

/// Kafka's `DynamicRemoteLogConfig.reconfigure` for the three keys a running
/// `RemoteLogManager` applies: each one that resolves to a value other than
/// the one in force is applied, and logged as Kafka logs it.
pub(crate) fn apply_image(
    image: &MetadataImage,
    node: NodeId,
    targets: &DynamicRemoteLogTargets<'_>,
) {
    let copy = positive_long(
        image,
        node,
        COPY_MAX_BYTES_PER_SECOND,
        UNLIMITED_BYTES_PER_SECOND,
    );
    let old = targets.quotas.copy.bytes_per_second();
    if copy != old {
        targets.quotas.copy.update_quota(copy);
        info!(
            old,
            new = copy,
            "dynamic remote log manager config: {COPY_MAX_BYTES_PER_SECOND} updated"
        );
    }
    let fetch = positive_long(
        image,
        node,
        FETCH_MAX_BYTES_PER_SECOND,
        UNLIMITED_BYTES_PER_SECOND,
    );
    let old = targets.quotas.fetch.bytes_per_second();
    if fetch != old {
        targets.quotas.fetch.update_quota(fetch);
        info!(
            old,
            new = fetch,
            "dynamic remote log manager config: {FETCH_MAX_BYTES_PER_SECOND} updated"
        );
    }
    if let Some((cache, static_size)) = targets.index_cache {
        let fallback = i64::try_from(static_size).unwrap_or(i64::MAX);
        let size = positive_long(image, node, INDEX_CACHE_TOTAL_SIZE_BYTES, fallback);
        let size = u64::try_from(size).unwrap_or(static_size);
        let old = cache.max_bytes();
        if size != old {
            cache.resize(size);
            info!(
                old,
                new = size,
                "dynamic remote log manager config: {INDEX_CACHE_TOTAL_SIZE_BYTES} updated"
            );
        }
    }
}

/// The index cache [`run`] resizes, and its static size.
pub(crate) struct WatchedIndexCache {
    pub(crate) cache: Arc<RemoteIndexCache>,
    pub(crate) static_size: u64,
}

/// Applies every metadata image to `quotas` and `index_cache` until
/// `shutdown`. See [`apply_image`].
pub(crate) async fn run(
    images: watch::Receiver<Arc<MetadataImage>>,
    node: NodeId,
    quotas: Arc<RemoteLogQuotas>,
    index_cache: WatchedIndexCache,
    shutdown: CancellationToken,
) {
    watch_image_loop(images, "dynamic remote log config", shutdown, |image| {
        apply_image(image, node, &DynamicRemoteLogTargets {
            quotas: &quotas,
            index_cache: Some((&index_cache.cache, index_cache.static_size)),
        });
    })
    .await;
}

#[cfg(test)]
mod tests;
