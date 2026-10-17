//! KIP-956's remote-log byte-rate quotas, and the image watch that applies the
//! dynamic tiered-storage broker configs to a running broker.
//!
//! Kafka's `RemoteLogManager` owns two `RLMQuotaManager`s. The copy quota
//! (`remote.log.manager.copy.max.bytes.per.second`) holds `RLMCopyTask` back
//! before it uploads a segment, and the fetch quota
//! (`remote.log.manager.fetch.max.bytes.per.second`) makes
//! `ReplicaManager.handleOffsetOutOfRangeError` skip the remote read of a
//! consumer fetch. Both are unlimited (`Long.MAX_VALUE`) by default.
//!
//! [`RlmQuotaManager`] is that manager. It measures with Kafka's `SimpleRate`
//! over a `SampledStat` of `window.num` samples of `window.size.seconds`
//! each, and it answers with the throttle time `QuotaUtils.throttleTime`
//! computes. The rate is measured over the bytes already recorded, so the
//! first read or copy after a quiet period always goes ahead, and the one after
//! it waits until the measured rate is back under the bound.
//!
//! [`run`] is Kafka's `DynamicRemoteLogConfig.reconfigure`: on every metadata
//! image it resolves the two quota keys and
//! `remote.log.index.file.cache.total.size.bytes` for this broker, and applies
//! a value that changed through `updateCopyQuota`, `updateFetchQuota` or
//! `resizeCacheSize`. `remote.fetch.max.wait.ms` is not here, because the
//! fetch path reads it on each request, as Kafka's `ReplicaManager` reads
//! `remoteFetchMaxWaitMs` when it builds each `DelayedRemoteFetch`.

use {
    crate::{config_keys::node_or_cluster_default, metadata_source::watch_image_loop},
    krabka_metadata::{MetadataImage, NodeId},
    krabka_remote_storage::RemoteIndexCache,
    num_traits::NumCast,
    std::sync::{Arc, Mutex},
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

/// Kafka's default for both quotas: `Long.MAX_VALUE`, which no measured rate
/// reaches.
pub(crate) const UNLIMITED_BYTES_PER_SECOND: i64 = i64::MAX;

/// The sample windows one quota measures over: Kafka's
/// `remote.log.manager.{copy,fetch}.quota.window.num` and
/// `.window.size.seconds`.
///
/// Both keys are read-only in Kafka, and krabka has no static setting for
/// them, so every broker measures over Kafka's defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuotaWindow {
    /// How many samples the rate keeps (`window.num`, 11 by default).
    pub(crate) samples: usize,
    /// How long one sample spans, in milliseconds (`window.size.seconds`,
    /// one second by default).
    pub(crate) size_ms: i64,
}

impl Default for QuotaWindow {
    fn default() -> Self {
        Self {
            samples: 11,
            size_ms: 1000,
        }
    }
}

/// One sample of a [`SampledRate`]: the bytes recorded since `last_window_ms`.
#[derive(Debug, Clone, Copy)]
struct Sample {
    last_window_ms: i64,
    value: f64,
}

impl Sample {
    const fn new(now_ms: i64) -> Self {
        Self {
            last_window_ms: now_ms,
            value: 0.0,
        }
    }
}

/// Kafka's `SimpleRate` over a `SampledStat` that sums: the bytes recorded in
/// the live samples, divided by the time since the oldest of them began, and
/// never by less than one sample window.
///
/// Every method takes the clock reading, so the arithmetic is the same
/// whether a test or the broker's clock supplies it.
#[derive(Debug)]
struct SampledRate {
    window: QuotaWindow,
    samples: Vec<Sample>,
    current: usize,
}

impl SampledRate {
    const fn new(window: QuotaWindow) -> Self {
        Self {
            window,
            samples: Vec::new(),
            current: 0,
        }
    }

    /// `SampledStat.record`: add `value` to the current sample, first moving
    /// to the next one when the current sample's window has passed.
    fn record(&mut self, value: f64, now_ms: i64) {
        if self.samples.is_empty() {
            self.samples.push(Sample::new(now_ms));
        }
        if now_ms - self.samples[self.current].last_window_ms >= self.window.size_ms {
            self.current = (self.current + 1) % self.window.samples.max(1);
            if self.current >= self.samples.len() {
                self.samples.push(Sample::new(now_ms));
            } else {
                self.samples[self.current] = Sample::new(now_ms);
            }
        }
        self.samples[self.current].value += value;
    }

    /// `SampledStat.purgeObsoleteSamples`: reset every sample older than the
    /// whole window.
    fn purge(&mut self, now_ms: i64) {
        let expire_age = i64::try_from(self.window.samples)
            .unwrap_or(i64::MAX)
            .saturating_mul(self.window.size_ms);
        for sample in &mut self.samples {
            if now_ms - sample.last_window_ms >= expire_age {
                *sample = Sample::new(now_ms);
            }
        }
    }

    /// `SimpleRate.windowSize`: the time since the oldest sample began, at
    /// least one sample window.
    fn window_size_ms(&mut self, now_ms: i64) -> i64 {
        self.purge(now_ms);
        let oldest = self
            .samples
            .iter()
            .map(|sample| sample.last_window_ms)
            .min()
            .unwrap_or(now_ms);
        (now_ms - oldest).max(self.window.size_ms)
    }

    /// `Rate.measure` in bytes per second.
    fn bytes_per_second(&mut self, now_ms: i64) -> f64 {
        self.purge(now_ms);
        let total: f64 = self.samples.iter().map(|sample| sample.value).sum();
        total / (as_f64(self.window_size_ms(now_ms)) / 1000.0)
    }
}

/// Widens a `long` to a `double`, as Kafka's rate arithmetic does.
fn as_f64(value: i64) -> f64 { NumCast::from(value).unwrap_or(f64::MAX) }

/// Narrows a `double` to a `long`, saturating where Java's cast saturates.
fn as_i64(value: f64) -> i64 {
    NumCast::from(value).unwrap_or(if value.is_sign_negative() {
        0
    } else {
        i64::MAX
    })
}

/// The rate and the bound it is checked against, behind one lock.
#[derive(Debug)]
struct QuotaState {
    rate: SampledRate,
    bound: f64,
}

impl QuotaState {
    /// `RLMQuotaManager.getThrottleTimeMs`: zero while the measured rate is at
    /// or under the bound, else `QuotaUtils.throttleTime`, the time the rate
    /// needs at the bound to fall back to it.
    fn throttle_time_ms(&mut self, now_ms: i64) -> i64 {
        let value = self.rate.bytes_per_second(now_ms);
        if value <= self.bound {
            return 0;
        }
        let window = as_f64(self.rate.window_size_ms(now_ms));
        let throttle = ((value - self.bound) / self.bound * window).round();
        as_i64(throttle).max(0)
    }
}

/// Kafka's `RLMQuotaManager`: one byte-rate quota on the remote tier.
#[derive(Debug)]
pub(crate) struct RlmQuotaManager {
    state: Mutex<QuotaState>,
    origin: std::time::Instant,
}

impl RlmQuotaManager {
    /// A quota of `bytes_per_second` measured over `window`.
    pub(crate) fn new(bytes_per_second: i64, window: QuotaWindow) -> Self {
        Self {
            state: Mutex::new(QuotaState {
                rate: SampledRate::new(window),
                bound: as_f64(bytes_per_second),
            }),
            origin: std::time::Instant::now(),
        }
    }

    /// A quota no rate reaches, Kafka's default.
    pub(crate) fn unlimited() -> Self {
        Self::new(UNLIMITED_BYTES_PER_SECOND, QuotaWindow::default())
    }

    fn now_ms(&self) -> i64 { i64::try_from(self.origin.elapsed().as_millis()).unwrap_or(i64::MAX) }

    fn lock(&self) -> std::sync::MutexGuard<'_, QuotaState> {
        self.state.lock().expect("remote-log quota lock poisoned")
    }

    /// The bound, in bytes per second.
    pub(crate) fn bytes_per_second(&self) -> i64 { as_i64(self.lock().bound) }

    /// `RLMQuotaManager.updateQuota`: a new bound over the samples already
    /// recorded, which Kafka keeps across the change.
    pub(crate) fn update_quota(&self, bytes_per_second: i64) {
        self.lock().bound = as_f64(bytes_per_second);
    }

    /// How long a caller must wait before the quota allows more bytes; zero
    /// when it allows them now.
    pub(crate) fn throttle_time_ms(&self) -> i64 { self.throttle_time_ms_at(self.now_ms()) }

    fn throttle_time_ms_at(&self, now_ms: i64) -> i64 { self.lock().throttle_time_ms(now_ms) }

    /// `RLMQuotaManager.record`: count `bytes` against the quota.
    pub(crate) fn record(&self, bytes: u64) { self.record_at(bytes, self.now_ms()); }

    fn record_at(&self, bytes: u64, now_ms: i64) {
        self.lock()
            .rate
            .record(as_f64(i64::try_from(bytes).unwrap_or(i64::MAX)), now_ms);
    }

    /// Records `bytes` if the quota allows more bytes now, else answers the
    /// throttle time and records nothing.
    ///
    /// The check and the record are one step, as `RLMCopyTask` makes them one
    /// under `copyQuotaManagerLock`: two copiers that both see room must not
    /// both go ahead on it.
    pub(crate) fn try_acquire(&self, bytes: u64) -> Result<(), i64> {
        self.try_acquire_at(bytes, self.now_ms())
    }

    fn try_acquire_at(&self, bytes: u64, now_ms: i64) -> Result<(), i64> {
        let mut state = self.lock();
        match state.throttle_time_ms(now_ms) {
            0 => {
                state
                    .rate
                    .record(as_f64(i64::try_from(bytes).unwrap_or(i64::MAX)), now_ms);
                Ok(())
            }
            throttle => Err(throttle),
        }
    }
}

/// The two quotas a broker's `RemoteLogManager` holds.
#[derive(Debug)]
pub(crate) struct RemoteLogQuotas {
    /// `remote.log.manager.copy.max.bytes.per.second`.
    pub(crate) copy: Arc<RlmQuotaManager>,
    /// `remote.log.manager.fetch.max.bytes.per.second`.
    pub(crate) fetch: RlmQuotaManager,
}

impl Default for RemoteLogQuotas {
    fn default() -> Self {
        Self {
            copy: Arc::new(RlmQuotaManager::unlimited()),
            fetch: RlmQuotaManager::unlimited(),
        }
    }
}

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
