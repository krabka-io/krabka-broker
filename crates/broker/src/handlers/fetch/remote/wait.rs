//! Kafka's `remote.fetch.max.wait.ms`: how long one `Fetch` waits for its
//! cold-tier reads before it answers without them.
//!
//! Kafka hands every remote read of a fetch to the reader pool and parks the
//! fetch as one `DelayedRemoteFetch`, whose delay is `remoteFetchMaxWaitMs`
//! as `ReplicaManager.processRemoteFetches` reads it for that request. The
//! request's own `max.wait.ms` plays no part: a fetch that needs the remote
//! tier waits for it up to this bound and no longer, and does not then wait
//! for `min.bytes` either, because `DelayedRemoteFetch.tryComplete` completes
//! once every remote read is done.
//!
//! krabka reads a fetch's cold partitions one after another, so the bound is
//! one deadline per request, armed by the first cold read and shared by every
//! later one: the whole fetch spends at most the configured time on the tier,
//! as Kafka's parallel reads under one delayed operation do.

use {
    crate::{broker::Broker, config_keys::node_or_cluster_default},
    std::{future::Future, time::Duration},
    tokio::time::Instant,
};

/// Kafka's `remote.fetch.max.wait.ms`.
pub(crate) const REMOTE_FETCH_MAX_WAIT_MS: &str = "remote.fetch.max.wait.ms";

/// Kafka's `DEFAULT_REMOTE_FETCH_MAX_WAIT_MS`. krabka has no static setting
/// for the key, so this is also the static value.
const DEFAULT_REMOTE_FETCH_MAX_WAIT: Duration = Duration::from_millis(500);

/// `remote.fetch.max.wait.ms` as `node` sees it in `image`: its own dynamic
/// value, else the cluster default, else Kafka's 500 ms. A value that does
/// not parse as a positive `Int` falls back as well; the alter path refuses
/// one, so only a hand-written record could carry it.
pub(crate) fn remote_fetch_max_wait(
    image: &krabka_metadata::MetadataImage,
    node: krabka_metadata::NodeId,
) -> Duration {
    node_or_cluster_default(image, node, REMOTE_FETCH_MAX_WAIT_MS)
        .and_then(|value| value.trim().parse::<i32>().ok())
        .and_then(|ms| u64::try_from(ms).ok())
        .filter(|ms| *ms >= 1)
        .map_or(DEFAULT_REMOTE_FETCH_MAX_WAIT, Duration::from_millis)
}

/// The remote-read deadline of one `Fetch` request.
#[derive(Debug, Default)]
pub(crate) struct RemoteFetchWait {
    /// Armed by the first cold read: Kafka starts the `DelayedRemoteFetch`
    /// timer when the first remote read is handed to the pool.
    deadline: Option<Instant>,
}

impl RemoteFetchWait {
    /// Whether this request has handed a read to the remote tier. Such a
    /// fetch answers once its cold reads finish or expire, without a
    /// `min.bytes` long poll.
    pub(crate) fn dispatched(&self) -> bool { self.deadline.is_some() }

    /// Runs `read` until it finishes or the request's deadline passes, and
    /// answers `None` on expiry.
    ///
    /// The first call arms the deadline from `broker`'s current image, so an
    /// altered `remote.fetch.max.wait.ms` applies from the next fetch on.
    ///
    /// An expired read is dropped, which is Kafka's `task.cancel(false)` for a
    /// read still waiting for a reader slot. One whose SPI call already runs
    /// on the blocking pool runs to its end there, as Kafka lets a started
    /// `RemoteLogReader` finish, but its result is discarded.
    pub(crate) async fn bound<F: Future>(&mut self, broker: &Broker, read: F) -> Option<F::Output> {
        self.bound_with(
            || remote_fetch_max_wait(&broker.controller.current_image(), broker.config.node_id),
            read,
        )
        .await
    }

    /// [`Self::bound`], with the wait a first call arms the deadline from
    /// given by `wait`.
    async fn bound_with<F: Future>(
        &mut self,
        wait: impl FnOnce() -> Duration,
        read: F,
    ) -> Option<F::Output> {
        let deadline = *self.deadline.get_or_insert_with(|| Instant::now() + wait());
        tokio::time::timeout_at(deadline, read).await.ok()
    }
}

#[cfg(test)]
mod tests;
