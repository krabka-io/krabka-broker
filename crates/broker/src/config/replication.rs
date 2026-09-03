//! The runtime policy a follower replication task reads: the shape of each
//! replication fetch and the backoffs between them.

use krabka_units::{ByteSize, Time, bytes, mebibytes, millis, secs};

/// Runtime policy for follower replication tasks.
///
/// It sets the size and the maximum wait of each replication fetch, and the
/// backoffs the follower loop applies between fetches.
///
/// This type is not `Eq`: every value here is a quantity, and its `f64`
/// storage is only `PartialEq`. Three of the fields reach the wire, as
/// `FetchRequest`'s `max_bytes`, `min_bytes`, and `max_wait_ms`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationRuntimeConfig {
    /// Maximum bytes requested from a leader in one replication fetch.
    pub fetch_max: ByteSize,
    /// Maximum leader wait for a replication fetch.
    pub fetch_max_wait: Time,
    /// Minimum bytes that satisfy a replication fetch.
    ///
    /// It reaches the leader as the request's `min_bytes`, and a krabka leader
    /// honours it as a floor the way Kafka does: the fetch is held until that
    /// many bytes are readable across its partitions or `fetch_max_wait`
    /// expires, however many appends it takes to get there.
    pub fetch_min: ByteSize,
    /// Delay after a replication throttle budget is exhausted.
    pub throttle_exhausted_backoff: Time,
    /// Retry delay after sending a replication request fails.
    pub send_error_backoff: Time,
    /// Retry delay when the leader does not yet know the topic.
    pub unknown_topic_retry_delay: Time,
    /// Retry delay after a leader-epoch fence.
    pub epoch_fence_backoff: Time,
    /// Retry delay after an unexpected replication error.
    pub unexpected_error_backoff: Time,
    /// Initial delay before reconnecting to a leader.
    pub reconnect_initial_delay: Time,
    /// Maximum delay between leader reconnection attempts.
    pub reconnect_delay_cap: Time,
}

impl Default for ReplicationRuntimeConfig {
    fn default() -> Self {
        Self {
            fetch_max: mebibytes(1),
            fetch_max_wait: millis(500),
            fetch_min: bytes(1),
            throttle_exhausted_backoff: millis(100),
            send_error_backoff: secs(1),
            unknown_topic_retry_delay: millis(100),
            epoch_fence_backoff: millis(200),
            unexpected_error_backoff: millis(500),
            reconnect_initial_delay: millis(100),
            reconnect_delay_cap: secs(5),
        }
    }
}
