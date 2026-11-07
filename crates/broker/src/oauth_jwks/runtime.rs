//! The JWKS publication state a signed OAUTHBEARER validator shares with its
//! refresher and with the authenticate paths.
//!
//! These are live handles, not configuration. They sit on
//! [`crate::config::BrokerConfig`] only so that `Broker::start` can hand them
//! from `[oauthbearer]` loading to the tasks that use them.

use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64},
};

/// The pair the authenticate paths' cache guard reads as one validation
/// snapshot, and the JWKS refresher publishes.
///
/// Every clone shares the same atomics. A default value is a cache that never
/// fetched: generation 0 and timestamp 0.
#[derive(Debug, Clone, Default)]
pub struct JwksSnapshotState {
    /// Seqlock generation that makes the JWKS keys and the successful-fetch
    /// timestamp one validation snapshot. Even values are stable; odd values
    /// mean the refresher is publishing a new pair.
    pub generation: Arc<AtomicU64>,
    /// Epoch ms of the last successful JWKS fetch. `0` means none yet. The
    /// signed validator's `JwksHandle` shares this `Arc`, so its expiry check
    /// sees the refresher's writes.
    pub last_successful_fetch_ms: Arc<AtomicI64>,
}
