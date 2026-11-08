//! The JWKS publication state a signed OAUTHBEARER validator shares with its
//! refresher and with the authenticate paths.
//!
//! These are live handles, not configuration. They sit on
//! [`crate::config::BrokerConfig`] only so that `Broker::start` can hand them
//! from `[oauthbearer]` loading to the tasks that use them.

use {
    std::sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64},
    },
    tokio::sync::mpsc,
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

/// The live state a signed validator shares with the JWKS refresher that
/// `Broker::start` spawns.
///
/// `[oauthbearer]` loading creates it for the signed validator only. Every
/// clone shares the same channel and atomics.
#[derive(Debug, Clone)]
pub struct JwksRuntime {
    /// Receiver half of the refresher signal channel. The sender is in the
    /// signed validator's `JwksHandle`.
    ///
    /// It is an `Arc<Mutex<…>>` so that the runtime stays `Clone`. Only
    /// `Broker::start` locks it and takes the receiver, and there is only ever
    /// one `Broker::start` for each validator construction.
    pub signal_rx: Arc<Mutex<Option<mpsc::Receiver<()>>>>,
    /// The publication the refresher writes and the authenticate paths read.
    pub snapshot: JwksSnapshotState,
    /// Epoch ms of the last on-demand refresh, for rate limiting. Only the
    /// refresher reads it.
    pub last_on_demand_refresh_ms: Arc<AtomicI64>,
}

impl JwksRuntime {
    /// A runtime that never fetched, and the signal sender to wire into the
    /// validator's `JwksHandle`.
    ///
    /// The channel holds one signal and the validator uses `try_send`, so
    /// signals coalesce.
    #[must_use]
    pub fn new() -> (Self, mpsc::Sender<()>) {
        let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
        let runtime = Self {
            signal_rx: Arc::new(Mutex::new(Some(signal_rx))),
            snapshot: JwksSnapshotState::default(),
            last_on_demand_refresh_ms: Arc::new(AtomicI64::new(0)),
        };
        (runtime, signal_tx)
    }
}
