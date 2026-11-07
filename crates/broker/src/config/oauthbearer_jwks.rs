//! The OAUTHBEARER authenticate paths' view of the JWKS runtime state.

use crate::{config::BrokerConfig, oauth_jwks::JwksSnapshotState};

impl BrokerConfig {
    /// The JWKS publication the OAUTHBEARER authenticate paths' cache guard
    /// reads: the signed validator's shared state, or a never-fetched one when
    /// `[oauthbearer]` built no signed validator.
    #[must_use]
    pub fn oauthbearer_jwks_snapshot(&self) -> JwksSnapshotState {
        self.oauthbearer_jwks_runtime
            .as_ref()
            .map(|runtime| runtime.snapshot.clone())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{config::BrokerConfig, oauth_jwks::JwksRuntime},
        assert2::assert,
        std::sync::{Arc, atomic::Ordering},
    };

    /// Without a signed validator's runtime the authenticate paths still get a
    /// snapshot: one that never fetched.
    #[test]
    fn a_config_without_a_jwks_runtime_reads_a_never_fetched_snapshot() {
        let snapshot = BrokerConfig::default().oauthbearer_jwks_snapshot();
        assert!(snapshot.generation.load(Ordering::Acquire) == 0);
        assert!(snapshot.last_successful_fetch_ms.load(Ordering::Acquire) == 0);
    }

    /// With a runtime, every read shares the runtime's atomics.
    #[test]
    fn a_config_with_a_jwks_runtime_reads_its_snapshot() {
        let (runtime, _signal_tx) = JwksRuntime::new();
        let config = BrokerConfig {
            oauthbearer_jwks_runtime: Some(runtime.clone()),
            ..BrokerConfig::default()
        };
        let snapshot = config.oauthbearer_jwks_snapshot();
        assert!(Arc::ptr_eq(
            &snapshot.generation,
            &runtime.snapshot.generation
        ));
        assert!(Arc::ptr_eq(
            &snapshot.last_successful_fetch_ms,
            &runtime.snapshot.last_successful_fetch_ms
        ));
    }
}
