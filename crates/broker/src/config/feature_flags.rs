//! The independent compatibility and protocol feature gates, together with
//! the two sets of defaults that the production and the test constructor use.

/// Construction-time configuration for [`crate::Broker::start`].
///
/// Build it directly when you embed the broker as a library. In production,
/// build it with the `krabka-broker` binary's clap CLI.
#[derive(Debug, Clone, Copy)]
pub struct BrokerFeatureFlags {
    pub oauthbearer_jwks_ignore_key_use: bool,
    pub auto_leader_rebalance_enable: bool,
    pub transaction_two_phase_commit_enable: bool,
}

pub(super) const fn test_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: false,
        transaction_two_phase_commit_enable: false,
    }
}

pub(super) const fn default_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: true,
        transaction_two_phase_commit_enable: false,
    }
}
