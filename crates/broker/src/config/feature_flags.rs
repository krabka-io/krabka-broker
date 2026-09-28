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
    /// Kafka's internal `unstable.api.versions.enable`, read from
    /// `server_properties` under that name. While it is
    /// [`Disabled`][crate::api_catalog::UnstableApiVersions::Disabled], the
    /// default, every listener -- broker and controller alike -- advertises
    /// each API only up to its latest stable version and closes a connection
    /// that sends the `latestVersionUnstable` version anyway.
    pub unstable_api_versions: crate::api_catalog::UnstableApiVersions,
}

pub(super) const fn test_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: false,
        transaction_two_phase_commit_enable: false,
        unstable_api_versions: crate::api_catalog::UnstableApiVersions::Disabled,
    }
}

pub(super) const fn default_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: true,
        transaction_two_phase_commit_enable: false,
        unstable_api_versions: crate::api_catalog::UnstableApiVersions::Disabled,
    }
}
