//! The whole-config check [`crate::Broker::start`] runs before any side
//! effect, and the ordering relations between runtime scalars that no single
//! domain owns.

use std::time::Duration;

use krabka_security::SaslMechanism;
use krabka_units::{
    ByteSize,
    convert::{ByteSizeExt, TimeExt},
};

use crate::{
    BrokerError,
    config::{BrokerConfig, RlmmKind},
};

impl BrokerConfig {
    /// Validates the listener and auth configuration.
    ///
    /// [`crate::Broker::start`] calls this before any side effects, so a
    /// mis-configuration shows immediately with a descriptive error instead
    /// of at the first connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` when:
    /// - Two listeners share the same `bind_addr`.
    /// - `inter_broker_listener_name` does not match any listener name.
    /// - A SASL listener is declared while `enabled_sasl_mechanisms` is empty.
    /// - The role set or the [`stretch`][Self::stretch] profile is incoherent.
    pub fn validate(&self) -> Result<(), BrokerError> {
        self.validate_log_io_policy()?;
        if self.roles.is_empty() {
            return Err(BrokerError::EmptyRoles);
        }
        self.validate_witness_roles()?;
        if !self.is_controller()
            && self
                .controller_quorum_voters
                .iter()
                .any(|(id, _)| *id == self.node_id)
        {
            return Err(BrokerError::NonControllerIsVoter {
                node_id: self.node_id,
            });
        }

        let listeners = self.effective_listeners();

        // Bind-address collisions.
        for (i, listener) in listeners.iter().enumerate() {
            for other in listeners.iter().skip(i + 1) {
                if listener.bind_addr == other.bind_addr {
                    return Err(BrokerError::ListenerConflict {
                        a: listener.name.clone(),
                        b: other.name.clone(),
                    });
                }
            }
        }

        // Inter-broker listener must exist.
        let inter_broker_listener = listeners
            .iter()
            .find(|listener| listener.name == self.inter_broker_listener_name)
            .ok_or_else(|| BrokerError::InvalidInterBrokerListener {
                name: self.inter_broker_listener_name.clone(),
            })?;

        // Every SASL listener requires at least one mechanism. Per-listener
        // sasl_mechanisms wins over the broker-wide default.
        for l in &listeners {
            if l.protocol.requires_sasl() {
                let mechanisms = l
                    .sasl_mechanisms
                    .as_deref()
                    .unwrap_or(&self.enabled_sasl_mechanisms);
                if mechanisms.is_empty() {
                    return Err(BrokerError::SaslListenerNoMechanisms {
                        name: l.name.clone(),
                    });
                }
            }
        }

        // GSSAPI, wherever it is enabled (per-listener override or broker-wide
        // default), requires a `gssapi` config block. Without it the dispatch
        // path has nothing to authenticate against, so reject at startup rather
        // than panicking when the first GSSAPI client connects.
        let gssapi_enabled = listeners.iter().any(|l| {
            l.protocol.requires_sasl()
                && l.sasl_mechanisms
                    .as_deref()
                    .unwrap_or(&self.enabled_sasl_mechanisms)
                    .contains(&SaslMechanism::Gssapi)
        }) || self
            .enabled_sasl_mechanisms
            .contains(&SaslMechanism::Gssapi);
        if gssapi_enabled && self.gssapi.is_none() {
            return Err(BrokerError::GssapiConfigMissing);
        }

        let cp = self.controller_listener_protocol;
        if cp.requires_tls() && self.tls_config.is_none() {
            return Err(BrokerError::Tls(
                "controller_listener_protocol requires TLS but tls_config is None".into(),
            ));
        }
        if cp.requires_sasl() && self.enabled_sasl_mechanisms.is_empty() {
            return Err(BrokerError::SaslListenerNoMechanisms {
                name: "controller".into(),
            });
        }
        self.validate_outbound_sasl(inter_broker_listener)?;
        self.validate_positive_runtime_scalars()?;
        self.validate_additional_runtime_scalars()?;
        // After the scalar checks: the stretch durability check reads the
        // replication factor and `min.insync.replicas`, and the kernel it
        // calls takes a positive replication factor.
        self.validate_stretch()?;
        self.record_decompression_policy()?;

        self.validate_runtime_relations()?;
        self.validate_additional_runtime_relations()?;

        let validate_group = |name: &str,
                              session_timeout: Duration,
                              heartbeat_interval: Duration,
                              min_session_timeout: Duration,
                              max_session_timeout: Duration,
                              min_heartbeat_interval: Duration,
                              max_heartbeat_interval: Duration,
                              max_size: Option<usize>| {
            if min_session_timeout.is_zero() {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum session timeout must be positive"
                )));
            }
            if min_session_timeout > max_session_timeout {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum session timeout exceeds maximum"
                )));
            }
            if !(min_session_timeout..=max_session_timeout).contains(&session_timeout) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} session timeout is outside its bounds"
                )));
            }
            if min_heartbeat_interval.is_zero() {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum heartbeat interval must be positive"
                )));
            }
            if min_heartbeat_interval > max_heartbeat_interval {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum heartbeat interval exceeds maximum"
                )));
            }
            if !(min_heartbeat_interval..=max_heartbeat_interval).contains(&heartbeat_interval) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} heartbeat interval is outside its bounds"
                )));
            }
            if max_size == Some(0) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} maximum size must be positive"
                )));
            }
            Ok(())
        };

        let consumer = &self.next_gen_consumer_group;
        validate_group(
            "consumer group",
            consumer.session_timeout,
            consumer.heartbeat_interval,
            consumer.min_session_timeout,
            consumer.max_session_timeout,
            consumer.min_heartbeat_interval,
            consumer.max_heartbeat_interval,
            Some(consumer.max_size),
        )?;
        let share = &self.share_group;
        validate_group(
            "share group",
            share.session_timeout,
            share.heartbeat_interval,
            share.min_session_timeout,
            share.max_session_timeout,
            share.min_heartbeat_interval,
            share.max_heartbeat_interval,
            Some(share.max_size),
        )?;
        let streams = &self.streams_group;
        validate_group(
            "streams group",
            streams.session_timeout,
            streams.heartbeat_interval,
            streams.min_session_timeout,
            streams.max_session_timeout,
            streams.min_heartbeat_interval,
            streams.max_heartbeat_interval,
            Some(streams.max_size),
        )?;

        if let RlmmKind::TopicBacked(config) = &self.remote_log_metadata {
            config.validate()?;
        }
        self.validate_remote_storage_worm()?;
        self.validate_leader_rebalance()
    }

    /// Checks the pairs of runtime scalars that must keep an order between
    /// them, such as a minimum below its maximum.
    fn validate_runtime_relations(&self) -> Result<(), BrokerError> {
        if self.self_registration_backoff_min > self.self_registration_backoff_max {
            return Err(BrokerError::InvalidRuntimeConfig(
                "self registration minimum backoff exceeds maximum".into(),
            ));
        }
        if self.rlmm_bootstrap_backoff_initial > self.rlmm_bootstrap_backoff_max {
            return Err(BrokerError::InvalidRuntimeConfig(
                "RLMM bootstrap initial backoff exceeds maximum".into(),
            ));
        }
        if self.replication.fetch_min > self.replication.fetch_max {
            return Err(BrokerError::InvalidRuntimeConfig(
                "replication fetch minimum bytes exceeds maximum".into(),
            ));
        }
        if self.replication.reconnect_initial_delay > self.replication.reconnect_delay_cap {
            return Err(BrokerError::InvalidRuntimeConfig(
                "replication reconnect initial delay exceeds cap".into(),
            ));
        }
        if self.heartbeat_interval >= self.heartbeat_timeout {
            return Err(BrokerError::InvalidRuntimeConfig(
                "broker heartbeat interval must be below timeout".into(),
            ));
        }
        if self.controller_heartbeat_interval >= self.controller_election_timeout {
            return Err(BrokerError::InvalidRuntimeConfig(
                "controller heartbeat interval must be below election timeout".into(),
            ));
        }
        if self.delegation_token_default_renew_period > self.delegation_token_max_lifetime {
            return Err(BrokerError::InvalidRuntimeConfig(
                "delegation token default renew period exceeds maximum lifetime".into(),
            ));
        }
        if self.client_metrics_stale_floor < self.client_metrics_eviction_tick {
            return Err(BrokerError::InvalidRuntimeConfig(
                "client metrics stale floor is below eviction tick".into(),
            ));
        }
        if self.unclean_recovery_aggressive_deadline > self.unclean_recovery_balanced_deadline {
            return Err(BrokerError::InvalidRuntimeConfig(
                "unclean recovery aggressive deadline exceeds balanced deadline".into(),
            ));
        }
        Ok(())
    }

    fn validate_additional_runtime_relations(&self) -> Result<(), BrokerError> {
        if self.socket_request_max > ByteSize::from_bytes(u64::from(u32::MAX)) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "socket_request_max exceeds u32::MAX bytes".into(),
            ));
        }
        if self.telemetry_decompressed_output_floor > self.telemetry_decompressed_output_ceiling {
            return Err(BrokerError::InvalidRuntimeConfig(
                "telemetry decompressed output floor exceeds ceiling".into(),
            ));
        }
        if self.inter_broker_server_name.is_empty() {
            return Err(BrokerError::InvalidRuntimeConfig(
                "inter_broker_server_name must be nonempty".into(),
            ));
        }
        if self.transaction_min_timeout >= self.transaction_max_timeout {
            return Err(BrokerError::InvalidRuntimeConfig(
                "transaction minimum timeout must be below maximum".into(),
            ));
        }
        // `transaction_max_timeout` is written into an `int32` millisecond wire
        // field, so the saturating conversion must not be the value that pins it.
        if self.transaction_max_timeout.millis_i32() == i32::MAX {
            return Err(BrokerError::InvalidRuntimeConfig(
                "transaction maximum timeout must be below i32::MAX milliseconds".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::{Time, bytes, millis};

    use super::*;
    use crate::config::test_support::{RuntimeInvalidator, assert_invalid_runtime};

    #[test]
    fn rejects_invalid_runtime_relations() {
        let mut config = BrokerConfig::default();
        config.self_registration_backoff_min = config.self_registration_backoff_max * 2.0;
        assert_invalid_runtime(&config, "self registration minimum backoff exceeds maximum");

        let mut config = BrokerConfig::default();
        config.rlmm_bootstrap_backoff_initial = config.rlmm_bootstrap_backoff_max * 2.0;
        assert_invalid_runtime(&config, "RLMM bootstrap initial backoff exceeds maximum");

        let mut config = BrokerConfig::default();
        config.replication.fetch_min = config.replication.fetch_max + bytes(1);
        assert_invalid_runtime(&config, "replication fetch minimum bytes exceeds maximum");

        let mut config = BrokerConfig::default();
        config.replication.reconnect_initial_delay = config.replication.reconnect_delay_cap * 2.0;
        assert_invalid_runtime(&config, "replication reconnect initial delay exceeds cap");

        let mut config = BrokerConfig::default();
        config.heartbeat_interval = config.heartbeat_timeout;
        assert_invalid_runtime(&config, "broker heartbeat interval must be below timeout");

        let mut config = BrokerConfig::default();
        config.controller_heartbeat_interval = config.controller_election_timeout;
        assert_invalid_runtime(
            &config,
            "controller heartbeat interval must be below election timeout",
        );

        let mut config = BrokerConfig::default();
        config.delegation_token_default_renew_period =
            config.delegation_token_max_lifetime + millis(1);
        assert_invalid_runtime(
            &config,
            "delegation token default renew period exceeds maximum lifetime",
        );

        let mut config = BrokerConfig::default();
        config.client_metrics_stale_floor = config.client_metrics_eviction_tick / 2.0;
        assert_invalid_runtime(&config, "client metrics stale floor is below eviction tick");

        let mut config = BrokerConfig::default();
        config.unclean_recovery_aggressive_deadline =
            config.unclean_recovery_balanced_deadline * 2.0;
        assert_invalid_runtime(
            &config,
            "unclean recovery aggressive deadline exceeds balanced deadline",
        );
    }

    #[test]
    fn rejects_invalid_additional_runtime_relations() {
        let cases: &[RuntimeInvalidator] = &[
            ("socket_request_max exceeds u32::MAX bytes", |c| {
                c.socket_request_max = ByteSize::from_bytes(u64::from(u32::MAX) + 1);
            }),
            ("telemetry decompressed output floor exceeds ceiling", |c| {
                c.telemetry_decompressed_output_floor =
                    c.telemetry_decompressed_output_ceiling + bytes(1);
            }),
            ("inter_broker_server_name must be nonempty", |c| {
                c.inter_broker_server_name.clear();
            }),
            ("transaction minimum timeout must be below maximum", |c| {
                c.transaction_min_timeout = c.transaction_max_timeout;
            }),
            (
                "transaction maximum timeout must be below i32::MAX milliseconds",
                |c| c.transaction_max_timeout = Time::from_millis(i64::from(i32::MAX)),
            ),
        ];

        for (expected, invalidate) in cases {
            let mut config = BrokerConfig::default();
            invalidate(&mut config);
            assert_invalid_runtime(&config, expected);
        }
    }

    #[test]
    fn rejects_invalid_group_bounds_and_defaults() {
        let mut config = BrokerConfig::default();
        config.next_gen_consumer_group.min_session_timeout =
            config.next_gen_consumer_group.max_session_timeout * 2;
        assert_invalid_runtime(
            &config,
            "consumer group minimum session timeout exceeds maximum",
        );

        let mut config = BrokerConfig::default();
        config.next_gen_consumer_group.session_timeout =
            config.next_gen_consumer_group.max_session_timeout * 2;
        assert_invalid_runtime(
            &config,
            "consumer group session timeout is outside its bounds",
        );

        let mut config = BrokerConfig::default();
        config.share_group.min_heartbeat_interval = config.share_group.max_heartbeat_interval * 2;
        assert_invalid_runtime(
            &config,
            "share group minimum heartbeat interval exceeds maximum",
        );

        let mut config = BrokerConfig::default();
        config.share_group.heartbeat_interval = config.share_group.max_heartbeat_interval * 2;
        assert_invalid_runtime(
            &config,
            "share group heartbeat interval is outside its bounds",
        );

        let mut config = BrokerConfig::default();
        config.streams_group.max_size = 0;
        assert_invalid_runtime(&config, "streams group maximum size must be positive");
    }
}
