//! Self-registration and bootstrap-record submission for this node. The
//! module holds the metadata records a broker or controller publishes about
//! itself, and the retrying submit paths that commit them, kept apart from the
//! startup sequence that calls them.

use std::sync::Arc;

use krabka_units::convert::TimeExt as _;

use crate::{
    broker::endpoints::parse_advertised_host_port, config::BrokerConfig, error::BrokerError,
};

fn self_registration_record(config: &BrokerConfig) -> krabka_metadata::BrokerRegistrationRecord {
    let (host, port) = parse_advertised_host_port(&config.advertised_listener);
    let endpoints = config
        .effective_listeners()
        .iter()
        .map(|listener| {
            let (host, port) = parse_advertised_host_port(&listener.advertised);
            krabka_metadata::BrokerEndpoint {
                name: listener.name.clone(),
                host,
                port,
                protocol: listener.protocol,
            }
        })
        .collect();
    let log_dirs = config.all_log_dirs();
    let log_dir_ids = crate::log_dir_id::LogDirIds::resolve(&log_dirs).ids_for(&log_dirs);

    krabka_metadata::BrokerRegistrationRecord {
        node_id: config.node_id,
        broker_epoch: 0,
        incarnation_id: config.incarnation_id,
        host,
        port,
        rack: config.rack.clone(),
        endpoints,
        log_dirs: log_dir_ids,
        features: krabka_metadata::supported_feature_ranges(),
    }
}

/// The broker config record that publishes this node's witness role.
///
/// `BrokerRegistrationRecord` lives in the protocol crate and carries no
/// role flag, so krabka publishes the role as a per-broker config instead.
/// A witness writes `broker.witness=true`. Every other node writes a
/// tombstone, which clears a flag that an earlier run of the same node id
/// set. The record always states the current truth, so the role never goes
/// stale across a restart.
fn self_witness_record(config: &BrokerConfig) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
        node_id: config.node_id,
        config_name: crate::config_keys::BROKER_WITNESS.to_string(),
        config_value: config
            .is_witness()
            .then(|| crate::config_keys::WITNESS_TRUE.to_string()),
    })
}

/// The batch this broker submits to register itself: the registration record
/// and the witness-role config for the same node id. One batch commits both,
/// so the controller never sees a registered node whose role it does not
/// know yet.
fn broker_registration_batch(config: &BrokerConfig) -> Vec<krabka_metadata::MetadataRecord> {
    vec![
        krabka_metadata::MetadataRecord::V1BrokerRegistration(self_registration_record(config)),
        self_witness_record(config),
    ]
}

/// The cluster-default broker config that names the stretch cluster's
/// preferred leader site. Site-aware placement reads it from the metadata
/// image, so every node that later becomes controller pins leadership to the
/// same site. A node with no stretch profile publishes nothing.
fn stretch_default_records(config: &BrokerConfig) -> Vec<krabka_metadata::MetadataRecord> {
    config
        .stretch
        .as_ref()
        .map(|profile| {
            krabka_metadata::MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: crate::config_keys::STRETCH_PREFERRED_LEADER_SITE.to_string(),
                config_value: Some(profile.preferred_leader_site.clone()),
            })
        })
        .into_iter()
        .collect()
}

fn self_controller_registration_record(
    config: &BrokerConfig,
) -> krabka_metadata::ControllerRegistrationRecord {
    let (host, port) = config
        .controller_quorum_voters
        .iter()
        .find(|(node_id, _)| *node_id == config.node_id)
        .and_then(|(_, endpoint)| crate::host_port::parse_host_port(endpoint))
        .unwrap_or_else(|| {
            (
                config.controller_listen_addr.ip().to_string(),
                config.controller_listen_addr.port(),
            )
        });
    krabka_metadata::ControllerRegistrationRecord {
        node_id: config.node_id,
        incarnation_id: config.incarnation_id,
        zk_migration_ready: false,
        endpoints: vec![krabka_metadata::BrokerEndpoint {
            name: "CONTROLLER".into(),
            host,
            port,
            protocol: config.controller_listener_protocol,
        }],
        features: krabka_metadata::supported_feature_ranges(),
    }
}

/// Submits one self-registration batch and retries it under backoff. The
/// whole batch commits together, so a caller can pair the registration record
/// with the configs that describe the same node.
async fn submit_self_registration(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
    registration: Vec<krabka_metadata::MetadataRecord>,
    role: &str,
) -> Result<(), BrokerError> {
    let backoff = exponential_backoff::Backoff::new(
        config.self_registration_max_attempts,
        config.self_registration_backoff_min.to_std(),
        Some(config.self_registration_backoff_max.to_std()),
    );
    for (attempt_index, delay) in backoff.into_iter().enumerate() {
        match controller.submit_change(registration.clone()).await {
            Ok(_) => return Ok(()),
            Err(error) => match delay {
                Some(delay) => {
                    tracing::warn!(attempt = attempt_index + 1, %error, role, "registration retry");
                    tokio::time::sleep(delay).await;
                }
                None => {
                    return Err(BrokerError::Startup(format!(
                        "{role} self-registration failed after {} attempts: {error}",
                        attempt_index + 1
                    )));
                }
            },
        }
    }
    Ok(())
}

pub(super) async fn register_controller(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
) -> Result<(), BrokerError> {
    if !config.is_controller() {
        return Ok(());
    }
    let registration = self_controller_registration_record(config);
    let Some(record) = controller_registration_update(&controller.current_image(), &registration)
    else {
        return Ok(());
    };
    submit_self_registration(config, controller, vec![record], "controller").await
}

fn controller_registration_update(
    image: &krabka_metadata::MetadataImage,
    registration: &krabka_metadata::ControllerRegistrationRecord,
) -> Option<krabka_metadata::MetadataRecord> {
    let registration_supported = image.finalized_metadata_version().is_some_and(|level| {
        level >= krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL
    });
    (registration_supported && image.controller(registration.node_id) != Some(registration))
        .then(|| krabka_metadata::MetadataRecord::V1ControllerRegistration(registration.clone()))
}

pub(super) fn spawn_deferred_controller_registration(
    config: &BrokerConfig,
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) {
    if !config.is_controller() {
        return;
    }
    let registration = self_controller_registration_record(config);
    let mut images = controller.watch_image();
    let controller = Arc::clone(controller);
    let retry_backoff = config.self_registration_backoff_min.to_std();
    tokio::spawn(async move {
        loop {
            let update = {
                let image = images.borrow();
                controller_registration_update(&image, &registration)
            };
            let Some(update) = update else {
                if images.borrow().controller(registration.node_id) == Some(&registration) {
                    return;
                }
                if images.changed().await.is_err() {
                    return;
                }
                continue;
            };

            match controller.submit_change(vec![update]).await {
                Ok(_) => {
                    // A successful submit normally publishes the committed
                    // image before returning. If publication trails the reply,
                    // wait for it rather than submitting the same registration
                    // twice.
                    while images.borrow().controller(registration.node_id) != Some(&registration) {
                        if images.changed().await.is_err() {
                            return;
                        }
                    }
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "deferred controller registration retry");
                    tokio::time::sleep(retry_backoff).await;
                }
            }
        }
    });
}

pub(super) async fn register_broker(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
) -> Result<(), BrokerError> {
    if !config.is_broker() {
        return Ok(());
    }
    submit_self_registration(
        config,
        controller,
        broker_registration_batch(config),
        "broker",
    )
    .await
}

pub(super) async fn submit_bootstrap_records(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
    mut records: Vec<krabka_metadata::MetadataRecord>,
) -> Result<(), BrokerError> {
    if !matches!(config.bootstrap_mode, crate::BootstrapMode::Bootstrap) {
        return Ok(());
    }
    records.retain(|record| {
        !matches!(
            record,
            krabka_metadata::MetadataRecord::V1Voters(_)
                | krabka_metadata::MetadataRecord::V1KRaftVersion(_)
        )
    });
    if config.is_controller() {
        records.extend(stretch_default_records(config));
    }
    if records.is_empty() {
        return Ok(());
    }
    tracing::info!(count = records.len(), "submitting bootstrap records");
    controller
        .submit_change(records)
        .await
        .map(|_| ())
        .map_err(|error| BrokerError::Replication(format!("bootstrap submit failed: {error}")))
}

#[cfg(test)]
mod tests;
