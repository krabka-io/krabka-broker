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

/// Submits one startup metadata batch and retries it under backoff.
async fn submit_startup_records(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
    records: Vec<krabka_metadata::MetadataRecord>,
    operation: &str,
) -> Result<(), BrokerError> {
    let backoff = exponential_backoff::Backoff::new(
        config.self_registration_max_attempts,
        config.self_registration_backoff_min.to_std(),
        Some(config.self_registration_backoff_max.to_std()),
    );
    for (attempt_index, delay) in backoff.into_iter().enumerate() {
        match controller.submit_change(records.clone()).await {
            Ok(_) => return Ok(()),
            Err(error) => match delay {
                Some(delay) => {
                    tracing::warn!(attempt = attempt_index + 1, %error, operation, "startup metadata submit retry");
                    tokio::time::sleep(delay).await;
                }
                None => {
                    return Err(BrokerError::Startup(format!(
                        "{operation} failed after {} attempts: {error}",
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
    submit_startup_records(
        config,
        controller,
        vec![record],
        "controller self-registration",
    )
    .await
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

/// The batch this broker submits to register itself after an unclean restart:
/// the ELR withdrawals its lost log tail requires, and then the ordinary
/// registration batch.
///
/// A crashed broker keeps its node id, and krabka keeps its incarnation id in
/// the log dir, so neither says anything about whether the log this node
/// brings back is the log its ELR membership claims. The clean-shutdown proof
/// does: a graceful stop wrote the broker epoch the cluster still holds, and
/// nothing else can. Absent or stale, the restart is unclean and every ELR
/// naming this node is withdrawn, exactly as
/// `ClusterControlManager.registerBroker` calls
/// `ReplicationControlManager.handleBrokerShutdown(id, isCleanShutdown=false,
/// ...)` before it appends its `RegisterBrokerRecord`.
fn broker_restart_batch(
    config: &BrokerConfig,
    image: &krabka_metadata::MetadataImage,
) -> Vec<krabka_metadata::MetadataRecord> {
    let mut records = if crate::clean_shutdown::restart_was_clean(
        image,
        config.node_id,
        config.previous_broker_epoch,
    ) {
        Vec::new()
    } else {
        crate::elr::withdraw_elr_membership(image, config.node_id)
    };
    records.extend(broker_registration_batch(config));
    records
}

pub(super) async fn register_broker(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
) -> Result<(), BrokerError> {
    if !config.is_broker() {
        return Ok(());
    }
    let image = controller.current_image();
    // Captured before the submit, not after: a restart's `broker_restart_batch`
    // republishes this node's registration even when nothing about it changed,
    // and `wait_for_self_registration_published` must recognize that fresh
    // commit rather than the pre-existing record already sitting in `image`.
    let previous = image.broker(config.node_id).cloned();
    let records = broker_restart_batch(config, &image);
    submit_startup_records(config, controller, records, "broker self-registration").await?;
    wait_for_self_registration_published(config, controller, previous.as_ref()).await
}

/// Waits until `current_image()` actually carries this broker's own,
/// just-submitted registration, rather than trusting that
/// [`submit_startup_records`]'s `Ok` already implies it.
///
/// `submit_change` returns once the record is committed AND applied on the
/// leader, but publishing the resulting `Arc<MetadataImage>` to
/// `current_image()`/`watch_image()` is a separate step that can trail that
/// reply by a scheduler tick -- `spawn_deferred_controller_registration`
/// below waits out the exact same gap for the controller-registration
/// record, with the comment that explains it. Left unclosed here, a handler
/// that reads `current_image()` immediately after `Broker::start` returns --
/// the first `CreateTopics` on a freshly booted broker, most commonly --
/// can still observe an image with no registered brokers at all. On a
/// single-node cluster this outraces `site_broker_views`'s own "no
/// registrations yet" fallback (see `crate::handlers::create_topics`): under
/// CPU-starved CI parallelism, this broker's very first `CreateTopics` at its
/// cluster default replication factor can see a live broker count of zero and
/// misreport `INVALID_REPLICATION_FACTOR`, even though it just finished
/// registering.
///
/// `previous` is this node's registration as `image` held it *before* the
/// submit, or `None` on a first boot. A restart resubmits the batch even when
/// nothing about it changed (`broker_restart_batch` always includes it), so a
/// bare "is a registration present" check would return the instant it read
/// the pre-existing record -- before the controller had republished anything
/// -- and every reader downstream of `Broker::start` (the heartbeat sender
/// among them) could keep running against the stale epoch, incarnation,
/// endpoints, or witness-role config the new commit was meant to replace.
/// Every commit assigns a fresh `broker_epoch`, so comparing against
/// `previous` by value reliably distinguishes the new record from the old
/// one even when every other field is identical.
///
/// The wait is bounded by `startup_leader_wait_timeout`, the same budget
/// `wait_for_metadata_leader` uses elsewhere in startup: a broker-only node
/// whose connection to the controller drops right after a forwarded submit
/// succeeded would otherwise never see its own registration published, and
/// `Broker::start` would hang forever waiting for it.
async fn wait_for_self_registration_published(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
    previous: Option<&krabka_metadata::BrokerRegistrationRecord>,
) -> Result<(), BrokerError> {
    let mut images = controller.watch_image();
    let wait_for_publish = async {
        loop {
            if images.borrow().broker(config.node_id) != previous {
                return;
            }
            if images.changed().await.is_err() {
                // The sender is gone; nothing more will ever publish.
                return;
            }
        }
    };
    tokio::time::timeout(
        config.startup_leader_wait_timeout.to_std(),
        wait_for_publish,
    )
    .await
    .map_err(|_| {
        BrokerError::Startup(format!(
            "broker self-registration did not become visible in current_image() within {:?}",
            config.startup_leader_wait_timeout.to_std()
        ))
    })
}

pub(super) async fn submit_bootstrap_records(
    config: &BrokerConfig,
    controller: &dyn crate::metadata_source::MetadataSource,
    mut records: Vec<krabka_metadata::MetadataRecord>,
) -> Result<(), BrokerError> {
    if !matches!(config.bootstrap_mode, crate::BootstrapMode::Bootstrap) {
        return Ok(());
    }
    let checkpoint_loaded = controller.current_image().finalized_features_epoch() >= 0;
    if checkpoint_loaded {
        tracing::info!("bootstrap records already loaded from metadata checkpoint");
    }
    records = bootstrap_records_to_submit(config, records, checkpoint_loaded);
    if records.is_empty() {
        return Ok(());
    }
    tracing::info!(count = records.len(), "submitting bootstrap records");
    submit_startup_records(config, controller, records, "bootstrap submit").await
}

fn bootstrap_records_to_submit(
    config: &BrokerConfig,
    mut records: Vec<krabka_metadata::MetadataRecord>,
    checkpoint_loaded: bool,
) -> Vec<krabka_metadata::MetadataRecord> {
    if checkpoint_loaded {
        records.clear();
    } else {
        records.retain(|record| {
            !matches!(
                record,
                krabka_metadata::MetadataRecord::V1Voters(_)
                    | krabka_metadata::MetadataRecord::V1KRaftVersion(_)
            )
        });
    }
    if config.is_controller() {
        records.extend(stretch_default_records(config));
    }
    records
}

#[cfg(test)]
mod tests;
