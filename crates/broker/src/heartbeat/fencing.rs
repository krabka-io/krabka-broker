//! Publication of the controller's fencing decisions into the metadata log.
//!
//! The heartbeat registry lives on the controller leader alone, so the state
//! it holds — who is past their session deadline, who has not yet proved
//! metadata catch-up — is invisible to every other node until it is written
//! down. Kafka writes it down as `BrokerRegistration.fenced`, set by a
//! `BrokerRegistrationChangeRecord` when `BrokerHeartbeatManager` fences or
//! unfences a broker, and `KRaftMetadataCache.isReplicaOffline` reads it back
//! (`fenced() || !hasOnlineDir(dir)`) on whichever broker serves the request.
//!
//! Krabka publishes the same bit as the controller-managed
//! [`BROKER_FENCED`](crate::config_keys::BROKER_FENCED) broker config, because
//! `BrokerRegistrationRecord` carries no fencing flag — the same reason the
//! witness role is a broker config. [`publish_fencing_changes`] runs on the
//! controller leader at every liveness tick and is level-triggered: it
//! compares the registry against the image and submits only the difference,
//! so a broker that stays dead costs one record, not one per tick.

use std::{sync::Arc, time::Duration};

use krabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord};

use crate::{
    config_keys::{BROKER_FENCED, FENCED_TRUE, resolve_broker_fenced},
    heartbeat::controller_state::ControllerLivenessState,
    metadata_source::MetadataSource,
};

/// Upper bound on the fencing commit, matching the failover submit the same
/// tick makes. A stalled raft commit must not wedge the liveness ticker; the
/// next tick recomputes the same difference and retries.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The records that bring the image's fencing state in line with `unavailable`.
///
/// `unavailable` is [`ControllerLivenessState::unavailable_snapshot`]: the
/// brokers that are fenced or past their heartbeat deadline. A registered
/// broker in that set gains `broker.fenced=true`; one that is out of it and
/// still carries the key gets a tombstone. Everything else yields nothing,
/// which is what makes a repeated tick silent.
fn fencing_changes(
    image: &MetadataImage,
    unavailable: &std::collections::HashSet<u64>,
) -> Vec<MetadataRecord> {
    image
        .brokers()
        .filter_map(|broker| {
            let want_fenced = unavailable.contains(&broker.node_id.0);
            (want_fenced != resolve_broker_fenced(image, broker.node_id)).then(|| {
                tracing::info!(
                    broker = broker.node_id.0,
                    fenced = want_fenced,
                    "publishing broker fencing state to the metadata log",
                );
                MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id: broker.node_id,
                    config_name: BROKER_FENCED.to_string(),
                    config_value: want_fenced.then(|| FENCED_TRUE.to_string()),
                })
            })
        })
        .collect()
}

/// Publish the controller's current fencing state, if the image does not
/// already carry it. The caller gates on controller leadership, as only the
/// leader holds a populated registry and only the leader can submit. A submit
/// failure is logged and does not propagate: the next tick retries.
pub(crate) async fn publish_fencing_changes(
    controller: &Arc<dyn MetadataSource>,
    liveness: &ControllerLivenessState,
) {
    let image = controller.current_image();
    let changes = fencing_changes(&image, &liveness.unavailable_snapshot().await);
    if changes.is_empty() {
        return;
    }
    match tokio::time::timeout(SUBMIT_TIMEOUT, controller.submit_change(changes)).await {
        Ok(Err(error)) => tracing::warn!(%error, "fencing-state submit_change failed"),
        Err(_elapsed) => tracing::warn!(
            timeout = ?SUBMIT_TIMEOUT,
            "fencing-state submit_change did not commit in time",
        ),
        Ok(Ok(_)) => {}
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::NodeId;

    use super::*;

    fn image_with(brokers: &[u64], fenced: &[u64]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for &node in brokers {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    node_id: NodeId(node),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
                    host: "127.0.0.1".into(),
                    port: 9_092,
                    rack: None,
                    endpoints: vec![],
                    log_dirs: vec![uuid::Uuid::from_u128(0x600d)],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        for &node in fenced {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(node),
                config_name: BROKER_FENCED.to_string(),
                config_value: Some(FENCED_TRUE.to_string()),
            }));
        }
        image
    }

    fn unavailable(ids: &[u64]) -> std::collections::HashSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn a_dead_broker_gains_the_fencing_key() {
        let image = image_with(&[1, 2], &[]);

        let changes = fencing_changes(&image, &unavailable(&[2]));

        assert!(
            changes
                == vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id: NodeId(2),
                    config_name: BROKER_FENCED.to_string(),
                    config_value: Some(FENCED_TRUE.to_string()),
                })]
        );
    }

    #[test]
    fn a_recovered_broker_has_the_fencing_key_tombstoned() {
        let image = image_with(&[1, 2], &[2]);

        let changes = fencing_changes(&image, &unavailable(&[]));

        assert!(
            changes
                == vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id: NodeId(2),
                    config_name: BROKER_FENCED.to_string(),
                    config_value: None,
                })]
        );
    }

    #[test]
    fn an_image_that_already_agrees_yields_no_change() {
        let image = image_with(&[1, 2, 3], &[3]);

        assert!(fencing_changes(&image, &unavailable(&[3])) == vec![]);
    }

    #[test]
    fn an_unregistered_broker_is_not_published() {
        // The liveness registry can name a broker the image no longer
        // registers. An unregistered replica is already offline by the
        // registration rule, so there is nothing to publish for it.
        let image = image_with(&[1], &[]);

        assert!(fencing_changes(&image, &unavailable(&[7])) == vec![]);
    }
}
