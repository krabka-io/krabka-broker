//! Provisioning of the internal metadata topic.
//!
//! One admin round-trip at startup reuses an existing topic, whose real
//! partition count and id then win over the configured values, or creates an
//! absent one with `cleanup.policy=delete` and `retention.ms=-1`. The same
//! round-trip resolves the topic `Uuid`, which the manual fetch path needs
//! because Fetch v13 and later carry `topic_id` and not the name.

use std::collections::BTreeMap;

use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_core::ConnectionOptions;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use tracing::{debug, instrument, warn};

use crate::{error::MetadataLogError, kafka_log::config::KafkaMetadataLogConfig};

/// Provision the topic if it is missing and return `(partition_count,
/// topic_id)`.
///
/// An existing topic's count and id win. This function re-reads a
/// freshly-created topic's id with a second metadata round-trip, because the
/// `CreateTopics` outcome does not reliably carry it.
#[instrument(skip_all, fields(topic = %cfg.topic), err)]
pub(super) async fn ensure_topic(
    cfg: &KafkaMetadataLogConfig,
) -> Result<(i32, WireUuid), MetadataLogError> {
    let mut admin = AdminClient::connect_with_options(
        std::slice::from_ref(&cfg.bootstrap),
        ConnectionOptions {
            client_id: format!("{}-admin", cfg.client_id),
            dispatch_queue_capacity: cfg.dispatch_queue_capacity,
            frame_max: cfg.frame_max,
            security: cfg.security.clone().map(Box::new),
            ..ConnectionOptions::default()
        },
    )
    .await
    .map_err(|e| MetadataLogError::Other(format!("admin connect failed: {e}")))?;

    let topic_ref = cfg.topic.as_str();
    let meta = admin
        .metadata(&[topic_ref])
        .await
        .map_err(|e| MetadataLogError::Other(format!("metadata failed: {e}")))?;

    if let Some(entry) = meta.topics.iter().find(|t| t.name == cfg.topic)
        && entry.error.is_none()
        && entry.partition_count > 0
    {
        debug!(
            topic = %cfg.topic,
            partition_count = entry.partition_count,
            "metadata topic already exists; reusing"
        );
        let topic_id = entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid);
        warn_if_zero_topic_id(&cfg.topic, topic_id);
        return Ok((entry.partition_count, topic_id));
    }

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "delete".to_string());
    configs.insert("retention.ms".to_string(), "-1".to_string());
    let spec = CreateTopicSpec {
        name: cfg.topic.clone(),
        partitions: cfg.num_partitions,
        replicas: cfg.replication,
        configs,
    };
    let outcomes = admin
        .create_topics(&[spec], cfg.topic_create_timeout)
        .await
        .map_err(|e| MetadataLogError::Other(format!("create_topics failed: {e}")))?;
    let outcome = outcomes
        .into_iter()
        .find(|o| o.name == cfg.topic)
        .ok_or_else(|| MetadataLogError::Other("create_topics returned no outcome".into()))?;
    if let Some(err) = outcome.error {
        return Err(MetadataLogError::Other(format!(
            "create_topics for {} failed: {err:?}",
            cfg.topic
        )));
    }
    debug!(
        topic = %cfg.topic,
        partition_count = cfg.num_partitions,
        "metadata topic created"
    );

    // Re-read metadata to learn the freshly-assigned topic id.
    let topic_id = if let Some(id) = outcome.topic_id {
        to_wire_uuid(id)
    } else {
        let meta = admin
            .metadata(&[topic_ref])
            .await
            .map_err(|e| MetadataLogError::Other(format!("metadata (post-create) failed: {e}")))?;
        meta.topics
            .iter()
            .find(|t| t.name == cfg.topic)
            .and_then(|t| t.topic_id)
            .map_or(WireUuid::ZERO, to_wire_uuid)
    };
    warn_if_zero_topic_id(&cfg.topic, topic_id);
    Ok((cfg.num_partitions, topic_id))
}

/// A zero topic id makes every Fetch v≥13 fail, because Fetch carries
/// `topic_id` and not the name. The metadata consumer then spins with no
/// progress. This function warns loudly, so an operator can diagnose the
/// misconfiguration instead of seeing a silent hang.
fn warn_if_zero_topic_id(topic: &str, topic_id: WireUuid) {
    if topic_id == WireUuid::ZERO {
        warn!(
            topic = %topic,
            "metadata topic resolved to a zero topic_id; Fetch v>=13 will fail \
             and the consumer will make no progress"
        );
    }
}

/// Convert the admin client's `uuid::Uuid` to the wire `Uuid` Fetch
/// requires.
fn to_wire_uuid(u: uuid::Uuid) -> WireUuid {
    WireUuid(*u.as_bytes())
}
