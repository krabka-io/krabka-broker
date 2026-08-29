//! Turning a coordinator key into the broker that owns it.
//!
//! Every key type reduces to the same question: which partition of the state
//! topic holds this key, and which broker leads that partition. This module
//! holds that lookup, the `Coordinator` rows it produces for a resolved and an
//! unavailable partition, the local-broker shortcut for key types this handler
//! does not route, and the parser for the share-coordinator's composite key.

use krabka_protocol::owned::find_coordinator_response::Coordinator;

use crate::{broker::Broker, codes, handlers::parse_advertised_host_port as parse_host_port};

pub(super) fn local_coordinators(
    keys: Vec<String>,
    broker_id: i32,
    advertised: &str,
) -> Vec<Coordinator> {
    let (host, port) = parse_host_port(advertised);
    keys.into_iter()
        .map(|key| Coordinator {
            key,
            node_id: broker_id,
            host: host.clone(),
            port: i32::from(port),
            error_code: codes::NONE,
            error_message: None,
            ..Default::default()
        })
        .collect()
}

pub(super) fn resolve_transaction_keys(
    broker: &Broker,
    keys: Vec<String>,
    advertised: &str,
    context: &crate::handlers::RequestContext<'_>,
) -> Vec<Coordinator> {
    keys.into_iter()
        .map(|key| {
            let partition = broker.txn_coordinator.partition_for(&key).get();
            resolve_partition_coordinator(
                broker,
                &broker.controller.current_image(),
                crate::txn::bootstrap::TOPIC,
                partition,
                key,
                advertised,
                context,
            )
        })
        .collect()
}

pub(super) fn resolve_partition_coordinator(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    state_topic: &str,
    partition: i32,
    key: String,
    advertised: &str,
    context: &crate::handlers::RequestContext<'_>,
) -> Coordinator {
    let Some(record) = image.partition(state_topic, partition) else {
        return unavailable_coordinator(key, "partition not found");
    };
    let leader = record.leader;
    let Some(registration) = image.broker(leader) else {
        return unavailable_coordinator(key, "leader broker not registered");
    };
    let (host, port) = if leader == broker.config.node_id {
        let (host, port) = parse_host_port(advertised);
        (host, i32::from(port))
    } else {
        crate::handlers::metadata::pick_endpoint_host_port(
            registration,
            context.connection_listener_name,
            &broker.config.inter_broker_listener_name,
        )
    };
    Coordinator {
        key,
        node_id: i32::try_from(leader.0).unwrap_or(-1),
        host,
        port,
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    }
}

fn unavailable_coordinator(key: String, message: &str) -> Coordinator {
    Coordinator {
        key,
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code: codes::COORDINATOR_NOT_AVAILABLE,
        error_message: Some(message.to_string()),
        ..Default::default()
    }
}

/// Parse a share-coordinator key `"{group}:{topicId}:{partition}"` into its
/// `(group, topic_id, partition)` parts.
///
/// A group id can itself contain `:`, so the function reads the partition and
/// the topic-id from the right. It returns `None` for a malformed partition int,
/// a malformed topic-id UUID, or missing segments.
pub(super) fn parse_share_key(key: &str) -> Option<(&str, uuid::Uuid, i32)> {
    let (rest, partition_str) = key.rsplit_once(':')?;
    let (group, topic_str) = rest.rsplit_once(':')?;
    let partition: i32 = partition_str.parse().ok()?;
    let topic_id = uuid::Uuid::parse_str(topic_str).ok()?;
    Some((group, topic_id, partition))
}
