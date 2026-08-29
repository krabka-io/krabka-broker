//! The two lookups that tell a client where a coordinator is.
//!
//! A group RPC first asks whether this broker leads the group's offsets
//! partition, and `FindCoordinator` then reports the host and port of the
//! broker that does.

/// Return the Kafka routing error for a group RPC sent to the wrong broker,
/// or `None` when this broker leads the group's offsets partition.
pub(crate) fn group_coordinator_error(
    broker: &crate::broker::Broker,
    group_id: &str,
) -> Option<i16> {
    use crate::coordinator::partitioner::{GroupRoutingError, local_partition_for_group};

    match local_partition_for_group(
        &broker.controller.current_image(),
        broker.config.node_id,
        group_id,
    ) {
        Ok(_) => None,
        Err(GroupRoutingError::Unavailable) => Some(crate::codes::COORDINATOR_NOT_AVAILABLE),
        Err(GroupRoutingError::NotCoordinator) => Some(crate::codes::NOT_COORDINATOR),
    }
}

pub(crate) fn parse_advertised_host_port(addr: &str) -> (String, u16) {
    if let Some(host_port) = crate::host_port::parse_host_port(addr) {
        return host_port;
    }
    tracing::warn!(
        addr,
        "advertised_listener not host:port; falling back to localhost:9092"
    );
    (
        crate::host_port::DEFAULT_KAFKA_HOST.into(),
        crate::host_port::DEFAULT_KAFKA_PORT,
    )
}
