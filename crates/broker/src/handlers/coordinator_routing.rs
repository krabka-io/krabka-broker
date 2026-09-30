//! The two lookups that tell a client where a coordinator is.
//!
//! A group RPC first asks whether this broker leads the group's offsets
//! partition, and `FindCoordinator` then reports the host and port of the
//! broker that does.

/// Return the Kafka routing error for a group RPC sent to the wrong broker,
/// `COORDINATOR_LOAD_IN_PROGRESS` while this broker still replays the group's
/// offsets partition, or `None` when this broker serves the group.
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
        Ok(partition) => group_partition_loading(broker, partition),
        Err(GroupRoutingError::Unavailable) => Some(crate::codes::COORDINATOR_NOT_AVAILABLE),
        Err(GroupRoutingError::NotCoordinator) => Some(crate::codes::NOT_COORDINATOR),
    }
}

/// `COORDINATOR_LOAD_IN_PROGRESS` while this broker still replays the offsets
/// `partition` it leads, or has not yet taken up the leadership term the
/// current image names, as Kafka's `CoordinatorRuntime` answers for a shard
/// that is not `ACTIVE`.
pub(crate) fn group_partition_loading(
    broker: &crate::broker::Broker,
    partition: i32,
) -> Option<i16> {
    let epoch = broker
        .controller
        .current_image()
        .partition(crate::coordinator::bootstrap::OFFSETS_TOPIC, partition)
        .map(|record| record.leader_epoch);
    epoch
        .is_none_or(|epoch| broker.group_coordinator.is_loading(partition, epoch))
        .then_some(crate::codes::COORDINATOR_LOAD_IN_PROGRESS)
}

/// `true` while any `__consumer_offsets` partition this broker leads is not
/// served yet: it is replaying, or the image watcher has not taken up its
/// leadership term. Kafka's `ListGroups` reads every local shard, and
/// `CoordinatorRuntime.withActiveContextOrThrow` throws
/// `COORDINATOR_LOAD_IN_PROGRESS` for one that is still `LOADING`, so the
/// whole answer is that error rather than the groups of the shards that are
/// ready.
pub(crate) fn any_group_partition_loading(broker: &crate::broker::Broker) -> bool {
    let coordinator = &broker.group_coordinator;
    coordinator.is_any_loading()
        || broker
            .controller
            .current_image()
            .partitions_of(crate::coordinator::bootstrap::OFFSETS_TOPIC)
            .filter(|record| record.leader == broker.config.node_id)
            .any(|record| coordinator.is_loading(record.partition, record.leader_epoch))
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
