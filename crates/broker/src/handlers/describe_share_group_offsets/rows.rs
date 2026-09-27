//! The topic and partition rows of a `DescribeShareGroupOffsets` response.
//!
//! This is where a `(group, topic, partition)` turns into numbers: the SPSO
//! comes from the share-state persister, while the leader epoch and the
//! best-effort lag come from the local data partition and fall back to `-1`
//! when the partition is not materialized on this broker. Keeping both row
//! builders together keeps that fallback, and the `UNKNOWN_TOPIC_OR_PARTITION`
//! shape an unresolvable topic name produces, in one place.

use krabka_protocol::{
    owned::{
        describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestTopic,
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue,
    share_coordinator::{
        coordinator::UNINITIALIZED_START_OFFSET, persister_client::SharePersister,
    },
};

/// Kafka's message for `TOPIC_AUTHORIZATION_FAILED`
/// (`Errors.TOPIC_AUTHORIZATION_FAILED.message()`).
const TOPIC_AUTHORIZATION_FAILED_MESSAGE: &str = "Topic authorization failed.";

/// The group's initialized partitions for `topic_id`, or empty when the
/// group has no metadata yet or none of it names this topic. Shared by
/// [`describe_topic`] and [`unauthorized_topic`], both of which must expand
/// an empty request `partitions` list to "every initialized partition" the
/// same way.
fn initialized_partitions(
    topic_id: uuid::Uuid,
    metadata: Option<&ShareGroupStatePartitionMetadataValue>,
) -> Vec<i32> {
    metadata
        .and_then(|m| {
            m.initialized
                .iter()
                .find(|topic| topic.topic_id == topic_id)
                .map(|topic| topic.partitions.clone())
        })
        .unwrap_or_default()
}

/// Build the response row for a topic the request named explicitly, but which
/// the caller may not `Describe`.
///
/// Kafka's `describeShareGroupOffsetsForGroup` partitions the requested names
/// by topic `Describe` before it ever dispatches to the coordinator, so an
/// unauthorized name never reaches the persister. Every partition the request
/// asked about for that topic answers `TOPIC_AUTHORIZATION_FAILED` with the
/// `-1` sentinels and the all-zero topic id, and the row is appended after the
/// coordinator-returned rows. An empty request `partitions` list means "every
/// initialized partition", same as [`describe_topic`] -- otherwise a denied
/// all-partitions request would silently answer with no rows at all.
pub(super) fn unauthorized_topic(
    image: &krabka_metadata::MetadataImage,
    metadata: Option<&ShareGroupStatePartitionMetadataValue>,
    rt: DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    let part_indices = if rt.partitions.is_empty() {
        image
            .topic(&rt.topic_name)
            .map(|t| initialized_partitions(t.topic_id, metadata))
            .unwrap_or_default()
    } else {
        rt.partitions
    };
    let partitions = part_indices
        .into_iter()
        .map(|p| DescribeShareGroupOffsetsResponsePartition {
            partition_index: p,
            start_offset: -1,
            leader_epoch: -1,
            lag: -1,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some(TOPIC_AUTHORIZATION_FAILED_MESSAGE.to_string()),
            ..Default::default()
        })
        .collect();
    DescribeShareGroupOffsetsResponseTopic {
        topic_name: rt.topic_name,
        topic_id: Uuid::default(),
        partitions,
        ..Default::default()
    }
}

/// Build one response topic. It resolves `name → id`, and an unknown name
/// gives per-partition `UNKNOWN_TOPIC_OR_PARTITION`. It enumerates the
/// initialized partitions when the request omits an explicit list. It then
/// builds one row per partition.
pub(super) async fn describe_topic(
    broker: &Broker,
    persister: &SharePersister,
    image: &krabka_metadata::MetadataImage,
    metadata: Option<&ShareGroupStatePartitionMetadataValue>,
    gid: &str,
    rt: DescribeShareGroupOffsetsRequestTopic,
) -> DescribeShareGroupOffsetsResponseTopic {
    let topic_name = rt.topic_name;
    let Some(topic_id) = image.topic(&topic_name).map(|t| t.topic_id) else {
        let partitions = rt
            .partitions
            .into_iter()
            .map(|p| DescribeShareGroupOffsetsResponsePartition {
                partition_index: p,
                start_offset: UNINITIALIZED_START_OFFSET,
                leader_epoch: -1,
                lag: -1,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                ..Default::default()
            })
            .collect();
        return DescribeShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: Uuid::default(),
            partitions,
            ..Default::default()
        };
    };

    // Empty request partitions ⇒ enumerate the group's initialized partitions
    // for this topic_id.
    let part_indices: Vec<i32> = if rt.partitions.is_empty() {
        initialized_partitions(topic_id, metadata)
    } else {
        rt.partitions
    };

    let mut partitions: Vec<DescribeShareGroupOffsetsResponsePartition> =
        Vec::with_capacity(part_indices.len());
    for p in part_indices {
        partitions.push(describe_partition(broker, persister, gid, &topic_name, topic_id, p).await);
    }

    DescribeShareGroupOffsetsResponseTopic {
        topic_name,
        topic_id: Uuid(*topic_id.as_bytes()),
        partitions,
        ..Default::default()
    }
}

/// Kafka's `PartitionFactory.DEFAULT_LEADER_EPOCH`.
const DEFAULT_LEADER_EPOCH: i32 = 0;
/// Kafka's `PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT`.
const UNINITIALIZED_DELIVERY_COMPLETE_COUNT: i32 = -1;
/// Kafka's `PartitionFactory.UNINITIALIZED_LAG`.
const UNINITIALIZED_LAG: i64 = -1;

/// Build one response partition from the persister's state summary, as
/// Kafka's `GroupCoordinatorService.describeShareGroupOffsets` does.
///
/// The leader epoch is the one the share state stores, and
/// [`DEFAULT_LEADER_EPOCH`] for a key with no state. The lag is Kafka's
/// `partition end offset - start offset - delivery complete count`, computed
/// only when both the start offset and the delivery complete count are
/// initialized, and [`UNINITIALIZED_LAG`] otherwise. Kafka asks the partition
/// leader for the end offset; krabka reads the high watermark of the local
/// partition and reports [`UNINITIALIZED_LAG`] for one that is not hosted here.
async fn describe_partition(
    broker: &Broker,
    persister: &SharePersister,
    gid: &str,
    topic_name: &str,
    topic_id: uuid::Uuid,
    p: i32,
) -> DescribeShareGroupOffsetsResponsePartition {
    let (start_offset, leader_epoch, delivery_complete_count, error_code) =
        match persister.read_summary(gid, topic_id, p).await {
            Ok(Some((_, leader_epoch, start_offset, delivery_complete_count))) => (
                start_offset.0,
                leader_epoch,
                delivery_complete_count,
                codes::NONE,
            ),
            Ok(None) => (
                UNINITIALIZED_START_OFFSET,
                DEFAULT_LEADER_EPOCH,
                UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                codes::NONE,
            ),
            Err(_) => (
                UNINITIALIZED_START_OFFSET,
                DEFAULT_LEADER_EPOCH,
                UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
        };
    let lag = if start_offset == UNINITIALIZED_START_OFFSET
        || delivery_complete_count == UNINITIALIZED_DELIVERY_COMPLETE_COUNT
    {
        UNINITIALIZED_LAG
    } else if let Some(part) = broker
        .partitions
        .get(topic_name, krabka_ids::PartitionIndex(p))
    {
        part.high_watermark().await.0 - start_offset - i64::from(delivery_complete_count)
    } else {
        UNINITIALIZED_LAG
    };
    DescribeShareGroupOffsetsResponsePartition {
        partition_index: p,
        start_offset,
        leader_epoch,
        lag,
        error_code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_log::Offset;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::handlers::describe_share_group_offsets::test_support::{
        image_with_topic, register_topic, start_broker,
    };

    #[tokio::test]
    async fn describe_topic_reads_persisted_partition_state() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer), true).await;
        let broker = broker_handle.broker_arc_for_test();
        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let topic_id = uuid::Uuid::from_u128(0xD5C0);
        let image = image_with_topic("orders", topic_id);
        register_topic(&broker, "orders", topic_id).await;
        persister
            .initialize("g-desc", topic_id, 0, 1, Offset(33))
            .await
            .expect("seed state");

        let topic = describe_topic(
            &broker,
            &persister,
            &image,
            None,
            "g-desc",
            DescribeShareGroupOffsetsRequestTopic {
                topic_name: "orders".into(),
                partitions: vec![0],
                ..Default::default()
            },
        )
        .await;

        let expected = DescribeShareGroupOffsetsResponseTopic {
            topic_name: "orders".into(),
            topic_id: Uuid(*topic_id.as_bytes()),
            partitions: vec![DescribeShareGroupOffsetsResponsePartition {
                partition_index: 0,
                start_offset: 33,
                // Kafka's initial leader epoch, and its lag over the empty
                // log: end offset 0 - start offset 33 - delivered 0.
                leader_epoch: 0,
                lag: -33,
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(topic == expected);
        broker_handle.shutdown().await;
    }
}
