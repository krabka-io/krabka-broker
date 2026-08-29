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
        metadata
            .and_then(|m| {
                m.initialized
                    .iter()
                    .find(|(tid, _)| *tid == topic_id)
                    .map(|(_, parts)| parts.clone())
            })
            .unwrap_or_default()
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

/// Build one response partition. It reads the SPSO from the persister. It then
/// computes the best-effort lag (HWM − SPSO) and the leader epoch from the
/// local data partition when that partition is materialized here, and returns
/// `-1` for both otherwise.
async fn describe_partition(
    broker: &Broker,
    persister: &SharePersister,
    gid: &str,
    topic_name: &str,
    topic_id: uuid::Uuid,
    p: i32,
) -> DescribeShareGroupOffsetsResponsePartition {
    let (start_offset, error_code) = match persister.read_state(gid, topic_id, p).await {
        Ok(Some(state)) => (state.start_offset.0, codes::NONE),
        Ok(None) => (UNINITIALIZED_START_OFFSET, codes::NONE),
        Err(_) => (UNINITIALIZED_START_OFFSET, codes::COORDINATOR_NOT_AVAILABLE),
    };
    let (leader_epoch, lag) = if let Some(part) = broker
        .partitions
        .get(topic_name, krabka_ids::PartitionIndex(p))
    {
        let hwm = part.high_watermark().await;
        let le = part
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        let lag = if start_offset >= 0 {
            (hwm.0 - start_offset).max(0)
        } else {
            -1
        };
        (le, lag)
    } else {
        (-1, -1)
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
        image_with_topic, start_broker,
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
                leader_epoch: -1,
                lag: -1,
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
