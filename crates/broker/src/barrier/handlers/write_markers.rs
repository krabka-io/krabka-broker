//! `WriteBarrierMarkers`, api key 1014.
//!
//! This is the receiving side of the marker fan-out. A coordinator sends the
//! request to the broker that leads a target partition, and this handler
//! appends one marker into each named partition and returns the offset that
//! each marker took. [`transport`][crate::barrier::handlers::transport] is the
//! sending side.
//!
//! A client never sends this request. It is inter-broker traffic, and Kafka
//! gates the same kind of traffic on `ClusterAction`.
//!
//! # What the handler refuses
//!
//! - A partition that this broker does not lead takes
//!   `NOT_LEADER_OR_FOLLOWER` (6). The coordinator then reads a fresh metadata
//!   image and retries against the new leader.
//! - A partition whose locally-installed leader epoch is below the epoch of
//!   the metadata image takes `FENCED_LEADER_EPOCH` (74). This broker has not
//!   applied the newest leadership change yet, so the marker batch would carry
//!   a stale `partition_leader_epoch` in its header.
//!
//! The refusal is per partition. One request that names both a led and an
//! unled partition marks the first and refuses the second.
//!
//! # The request carries no expected leader epoch
//!
//! `WriteBarrierMarkersRequest` holds `group`, `epoch`, `triggered_at`, and the
//! target partitions. It holds no epoch of the target partition, so the handler
//! cannot compare the coordinator's view against its own. The fence above is
//! what this broker can check on its own, and it is the check that keeps a
//! stale epoch out of a marker header.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use crabka_ids::{NodeId, PartitionIndex};
use crabka_metadata::MetadataImage;
use crabka_protocol::{
    Decode,
    krabka::barrier::{
        WritableBarrierTopic, WriteBarrierMarkersRequest, WriteBarrierMarkersResponse,
        WrittenBarrierPartition, WrittenBarrierTopic,
    },
};
use tracing::warn;

use crate::{
    barrier::{handlers::cluster_action_denied, injection::append_marker, marker::BarrierMarker},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, encode_response},
    partition::Partition,
    partition_registry::PartitionRegistry,
};

/// The `offset` of a partition row that placed no marker.
const NO_OFFSET: i64 = -1;

#[tracing::instrument(
    name = "handle_write_barrier_markers",
    level = "debug",
    skip_all,
    fields(api = "WriteBarrierMarkers"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = WriteBarrierMarkersRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    if cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        let topics = req
            .topics
            .iter()
            .map(|topic| refused_topic(topic, codes::CLUSTER_AUTHORIZATION_FAILED))
            .collect();
        return encode_response(&response(topics), version);
    }

    let marker = BarrierMarker {
        group: req.group.clone(),
        epoch: req.epoch,
        triggered_at: req.triggered_at,
    };

    let mut topics = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for &index in &topic.partitions {
            partitions.push(
                mark(
                    &broker.partitions,
                    &image,
                    broker.config.node_id,
                    &marker,
                    &topic.topic,
                    PartitionIndex(index),
                )
                .await,
            );
        }
        topics.push(WrittenBarrierTopic {
            topic: topic.topic.clone(),
            partitions,
            ..WrittenBarrierTopic::default()
        });
    }

    encode_response(&response(topics), version)
}

/// Append one marker into one partition, and report the outcome.
async fn mark(
    partitions: &PartitionRegistry,
    image: &MetadataImage,
    node_id: NodeId,
    marker: &BarrierMarker,
    topic: &str,
    index: PartitionIndex,
) -> WrittenBarrierPartition {
    let Some(partition) = partitions.get(topic, index) else {
        return row(index, codes::NOT_LEADER_OR_FOLLOWER, NO_OFFSET);
    };
    if let Some(code) = leadership_fault(&partition, image, node_id, topic, index) {
        return row(index, code, NO_OFFSET);
    }
    match append_marker(&partition, marker).await {
        Ok(offset) => row(index, codes::NONE, offset.get()),
        Err(error) => {
            warn!(
                topic,
                partition = index.get(),
                %error,
                "WriteBarrierMarkers: the marker append failed"
            );
            row(index, codes::UNKNOWN_SERVER_ERROR, NO_OFFSET)
        }
    }
}

/// The code that refuses a partition, or `None` when this broker may mark it.
fn leadership_fault(
    partition: &Partition,
    image: &MetadataImage,
    node_id: NodeId,
    topic: &str,
    index: PartitionIndex,
) -> Option<i16> {
    if partition.current_leader.load(Ordering::Acquire) != node_id.get() {
        return Some(codes::NOT_LEADER_OR_FOLLOWER);
    }
    let local_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
    let image_epoch = image
        .partition(topic, index.get())
        .map_or(local_epoch, |record| record.leader_epoch.get());
    if local_epoch < image_epoch {
        return Some(codes::FENCED_LEADER_EPOCH);
    }
    None
}

/// One partition row of the response.
fn row(index: PartitionIndex, error_code: i16, offset: i64) -> WrittenBarrierPartition {
    WrittenBarrierPartition {
        partition: index.get(),
        error_code,
        offset,
        ..WrittenBarrierPartition::default()
    }
}

/// Every partition of one requested topic, stamped with one code.
fn refused_topic(topic: &WritableBarrierTopic, error_code: i16) -> WrittenBarrierTopic {
    WrittenBarrierTopic {
        topic: topic.topic.clone(),
        partitions: topic
            .partitions
            .iter()
            .map(|index| row(PartitionIndex(*index), error_code, NO_OFFSET))
            .collect(),
        ..WrittenBarrierTopic::default()
    }
}

/// The response around a list of topic rows.
fn response(topics: Vec<WrittenBarrierTopic>) -> WriteBarrierMarkersResponse {
    WriteBarrierMarkersResponse {
        topics,
        ..WriteBarrierMarkersResponse::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_log::Offset;
    use crabka_metadata::MetadataRecord;
    use crabka_units::mebibytes;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::barrier::{
        marker::parse_barrier_marker,
        test_support::{open_partition, topic_records},
    };

    const LOCAL: NodeId = NodeId(1);

    fn marker() -> BarrierMarker {
        BarrierMarker {
            group: "orders-cut".to_owned(),
            epoch: 4,
            triggered_at: 1_724_500_000_000,
        }
    }

    fn image(records: &[MetadataRecord]) -> MetadataImage {
        MetadataImage::from_records(Uuid::nil(), records)
    }

    /// A registry with `orders-0` open, and the local broker installed as its
    /// leader at `leader_epoch`.
    async fn registry_with_leader(
        dir: &std::path::Path,
        leader: NodeId,
        leader_epoch: i32,
    ) -> PartitionRegistry {
        let registry = PartitionRegistry::new();
        open_partition(&registry, dir, "orders", 0);
        let partition = registry
            .get("orders", PartitionIndex(0))
            .expect("the partition is open");
        partition
            .install_leader_change(leader.get(), leader_epoch)
            .await;
        registry
    }

    #[tokio::test]
    async fn a_partition_that_is_not_open_here_is_not_led_here() {
        let registry = PartitionRegistry::new();
        let image = image(&topic_records("orders", 1, LOCAL));
        let written = mark(
            &registry,
            &image,
            LOCAL,
            &marker(),
            "orders",
            PartitionIndex(0),
        )
        .await;
        check!(written == row(PartitionIndex(0), codes::NOT_LEADER_OR_FOLLOWER, NO_OFFSET));
    }

    #[tokio::test]
    async fn a_partition_that_another_broker_leads_is_refused() {
        let dir = tempdir().expect("tempdir");
        let registry = registry_with_leader(dir.path(), NodeId(2), 3).await;
        let image = image(&topic_records("orders", 1, NodeId(2)));
        let written = mark(
            &registry,
            &image,
            LOCAL,
            &marker(),
            "orders",
            PartitionIndex(0),
        )
        .await;
        check!(written == row(PartitionIndex(0), codes::NOT_LEADER_OR_FOLLOWER, NO_OFFSET));
    }

    #[tokio::test]
    async fn a_leader_epoch_below_the_image_is_fenced() {
        let dir = tempdir().expect("tempdir");
        // `topic_records` builds the image at leader epoch 3, and this replica
        // still carries epoch 2.
        let registry = registry_with_leader(dir.path(), LOCAL, 2).await;
        let image = image(&topic_records("orders", 1, LOCAL));
        let written = mark(
            &registry,
            &image,
            LOCAL,
            &marker(),
            "orders",
            PartitionIndex(0),
        )
        .await;
        check!(written == row(PartitionIndex(0), codes::FENCED_LEADER_EPOCH, NO_OFFSET));
    }

    #[tokio::test]
    async fn a_led_partition_takes_the_marker_and_returns_its_offset() {
        let dir = tempdir().expect("tempdir");
        let registry = registry_with_leader(dir.path(), LOCAL, 3).await;
        let image = image(&topic_records("orders", 1, LOCAL));
        let written = mark(
            &registry,
            &image,
            LOCAL,
            &marker(),
            "orders",
            PartitionIndex(0),
        )
        .await;
        check!(written == row(PartitionIndex(0), codes::NONE, 0));

        // The record at the returned offset is the marker the request named.
        let partition = registry
            .get("orders", PartitionIndex(0))
            .expect("the partition is open");
        let read = partition
            .read_log(Offset(0), mebibytes(1))
            .expect("read the log back");
        check!(read.batches.len() == 1);
        let batch = &read.batches[0];
        check!(batch.attributes.is_control_batch());
        check!(parse_barrier_marker(&batch.records[0]).ok() == Some(marker()));
    }

    #[test]
    fn a_denied_request_stamps_every_named_partition() {
        let topic = WritableBarrierTopic {
            topic: "orders".to_owned(),
            partitions: vec![0, 2],
            ..WritableBarrierTopic::default()
        };
        let expected = WrittenBarrierTopic {
            topic: "orders".to_owned(),
            partitions: vec![
                row(
                    PartitionIndex(0),
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    NO_OFFSET,
                ),
                row(
                    PartitionIndex(2),
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    NO_OFFSET,
                ),
            ],
            ..WrittenBarrierTopic::default()
        };
        check!(refused_topic(&topic, codes::CLUSTER_AUTHORIZATION_FAILED) == expected);
    }
}
