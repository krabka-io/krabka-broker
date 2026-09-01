//! `WriteTxnMarkers` (`api_key=27`). Receives a fan-out from the transaction
//! coordinator (`EndTxn`) and appends control-marker batches to each
//! locally-led partition named in the request.
//!
//! ## Flow
//!
//! For each marker entry in the request:
//! 1. Determine commit or abort from `transaction_result`.
//! 2. For each (topic, partition) named in the marker:
//!    - If the partition is locally led, that is, if it is in
//!      `broker.partitions`, build a marker batch and call
//!      `Partition::produce_batch`.
//!    - If it is not local, return per-partition `NOT_LEADER_OR_FOLLOWER`.
//! 3. Return a nested per-marker → per-topic → per-partition response.
//!
//! Wire format: v1 flexible with tagged fields, and v2 flexible with
//! `transaction_version`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use krabka_ids::PartitionIndex;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        write_txn_markers_request::WriteTxnMarkersRequest,
        write_txn_markers_response::{
            WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
            WritableTxnMarkerTopicResult, WriteTxnMarkersResponse,
        },
    },
};

mod materialize;
mod offsets;

#[cfg(test)]
mod test_support;

pub(crate) use self::materialize::{MarkerAppend, append_marker_and_materialize};
use crate::{broker::Broker, codes, error::BrokerError, txn::marker::MarkerType};

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let group_coordinator = broker.group_coordinator.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = WriteTxnMarkersRequest::decode(&mut cur, version)?;

        let mut marker_results: Vec<WritableTxnMarkerResult> = Vec::new();

        for marker_entry in &req.markers {
            let marker_type = if marker_entry.transaction_result {
                MarkerType::Commit
            } else {
                MarkerType::Abort
            };
            // Wrap the wire `i64` into `ProducerId` for the marker builder;
            // unwrapped again below for the raw-`i64` response field.
            let pid = krabka_log::ProducerId(marker_entry.producer_id);
            let epoch = marker_entry.producer_epoch;

            let mut topic_results: Vec<WritableTxnMarkerTopicResult> = Vec::new();

            for topic in &marker_entry.topics {
                let mut partition_results: Vec<WritableTxnMarkerPartitionResult> = Vec::new();

                for &p in &topic.partition_indexes {
                    let error_code = match partitions.get(&topic.name, PartitionIndex(p)) {
                        None => {
                            tracing::debug!(
                                topic = %topic.name,
                                partition = p,
                                "WriteTxnMarkers: partition not local; returning NOT_LEADER_OR_FOLLOWER"
                            );
                            codes::NOT_LEADER_OR_FOLLOWER
                        }
                        Some(part) => {
                            match append_marker_and_materialize(
                                &part,
                                Some(&group_coordinator),
                                &topic.name,
                                MarkerAppend {
                                    producer_id: pid,
                                    producer_epoch: epoch,
                                    marker_type,
                                    coordinator_epoch: marker_entry.coordinator_epoch,
                                    commit_stamp: None,
                                },
                            )
                            .await
                            {
                                Ok(()) => codes::NONE,
                                Err(e) => {
                                    tracing::warn!(
                                        topic = %topic.name,
                                        partition = p,
                                        error = %e,
                                        "WriteTxnMarkers: produce_batch failed"
                                    );
                                    codes::UNKNOWN_SERVER_ERROR
                                }
                            }
                        }
                    };

                    partition_results.push(WritableTxnMarkerPartitionResult {
                        partition_index: p,
                        error_code,
                        ..Default::default()
                    });
                }

                topic_results.push(WritableTxnMarkerTopicResult {
                    name: topic.name.clone(),
                    partitions: partition_results,
                    ..Default::default()
                });
            }

            marker_results.push(WritableTxnMarkerResult {
                producer_id: pid.get(),
                topics: topic_results,
                ..Default::default()
            });
        }

        let resp = WriteTxnMarkersResponse {
            markers: marker_results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::write_txn_markers_request::{WritableTxnMarker, WritableTxnMarkerTopic},
    };

    use super::*;
    use crate::{
        coordinator::{
            bootstrap::OFFSETS_TOPIC, persistence::OffsetCommitValue,
            unified::actor::GroupActorMessage,
        },
        txn::handlers::write_txn_markers::test_support::{open_partition, start_broker},
    };

    const VERSION: i16 = 2;

    crate::test_support::codec_helpers!(
        WriteTxnMarkersRequest,
        WriteTxnMarkersResponse,
        version = VERSION
    );

    #[tokio::test]
    async fn handle_returns_marker_topic_and_partition_result_rows() {
        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(&broker, dir.path(), "orders", 1);
        let req = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result: true,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: "orders".into(),
                    partition_indexes: vec![1, 2],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req);

        let bytes = super::handle(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        let expected = WriteTxnMarkersResponse {
            markers: vec![WritableTxnMarkerResult {
                producer_id: 91,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "orders".into(),
                    partitions: vec![
                        WritableTxnMarkerPartitionResult {
                            partition_index: 1,
                            error_code: codes::NONE,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                        WritableTxnMarkerPartitionResult {
                            partition_index: 2,
                            error_code: codes::NOT_LEADER_OR_FOLLOWER,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    ],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn committed_offsets_are_published_by_the_offsets_partition_marker() {
        use krabka_log::Offset;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "marker-materialization-group";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let value = OffsetCommitValue {
            offset: Offset(42),
            leader_epoch: 3,
            metadata: "txn".into(),
            // A real commit timestamp: an epoch-relative one is older than
            // `offsets.retention.minutes`, so the KIP-211 sweep would
            // correctly reap the offset this test is about.
            commit_timestamp_ms: crate::time_util::now_ms(),
            expire_timestamp_ms: None,
        };
        part.produce_batch(RecordBatch {
            producer_id: 91,
            producer_epoch: 4,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 2)),
                value: Some(value.encode_value()),
                ..Default::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offset");

        let req = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result: true,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: OFFSETS_TOPIC.into(),
                    partition_indexes: vec![offsets_partition],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = super::handle(&broker, VERSION, 1, &encode_request(&req))
            .await
            .expect("commit marker");
        let response = decode_response(&response);
        assert!(response.markers[0].topics[0].partitions[0].error_code == codes::NONE);

        let handle = broker
            .group_coordinator
            .find(group_id)
            .expect("offset home actor");
        let (reply, result) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply })
            .await
            .unwrap();
        let committed = result.await.unwrap().committed;
        let entry = committed
            .get(&("orders".to_string(), 2))
            .expect("committed offset visible");
        assert!(entry.offset == 42);
        assert!(entry.leader_epoch == 3);
        assert!(entry.metadata == "txn");
        broker_handle.shutdown().await;
    }

    /// An abort marker publishes nothing, so a group whose actor has already
    /// exited has nothing left to resolve: its KIP-447 pending marks died with
    /// it. The marker is durable by the time the coordinator is consulted, so
    /// reporting a failure would only make the transaction coordinator retry a
    /// marker that has already landed.
    #[tokio::test]
    async fn abort_marker_succeeds_when_the_groups_actor_has_exited() {
        use krabka_log::Offset;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        use crate::coordinator::unified::actor::GroupKindTag;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "abort-after-actor-exit";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        part.produce_batch(RecordBatch {
            producer_id: 91,
            producer_epoch: 4,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 2)),
                value: Some(
                    OffsetCommitValue {
                        offset: Offset(42),
                        leader_epoch: 3,
                        metadata: "txn".into(),
                        commit_timestamp_ms: 123,
                        expire_timestamp_ms: None,
                    }
                    .encode_value(),
                ),
                ..Default::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offset");

        // The actor takes the transaction's pending marks and then exits,
        // leaving a closed handle behind in the registry.
        let handle = broker
            .group_coordinator
            .get_or_create_group(group_id, GroupKindTag::Classic);
        let (reply, ack) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::AddPendingTxnOffsets {
                producer_id: 91,
                written_at: 0,
                keys: vec![("orders".to_string(), 2)],
                reply,
            })
            .await
            .expect("send AddPendingTxnOffsets");
        ack.await.expect("AddPendingTxnOffsets ack");
        let (reply, ack) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Shutdown(reply))
            .await
            .expect("send Shutdown");
        ack.await.expect("Shutdown ack");
        for _ in 0..1000 {
            if handle.tx.is_closed() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(handle.tx.is_closed());

        let req = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result: false,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: OFFSETS_TOPIC.into(),
                    partition_indexes: vec![offsets_partition],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = super::handle(&broker, VERSION, 1, &encode_request(&req))
            .await
            .expect("abort marker");
        let response = decode_response(&response);
        assert!(
            response
                == WriteTxnMarkersResponse {
                    markers: vec![WritableTxnMarkerResult {
                        producer_id: 91,
                        topics: vec![WritableTxnMarkerTopicResult {
                            name: OFFSETS_TOPIC.into(),
                            partitions: vec![WritableTxnMarkerPartitionResult {
                                partition_index: offsets_partition,
                                error_code: codes::NONE,
                                unknown_tagged_fields: UnknownTaggedFields::default(),
                            }],
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    }],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
        broker_handle.shutdown().await;
    }

    /// One transaction can carry offset commits for several groups on the same
    /// offsets partition, and its marker has to resolve every one of them. The
    /// marker is durable before the coordinator is consulted, and it ended the
    /// log's pending transaction, so a group the resolution skips keeps its
    /// KIP-447 marks and loses its offsets for good: no marker retry can
    /// rediscover them.
    #[tokio::test]
    async fn a_commit_marker_resolves_every_group_in_the_transaction() {
        use krabka_log::Offset;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        use crate::coordinator::unified::actor::GroupKindTag;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let first = "marker-two-groups-a";
        let second = "marker-two-groups-b";
        // Both groups' records go into one batch on one partition, which is
        // what a single marker resolves; the group ids come off the record
        // keys, not from the partition.
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            first,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let row = |group_id, topic, partition, offset, delta| Record {
            offset_delta: delta,
            key: Some(OffsetCommitValue::encode_key(group_id, topic, partition)),
            value: Some(
                OffsetCommitValue {
                    offset: Offset(offset),
                    leader_epoch: 3,
                    metadata: "txn".into(),
                    commit_timestamp_ms: 123,
                    expire_timestamp_ms: None,
                }
                .encode_value(),
            ),
            ..Default::default()
        };
        part.produce_batch(RecordBatch {
            producer_id: 91,
            producer_epoch: 4,
            attributes: Attributes::default().with_transactional(true),
            last_offset_delta: 1,
            records: vec![
                row(first, "orders", 2, 42, 0),
                row(second, "payments", 5, 7, 1),
            ],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offsets");

        // Both groups hold the transaction's pending marks, the way
        // `TxnOffsetCommit` leaves them.
        for (group_id, topic, partition) in [(first, "orders", 2), (second, "payments", 5)] {
            let handle = broker
                .group_coordinator
                .get_or_create_group(group_id, GroupKindTag::Classic);
            let (reply, ack) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(GroupActorMessage::AddPendingTxnOffsets {
                    producer_id: 91,
                    written_at: 0,
                    keys: vec![(topic.to_string(), partition)],
                    reply,
                })
                .await
                .expect("send AddPendingTxnOffsets");
            ack.await.expect("AddPendingTxnOffsets ack");
        }

        let req = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 91,
                producer_epoch: 4,
                transaction_result: true,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: OFFSETS_TOPIC.into(),
                    partition_indexes: vec![offsets_partition],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = super::handle(&broker, VERSION, 1, &encode_request(&req))
            .await
            .expect("commit marker");
        assert!(
            decode_response(&response).markers[0].topics[0].partitions[0].error_code == codes::NONE
        );

        for (group_id, topic, partition, offset) in
            [(first, "orders", 2, 42), (second, "payments", 5, 7)]
        {
            let handle = broker
                .group_coordinator
                .find(group_id)
                .expect("offset home actor");
            let (reply, result) = tokio::sync::oneshot::channel();
            handle
                .tx
                .send(GroupActorMessage::FetchOffsets { reply })
                .await
                .expect("send FetchOffsets");
            let offsets = result.await.expect("FetchOffsets reply");
            assert!(
                offsets
                    .committed
                    .get(&(topic.to_string(), partition))
                    .map(|entry| entry.offset)
                    == Some(krabka_log::Offset(offset))
            );
            assert!(offsets.pending_txn.is_empty());
        }
        broker_handle.shutdown().await;
    }
}
