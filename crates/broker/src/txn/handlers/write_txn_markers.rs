//! `WriteTxnMarkers` (`api_key=27`). Receives a fan-out from the transaction
//! coordinator (`EndTxn`) and appends control-marker batches to each
//! partition this broker leads.
//!
//! ## Flow
//!
//! For each marker entry in the request:
//! 1. Determine commit or abort from `transaction_result`.
//! 2. For each (topic, partition) named in the marker, as Kafka's
//!    `KafkaApis.handleWriteTxnMarkersRequest` does:
//!    - A partition this broker does not host, or hosts in an offline log
//!      directory, answers `UNKNOWN_TOPIC_OR_PARTITION`. Kafka's
//!      `ReplicaManager.onlinePartition` finds no online partition for both.
//!    - A partition this broker hosts but does not lead answers
//!      `NOT_LEADER_OR_FOLLOWER`, and nothing is appended. Kafka appends with
//!      `AppendOrigin.COORDINATOR`, and the leader append refuses a follower.
//!    - Otherwise the handler appends the marker batch.
//! 3. Return a nested per-producer → per-topic → per-partition response.
//!    Kafka keys the results by producer id, so two marker entries for one
//!    producer come back as one result.
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
pub(crate) mod test_support;

pub(crate) use self::{
    materialize::{MarkerAppend, append_marker_and_materialize},
    offsets::CommittedOffsets,
};
use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, cluster_action_denied, cluster_alter_denied},
    txn::marker::MarkerType,
};

/// Authorizes the request, then writes the markers.
///
/// Kafka's `KafkaApis.handleWriteTxnMarkersRequest` allows a principal that
/// holds `Alter` or `ClusterAction` on the cluster. Any other principal gets
/// `WriteTxnMarkersRequest.getErrorResponse` with
/// `CLUSTER_AUTHORIZATION_FAILED`: that code on every requested partition,
/// and no marker is written.
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = WriteTxnMarkersRequest::decode(&mut cur, version)?;
    let authorizer = broker.config.authorizer.as_ref();
    let image = broker.controller.current_image();
    let resp = if cluster_alter_denied(authorizer, &image, ctx)
        && cluster_action_denied(authorizer, &image, ctx)
    {
        cluster_authorization_failed(&req)
    } else {
        serve(broker, req).await
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// The response Kafka's `WriteTxnMarkersRequest.getErrorResponse` builds for
/// `CLUSTER_AUTHORIZATION_FAILED`: every requested partition of every marker
/// carries the code.
fn cluster_authorization_failed(req: &WriteTxnMarkersRequest) -> WriteTxnMarkersResponse {
    WriteTxnMarkersResponse {
        markers: req
            .markers
            .iter()
            .map(|marker| WritableTxnMarkerResult {
                producer_id: marker.producer_id,
                topics: marker
                    .topics
                    .iter()
                    .map(|topic| WritableTxnMarkerTopicResult {
                        name: topic.name.clone(),
                        partitions: topic
                            .partition_indexes
                            .iter()
                            .map(|&partition_index| WritableTxnMarkerPartitionResult {
                                partition_index,
                                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn serve(
    broker: &Broker,
    req: WriteTxnMarkersRequest,
) -> BoxFuture<'static, WriteTxnMarkersResponse> {
    let partitions = broker.partitions.clone();
    let group_coordinator = broker.group_coordinator.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let node_id = broker.config.node_id;
    Box::pin(async move {
        let mut marker_results = MarkerResults::default();

        for marker_entry in &req.markers {
            let marker_type = if marker_entry.transaction_result {
                MarkerType::Commit
            } else {
                MarkerType::Abort
            };
            // Wrap the wire `i64` into `ProducerId` for the marker builder;
            // unwrapped again below for the raw-`i64` response field.
            let pid = krabka_log::ProducerId(marker_entry.producer_id);
            let marker = MarkerAppend {
                producer_id: pid,
                producer_epoch: marker_entry.producer_epoch,
                marker_type,
                coordinator_epoch: marker_entry.coordinator_epoch,
                commit_stamp: None,
            };

            for topic in &marker_entry.topics {
                for &p in &topic.partition_indexes {
                    let error_code = match partitions.get(&topic.name, PartitionIndex(p)) {
                        Some(part) if !log_dir_status.is_offline(&part.log_dir.load()) => {
                            append_to_led_partition(
                                &part,
                                node_id,
                                &group_coordinator,
                                &topic.name,
                                marker,
                            )
                            .await
                        }
                        _ => {
                            tracing::debug!(
                                topic = %topic.name,
                                partition = p,
                                "WriteTxnMarkers: partition not online here; returning UNKNOWN_TOPIC_OR_PARTITION"
                            );
                            codes::UNKNOWN_TOPIC_OR_PARTITION
                        }
                    };
                    marker_results.record(pid.get(), &topic.name, p, error_code);
                }
            }
        }

        WriteTxnMarkersResponse {
            markers: marker_results.markers,
            ..Default::default()
        }
    })
}

/// Append one marker to a partition this broker hosts, and answer the code for
/// its response row.
///
/// The partition's replication-target read guard spans the leader check and
/// the append, as it does for a Produce. A leadership change takes the write
/// guard, so it cannot move the partition to a follower between the check and
/// the append.
async fn append_to_led_partition(
    part: &crate::partition::Partition,
    node_id: krabka_metadata::NodeId,
    group_coordinator: &std::sync::Arc<crate::coordinator::GroupCoordinator>,
    topic: &str,
    marker: MarkerAppend,
) -> i16 {
    let transition = part.lock_produce_transition().await;
    if transition.leader_node_id != node_id && !part.diskless {
        tracing::debug!(
            topic,
            partition = part.index.get(),
            leader = transition.leader_node_id.0,
            "WriteTxnMarkers: partition not led here; returning NOT_LEADER_OR_FOLLOWER"
        );
        return codes::NOT_LEADER_OR_FOLLOWER;
    }
    let result = append_marker_and_materialize(part, Some(group_coordinator), topic, marker).await;
    drop(transition);
    match result {
        Ok(()) => codes::NONE,
        Err(error) => {
            tracing::warn!(
                topic,
                partition = part.index.get(),
                %error,
                "WriteTxnMarkers: marker append failed"
            );
            marker_error_code(&error)
        }
    }
}

/// The response code for a marker append that failed.
///
/// A log failure is Kafka's `KafkaStorageException`, which the transaction
/// coordinator retries (`TransactionMarkerRequestCompletionHandler`). Every
/// other failure keeps its broker-wide code.
fn marker_error_code(error: &BrokerError) -> i16 {
    match error {
        BrokerError::Log(_) | BrokerError::Io(_) => codes::KAFKA_STORAGE_ERROR,
        other => codes::from_broker_error(other),
    }
}

/// The response under construction, in Kafka's shape: one result per
/// producer id, one topic row per topic name, and one row per partition. The
/// index maps keep each insert constant time, so a request with many
/// partitions builds its response in one linear pass.
#[derive(Default)]
struct MarkerResults {
    markers: Vec<WritableTxnMarkerResult>,
    producers: std::collections::HashMap<i64, usize>,
    topics: std::collections::HashMap<(i64, String), usize>,
    partitions: std::collections::HashMap<(i64, String, i32), usize>,
}

impl MarkerResults {
    /// Put one partition's code into the response. A repeated partition keeps
    /// the last code, as Kafka's map does.
    fn record(&mut self, producer_id: i64, topic: &str, partition_index: i32, error_code: i16) {
        let marker = *self.producers.entry(producer_id).or_insert_with(|| {
            self.markers.push(WritableTxnMarkerResult {
                producer_id,
                ..Default::default()
            });
            self.markers.len() - 1
        });
        let topics = &mut self.markers[marker].topics;
        let topic_row = *self
            .topics
            .entry((producer_id, topic.to_owned()))
            .or_insert_with(|| {
                topics.push(WritableTxnMarkerTopicResult {
                    name: topic.to_owned(),
                    ..Default::default()
                });
                topics.len() - 1
            });
        let partitions = &mut topics[topic_row].partitions;
        match self
            .partitions
            .entry((producer_id, topic.to_owned(), partition_index))
        {
            std::collections::hash_map::Entry::Occupied(row) => {
                partitions[*row.get()].error_code = error_code;
            }
            std::collections::hash_map::Entry::Vacant(row) => {
                row.insert(partitions.len());
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index,
                    error_code,
                    ..Default::default()
                });
            }
        }
    }
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

    /// Serves a request as a principal that the default `AllowAllAuthorizer`
    /// allows.
    async fn handle_allowed(
        broker: &Broker,
        version: i16,
        correlation_id: i32,
        body: &[u8],
    ) -> Result<Bytes, BrokerError> {
        let user = crate::test_support::principal("ANONYMOUS");
        let address = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&user, &address, "write-txn-markers-test");
        super::handle(broker, version, correlation_id, body, &ctx).await
    }

    fn marker(producer_id: i64, topic: &str, partitions: Vec<i32>) -> WritableTxnMarker {
        WritableTxnMarker {
            producer_id,
            producer_epoch: 4,
            transaction_result: true,
            transaction_version: 1,
            topics: vec![WritableTxnMarkerTopic {
                name: topic.into(),
                partition_indexes: partitions,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn result(producer_id: i64, topic: &str, rows: &[(i32, i16)]) -> WritableTxnMarkerResult {
        WritableTxnMarkerResult {
            producer_id,
            topics: vec![WritableTxnMarkerTopicResult {
                name: topic.into(),
                partitions: rows
                    .iter()
                    .map(
                        |&(partition_index, error_code)| WritableTxnMarkerPartitionResult {
                            partition_index,
                            error_code,
                            unknown_tagged_fields: UnknownTaggedFields::default(),
                        },
                    )
                    .collect(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }
    }

    /// Kafka's `KafkaApis.handleWriteTxnMarkersRequest` answers
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a partition that is not online on this
    /// broker, and appends every other marker with `AppendOrigin.COORDINATOR`,
    /// which refuses a follower with `NOT_LEADER_OR_FOLLOWER`.
    #[tokio::test]
    async fn a_marker_appends_only_to_an_online_partition_this_broker_leads() {
        enum Hosting {
            Led,
            Followed,
            NotHosted,
            LedInOfflineLogDir,
        }
        let cases = [
            ("hosted and led", Hosting::Led, codes::NONE, 1),
            (
                "hosted, led elsewhere",
                Hosting::Followed,
                codes::NOT_LEADER_OR_FOLLOWER,
                0,
            ),
            (
                "not hosted",
                Hosting::NotHosted,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                0,
            ),
            (
                "led, log directory offline",
                Hosting::LedInOfflineLogDir,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                0,
            ),
        ];
        for (name, hosting, expected_code, expected_log_end) in cases {
            let (broker_handle, dir) = start_broker().await;
            let broker = broker_handle.broker_arc_for_test();
            let node_id = broker.config.node_id.0;
            let log_dir = dir.path().join("markers");
            let part = match hosting {
                Hosting::NotHosted => None,
                Hosting::Led | Hosting::Followed | Hosting::LedInOfflineLogDir => {
                    let part = open_partition(&broker, &log_dir, "orders", 1);
                    let leader = if matches!(hosting, Hosting::Followed) {
                        node_id + 1
                    } else {
                        node_id
                    };
                    part.install_replication_target(None, leader, 3).await;
                    if matches!(hosting, Hosting::LedInOfflineLogDir) {
                        broker.log_dir_status.mark_offline(&log_dir, "test");
                    }
                    Some(part)
                }
            };

            let bytes = handle_allowed(
                &broker,
                VERSION,
                123,
                &encode_request(&WriteTxnMarkersRequest {
                    markers: vec![marker(91, "orders", vec![1])],
                    ..Default::default()
                }),
            )
            .await
            .expect("handle");

            assert!(
                decode_response(&bytes)
                    == WriteTxnMarkersResponse {
                        markers: vec![result(91, "orders", &[(1, expected_code)])],
                        unknown_tagged_fields: UnknownTaggedFields::default(),
                    },
                "{name}"
            );
            if let Some(part) = part {
                assert!(part.log_end_offset().0 == expected_log_end, "{name}");
            }
            broker_handle.shutdown().await;
        }
    }

    /// Kafka keys the results by producer id, so two marker entries for one
    /// producer come back as one result.
    #[tokio::test]
    async fn marker_entries_for_one_producer_share_one_result() {
        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let node_id = broker.config.node_id.0;
        for partition in [1, 2] {
            open_partition(&broker, dir.path(), "orders", partition)
                .install_replication_target(None, node_id, 3)
                .await;
        }

        let bytes = handle_allowed(
            &broker,
            VERSION,
            123,
            &encode_request(&WriteTxnMarkersRequest {
                markers: vec![
                    marker(91, "orders", vec![1]),
                    marker(92, "orders", vec![1]),
                    marker(91, "orders", vec![2]),
                ],
                ..Default::default()
            }),
        )
        .await
        .expect("handle");

        assert!(
            decode_response(&bytes)
                == WriteTxnMarkersResponse {
                    markers: vec![
                        result(91, "orders", &[(1, codes::NONE), (2, codes::NONE)]),
                        result(92, "orders", &[(1, codes::NONE)]),
                    ],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
        );
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
            base_sequence: 0,
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
        let response = handle_allowed(&broker, VERSION, 1, &encode_request(&req))
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
            base_sequence: 0,
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
        let response = handle_allowed(&broker, VERSION, 1, &encode_request(&req))
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
            base_sequence: 0,
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
        let response = handle_allowed(&broker, VERSION, 1, &encode_request(&req))
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
