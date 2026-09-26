//! Phase 2 of `EndTxn`: the `WriteTxnMarkers` fan-out. This module groups the
//! transaction's partitions by their current leader, appends the marker batch
//! directly to every partition this broker leads, and hands each remote leader
//! to the RPC in [`super::marker_rpc`].

use std::{collections::HashMap, sync::atomic::Ordering};

use krabka_metadata::{MetadataImage, NodeId};
use krabka_security::ListenerProtocol;

use super::marker_rpc::send_write_txn_markers;
use crate::{
    broker::Broker,
    error::BrokerError,
    network::client::InterBrokerClient,
    txn::{
        handlers::write_txn_markers::{MarkerAppend, append_marker_and_materialize},
        marker::{MarkerFailureClass, MarkerType, classify_marker_failure},
        state::{TopicPartition, TxnEntry},
    },
};

/// The outcome of one attempt to fan out `WriteTxnMarkers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MarkerFanOutOutcome {
    /// Every partition's marker is durable.
    Complete,
    /// At least one partition failed with a retriable code. The `Prepare*`
    /// record is durable, so the caller hands the transaction to the
    /// completion task, which retries the fan-out.
    Retry,
    /// A fenced producer or coordinator generation cancelled the fan-out
    /// (#882). A newer generation has already superseded this attempt, so
    /// retrying cannot succeed; the caller must not queue it for completion.
    GivenUp,
}

/// What one marker fan-out attempt achieved.
#[derive(Debug, Default)]
pub(crate) struct MarkerFanOut {
    /// The partitions whose marker is durable, and those that no longer exist
    /// and so need none. Kafka's `TransactionMarkerRequestCompletionHandler`
    /// removes each of them from the transaction's partition set.
    pub(crate) written: Vec<TopicPartition>,
    /// The most severe failure of the attempt, if any partition failed.
    pub(crate) failure: Option<BrokerError>,
}

impl MarkerFanOut {
    /// Records `error`, keeping the most severe failure seen so far.
    pub(crate) fn fail(&mut self, error: BrokerError) {
        record_worse_failure(&mut self.failure, error);
    }

    /// Folds another group's outcome into this one.
    pub(crate) fn merge(&mut self, other: Self) {
        self.written.extend(other.written);
        if let Some(error) = other.failure {
            self.fail(error);
        }
    }
}

/// Write the markers for a prepared transaction.
pub(super) async fn dispatch_transaction_markers(
    broker: &Broker,
    snapshot: &mut TxnEntry,
    marker_type: MarkerType,
    transactional_id: &str,
) -> MarkerFanOutOutcome {
    match broker
        .txn_coordinator
        .dispatch_transaction_markers(snapshot, marker_type)
        .await
    {
        Ok(()) => MarkerFanOutOutcome::Complete,
        Err(error) if classify_marker_failure(&error) == MarkerFailureClass::Fatal => {
            tracing::warn!(
                tid = transactional_id,
                error = %error,
                "EndTxn: WriteTxnMarkers fan-out fenced by a newer generation; giving up"
            );
            MarkerFanOutOutcome::GivenUp
        }
        Err(error) => {
            tracing::warn!(
                tid = transactional_id,
                error = %error,
                "EndTxn: WriteTxnMarkers fan-out failed; queued for completion"
            );
            MarkerFanOutOutcome::Retry
        }
    }
}

/// Dispatch `WriteTxnMarkers` to every partition leader involved in the
/// transaction. The function groups partitions by leader node:
///
/// - **local** (leader == `node_id`): directly calls
///   [`Partition::produce_batch`] on the in-memory handle.
/// - **remote**: sends a
///   [`WriteTxnMarkersRequest`](krabka_protocol::owned::write_txn_markers_request::WriteTxnMarkersRequest)
///   over the shared
///   [`InterBrokerClient`], which runs TLS / SASL when the inter-broker
///   listener demands them.
///
/// Any `__consumer_offsets` partitions registered through `AddOffsetsToTxn`
/// live in `entry.partitions`, because Kafka's model has no separate group
/// list. The same loop therefore fans them out with the data partitions.
#[derive(Clone, Copy)]
pub(crate) struct MarkerDispatchContext<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) coordinator_epoch: i32,
    pub(crate) image: &'a MetadataImage,
    pub(crate) inter_broker_client: &'a InterBrokerClient,
    pub(crate) inter_broker_protocol: ListenerProtocol,
    pub(crate) inter_broker_listener_name: &'a str,
    pub(crate) inter_broker_server_name: &'a str,
    pub(crate) group_coordinator: Option<&'a std::sync::Arc<crate::coordinator::GroupCoordinator>>,
}

pub(crate) async fn dispatch_markers(
    context: MarkerDispatchContext<'_>,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    entry: &TxnEntry,
    marker_type: MarkerType,
) -> MarkerFanOut {
    let MarkerDispatchContext {
        node_id,
        coordinator_epoch,
        image,
        ..
    } = context;
    // Group every involved (topic, partition) by its current leader.
    let mut by_leader: HashMap<NodeId, Vec<TopicPartition>> = HashMap::new();
    let mut outcome = MarkerFanOut::default();

    for tp in &entry.partitions {
        let leader = if let Some(partition) = image.partition(&tp.topic, tp.partition.get()) {
            Some(partition.leader)
        } else if let Some(partition) = partitions.get(&tp.topic, tp.partition) {
            if partition.current_leader.load(Ordering::Acquire) != node_id.0 {
                return MarkerFanOut {
                    written: Vec::new(),
                    failure: Some(BrokerError::Txn(format!(
                        "transaction marker target {}-{} is materialized locally but missing from metadata",
                        tp.topic,
                        tp.partition.get()
                    ))),
                };
            }
            Some(node_id)
        } else {
            // The partition was deleted after joining the transaction. There
            // is no log left to mark, so it must not block completion.
            None
        };
        match leader {
            Some(leader) => by_leader.entry(leader).or_default().push(tp.clone()),
            None => outcome.written.push(tp.clone()),
        }
    }

    // Every leader group is attempted, even after an earlier group fails: a
    // partition whose marker already landed must not be abandoned because a
    // different partition in the same fan-out round needs a retry (#882). The
    // worst classified failure (a fatal one over a retriable one) is what the
    // caller sees, so a fenced generation still cancels the whole attempt.
    for (leader, tps) in by_leader {
        if leader == node_id {
            // Local path: directly append a marker batch to each partition.
            for tp in tps {
                let Some(part) = partitions.get(&tp.topic, tp.partition) else {
                    outcome.fail(BrokerError::Txn(format!(
                        "transaction marker target {}-{} is led locally but is not materialized",
                        tp.topic,
                        tp.partition.get()
                    )));
                    continue;
                };
                match append_marker_and_materialize(
                    &part,
                    context.group_coordinator,
                    &tp.topic,
                    MarkerAppend {
                        producer_id: entry.producer_id,
                        producer_epoch: entry.producer_epoch,
                        marker_type,
                        coordinator_epoch,
                        commit_stamp: None,
                        transaction_version: entry.client_transaction_version,
                    },
                )
                .await
                {
                    Ok(()) => outcome.written.push(tp),
                    Err(error) => outcome.fail(error),
                }
            }
        } else {
            // Remote path: send WriteTxnMarkersRequest to the leader.
            outcome.merge(send_write_txn_markers(context, leader, entry, marker_type, &tps).await);
        }
    }
    outcome
}

/// Keeps `worst` at the most severe of what it already holds and `error`: a
/// fatal classification always wins over a retriable one, and the first
/// error is kept when both are the same class.
fn record_worse_failure(worst: &mut Option<BrokerError>, error: BrokerError) {
    let replace = match worst {
        None => true,
        Some(existing) => {
            classify_marker_failure(&error) == MarkerFailureClass::Fatal
                && classify_marker_failure(existing) != MarkerFailureClass::Fatal
        }
    };
    if replace {
        *worst = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::PartitionIndex;

    use super::*;
    use crate::txn::handlers::end_txn::test_support::{marker_entry, plaintext_client, tps};

    #[tokio::test]
    async fn marker_dispatch_skips_deleted_partition() {
        let image = MetadataImage::default();
        let client = plaintext_client();
        let partitions = std::sync::Arc::new(crate::partition_registry::PartitionRegistry::new());
        let mut entry = marker_entry();
        entry.partitions.insert(tps().remove(0));

        let result = dispatch_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image: &image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Plaintext,
                inter_broker_listener_name: "PLAINTEXT",
                inter_broker_server_name: "localhost",
                group_coordinator: None,
            },
            &partitions,
            &entry,
            MarkerType::Commit,
        )
        .await;

        // A deleted partition needs no marker, so it leaves the transaction.
        assert!(result.failure.is_none());
        assert!(result.written == tps());
    }

    #[tokio::test]
    async fn marker_dispatch_marks_materialized_partition_missing_from_image() {
        use krabka_log::{Log, LogConfig, Offset};

        let image = MetadataImage::default();
        let client = plaintext_client();
        let partitions = std::sync::Arc::new(crate::partition_registry::PartitionRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        let part_dir = crate::log_dir::partition_dir(dir.path(), "t", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let part = crate::broker::spawn_partition(
            "t".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).unwrap(),
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        part.current_leader.store(2, Ordering::Release);
        partitions.insert("t".into(), PartitionIndex(0), part.clone());
        let mut entry = marker_entry();
        entry.partitions.insert(tps().remove(0));

        let context = MarkerDispatchContext {
            node_id: NodeId(1),
            coordinator_epoch: 0,
            image: &image,
            inter_broker_client: &client,
            inter_broker_protocol: ListenerProtocol::Plaintext,
            inter_broker_listener_name: "PLAINTEXT",
            inter_broker_server_name: "localhost",
            group_coordinator: None,
        };
        assert!(
            dispatch_markers(context, &partitions, &entry, MarkerType::Commit)
                .await
                .failure
                .is_some()
        );
        assert!(part.log_end_offset() == Offset(0));

        part.current_leader.store(1, Ordering::Release);
        let written = dispatch_markers(context, &partitions, &entry, MarkerType::Commit).await;
        assert!(written.failure.is_none());

        assert!(part.log_end_offset() == Offset(1));
        assert!(part.last_stable_offset(Offset(1)) == Offset(1));
    }

    /// #882: a marker failure on one partition must not abandon the others
    /// in the same fan-out round. The local, reachable partition still gets
    /// its marker even though the remote, unreachable one fails.
    #[tokio::test]
    async fn marker_dispatch_appends_every_reachable_partition_despite_one_failing() {
        use krabka_ids::PartitionIndex as PIdx;
        use krabka_log::{Log, LogConfig, Offset};
        use krabka_metadata::{
            BrokerRegistrationRecord, MetadataRecord, PartitionRecord, TopicRecord,
        };

        let mut image = MetadataImage::default();
        for (topic, leader) in [("local-topic", NodeId(1)), ("remote-topic", NodeId(2))] {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: topic.to_owned(),
                topic_id: uuid::Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }));
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_owned(),
                partition: 0,
                leader,
                replicas: vec![leader],
                isr: vec![leader],
                ..Default::default()
            }));
        }
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                // Discard port: refuses connections immediately.
                port: 9,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));

        let client = plaintext_client();
        let partitions = std::sync::Arc::new(crate::partition_registry::PartitionRegistry::new());
        let dir = tempfile::tempdir().unwrap();
        let part_dir = crate::log_dir::partition_dir(dir.path(), "local-topic", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let local_partition = crate::broker::spawn_partition(
            "local-topic".to_string(),
            PIdx(0),
            dir.path().to_path_buf(),
            Log::open(&part_dir, LogConfig::default()).unwrap(),
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        partitions.insert("local-topic".into(), PIdx(0), local_partition.clone());

        let mut entry = marker_entry();
        entry.partitions.insert(TopicPartition {
            topic: "local-topic".into(),
            partition: PIdx(0),
        });
        entry.partitions.insert(TopicPartition {
            topic: "remote-topic".into(),
            partition: PIdx(0),
        });

        let result = dispatch_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image: &image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Plaintext,
                inter_broker_listener_name: "PLAINTEXT",
                inter_broker_server_name: "localhost",
                group_coordinator: None,
            },
            &partitions,
            &entry,
            MarkerType::Commit,
        )
        .await;

        // Retriable: the remote connect failure is not a fenced generation.
        // #852: only the local partition's marker is written.
        assert!(
            result.written
                == vec![TopicPartition {
                    topic: "local-topic".into(),
                    partition: PIdx(0),
                }]
        );
        let error = result
            .failure
            .expect("the unreachable remote partition must fail");
        assert!(classify_marker_failure(&error) == MarkerFailureClass::Retriable);
        // The local partition's marker landed despite the remote failure.
        assert!(local_partition.log_end_offset() == Offset(1));
    }
}
