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
    codes,
    error::BrokerError,
    network::client::InterBrokerClient,
    txn::{
        handlers::write_txn_markers::{MarkerAppend, append_marker_and_materialize},
        marker::MarkerType,
        state::{TopicPartition, TxnEntry},
    },
};

pub(super) async fn dispatch_transaction_markers(
    broker: &Broker,
    snapshot: &TxnEntry,
    marker_type: MarkerType,
    transactional_id: &str,
) -> Result<(), i16> {
    broker
        .txn_coordinator
        .dispatch_transaction_markers(snapshot, marker_type)
        .await
        .map_err(|error| {
            tracing::error!(
                tid = transactional_id,
                error = %error,
                "EndTxn: WriteTxnMarkers fan-out failed; returning retriable error"
            );
            codes::UNKNOWN_SERVER_ERROR
        })
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
) -> Result<(), BrokerError> {
    let MarkerDispatchContext {
        node_id,
        coordinator_epoch,
        image,
        ..
    } = context;
    // Group every involved (topic, partition) by its current leader.
    let mut by_leader: HashMap<NodeId, Vec<TopicPartition>> = HashMap::new();

    for tp in &entry.partitions {
        let leader = if let Some(partition) = image.partition(&tp.topic, tp.partition.get()) {
            Some(partition.leader)
        } else if let Some(partition) = partitions.get(&tp.topic, tp.partition) {
            if partition.current_leader.load(Ordering::Acquire) != node_id.0 {
                return Err(BrokerError::Txn(format!(
                    "transaction marker target {}-{} is materialized locally but missing from metadata",
                    tp.topic,
                    tp.partition.get()
                )));
            }
            Some(node_id)
        } else {
            // The partition was deleted after joining the transaction. There
            // is no log left to mark, so it must not block completion.
            None
        };
        if let Some(leader) = leader {
            by_leader.entry(leader).or_default().push(tp.clone());
        }
    }

    for (leader, tps) in by_leader {
        if leader == node_id {
            // Local path: directly append a marker batch to each partition.
            for tp in &tps {
                let part = partitions.get(&tp.topic, tp.partition).ok_or_else(|| {
                    BrokerError::Txn(format!(
                        "transaction marker target {}-{} is led locally but is not materialized",
                        tp.topic,
                        tp.partition.get()
                    ))
                })?;
                append_marker_and_materialize(
                    &part,
                    context.group_coordinator,
                    &tp.topic,
                    MarkerAppend {
                        producer_id: entry.producer_id,
                        producer_epoch: entry.producer_epoch,
                        marker_type,
                        coordinator_epoch,
                        commit_stamp: None,
                    },
                )
                .await?;
            }
        } else {
            // Remote path: send WriteTxnMarkersRequest to the leader.
            send_write_txn_markers(context, leader, entry, marker_type, &tps).await?;
        }
    }

    Ok(())
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

        assert!(result.is_ok());
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
                .is_err()
        );
        assert!(part.log_end_offset() == Offset(0));

        part.current_leader.store(1, Ordering::Release);
        dispatch_markers(context, &partitions, &entry, MarkerType::Commit)
            .await
            .unwrap();

        assert!(part.log_end_offset() == Offset(1));
        assert!(part.lso() == Offset(1));
    }
}
