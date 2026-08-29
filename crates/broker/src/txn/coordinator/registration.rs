//! Partition enrollment into a locally-coordinated transaction.
//!
//! The module holds the `AddPartitionsToTxn` transition that validates the
//! producer identity, moves the entry to `Ongoing`, and records the partitions
//! the transaction writes. It also holds the KIP-890 path that routes an
//! offsets-partition enrollment to the broker that coordinates the
//! `transactional_id`, over the inter-broker client when that broker is remote.

use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use krabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
};

use super::TxnCoordinator;
use crate::{
    coordinator::bootstrap::OFFSETS_TOPIC,
    txn::{bootstrap, state::TxnState, version::TxnVersion},
};

impl TxnCoordinator {
    /// Add partitions to the locally-coordinated transaction after validating
    /// its producer identity. This is shared by client `AddPartitionsToTxn` and
    /// the KIP-890 server-side `TxnOffsetCommit` path.
    pub(crate) async fn register_partitions(
        &self,
        tid: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        partitions: Vec<crate::txn::state::TopicPartition>,
        txnv: TxnVersion,
    ) -> i16 {
        if !self.is_coordinator_for(tid).await {
            return crate::codes::NOT_COORDINATOR;
        }
        let Some(entry_mutex) = self.get(tid) else {
            return crate::codes::INVALID_PRODUCER_ID_MAPPING;
        };
        let mut entry = entry_mutex.lock().await;
        if entry.has_staged_producer_identity() {
            return crate::codes::INVALID_TXN_STATE;
        }
        if entry.producer_id != producer_id || entry.producer_epoch != producer_epoch {
            return crate::codes::INVALID_PRODUCER_EPOCH;
        }
        if !entry.state.can_transition_to(TxnState::Ongoing) {
            return crate::codes::INVALID_TXN_STATE;
        }
        let prior_state = entry.state;
        if matches!(
            prior_state,
            TxnState::CompleteCommit | TxnState::CompleteAbort
        ) {
            entry.partitions.clear();
        }
        entry.state = TxnState::Ongoing;
        if prior_state != TxnState::Ongoing {
            entry.start_ms = crate::txn::util::now_millis();
        }
        entry.partitions.extend(partitions);
        entry.last_update_ms = crate::txn::util::now_millis();
        let snapshot = entry.clone();
        drop(entry);

        if let Err(error) = self.put(snapshot, txnv).await {
            tracing::error!(tid, %error, "failed to persist registered transaction partitions");
            return crate::codes::UNKNOWN_SERVER_ERROR;
        }
        crate::codes::NONE
    }

    /// KIP-890: route the offsets partition enrollment to the transaction
    /// coordinator before a v5+ `TxnOffsetCommit` append.
    pub(crate) async fn register_offsets_partition(
        &self,
        tid: &str,
        producer_id: ProducerId,
        producer_epoch: i16,
        offsets_partition: PartitionIndex,
        txnv: TxnVersion,
    ) -> i16 {
        let Some(transport) = &self.marker_transport else {
            return self
                .register_partitions(
                    tid,
                    producer_id,
                    producer_epoch,
                    vec![crate::txn::state::TopicPartition {
                        topic: OFFSETS_TOPIC.to_string(),
                        partition: offsets_partition,
                    }],
                    txnv,
                )
                .await;
        };
        let image = transport.controller.current_image();
        self.refresh_leader_partitions(&image).await;
        let coordinator_partition = self.partition_for(tid);
        let Some(leader) = image
            .partition(bootstrap::TOPIC, coordinator_partition.get())
            .map(|partition| partition.leader)
        else {
            return crate::codes::COORDINATOR_NOT_AVAILABLE;
        };
        if leader == self.node_id {
            return self
                .register_partitions(
                    tid,
                    producer_id,
                    producer_epoch,
                    vec![crate::txn::state::TopicPartition {
                        topic: OFFSETS_TOPIC.to_string(),
                        partition: offsets_partition,
                    }],
                    txnv,
                )
                .await;
        }
        let Some(broker) = image.broker(leader) else {
            return crate::codes::COORDINATOR_NOT_AVAILABLE;
        };
        let (host, port) = broker
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == transport.listener_name)
            .map_or_else(
                || (broker.host.clone(), broker.port),
                |endpoint| (endpoint.host.clone(), endpoint.port),
            );
        let topic = AddPartitionsToTxnTopic {
            name: OFFSETS_TOPIC.to_string(),
            partitions: vec![offsets_partition.get()],
            ..Default::default()
        };
        let request = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: tid.to_string(),
                producer_id: producer_id.get(),
                producer_epoch,
                topics: vec![topic.clone()],
                verify_only: false,
                ..Default::default()
            }],
            v3_and_below_transactional_id: tid.to_string(),
            v3_and_below_producer_id: producer_id.get(),
            v3_and_below_producer_epoch: producer_epoch,
            v3_and_below_topics: vec![topic],
            ..Default::default()
        };
        let options = krabka_client_core::ConnectionOptions {
            client_id: format!("krabka-broker-txn-{}", self.node_id),
            ..Default::default()
        };
        let connection = match transport
            .inter_broker_client
            .connect_as_connection(
                &host,
                port,
                transport.protocol,
                &transport.server_name,
                options,
            )
            .await
        {
            Ok(connection) => connection,
            Err(error) => {
                tracing::warn!(%error, %host, port, "TxnOffsetCommit coordinator connect failed");
                return crate::codes::COORDINATOR_NOT_AVAILABLE;
            }
        };
        let response = match connection.send(request).await {
            Ok(response) => response,
            Err(error) => {
                connection.close();
                tracing::warn!(%error, %host, port, "TxnOffsetCommit partition enrollment failed");
                return crate::codes::COORDINATOR_NOT_AVAILABLE;
            }
        };
        connection.close();
        response
            .results_by_transaction
            .iter()
            .find(|transaction| transaction.transactional_id == tid)
            .and_then(|transaction| {
                transaction
                    .topic_results
                    .iter()
                    .find(|topic| topic.name == OFFSETS_TOPIC)
            })
            .and_then(|topic| {
                topic
                    .results_by_partition
                    .iter()
                    .find(|partition| partition.partition_index == offsets_partition.get())
            })
            .map_or(response.error_code, |partition| {
                partition.partition_error_code
            })
    }
}
