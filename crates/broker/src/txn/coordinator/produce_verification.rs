//! KIP-890 part 1: the transaction check that a partition leader asks the
//! transaction coordinator for before a transactional `Produce` starts a
//! transaction on the partition.
//!
//! Kafka's `ReplicaManager.handleProduceAppend` sends the check through
//! `AddPartitionsToTxnManager.addOrVerifyTransaction`. From `Produce` v12
//! (transaction version 2) the check adds the partition to the transaction.
//! Below v12 it only verifies that the client added the partition with its own
//! `AddPartitionsToTxn`. The call always goes to the coordinator's broker, also
//! when that is this broker, so the answer does not depend on where the
//! coordinator lives.

use krabka_log::ProducerId;
use krabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
};

use super::TxnCoordinator;
use crate::{
    codes,
    txn::{
        bootstrap,
        state::{TopicPartition, TxnState},
        version::TxnVersion,
    },
};

/// One partition check for one transactional producer.
#[derive(Debug, Clone)]
pub(crate) struct PartitionCheck<'a> {
    pub(crate) transactional_id: &'a str,
    pub(crate) producer_id: ProducerId,
    pub(crate) producer_epoch: i16,
    pub(crate) partition: TopicPartition,
    /// `true` asks only whether the partition is in the transaction. `false`
    /// adds it.
    pub(crate) verify_only: bool,
}

impl TxnCoordinator {
    /// Ask the coordinator of `check.transactional_id` to add or verify one
    /// partition, and return the coordinator's answer for that partition.
    ///
    /// The answer is the partition error code of the `AddPartitionsToTxn`
    /// response, or the top-level error code when the response has one. A
    /// coordinator this broker cannot find answers `COORDINATOR_NOT_AVAILABLE`,
    /// and a call that does not complete answers `NETWORK_EXCEPTION`, as
    /// Kafka's `AddPartitionsToTxnManager` reports them.
    pub(crate) async fn add_or_verify_partition(
        &self,
        check: PartitionCheck<'_>,
        txnv: TxnVersion,
    ) -> i16 {
        let Some(transport) = &self.marker_transport else {
            return self.add_or_verify_locally(check, txnv).await;
        };
        let image = transport.controller.current_image();
        self.refresh_leader_partitions(&image).await;
        let coordinator_partition = self.partition_for(check.transactional_id);
        let Some(leader) = image
            .partition(bootstrap::TOPIC, coordinator_partition.get())
            .map(|partition| partition.leader)
        else {
            return codes::COORDINATOR_NOT_AVAILABLE;
        };
        if leader == self.node_id {
            return self.add_or_verify_locally(check, txnv).await;
        }
        let Some(broker) = image.broker(leader) else {
            return codes::COORDINATOR_NOT_AVAILABLE;
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
            name: check.partition.topic.clone(),
            partitions: vec![check.partition.partition.get()],
            ..Default::default()
        };
        let request = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: check.transactional_id.to_string(),
                producer_id: check.producer_id.get(),
                producer_epoch: check.producer_epoch,
                topics: vec![topic.clone()],
                verify_only: check.verify_only,
                ..Default::default()
            }],
            v3_and_below_transactional_id: check.transactional_id.to_string(),
            v3_and_below_producer_id: check.producer_id.get(),
            v3_and_below_producer_epoch: check.producer_epoch,
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
                tracing::warn!(%error, %host, port, "transaction partition check connect failed");
                return codes::NETWORK_EXCEPTION;
            }
        };
        let response = match connection.send(request).await {
            Ok(response) => response,
            Err(error) => {
                connection.close();
                tracing::warn!(%error, %host, port, "transaction partition check failed");
                return codes::NETWORK_EXCEPTION;
            }
        };
        connection.close();
        if response.error_code != codes::NONE {
            return response.error_code;
        }
        response
            .results_by_transaction
            .iter()
            .find(|transaction| transaction.transactional_id == check.transactional_id)
            .and_then(|transaction| {
                transaction
                    .topic_results
                    .iter()
                    .find(|topic| topic.name == check.partition.topic)
            })
            .and_then(|topic| {
                topic
                    .results_by_partition
                    .iter()
                    .find(|partition| partition.partition_index == check.partition.partition.get())
            })
            .map_or(codes::UNKNOWN_SERVER_ERROR, |partition| {
                partition.partition_error_code
            })
    }

    async fn add_or_verify_locally(&self, check: PartitionCheck<'_>, txnv: TxnVersion) -> i16 {
        if check.verify_only {
            self.verify_partition_in_transaction(&check).await
        } else {
            self.register_partitions(
                check.transactional_id,
                check.producer_id,
                check.producer_epoch,
                vec![check.partition],
                txnv,
            )
            .await
        }
    }

    /// Whether the partition is in the producer's transaction. Kafka's
    /// `TransactionCoordinator.handleVerifyPartitionsInTransaction`.
    pub(crate) async fn verify_partition_in_transaction(&self, check: &PartitionCheck<'_>) -> i16 {
        if !self.is_coordinator_for(check.transactional_id).await {
            return codes::NOT_COORDINATOR;
        }
        let Some(entry) = self.get(check.transactional_id) else {
            return codes::INVALID_PRODUCER_ID_MAPPING;
        };
        let entry = entry.lock().await;
        verification_code(
            (entry.producer_id, entry.producer_epoch, entry.state),
            entry.partitions.contains(&check.partition),
            (check.producer_id, check.producer_epoch),
        )
    }
}

/// The verify-only answer for one partition, from the transaction's identity,
/// state and partition set. Kafka's
/// `TransactionCoordinator.handleVerifyPartitionsInTransaction`.
pub(crate) fn verification_code(
    (producer_id, producer_epoch, state): (ProducerId, i16, TxnState),
    contains_partition: bool,
    (requested_producer_id, requested_epoch): (ProducerId, i16),
) -> i16 {
    if producer_id != requested_producer_id {
        codes::INVALID_PRODUCER_ID_MAPPING
    } else if producer_epoch != requested_epoch {
        codes::PRODUCER_FENCED
    } else if matches!(state, TxnState::PrepareCommit | TxnState::PrepareAbort) {
        codes::CONCURRENT_TRANSACTIONS
    } else if contains_partition {
        codes::NONE
    } else {
        codes::TRANSACTION_ABORTABLE
    }
}

#[cfg(test)]
mod tests;
