//! Remote transaction partition verification and its cached inter-broker connections.

use {
    super::produce_verification::TransactionCheck,
    crate::{
        broker::registered_listener_endpoint, codes, network::client::InterBrokerClient,
        txn::state::TopicPartition,
    },
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_metadata::{MetadataImage, NodeId},
    krabka_protocol::owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    },
    krabka_security::ListenerProtocol,
    std::{collections::HashMap, sync::Arc},
    tokio::sync::Mutex,
};

/// The remote verification protocol and connection lifecycle, separate from
/// transaction state and leadership locks.
pub(super) struct VerificationClient {
    node_id: NodeId,
    inter_broker_client: Arc<InterBrokerClient>,
    protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
    connections: Mutex<HashMap<NodeId, VerificationConnection>>,
}

/// A cached connection and the endpoint at which it was dialed.
struct VerificationConnection {
    address: VerificationEndpoint,
    connection: Connection,
}

struct VerificationEndpoint {
    host: String,
    port: u16,
}

impl VerificationClient {
    pub(super) fn new(
        node_id: NodeId,
        inter_broker_client: Arc<InterBrokerClient>,
        protocol: ListenerProtocol,
        listener_name: String,
        server_name: String,
    ) -> Self {
        Self {
            node_id,
            inter_broker_client,
            protocol,
            listener_name,
            server_name,
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Send one check and return answers in the caller's partition order.
    pub(super) async fn add_or_verify(
        &self,
        image: &MetadataImage,
        leader: NodeId,
        check: &TransactionCheck<'_>,
    ) -> Result<Vec<(TopicPartition, i16)>, i16> {
        let Some(broker) = image.broker(leader) else {
            return Err(codes::COORDINATOR_NOT_AVAILABLE);
        };
        let (host, port) = registered_listener_endpoint(broker, &self.listener_name);
        let host = host.to_owned();
        let mut topics: Vec<AddPartitionsToTxnTopic> = Vec::new();
        for partition in &check.partitions {
            match topics
                .iter_mut()
                .find(|topic| topic.name == partition.topic)
            {
                Some(topic) => topic.partitions.push(partition.partition.get()),
                None => topics.push(AddPartitionsToTxnTopic {
                    name: partition.topic.clone(),
                    partitions: vec![partition.partition.get()],
                    ..Default::default()
                }),
            }
        }
        let request = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: check.transactional_id.to_string(),
                producer_id: check.producer_id.get(),
                producer_epoch: check.producer_epoch,
                topics: topics.clone(),
                verify_only: check.verify_only,
                ..Default::default()
            }],
            v3_and_below_transactional_id: check.transactional_id.to_string(),
            v3_and_below_producer_id: check.producer_id.get(),
            v3_and_below_producer_epoch: check.producer_epoch,
            v3_and_below_topics: topics,
            ..Default::default()
        };
        let Some(connection) = self.verification_connection(leader, &host, port).await else {
            return Err(codes::NETWORK_EXCEPTION);
        };
        let response = match connection.send(request).await {
            Ok(response) => response,
            Err(error) => {
                self.drop_verification_connection(leader).await;
                tracing::warn!(%error, %host, port, "transaction partition check failed");
                return Err(codes::NETWORK_EXCEPTION);
            }
        };
        if response.error_code != codes::NONE {
            return Err(response.error_code);
        }
        let transaction = response
            .results_by_transaction
            .iter()
            .find(|transaction| transaction.transactional_id == check.transactional_id);
        Ok(check
            .partitions
            .iter()
            .map(|partition| {
                let code = transaction
                    .and_then(|transaction| {
                        transaction
                            .topic_results
                            .iter()
                            .find(|topic| topic.name == partition.topic)
                    })
                    .and_then(|topic| {
                        topic
                            .results_by_partition
                            .iter()
                            .find(|answer| answer.partition_index == partition.partition.get())
                    })
                    .map_or(codes::UNKNOWN_SERVER_ERROR, |answer| {
                        answer.partition_error_code
                    });
                (partition.clone(), code)
            })
            .collect())
    }

    /// The open connection to the coordinator on `leader`, dialed on first
    /// use and kept for the next check, as Kafka's `AddPartitionsToTxnManager`
    /// keeps its `NetworkClient` connection. `None` when the dial fails.
    async fn verification_connection(
        &self,
        leader: NodeId,
        host: &str,
        port: u16,
    ) -> Option<Connection> {
        let mut connections = self.connections.lock().await;
        if let Some(cached) = connections.get(&leader)
            && cached.address.host == host
            && cached.address.port == port
            && !cached.connection.is_closed()
        {
            return Some(cached.connection.clone());
        }
        let options = ConnectionOptions {
            client_id: format!("krabka-broker-txn-{}", self.node_id),
            ..Default::default()
        };
        match self
            .inter_broker_client
            .connect_as_connection(host, port, self.protocol, &self.server_name, options)
            .await
        {
            Ok(connection) => {
                if let Some(stale) = connections.insert(leader, VerificationConnection {
                    address: VerificationEndpoint {
                        host: host.to_string(),
                        port,
                    },
                    connection: connection.clone(),
                }) {
                    stale.connection.close();
                }
                Some(connection)
            }
            Err(error) => {
                tracing::warn!(%error, %host, port, "transaction partition check connect failed");
                None
            }
        }
    }

    /// Forget and close the connection to `leader` after a failed call.
    async fn drop_verification_connection(&self, leader: NodeId) {
        if let Some(stale) = self.connections.lock().await.remove(&leader) {
            stale.connection.close();
        }
    }
}

#[cfg(test)]
mod tests;
