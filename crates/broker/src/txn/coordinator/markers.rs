//! Transaction-marker fan-out from the transaction coordinator.
//!
//! The module holds the `TxnCoordinator` methods that write `COMMIT` and
//! `ABORT` control markers to every partition a transaction touched. One path
//! fans the markers out to the partition leaders through the inter-broker
//! client, and the fallback path appends to locally-held partitions when no
//! marker transport is configured.

use super::TxnCoordinator;
use crate::{
    error::BrokerError,
    txn::{
        bootstrap,
        handlers::{
            end_txn::{MarkerDispatchContext, dispatch_markers},
            write_txn_markers::{MarkerAppend, append_marker_and_materialize},
        },
        marker::MarkerType,
        state::TxnEntry,
    },
};

const UNKNOWN_COORDINATOR_EPOCH: i32 = -1;

impl TxnCoordinator {
    pub(crate) async fn dispatch_transaction_markers(
        &self,
        entry: &TxnEntry,
        marker_type: MarkerType,
    ) -> Result<(), BrokerError> {
        #[cfg(any(test, feature = "test-helpers"))]
        self.marker_fanout_gate.pass().await?;
        let Some(transport) = &self.marker_transport else {
            return self.dispatch_local_markers(entry, marker_type).await;
        };
        let image = transport.controller.current_image();
        let coordinator_partition = self.partition_for(&entry.transactional_id);
        // Kafka sends the coordinator epoch that the partition was loaded at,
        // not the epoch of a later image.
        let coordinator_epoch = self
            .loaded_leader_epoch(coordinator_partition)
            .await
            .ok_or_else(|| {
                BrokerError::Txn(format!(
                    "this broker does not coordinate {}-{}",
                    bootstrap::TOPIC,
                    coordinator_partition.get()
                ))
            })?;
        dispatch_markers(
            MarkerDispatchContext {
                node_id: self.node_id,
                coordinator_epoch,
                image: &image,
                inter_broker_client: &transport.inter_broker_client,
                inter_broker_protocol: transport.protocol,
                inter_broker_listener_name: &transport.listener_name,
                inter_broker_server_name: &transport.server_name,
                group_coordinator: self.group_coordinator.as_ref(),
            },
            &self.partitions,
            entry,
            marker_type,
        )
        .await
    }

    async fn dispatch_local_markers(
        &self,
        entry: &TxnEntry,
        marker_type: MarkerType,
    ) -> Result<(), BrokerError> {
        for tp in &entry.partitions {
            let part = self
                .partitions
                .get(&tp.topic, tp.partition)
                .ok_or_else(|| {
                    BrokerError::Txn(format!(
                        "transaction marker transport is not configured for remote partition {}-{}",
                        tp.topic,
                        tp.partition.get()
                    ))
                })?;
            append_marker_and_materialize(
                &part,
                self.group_coordinator.as_ref(),
                &tp.topic,
                MarkerAppend {
                    producer_id: entry.producer_id,
                    producer_epoch: entry.producer_epoch,
                    marker_type,
                    coordinator_epoch: UNKNOWN_COORDINATOR_EPOCH,
                    commit_stamp: None,
                },
            )
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn local_marker_uses_the_unknown_coordinator_epoch_sentinel() {
        check!(UNKNOWN_COORDINATOR_EPOCH == -1);
    }
}
