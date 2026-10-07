//! `AlterPartition` (`api_key=56`). Controller-side ISR update handler.
//!
//! The handler validates that this broker is the openraft leader, and returns
//! `NOT_CONTROLLER` if it is not. It then checks the sender's broker epoch,
//! validates each partition row in the order of Kafka's
//! `ReplicationControlManager.validateAlterPartitionData` (see `isr_update`),
//! and submits the updated `PartitionRecord`s through
//! `controller.submit_change`.

use krabka_metadata::MetadataRecord;
use krabka_protocol::{
    owned::{
        alter_partition_request::AlterPartitionRequest,
        alter_partition_response::{
            AlterPartitionResponse, PartitionData as RespPartitionData, TopicData as RespTopicData,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

mod isr_update;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::isr_update::handle_partition_with_recovery;
use crate::{
    codes,
    elr::ElrPublisher,
    error::BrokerError,
    handlers::{cluster_action_denied, forward_to_controller::is_active_controller},
};

context_handler! {
    AlterPartitionRequest => AlterPartitionResponse,
    (broker, req, _version, ctx),
    {
        let controller = broker.controller.clone();

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        // Then only the active controller handles AlterPartition; any other node
        // answers a whole-response `NOT_CONTROLLER`.
        let refusal = if cluster_action_denied(
            broker.config.authorizer.as_ref(),
            &controller.current_image(),
            ctx,
        ) {
            Some(codes::CLUSTER_AUTHORIZATION_FAILED)
        } else if is_active_controller(broker) {
            None
        } else {
            Some(codes::NOT_CONTROLLER)
        };
        if let Some(error_code) = refusal {
            return Ok(unthrottled_wire!(AlterPartitionResponse {
                error_code,
                topics: Vec::new(),
            }));
        }

        let image = controller.current_image();
        // Kafka's `ReplicationControlManager.alterPartition` starts with
        // `ClusterControlManager.checkBrokerEpoch`: a sender that is not
        // registered, or that sends another epoch than its registration's,
        // gets a top-level `STALE_BROKER_EPOCH` and no rows.
        let sender_epoch = u64::try_from(req.broker_id)
            .ok()
            .and_then(|id| image.broker_epoch(krabka_metadata::NodeId(id)));
        if sender_epoch != Some(req.broker_epoch) {
            return Ok(unthrottled_wire!(AlterPartitionResponse {
                error_code: codes::STALE_BROKER_EPOCH,
                topics: Vec::new(),
            }));
        }
        // One snapshot of the brokers that may sit in an ISR: alive, unfenced
        // and not in controlled shutdown. The registry is seeded for this term
        // first, so a request served right after a failover does not read
        // the registry an earlier term left.
        broker
            .liveness
            .seed_term(
                controller.current_controller_epoch(),
                crate::heartbeat::controller_state::replicated_registrations(&image),
            )
            .await;
        let active = broker.liveness.alive_snapshot().await;
        let mut changes: Vec<MetadataRecord> = Vec::new();
        let mut resp_topics: Vec<RespTopicData> = Vec::new();

        for req_topic in &req.topics {
            // Kafka's `ReplicationControlManager.alterPartition` answers
            // UNKNOWN_TOPIC_ID on every partition row when the topic id is
            // zero or names no topic. An unknown partition of a known topic
            // answers UNKNOWN_TOPIC_OR_PARTITION in `isr_update`.
            let topic_name = (req_topic.topic_id != WireUuid::ZERO)
                .then(|| image.topic_name_by_id(&uuid::Uuid::from_bytes(req_topic.topic_id.0)))
                .flatten();
            let resp_partitions: Vec<RespPartitionData> = match topic_name {
                None => req_topic
                    .partitions
                    .iter()
                    .map(|req_part| RespPartitionData {
                        partition_index: req_part.partition_index,
                        error_code: codes::UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    })
                    .collect(),
                Some(topic_name) => req_topic
                    .partitions
                    .iter()
                    .map(|req_part| {
                        handle_partition_with_recovery(
                            &image,
                            &active,
                            req.broker_id,
                            topic_name,
                            req_part,
                            &mut changes,
                        )
                    })
                    .collect(),
            };

            resp_topics.push(tagged_wire!(RespTopicData {
                topic_id: req_topic.topic_id,
                partitions: resp_partitions,
            }));
        }

        // KIP-966: the ISR moves this request just approved decide the
        // partition's eligible-leader set, so the state that carries them
        // rides the same batch.
        ElrPublisher::new(&image).extend(&mut changes);

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

        Ok(unthrottled_wire!(AlterPartitionResponse {
            error_code: codes::NONE,
            topics: resp_topics,
        }))
    }
}
