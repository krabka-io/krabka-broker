//! `AlterPartition` (`api_key=56`). Controller-side ISR update handler.
//!
//! The handler validates that this broker is the openraft leader, and returns
//! `NOT_CONTROLLER` if it is not. It then checks leader-epoch fencing for each
//! partition, validates that the proposed ISR is a non-empty subset of the
//! partition's replicas, and submits the updated `PartitionRecord` through
//! `controller.submit_change`.

use bytes::Bytes;
use krabka_metadata::MetadataRecord;
use krabka_protocol::{
    Decode, UnknownTaggedFields,
    owned::{
        alter_partition_request::AlterPartitionRequest,
        alter_partition_response::{
            AlterPartitionResponse, PartitionData as RespPartitionData, TopicData as RespTopicData,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

mod authorization;
mod isr_update;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    authorization::{cluster_action_denied, denied_response},
    isr_update::handle_partition_with_recovery,
};
use crate::{broker::Broker, codes, elr::ElrPublisher, error::BrokerError};

#[tracing::instrument(
    name = "handle_alter_partition",
    level = "info",
    skip_all,
    fields(api = "AlterPartition", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;

    {
        let mut cur: &[u8] = req_bytes;
        let req = AlterPartitionRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        {
            let image = controller.current_image();
            if cluster_action_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
            ) {
                return denied_response(version);
            }
        }

        // Only the openraft leader handles AlterPartition.
        let is_leader = controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == node_id);
        if !is_leader {
            return encode_resp(
                version,
                &AlterPartitionResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NOT_CONTROLLER,
                    topics: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            );
        }

        let image = controller.current_image();
        // One snapshot of the brokers that may sit in an ISR: alive, unfenced
        // and not in controlled shutdown.
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
                            topic_name,
                            req_part,
                            &mut changes,
                        )
                    })
                    .collect(),
            };

            resp_topics.push(RespTopicData {
                topic_id: req_topic.topic_id,
                partitions: resp_partitions,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            });
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

        encode_resp(
            version,
            &AlterPartitionResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                topics: resp_topics,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        )
    }
}

fn encode_resp(version: i16, resp: &AlterPartitionResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}
