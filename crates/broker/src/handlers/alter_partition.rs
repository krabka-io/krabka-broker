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
};

mod authorization;
mod isr_update;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    authorization::{cluster_action_denied, denied_response},
    isr_update::handle_partition,
};
use crate::{broker::Broker, codes, error::BrokerError};

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
        let mut changes: Vec<MetadataRecord> = Vec::new();
        let mut resp_topics: Vec<RespTopicData> = Vec::new();

        for req_topic in &req.topics {
            // Find the topic name via topic_id from the metadata image.
            let topic_name_opt = image
                .topics()
                .find(|t| t.topic_id.as_bytes() == &req_topic.topic_id.0)
                .map(|t| t.name.clone());

            let mut resp_partitions: Vec<RespPartitionData> = Vec::new();
            for req_part in &req_topic.partitions {
                let resp_part = handle_partition(
                    &image,
                    topic_name_opt.as_deref(),
                    req_part.partition_index,
                    req_part.leader_epoch,
                    &req_part.new_isr,
                    &req_part.new_isr_with_epochs,
                    &mut changes,
                );
                resp_partitions.push(resp_part);
            }

            resp_topics.push(RespTopicData {
                topic_id: req_topic.topic_id,
                partitions: resp_partitions,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            });
        }

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
