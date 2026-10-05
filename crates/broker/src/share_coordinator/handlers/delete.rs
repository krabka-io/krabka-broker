//! `DeleteShareGroupState` (`api_key=86`). The handler tombstones the durable
//! share state for each `(group, topic, partition)` that has state and drops
//! the in-memory entry. The handler gates on local leadership of the target
//! `__share_group_state` partition.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::{BoxFuture, join_all};
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Encode,
    owned::{
        delete_share_group_state_request::DeleteShareGroupStateRequest,
        delete_share_group_state_response::{
            DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult,
        },
    },
};

use crate::{
    broker::Broker, codes, error::BrokerError, share_coordinator::coordinator::ShareCoordinator,
};

/// Checks `ClusterAction` on the cluster, then serves the request.
///
/// Kafka's `KafkaApis` answers a denied principal with
/// `DeleteShareGroupStateResponse.toGlobalErrorResponse`: `CLUSTER_AUTHORIZATION_FAILED` on
/// every requested partition, and the share coordinator does not run.
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    if super::cluster_action_denied(broker, ctx) {
        let mut cur: &[u8] = req_bytes;
        let req: DeleteShareGroupStateRequest =
            crate::handlers::decode_group_request(&mut cur, version)?;
        let resp = super::cluster_authorization_failed!(
            req,
            DeleteShareGroupStateResponse,
            DeleteStateResult,
            PartitionResult
        );
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        return Ok(buf.freeze());
    }
    serve(broker, version, correlation_id, req_bytes).await
}

fn serve(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coordinator = Arc::clone(&broker.share_coordinator);
    let controller = Arc::clone(&broker.controller);
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req: DeleteShareGroupStateRequest =
            crate::handlers::decode_group_request(&mut cur, version)?;
        let resp = delete_state(&coordinator, &controller.current_image(), req).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Deletes every partition of `req`, as Kafka's
/// `ShareCoordinatorService.deleteState` does.
///
/// An empty group id, an empty topic list, or a topic with no partitions gets
/// a response with no results.
async fn delete_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: DeleteShareGroupStateRequest,
) -> DeleteShareGroupStateResponse {
    if req.group_id.is_empty()
        || req.topics.is_empty()
        || req.topics.iter().any(|topic| topic.partitions.is_empty())
    {
        return DeleteShareGroupStateResponse::default();
    }
    let group_id = req.group_id.as_str();

    // Kafka's `ShareCoordinatorService` schedules one operation for each
    // partition and answers when every one of them completes. Each operation
    // waits until its records commit, so the partitions run together.
    let results: Vec<DeleteStateResult> =
        join_all(req.topics.into_iter().map(|topic| async move {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            let partitions = join_all(topic.partitions.into_iter().map(|pd| async move {
                let result = coordinator
                    .delete(image, group_id, topic_id, pd.partition)
                    .await;
                let (error_code, error_message) = match result {
                    Ok(()) => (codes::NONE, None),
                    Err(error) => (error.code(), Some(error.row_message("delete"))),
                };
                PartitionResult {
                    partition: pd.partition,
                    error_code,
                    error_message,
                    ..Default::default()
                }
            }))
            .await;
            DeleteStateResult {
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            }
        }))
        .await;

    DeleteShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_log::Offset;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::delete_share_group_state_request::{DeleteStateData, PartitionData},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::share_coordinator::coordinator::test_support::{
        Logged, image_with_topic, logged_records,
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([31; 16]);
    const UNKNOWN_TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([34; 16]);

    fn request(
        group_id: &str,
        topic_id: uuid::Uuid,
        partitions: &[i32],
    ) -> DeleteShareGroupStateRequest {
        DeleteShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![DeleteStateData {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: partitions
                    .iter()
                    .map(|&partition| PartitionData {
                        partition,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn response(
        topic_id: uuid::Uuid,
        partition: i32,
        error_code: i16,
        message: Option<&str>,
    ) -> DeleteShareGroupStateResponse {
        DeleteShareGroupStateResponse {
            results: vec![DeleteStateResult {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: vec![PartitionResult {
                    partition,
                    error_code,
                    error_message: message.map(str::to_owned),
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        }
    }

    /// The whole response and the appended records for each request, over a
    /// state for partition 0 only of a two-partition topic, as Kafka's
    /// `ShareCoordinatorShard.deleteState` answers it.
    #[tokio::test]
    async fn delete_state_answers_as_kafka() {
        let unknown = Some("This server does not host this topic-partition.");
        // (name, led, request, expected response, appended records, partition
        //  0 still has state)
        let rows = [
            (
                "a key with state is tombstoned",
                true,
                request("g", TOPIC, &[0]),
                response(TOPIC, 0, codes::NONE, None),
                vec![Logged::Tombstone],
                false,
            ),
            (
                "a key with no state writes nothing",
                true,
                request("g", TOPIC, &[1]),
                response(TOPIC, 1, codes::NONE, None),
                vec![],
                true,
            ),
            (
                "a negative partition",
                true,
                request("g", TOPIC, &[-1]),
                response(
                    TOPIC,
                    -1,
                    codes::INVALID_REQUEST,
                    Some("The partition id cannot be a negative number."),
                ),
                vec![],
                true,
            ),
            (
                "a partition past the partition count",
                true,
                request("g", TOPIC, &[2]),
                response(TOPIC, 2, codes::UNKNOWN_TOPIC_OR_PARTITION, unknown),
                vec![],
                true,
            ),
            (
                "a topic id the image does not hold",
                true,
                request("g", UNKNOWN_TOPIC, &[0]),
                response(UNKNOWN_TOPIC, 0, codes::UNKNOWN_TOPIC_OR_PARTITION, unknown),
                vec![],
                true,
            ),
            (
                "an empty group id",
                true,
                request("", TOPIC, &[0]),
                DeleteShareGroupStateResponse::default(),
                vec![],
                true,
            ),
            (
                "a topic with no partitions",
                true,
                request("g", TOPIC, &[]),
                DeleteShareGroupStateResponse::default(),
                vec![],
                true,
            ),
            (
                "a state partition this broker does not lead",
                false,
                request("g", TOPIC, &[0]),
                response(
                    TOPIC,
                    0,
                    codes::NOT_COORDINATOR,
                    Some(
                        "Unable to delete share group state: This is not the correct coordinator.",
                    ),
                ),
                vec![],
                // The resignation itself drops the in-memory keys.
                false,
            ),
        ];

        for (name, led, req, expected, appended, kept) in rows {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let coordinator = super::super::test_support::coordinator(dir.path());
            let image = image_with_topic(TOPIC, 2);
            coordinator.lead_all_partitions_for_test().await;
            coordinator
                .initialize(&image, "g", TOPIC, 0, 1, Offset(0))
                .await
                .expect("seed state");
            let partition = req.topics[0].partitions.first().map_or(0, |p| p.partition);
            let topic_id = uuid::Uuid::from_bytes(req.topics[0].topic_id.0);
            let state_partition = coordinator.state_partition_for("g", &topic_id, partition);
            let before = logged_records(&coordinator, state_partition).len();
            if !led {
                coordinator
                    .refresh_leader_partitions(&MetadataImage::default())
                    .await
                    .finished()
                    .await;
            }

            let resp = delete_state(&coordinator, &image, req).await;
            check!(resp == expected, "{name}");
            let logged: Vec<Logged> = logged_records(&coordinator, state_partition)
                .into_iter()
                .skip(before)
                .map(|(_, logged)| logged)
                .collect();
            check!(logged == appended, "{name}");
            check!(
                coordinator.state_for_test("g", TOPIC, 0).await.is_some() == kept,
                "{name}"
            );
        }
    }
}
