//! `ReadShareGroupState` (`api_key=84`). The handler returns the durable
//! delivery state for each `(group, topic, partition)`: the start offset and
//! the state batches. A partition this broker does not lead returns
//! per-partition `NOT_COORDINATOR`, and a partition that still loads returns
//! `COORDINATOR_LOAD_IN_PROGRESS`. A key with no state returns
//! `INVALID_REQUEST`. A request leader epoch above the stored one is persisted
//! before the answer, so the older share-partition leader is fenced.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        read_share_group_state_request::ReadShareGroupStateRequest,
        read_share_group_state_response::{
            PartitionResult, ReadShareGroupStateResponse, ReadStateResult, StateBatch,
        },
    },
};

use crate::{broker::Broker, error::BrokerError, share_coordinator::coordinator::ShareCoordinator};

/// Checks `ClusterAction` on the cluster, then serves the request.
///
/// Kafka's `KafkaApis` answers a denied principal with
/// `ReadShareGroupStateResponse.toGlobalErrorResponse`: `CLUSTER_AUTHORIZATION_FAILED` on
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
        let req = ReadShareGroupStateRequest::decode(&mut cur, version)?;
        let resp = super::cluster_authorization_failed!(
            req,
            ReadShareGroupStateResponse,
            ReadStateResult,
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
        let req = ReadShareGroupStateRequest::decode(&mut cur, version)?;
        let resp = read_state(&coordinator, &controller.current_image(), req).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Serves every partition of `req`, as Kafka's
/// `ShareCoordinatorService.readState` does.
///
/// An empty topic list, a topic with no partitions, or an empty group id gets
/// a response with no results.
async fn read_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: ReadShareGroupStateRequest,
) -> ReadShareGroupStateResponse {
    if req.topics.is_empty()
        || req.topics.iter().any(|topic| topic.partitions.is_empty())
        || req.group_id.is_empty()
    {
        return ReadShareGroupStateResponse::default();
    }
    let group_id = req.group_id;

    let mut results: Vec<ReadStateResult> = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
        for pd in topic.partitions {
            let result = match coordinator
                .read(image, &group_id, topic_id, pd.partition, pd.leader_epoch)
                .await
            {
                Ok(st) => PartitionResult {
                    partition: pd.partition,
                    state_epoch: st.state_epoch,
                    start_offset: st.start_offset.0,
                    state_batches: st
                        .state_batches
                        .iter()
                        .map(|b| StateBatch {
                            first_offset: b.first_offset.0,
                            last_offset: b.last_offset.0,
                            delivery_state: b.delivery_state,
                            delivery_count: b.delivery_count,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                // Kafka's `toErrorResponseData`: the code and the message,
                // and the default for every other field.
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error.row_message("read")),
                    ..Default::default()
                },
            };
            partitions.push(result);
        }
        results.push(ReadStateResult {
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    ReadShareGroupStateResponse {
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
        owned::read_share_group_state_request::{PartitionData, ReadStateData},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::{
        codes,
        share_coordinator::coordinator::test_support::{image_with_topic, share_write},
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([33; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([33; 16]);

    fn request(group_id: &str, partitions: &[(i32, i32)]) -> ReadShareGroupStateRequest {
        ReadShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![ReadStateData {
                topic_id: WIRE_TOPIC,
                partitions: partitions
                    .iter()
                    .map(|&(partition, leader_epoch)| PartitionData {
                        partition,
                        leader_epoch,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn response(partitions: Vec<PartitionResult>) -> ReadShareGroupStateResponse {
        ReadShareGroupStateResponse {
            results: vec![ReadStateResult {
                topic_id: WIRE_TOPIC,
                partitions,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        }
    }

    fn error_row(partition: i32, error_code: i16, message: &str) -> PartitionResult {
        PartitionResult {
            partition,
            error_code,
            error_message: Some(message.to_owned()),
            ..Default::default()
        }
    }

    /// The whole `ReadShareGroupStateResponse` for each request shape, over
    /// one stored key: partition 4 at state epoch 17, start offset 101, one
    /// batch, and leader epoch 3.
    #[tokio::test]
    async fn read_state_answers_as_kafka() {
        let stored = PartitionResult {
            partition: 4,
            error_code: codes::NONE,
            error_message: None,
            state_epoch: 17,
            start_offset: 101,
            state_batches: vec![StateBatch {
                first_offset: 101,
                last_offset: 105,
                delivery_state: 2,
                delivery_count: 3,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        let empty = ReadShareGroupStateResponse::default();
        // (led, request, expected response)
        let rows = [
            (
                true,
                request("share-group", &[(4, 3)]),
                response(vec![stored]),
            ),
            (
                true,
                request("share-group", &[(6, 3)]),
                response(vec![error_row(
                    6,
                    codes::INVALID_REQUEST,
                    "Read operation on uninitialized share partition not allowed.",
                )]),
            ),
            (
                true,
                request("share-group", &[(4, 2)]),
                response(vec![error_row(
                    4,
                    codes::FENCED_LEADER_EPOCH,
                    "The leader epoch in the request is older than the epoch on the broker.",
                )]),
            ),
            (
                false,
                request("share-group", &[(4, 3)]),
                response(vec![error_row(
                    4,
                    codes::NOT_COORDINATOR,
                    "Unable to read share group state: This is not the correct coordinator.",
                )]),
            ),
            (true, request("", &[(4, 3)]), empty.clone()),
            (true, request("share-group", &[]), empty.clone()),
            (
                true,
                ReadShareGroupStateRequest {
                    group_id: "share-group".into(),
                    ..Default::default()
                },
                empty,
            ),
        ];

        for (index, (led, req, expected)) in rows.into_iter().enumerate() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let coordinator = super::super::test_support::coordinator(dir.path());
            let image = image_with_topic(TOPIC, 8);
            coordinator.lead_all_partitions_for_test().await;
            coordinator
                .initialize("share-group", TOPIC, 4, 17, Offset(90))
                .await
                .expect("initialize state");
            coordinator
                .read(&image, "share-group", TOPIC, 4, 3)
                .await
                .expect("raise the stored leader epoch");
            coordinator
                .write(
                    &image,
                    "share-group",
                    TOPIC,
                    4,
                    share_write(
                        (17, 3),
                        (101, 9),
                        vec![super::super::test_support::batch(101, 105)],
                    ),
                )
                .await
                .expect("write state");
            if !led {
                coordinator
                    .refresh_leader_partitions(&krabka_metadata::MetadataImage::default())
                    .await
                    .finished()
                    .await;
            }

            let resp = read_state(&coordinator, &image, req).await;
            check!(resp == expected, "row {index}");
        }
    }
}
