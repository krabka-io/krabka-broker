//! `ReadShareGroupStateSummary` (`api_key=87`).
//!
//! This handler returns the small summary for each
//! `(group, topic, partition)` without the full state-batch list, as Kafka's
//! `ShareCoordinatorService.readStateSummary` does. The summary holds the
//! state epoch, the leader epoch, the start offset, and the delivery-complete
//! count. An empty group id, an empty topic list or a topic with no
//! partitions gets an empty response. A partition this broker does not lead
//! returns `NOT_COORDINATOR`, one that still loads
//! `COORDINATOR_LOAD_IN_PROGRESS`, a negative partition `INVALID_REQUEST`, and
//! a topic partition the metadata image lacks `UNKNOWN_TOPIC_OR_PARTITION`.
//! A key this broker leads but does not know returns Kafka's uninitialized
//! summary: start offset and delivery-complete count `-1`, epochs `0`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest,
        read_share_group_state_summary_response::{
            PartitionResult, ReadShareGroupStateSummaryResponse, ReadStateSummaryResult,
        },
    },
};

use crate::{
    broker::Broker,
    error::BrokerError,
    share_coordinator::coordinator::{
        ShareCoordinator, ShareStateError, UNINITIALIZED_START_OFFSET,
    },
};

/// Checks `ClusterAction` on the cluster, then serves the request.
///
/// Kafka's `KafkaApis` answers a denied principal with
/// `ReadShareGroupStateSummaryResponse.toGlobalErrorResponse`: `CLUSTER_AUTHORIZATION_FAILED` on
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
        let req = ReadShareGroupStateSummaryRequest::decode(&mut cur, version)?;
        let resp = super::cluster_authorization_failed!(
            req,
            ReadShareGroupStateSummaryResponse,
            ReadStateSummaryResult,
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
        let req = ReadShareGroupStateSummaryRequest::decode(&mut cur, version)?;
        let resp = read_summaries(&coordinator, &controller.current_image(), req).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Serves every partition of `req`, as Kafka's
/// `ShareCoordinatorService.readStateSummary` does.
async fn read_summaries(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: ReadShareGroupStateSummaryRequest,
) -> ReadShareGroupStateSummaryResponse {
    if req.group_id.is_empty()
        || req.topics.is_empty()
        || req.topics.iter().any(|topic| topic.partitions.is_empty())
    {
        return ReadShareGroupStateSummaryResponse::default();
    }
    let group_id = req.group_id;

    let mut results: Vec<ReadStateSummaryResult> = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
        for pd in topic.partitions {
            let result = match coordinator
                .read_summary_checked(image, &group_id, topic_id, pd.partition)
                .await
            {
                Ok(Some((state_epoch, leader_epoch, start_offset, delivery_complete_count))) => {
                    PartitionResult {
                        partition: pd.partition,
                        state_epoch,
                        leader_epoch,
                        start_offset: start_offset.0,
                        delivery_complete_count,
                        ..Default::default()
                    }
                }
                // Kafka's `PartitionFactory` sentinels for a key with no state.
                Ok(None) => PartitionResult {
                    partition: pd.partition,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: UNINITIALIZED_START_OFFSET,
                    delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                    ..Default::default()
                },
                // Kafka's `toErrorResponseData`: the code and the message, and
                // the schema default for every other field.
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error_message(error)),
                    ..Default::default()
                },
            };
            partitions.push(result);
        }
        results.push(ReadStateSummaryResult {
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    ReadShareGroupStateSummaryResponse {
        results,
        ..Default::default()
    }
}

/// Kafka's `PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT`.
const UNINITIALIZED_DELIVERY_COMPLETE_COUNT: i32 = -1;

/// The row message: a refusal's own message, or the operation error that
/// `readStateSummary` prefixes with `Unable to read share group state summary: `.
fn error_message(error: ShareStateError) -> String {
    match error {
        ShareStateError::Refused { message, .. } => message.to_owned(),
        ShareStateError::Operation { message, .. } => {
            format!("Unable to read share group state summary: {message}")
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_log::Offset;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::read_share_group_state_summary_request::{PartitionData, ReadStateSummaryData},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::{
        codes,
        share_coordinator::coordinator::test_support::{image_with_topic, share_write},
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([41; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([41; 16]);

    fn request(group_id: &str, partitions: &[i32]) -> ReadShareGroupStateSummaryRequest {
        ReadShareGroupStateSummaryRequest {
            group_id: group_id.into(),
            topics: vec![ReadStateSummaryData {
                topic_id: WIRE_TOPIC,
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

    fn response(partitions: Vec<PartitionResult>) -> ReadShareGroupStateSummaryResponse {
        ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
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

    /// The whole `ReadShareGroupStateSummaryResponse` for each request shape,
    /// over topic T with two partitions and state for partition 0 only, as
    /// Kafka's `ShareCoordinatorService.readStateSummary` and
    /// `ShareCoordinatorShard.readStateSummary` answer.
    #[tokio::test]
    async fn read_summary_answers_as_kafka() {
        let stored = PartitionResult {
            partition: 0,
            error_code: codes::NONE,
            error_message: None,
            state_epoch: 17,
            leader_epoch: 3,
            start_offset: 101,
            delivery_complete_count: 9,
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        let missing = PartitionResult {
            partition: 1,
            error_code: codes::NONE,
            error_message: None,
            state_epoch: 0,
            leader_epoch: 0,
            start_offset: -1,
            delivery_complete_count: -1,
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        let empty = ReadShareGroupStateSummaryResponse::default();
        // (led, request, expected response)
        let rows = [
            (true, request("share-group", &[0]), response(vec![stored])),
            (true, request("share-group", &[1]), response(vec![missing])),
            (
                false,
                request("share-group", &[0]),
                response(vec![error_row(
                    0,
                    codes::NOT_COORDINATOR,
                    "Unable to read share group state summary: This is not the correct coordinator.",
                )]),
            ),
            (
                true,
                request("share-group", &[-1]),
                response(vec![error_row(
                    -1,
                    codes::INVALID_REQUEST,
                    "The partition id cannot be a negative number.",
                )]),
            ),
            (
                true,
                request("share-group", &[2]),
                response(vec![error_row(
                    2,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "This server does not host this topic-partition.",
                )]),
            ),
            (true, request("", &[0]), empty.clone()),
            (true, request("share-group", &[]), empty.clone()),
            (
                true,
                ReadShareGroupStateSummaryRequest {
                    group_id: "share-group".into(),
                    ..Default::default()
                },
                empty,
            ),
        ];

        for (index, (led, req, expected)) in rows.into_iter().enumerate() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let coordinator = super::super::test_support::coordinator(dir.path());
            let image = image_with_topic(TOPIC, 2);
            coordinator.lead_all_partitions_for_test().await;
            coordinator
                .initialize(&image, "share-group", TOPIC, 0, 17, Offset(90))
                .await
                .expect("initialize state");
            coordinator
                .read(&image, "share-group", TOPIC, 0, 3)
                .await
                .expect("raise the stored leader epoch");
            coordinator
                .write(
                    &image,
                    "share-group",
                    TOPIC,
                    0,
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

            let resp = read_summaries(&coordinator, &image, req).await;
            check!(resp == expected, "row {index}");
        }
    }
}
