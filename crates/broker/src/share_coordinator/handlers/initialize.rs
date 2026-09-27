//! `InitializeShareGroupState` (`api_key=83`). It seeds the durable share state
//! for each `(group, topic, partition)` at the requested `state_epoch` and
//! `start_offset`. It gates every partition on local leadership of that
//! partition's `__share_group_state` partition.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        initialize_share_group_state_request::InitializeShareGroupStateRequest,
        initialize_share_group_state_response::{
            InitializeShareGroupStateResponse, InitializeStateResult, PartitionResult,
        },
    },
};

use crate::{
    broker::Broker, codes, error::BrokerError, share_coordinator::coordinator::ShareCoordinator,
};

/// Checks `ClusterAction` on the cluster, then serves the request.
///
/// Kafka's `KafkaApis` answers a denied principal with
/// `InitializeShareGroupStateResponse.toGlobalErrorResponse`: `CLUSTER_AUTHORIZATION_FAILED` on
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
        let req = InitializeShareGroupStateRequest::decode(&mut cur, version)?;
        let resp = super::cluster_authorization_failed!(
            req,
            InitializeShareGroupStateResponse,
            InitializeStateResult,
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
        let req = InitializeShareGroupStateRequest::decode(&mut cur, version)?;
        let resp = initialize_state(&coordinator, &controller.current_image(), req).await;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

/// Initializes every partition of `req`, as Kafka's
/// `ShareCoordinatorService.initializeState` does.
///
/// An empty group id or an empty topic list gets a response with no results.
async fn initialize_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: InitializeShareGroupStateRequest,
) -> InitializeShareGroupStateResponse {
    if req.group_id.is_empty() || req.topics.is_empty() {
        return InitializeShareGroupStateResponse::default();
    }
    let group_id = req.group_id;

    let mut results: Vec<InitializeStateResult> = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
        for pd in topic.partitions {
            let result = coordinator
                .initialize(
                    image,
                    &group_id,
                    topic_id,
                    pd.partition,
                    pd.state_epoch,
                    Offset(pd.start_offset),
                )
                .await;
            let (error_code, error_message) = match result {
                Ok(()) => (codes::NONE, None),
                Err(error) => (error.code(), Some(error.row_message("initialize"))),
            };
            partitions.push(PartitionResult {
                partition: pd.partition,
                error_code,
                error_message,
                ..Default::default()
            });
        }
        results.push(InitializeStateResult {
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    InitializeShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::initialize_share_group_state_request::{InitializeStateData, PartitionData},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::share_coordinator::{
        coordinator::{
            ShareStateSummary,
            test_support::{Logged, NOW_MS, image_with_topic, logged_records},
        },
        persistence::ShareSnapshotValue,
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([32; 16]);
    const UNKNOWN_TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([33; 16]);

    fn request(
        group_id: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: i32,
        start_offset: i64,
    ) -> InitializeShareGroupStateRequest {
        InitializeShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![InitializeStateData {
                topic_id: ProtoUuid(*topic_id.as_bytes()),
                partitions: vec![PartitionData {
                    partition,
                    state_epoch,
                    start_offset,
                    ..Default::default()
                }],
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
    ) -> InitializeShareGroupStateResponse {
        InitializeShareGroupStateResponse {
            results: vec![InitializeStateResult {
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

    fn snapshot(
        snapshot_epoch: i32,
        state_epoch: i32,
        start_offset: i64,
        delivery_complete_count: i32,
    ) -> Logged {
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch,
            state_epoch,
            leader_epoch: 0,
            start_offset: Offset(start_offset),
            delivery_complete_count,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![],
        })
    }

    /// One `InitializeShareGroupState` row, as Kafka's
    /// `ShareCoordinatorShard.initializeState` answers it.
    struct Row {
        name: &'static str,
        led: bool,
        request: InitializeShareGroupStateRequest,
        response: InitializeShareGroupStateResponse,
        /// The summary of the requested key after the request.
        summary: Option<ShareStateSummary>,
        /// The records the request appended to the state partition of the key.
        appended: Vec<Logged>,
    }

    fn rows() -> Vec<Row> {
        let fenced = "The coordinator rejected the request because the state epoch did not match.";
        let unknown = "This server does not host this topic-partition.";
        vec![
            Row {
                name: "an equal epoch and start offset is a no-op",
                led: true,
                request: request("g", TOPIC, 0, 5, 10),
                response: response(TOPIC, 0, codes::NONE, None),
                summary: Some((5, 0, Offset(10), 0)),
                appended: vec![],
            },
            Row {
                name: "an equal epoch with a new start offset writes the next snapshot",
                led: true,
                request: request("g", TOPIC, 0, 5, 20),
                response: response(TOPIC, 0, codes::NONE, None),
                summary: Some((5, 0, Offset(20), 0)),
                appended: vec![snapshot(1, 5, 20, 0)],
            },
            Row {
                name: "an older epoch is fenced",
                led: true,
                request: request("g", TOPIC, 0, 4, 10),
                response: response(TOPIC, 0, codes::FENCED_STATE_EPOCH, Some(fenced)),
                summary: Some((5, 0, Offset(10), 0)),
                appended: vec![],
            },
            Row {
                name: "a new key at an uninitialized start offset",
                led: true,
                request: request("g", TOPIC, 1, 1, -1),
                response: response(TOPIC, 1, codes::NONE, None),
                summary: Some((1, 0, Offset(-1), -1)),
                appended: vec![snapshot(0, 1, -1, -1)],
            },
            Row {
                name: "a negative partition",
                led: true,
                request: request("g", TOPIC, -1, 1, 0),
                response: response(
                    TOPIC,
                    -1,
                    codes::INVALID_REQUEST,
                    Some("The partition id cannot be a negative number."),
                ),
                summary: None,
                appended: vec![],
            },
            Row {
                name: "a negative state epoch",
                led: true,
                request: request("g", TOPIC, 0, -1, 0),
                response: response(
                    TOPIC,
                    0,
                    codes::INVALID_REQUEST,
                    Some("The state epoch cannot be a negative number."),
                ),
                summary: Some((5, 0, Offset(10), 0)),
                appended: vec![],
            },
            Row {
                name: "a partition past the partition count",
                led: true,
                request: request("g", TOPIC, 3, 1, 0),
                response: response(TOPIC, 3, codes::UNKNOWN_TOPIC_OR_PARTITION, Some(unknown)),
                summary: None,
                appended: vec![],
            },
            Row {
                name: "a topic id the image does not hold",
                led: true,
                request: request("g", UNKNOWN_TOPIC, 0, 1, 0),
                response: response(
                    UNKNOWN_TOPIC,
                    0,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    Some(unknown),
                ),
                summary: None,
                appended: vec![],
            },
            Row {
                name: "an empty group id",
                led: true,
                request: request("", TOPIC, 0, 6, 0),
                response: InitializeShareGroupStateResponse::default(),
                summary: Some((5, 0, Offset(10), 0)),
                appended: vec![],
            },
            Row {
                name: "a state partition this broker does not lead",
                led: false,
                request: request("g", TOPIC, 0, 6, 0),
                response: response(
                    TOPIC,
                    0,
                    codes::NOT_COORDINATOR,
                    Some(
                        "Unable to initialize share group state: This is not the correct coordinator.",
                    ),
                ),
                summary: None,
                appended: vec![],
            },
        ]
    }

    /// The whole response, the stored summary, and the appended records for
    /// each request, over a stored state of epoch 5 and start offset 10 on
    /// partition 0 of a three-partition topic.
    #[tokio::test]
    async fn initialize_state_answers_as_kafka() {
        for row in rows() {
            let dir = tempfile::TempDir::new().expect("tempdir");
            let coordinator = super::super::test_support::coordinator(dir.path());
            let image = image_with_topic(TOPIC, 3);
            coordinator.lead_all_partitions_for_test().await;
            coordinator
                .initialize(&image, "g", TOPIC, 0, 5, Offset(10))
                .await
                .expect("seed state");
            let topic = row.request.topics[0].topic_id;
            let partition = row.request.topics[0].partitions[0].partition;
            // An empty group id touches no key; watch the seeded key's group.
            let key_group = if row.request.group_id.is_empty() {
                "g".to_owned()
            } else {
                row.request.group_id.clone()
            };
            let topic_id = uuid::Uuid::from_bytes(topic.0);
            let state_partition = coordinator.state_partition_for(&key_group, &topic_id, partition);
            let before = logged_records(&coordinator, state_partition).len();
            if !row.led {
                coordinator
                    .refresh_leader_partitions(&MetadataImage::default())
                    .await
                    .finished()
                    .await;
            }

            let resp = initialize_state(&coordinator, &image, row.request).await;
            check!(resp == row.response, "{}", row.name);
            let appended: Vec<Logged> = logged_records(&coordinator, state_partition)
                .into_iter()
                .skip(before)
                .map(|(_, logged)| logged)
                .collect();
            check!(appended == row.appended, "{}", row.name);
            let summary = coordinator
                .read_summary(&key_group, topic_id, partition)
                .await
                .ok()
                .flatten();
            check!(summary == row.summary, "{}", row.name);
        }
    }

    /// An empty topic list gets a response with no results.
    #[tokio::test]
    async fn empty_topics_get_no_results() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let coordinator = super::super::test_support::coordinator(dir.path());
        let req = InitializeShareGroupStateRequest {
            group_id: "g".into(),
            ..Default::default()
        };
        let resp = initialize_state(&coordinator, &image_with_topic(TOPIC, 3), req).await;
        check!(resp == InitializeShareGroupStateResponse::default());
    }
}
