//! `ReadShareGroupStateSummary` (`api_key=87`).
//!
//! This handler returns the small summary for each
//! `(group, topic, partition)` without the full state-batch list. The summary
//! holds the state epoch, the leader epoch, the start offset, and the
//! delivery-complete count. A partition this broker does not lead returns
//! per-partition `NOT_COORDINATOR`, and a partition that still loads returns
//! `COORDINATOR_LOAD_IN_PROGRESS`. A key that this broker leads but does not
//! know returns the initial summary, `start_offset = -1`, with
//! `error_code = 0`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
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
    broker::Broker, error::BrokerError, share_coordinator::coordinator::UNINITIALIZED_START_OFFSET,
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
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ReadShareGroupStateSummaryRequest::decode(&mut cur, version)?;
        let group_id = req.group_id;

        let mut results: Vec<ReadStateSummaryResult> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
            for pd in topic.partitions {
                let result = match coordinator
                    .read_summary(&group_id, topic_id, pd.partition)
                    .await
                {
                    Ok(Some((
                        state_epoch,
                        leader_epoch,
                        start_offset,
                        delivery_complete_count,
                    ))) => PartitionResult {
                        partition: pd.partition,
                        state_epoch,
                        leader_epoch,
                        start_offset: start_offset.0,
                        delivery_complete_count,
                        ..Default::default()
                    },
                    Ok(None) => PartitionResult {
                        partition: pd.partition,
                        start_offset: UNINITIALIZED_START_OFFSET,
                        delivery_complete_count: 0,
                        ..Default::default()
                    },
                    // Not the leader, or the state partition still loads.
                    Err(error_code) => PartitionResult {
                        partition: pd.partition,
                        error_code,
                        start_offset: UNINITIALIZED_START_OFFSET,
                        delivery_complete_count: 0,
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

        let resp = ReadShareGroupStateSummaryResponse {
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            read_share_group_state_summary_request::{
                PartitionData, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
            },
            read_share_group_state_summary_response::ReadShareGroupStateSummaryResponse,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::codes;

    const VERSION: i16 = 1;

    fn decode(bytes: &Bytes) -> ReadShareGroupStateSummaryResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            ReadShareGroupStateSummaryResponse::decode(&mut cur, VERSION).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn encode_request(req: &ReadShareGroupStateSummaryRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION).expect("encode request");
        buf.freeze()
    }

    fn request(
        group_id: &str,
        topic_id: ProtoUuid,
        partition: i32,
    ) -> ReadShareGroupStateSummaryRequest {
        ReadShareGroupStateSummaryRequest {
            group_id: group_id.into(),
            topics: vec![ReadStateSummaryData {
                topic_id,
                partitions: vec![PartitionData {
                    partition,
                    leader_epoch: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn returns_persisted_summary_for_led_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = uuid::Uuid::from_bytes([41; 16]);
        let wire_topic_id = ProtoUuid(*topic_id.as_bytes());
        broker
            .share_coordinator
            .initialize("share-group", topic_id, 4, 17, Offset(90))
            .await
            .expect("initialize state");
        let image =
            crate::share_coordinator::coordinator::test_support::image_with_topic(topic_id, 5);
        broker
            .share_coordinator
            .read(&image, "share-group", topic_id, 4, 3)
            .await
            .expect("raise the stored leader epoch");
        broker
            .share_coordinator
            .write(
                &image,
                "share-group",
                topic_id,
                4,
                crate::share_coordinator::coordinator::test_support::share_write(
                    (17, 3),
                    (101, 9),
                    vec![super::super::test_support::batch(101, 105)],
                ),
            )
            .await
            .expect("write state");
        let req = request("share-group", wire_topic_id, 4);
        let req_bytes = encode_request(&req);

        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        let bytes = super::serve(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
                topic_id: wire_topic_id,
                partitions: vec![PartitionResult {
                    partition: 4,
                    error_code: codes::NONE,
                    error_message: None,
                    state_epoch: 17,
                    leader_epoch: 3,
                    start_offset: 101,
                    delivery_complete_count: 9,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn returns_initial_summary_for_led_missing_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = ProtoUuid([42; 16]);
        let req = request("share-group", topic_id, 6);
        let req_bytes = encode_request(&req);

        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        let bytes = super::serve(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 6,
                    error_code: codes::NONE,
                    error_message: None,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: -1,
                    delivery_complete_count: 0,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn returns_not_coordinator_for_unled_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) = super::super::test_support::broker(dir.path()).await;
        let topic_id = ProtoUuid([43; 16]);
        let req = request("share-group", topic_id, 8);
        let req_bytes = encode_request(&req);

        let bytes = super::serve(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 8,
                    error_code: codes::NOT_COORDINATOR,
                    error_message: None,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: -1,
                    delivery_complete_count: 0,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
