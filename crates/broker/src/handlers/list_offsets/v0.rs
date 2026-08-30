//! The legacy `ListOffsets` v0 wire shape and segment-boundary lookup.

use bytes::{Bytes, BytesMut};
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::primitives::{
    array::{get_array_len, put_array_len},
    fixed::{get_i32, get_i64, put_i16, put_i32, put_i64},
    string_bytes::{get_string_owned, put_string},
};

use super::{
    bound::{fetch_bound, last_fetchable_offset},
    local::latest_offset,
    remote::concurrently,
};
use crate::{broker::Broker, codes, error::BrokerError};

const LATEST_TIMESTAMP: i64 = -1;
const EARLIEST_TIMESTAMP: i64 = -2;

struct Request {
    replica_id: i32,
    topics: Vec<TopicRequest>,
}

struct TopicRequest {
    name: String,
    partitions: Vec<PartitionRequest>,
}

struct PartitionRequest {
    index: i32,
    timestamp: i64,
    max_num_offsets: i32,
}

struct TopicResponse {
    name: String,
    partitions: Vec<PartitionResponse>,
}

struct PartitionResponse {
    index: i32,
    error_code: i16,
    offsets: Vec<i64>,
}

fn error_response(index: i32, error_code: i16) -> PartitionResponse {
    PartitionResponse {
        index,
        error_code,
        offsets: Vec::new(),
    }
}

fn decode(mut bytes: &[u8]) -> Result<Request, BrokerError> {
    let replica_id = get_i32(&mut bytes)?;
    let topic_count = get_array_len(&mut bytes, false)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = get_string_owned(&mut bytes)?;
        let partition_count = get_array_len(&mut bytes, false)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(PartitionRequest {
                index: get_i32(&mut bytes)?,
                timestamp: get_i64(&mut bytes)?,
                max_num_offsets: get_i32(&mut bytes)?,
            });
        }
        topics.push(TopicRequest { name, partitions });
    }
    Ok(Request { replica_id, topics })
}

fn encode(topics: &[TopicResponse]) -> Bytes {
    let mut buf = BytesMut::new();
    put_array_len(&mut buf, topics.len(), false);
    for topic in topics {
        put_string(&mut buf, &topic.name);
        put_array_len(&mut buf, topic.partitions.len(), false);
        for partition in &topic.partitions {
            put_i32(&mut buf, partition.index);
            put_i16(&mut buf, partition.error_code);
            put_array_len(&mut buf, partition.offsets.len(), false);
            for offset in &partition.offsets {
                put_i64(&mut buf, *offset);
            }
        }
    }
    buf.freeze()
}

async fn resolve_partition(
    broker: &Broker,
    topic: &str,
    request: PartitionRequest,
    replica_id: i32,
) -> PartitionResponse {
    if request.timestamp < EARLIEST_TIMESTAMP {
        return error_response(request.index, codes::UNSUPPORTED_VERSION);
    }
    let Ok(max_num_offsets) = usize::try_from(request.max_num_offsets) else {
        return error_response(request.index, codes::INVALID_REQUEST);
    };
    let Some(partition) = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(request.index))
    else {
        return error_response(request.index, codes::UNKNOWN_TOPIC_OR_PARTITION);
    };

    let (local_end, policy, offsets) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        (
            log.log_end_offset().0,
            log.config_snapshot().delivery_policy,
            log.legacy_offsets_before(request.timestamp, max_num_offsets),
        )
    };
    let Ok(mut offsets) = offsets else {
        return error_response(request.index, codes::KAFKA_STORAGE_ERROR);
    };
    let mut upper = last_fetchable_offset(&partition, fetch_bound(replica_id, 0), local_end).await;
    if request.timestamp == LATEST_TIMESTAMP {
        upper = upper.min(latest_offset(&partition, policy, local_end));
    }
    if offsets.iter().any(|offset| offset.0 > upper) {
        offsets = std::iter::once(krabka_ids::Offset(upper))
            .chain(offsets.into_iter().skip_while(|offset| offset.0 > upper))
            .collect();
    }

    PartitionResponse {
        index: request.index,
        error_code: codes::NONE,
        offsets: offsets.into_iter().map(|offset| offset.0).collect(),
    }
}

pub(super) async fn handle(
    broker: &Broker,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let request = decode(req_bytes)?;
    let acl_image = broker.controller.current_image();
    let topics = concurrently(request.topics.into_iter().map(|topic| {
        let acl_image = acl_image.clone();
        async move {
            let name = topic.name;
            let partitions = if crate::handlers::acl_denied(
                broker.config.authorizer.as_ref(),
                &acl_image,
                ctx,
                ResourceType::Topic,
                &name,
                AclOperation::Describe,
            ) {
                topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        error_response(partition.index, codes::TOPIC_AUTHORIZATION_FAILED)
                    })
                    .collect()
            } else {
                concurrently(topic.partitions.into_iter().map(|partition| {
                    resolve_partition(broker, &name, partition, request.replica_id)
                }))
                .await
            };
            TopicResponse { name, partitions }
        }
    }))
    .await;
    Ok(encode(&topics))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_protocol::primitives::{
        array::{get_array_len, put_array_len},
        fixed::{get_i16, get_i32, get_i64, put_i32, put_i64},
        string_bytes::{get_string_owned, put_string},
    };
    use krabka_units::prelude::bytes;

    use super::*;
    use crate::{
        handlers::list_offsets::test_support::{client_for, create_topic, test_context},
        test_support::{peer, principal, start_broker_with_authorizer_no_audit},
    };

    #[tokio::test]
    async fn max_num_offsets_caps_legacy_segment_boundaries() {
        const TOPIC: &str = "list-offsets-v0-max";
        let (broker_handle, _dir) =
            start_broker_with_authorizer_no_audit(Arc::new(crate::authorizer::AllowAllAuthorizer))
                .await;
        let client = client_for(&broker_handle).await;
        create_topic(
            &client,
            TOPIC,
            vec![
                krabka_protocol::owned::create_topics_request::CreatableTopicConfig {
                    name: "segment.bytes".into(),
                    value: Some("1".into()),
                    ..Default::default()
                },
            ],
        )
        .await;
        broker_handle.wait_until_partition_present(TOPIC, 0).await;
        assert!(
            broker_handle
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| config.segment_size == bytes(1))
        );
        broker_handle
            .produce_records_for_test(TOPIC, 0, 4)
            .await
            .expect("produce");

        let mut request = BytesMut::new();
        put_i32(&mut request, -2); // debugging replica: no high-watermark cap
        put_array_len(&mut request, 1, false);
        put_string(&mut request, TOPIC);
        put_array_len(&mut request, 1, false);
        put_i32(&mut request, 0);
        put_i64(&mut request, LATEST_TIMESTAMP);
        put_i32(&mut request, 3);

        let broker = broker_handle.broker_arc_for_test();
        let admin = principal("admin");
        let socket = peer();
        let response = handle(&broker, &request, &test_context(&admin, &socket))
            .await
            .expect("ListOffsets v0");
        let mut response: &[u8] = &response;
        assert!(get_array_len(&mut response, false).unwrap() == 1);
        assert!(get_string_owned(&mut response).unwrap() == TOPIC);
        assert!(get_array_len(&mut response, false).unwrap() == 1);
        assert!(get_i32(&mut response).unwrap() == 0);
        assert!(get_i16(&mut response).unwrap() == codes::NONE);
        let count = get_array_len(&mut response, false).unwrap();
        let offsets: Vec<i64> = (0..count)
            .map(|_| get_i64(&mut response).unwrap())
            .collect();
        assert!(offsets == vec![4, 3, 2], "{offsets:?}");
        assert!(response.is_empty());

        broker_handle.shutdown().await;
    }
}
