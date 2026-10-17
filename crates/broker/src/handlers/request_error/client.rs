//! The produce, fetch, metadata and connection apis.

use {
    super::{ErrorAnswer, Request, RequestFailure},
    crate::handlers::metadata,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            api_versions_request::ApiVersionsRequest,
            api_versions_response::ApiVersionsResponse,
            delete_records_request::DeleteRecordsRequest,
            delete_records_response::{
                DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
            },
            describe_producers_request::DescribeProducersRequest,
            describe_producers_response::{
                DescribeProducersResponse, PartitionResponse as DescribeProducersPartition,
                TopicResponse as DescribeProducersTopic,
            },
            describe_topic_partitions_request::DescribeTopicPartitionsRequest,
            describe_topic_partitions_response::{
                DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponseTopic,
            },
            fetch_request::FetchRequest,
            fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
            get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
            get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
            init_producer_id_request::InitProducerIdRequest,
            init_producer_id_response::InitProducerIdResponse,
            list_offsets_request::ListOffsetsRequest,
            list_offsets_response::{
                ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
            },
            metadata_request::MetadataRequest,
            offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
            offset_for_leader_epoch_response::{
                EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
            },
            produce_request::ProduceRequest,
            produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
            push_telemetry_request::PushTelemetryRequest,
            push_telemetry_response::PushTelemetryResponse,
            sasl_authenticate_request::SaslAuthenticateRequest,
            sasl_authenticate_response::SaslAuthenticateResponse,
            sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
        },
        primitives::uuid::Uuid,
        records::RecordsPayload,
    },
};

/// `ProduceResponse.INVALID_OFFSET`, `ListOffsetsResponse.UNKNOWN_OFFSET` and
/// `DeleteRecordsResponse.INVALID_LOW_WATERMARK`.
const INVALID_OFFSET: i64 = -1;

/// `RecordBatch.NO_TIMESTAMP` and `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
const NO_TIMESTAMP: i64 = -1;

/// `OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH`.
const UNDEFINED_EPOCH: i32 = -1;

/// `RecordBatch.NO_PRODUCER_ID`.
const NO_PRODUCER_ID: i64 = -1;

/// `RecordBatch.NO_PRODUCER_EPOCH`.
const NO_PRODUCER_EPOCH: i16 = -1;

/// `FetchResponse.INVALID_HIGH_WATERMARK`.
const INVALID_HIGH_WATERMARK: i64 = -1;

/// The first `Fetch` version whose error response has no partitions
/// (`FetchRequest.getErrorResponse`).
const FETCH_TOP_LEVEL_ERROR_ONLY_MIN_VERSION: i16 = 13;

pub(super) fn answer(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let failure = request.failure;
    Some(match api {
        ApiKey::Produce => request.maybe_respond(|req| produce(&req, failure)),
        ApiKey::Fetch => request.respond(|req| fetch(&req, request.version, code)),
        ApiKey::ListOffsets => request.respond(|req| list_offsets(&req, code)),
        ApiKey::Metadata => {
            request.respond(|req: MetadataRequest| metadata::error_response(&req, code))
        }
        ApiKey::OffsetForLeaderEpoch => request.respond(|req| offset_for_leader_epoch(&req, code)),
        ApiKey::DeleteRecords => request.respond(|req| delete_records(&req, code)),
        ApiKey::DescribeProducers => request.respond(|req| describe_producers(&req, code)),
        ApiKey::DescribeTopicPartitions => {
            request.respond(|req| describe_topic_partitions(&req, code))
        }
        ApiKey::InitProducerId => {
            request.respond(|_: InitProducerIdRequest| InitProducerIdResponse {
                error_code: code,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                ..Default::default()
            })
        }
        ApiKey::GetTelemetrySubscriptions => {
            request.respond(|_: GetTelemetrySubscriptionsRequest| {
                GetTelemetrySubscriptionsResponse {
                    error_code: code,
                    ..Default::default()
                }
            })
        }
        ApiKey::PushTelemetry => request.respond(|_: PushTelemetryRequest| PushTelemetryResponse {
            error_code: code,
            ..Default::default()
        }),
        // `ApiVersionsRequest.getErrorResponse` also lists `ApiVersions`
        // itself for `UNSUPPORTED_VERSION`, which
        // `api_versions::unsupported_version_response` answers.
        ApiKey::ApiVersions => request.respond(|_: ApiVersionsRequest| ApiVersionsResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::SaslHandshake => request.respond(|_: SaslHandshakeRequest| SaslHandshakeResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::SaslAuthenticate => {
            request.respond(|_: SaslAuthenticateRequest| SaslAuthenticateResponse {
                error_code: code,
                error_message: failure.api_message(),
                ..Default::default()
            })
        }
        _ => return None,
    })
}

/// `ProduceRequest.getErrorResponse`: no response for `acks = 0`. Otherwise
/// one row per distinct topic partition, grouped under its topic.
fn produce(req: &ProduceRequest, failure: &RequestFailure<'_>) -> Option<ProduceResponse> {
    if req.acks == 0 {
        return None;
    }
    let mut responses: Vec<TopicProduceResponse> = Vec::new();
    for topic in &req.topic_data {
        for partition in &topic.partition_data {
            let rows =
                &mut topic_rows(&mut responses, &topic.name, topic.topic_id).partition_responses;
            if rows.iter().all(|row| row.index != partition.index) {
                rows.push(PartitionProduceResponse {
                    index: partition.index,
                    error_code: failure.error_code,
                    base_offset: INVALID_OFFSET,
                    log_append_time_ms: NO_TIMESTAMP,
                    log_start_offset: INVALID_OFFSET,
                    record_errors: Vec::new(),
                    error_message: failure.api_message(),
                    ..Default::default()
                });
            }
        }
    }
    Some(ProduceResponse {
        responses,
        ..Default::default()
    })
}

/// The response topic named `name` and `topic_id`, added at the end when it
/// is not there yet (`TopicProduceResponseCollection.find`).
fn topic_rows<'a>(
    responses: &'a mut Vec<TopicProduceResponse>,
    name: &str,
    topic_id: Uuid,
) -> &'a mut TopicProduceResponse {
    let index = responses
        .iter()
        .position(|topic| topic.name == name && topic.topic_id == topic_id)
        .unwrap_or_else(|| {
            responses.push(TopicProduceResponse {
                name: name.to_owned(),
                topic_id,
                ..Default::default()
            });
            responses.len() - 1
        });
    &mut responses[index]
}

/// `FetchRequest.getErrorResponse`: the top-level code, the request's session
/// id, and before v13 every requested partition with the code.
fn fetch(req: &FetchRequest, version: i16, code: i16) -> FetchResponse {
    let responses = if version < FETCH_TOP_LEVEL_ERROR_ONLY_MIN_VERSION {
        req.topics
            .iter()
            .map(|topic| FetchableTopicResponse {
                topic: topic.topic.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionData {
                        partition_index: partition.partition,
                        error_code: code,
                        high_watermark: INVALID_HIGH_WATERMARK,
                        records: Some(RecordsPayload::default()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect()
    } else {
        Vec::new()
    };
    FetchResponse {
        error_code: code,
        session_id: req.session_id,
        responses,
        ..Default::default()
    }
}

/// `ListOffsetsRequest.getErrorResponse`: every requested partition with the
/// code and an unknown offset and timestamp.
fn list_offsets(req: &ListOffsetsRequest, code: i16) -> ListOffsetsResponse {
    ListOffsetsResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| ListOffsetsTopicResponse {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| ListOffsetsPartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: code,
                        timestamp: NO_TIMESTAMP,
                        offset: INVALID_OFFSET,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `OffsetsForLeaderEpochRequest.getErrorResponse`: every requested partition
/// with the code and an undefined epoch and end offset.
fn offset_for_leader_epoch(
    req: &OffsetForLeaderEpochRequest,
    code: i16,
) -> OffsetForLeaderEpochResponse {
    OffsetForLeaderEpochResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| OffsetForLeaderTopicResult {
                topic: topic.topic.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| EpochEndOffset {
                        error_code: code,
                        partition: partition.partition,
                        leader_epoch: UNDEFINED_EPOCH,
                        end_offset: INVALID_OFFSET,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DeleteRecordsRequest.getErrorResponse`: every requested partition with the
/// code and an invalid low watermark.
fn delete_records(req: &DeleteRecordsRequest, code: i16) -> DeleteRecordsResponse {
    DeleteRecordsResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| DeleteRecordsTopicResult {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| DeleteRecordsPartitionResult {
                        partition_index: partition.partition_index,
                        low_watermark: INVALID_OFFSET,
                        error_code: code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DescribeProducersRequest.getErrorResponse`: every requested partition with
/// the code.
fn describe_producers(req: &DescribeProducersRequest, code: i16) -> DescribeProducersResponse {
    DescribeProducersResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| DescribeProducersTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partition_indexes
                    .iter()
                    .map(|&partition_index| DescribeProducersPartition {
                        partition_index,
                        error_code: code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DescribeTopicPartitionsRequest.getErrorResponse`: every requested topic
/// with the code and no partitions.
fn describe_topic_partitions(
    req: &DescribeTopicPartitionsRequest,
    code: i16,
) -> DescribeTopicPartitionsResponse {
    DescribeTopicPartitionsResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| DescribeTopicPartitionsResponseTopic {
                error_code: code,
                name: Some(topic.name.clone()),
                is_internal: false,
                partitions: Vec::new(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}
