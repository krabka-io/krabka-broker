use {
    super::{TOPIC_ID, case, check, exception_message},
    crate::codes::ILLEGAL_SASL_STATE,
    bytes::Bytes,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            api_versions_request::ApiVersionsRequest,
            api_versions_response::ApiVersionsResponse,
            delete_records_request::{
                DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
            },
            delete_records_response::{
                DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
            },
            describe_producers_request::{DescribeProducersRequest, TopicRequest},
            describe_producers_response::{
                DescribeProducersResponse, PartitionResponse as ProducersPartition,
                TopicResponse as ProducersTopic,
            },
            describe_topic_partitions_request::{
                DescribeTopicPartitionsRequest, TopicRequest as DescribedTopic,
            },
            describe_topic_partitions_response::{
                DescribeTopicPartitionsResponse, DescribeTopicPartitionsResponseTopic,
            },
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
            get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
            get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
            init_producer_id_request::InitProducerIdRequest,
            init_producer_id_response::InitProducerIdResponse,
            list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
            list_offsets_response::{
                ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
            },
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            metadata_response::{MetadataResponse, MetadataResponseTopic},
            offset_for_leader_epoch_request::{
                OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
            },
            offset_for_leader_epoch_response::{
                EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
            },
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
            push_telemetry_request::PushTelemetryRequest,
            push_telemetry_response::PushTelemetryResponse,
            sasl_authenticate_request::SaslAuthenticateRequest,
            sasl_authenticate_response::SaslAuthenticateResponse,
            sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
        },
        records::RecordsPayload,
    },
};

/// A `Produce` request for `orders` partitions 0, 1 and 0 again, and
/// `payments` partition 2, with `acks`.
fn produce_request(acks: i16) -> ProduceRequest {
    let partition = |index| PartitionProduceData {
        index,
        ..Default::default()
    };
    ProduceRequest {
        acks,
        topic_data: vec![
            TopicProduceData {
                name: "orders".to_owned(),
                partition_data: vec![partition(0), partition(1)],
                ..Default::default()
            },
            TopicProduceData {
                name: "payments".to_owned(),
                partition_data: vec![partition(2)],
                ..Default::default()
            },
            TopicProduceData {
                name: "orders".to_owned(),
                partition_data: vec![partition(0)],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

/// The `Produce` row Kafka builds for partition `index`.
fn produce_row(index: i32) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index,
        error_code: ILLEGAL_SASL_STATE,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: Vec::new(),
        error_message: Some(exception_message()),
        ..Default::default()
    }
}

/// `Produce` answers each distinct partition once, under its topic, and
/// nothing at all for `acks = 0`.
#[test]
fn produce_answers_each_partition_unless_acks_is_zero() {
    check(vec![
        case(
            ApiKey::Produce,
            9,
            produce_request(1),
            Some(ProduceResponse {
                responses: vec![
                    TopicProduceResponse {
                        name: "orders".to_owned(),
                        partition_responses: vec![produce_row(0), produce_row(1)],
                        ..Default::default()
                    },
                    TopicProduceResponse {
                        name: "payments".to_owned(),
                        partition_responses: vec![produce_row(2)],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::Produce,
            9,
            produce_request(0),
            None::<ProduceResponse>,
        ),
    ]);
}

/// A `Fetch` request for `orders` partitions 3 and 4, in session 12.
fn fetch_request() -> FetchRequest {
    let partition = |partition| FetchPartition {
        partition,
        ..Default::default()
    };
    FetchRequest {
        session_id: 12,
        topics: vec![FetchTopic {
            topic: "orders".to_owned(),
            topic_id: TOPIC_ID,
            partitions: vec![partition(3), partition(4)],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The `Fetch` partition row `FetchResponse.partitionResponse` builds. Its
/// empty, non-null records decode as an empty pre-v2 payload.
fn fetch_row(partition_index: i32) -> PartitionData {
    PartitionData {
        partition_index,
        error_code: ILLEGAL_SASL_STATE,
        high_watermark: -1,
        records: Some(RecordsPayload::Legacy(Bytes::new())),
        ..Default::default()
    }
}

/// Before v13 `Fetch` repeats every partition with the code; from v13 only
/// the top level carries it.
#[test]
fn fetch_lists_partitions_only_before_v13() {
    check(vec![
        case(
            ApiKey::Fetch,
            12,
            fetch_request(),
            Some(FetchResponse {
                error_code: ILLEGAL_SASL_STATE,
                session_id: 12,
                responses: vec![FetchableTopicResponse {
                    topic: "orders".to_owned(),
                    partitions: vec![fetch_row(3), fetch_row(4)],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::Fetch,
            13,
            fetch_request(),
            Some(FetchResponse {
                error_code: ILLEGAL_SASL_STATE,
                session_id: 12,
                ..Default::default()
            }),
        ),
    ]);
}

/// The apis whose rows repeat the requested topics and partitions.
#[test]
fn partition_apis_repeat_every_requested_partition() {
    check(vec![
        case(
            ApiKey::ListOffsets,
            9,
            ListOffsetsRequest {
                topics: vec![ListOffsetsTopic {
                    name: "orders".to_owned(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: 5,
                        timestamp: 77,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(ListOffsetsResponse {
                topics: vec![ListOffsetsTopicResponse {
                    name: "orders".to_owned(),
                    partitions: vec![ListOffsetsPartitionResponse {
                        partition_index: 5,
                        error_code: ILLEGAL_SASL_STATE,
                        timestamp: -1,
                        offset: -1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::OffsetForLeaderEpoch,
            4,
            OffsetForLeaderEpochRequest {
                topics: vec![OffsetForLeaderTopic {
                    topic: "orders".to_owned(),
                    partitions: vec![OffsetForLeaderPartition {
                        partition: 6,
                        leader_epoch: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(OffsetForLeaderEpochResponse {
                topics: vec![OffsetForLeaderTopicResult {
                    topic: "orders".to_owned(),
                    partitions: vec![EpochEndOffset {
                        error_code: ILLEGAL_SASL_STATE,
                        partition: 6,
                        leader_epoch: -1,
                        end_offset: -1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteRecords,
            2,
            DeleteRecordsRequest {
                topics: vec![DeleteRecordsTopic {
                    name: "orders".to_owned(),
                    partitions: vec![DeleteRecordsPartition {
                        partition_index: 7,
                        offset: 100,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(DeleteRecordsResponse {
                topics: vec![DeleteRecordsTopicResult {
                    name: "orders".to_owned(),
                    partitions: vec![DeleteRecordsPartitionResult {
                        partition_index: 7,
                        low_watermark: -1,
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeProducers,
            0,
            DescribeProducersRequest {
                topics: vec![TopicRequest {
                    name: "orders".to_owned(),
                    partition_indexes: vec![8, 9],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(DescribeProducersResponse {
                topics: vec![ProducersTopic {
                    name: "orders".to_owned(),
                    partitions: vec![
                        ProducersPartition {
                            partition_index: 8,
                            error_code: ILLEGAL_SASL_STATE,
                            ..Default::default()
                        },
                        ProducersPartition {
                            partition_index: 9,
                            error_code: ILLEGAL_SASL_STATE,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
    ]);
}

/// The topic-level apis: `Metadata` and `DescribeTopicPartitions` answer each
/// requested topic with no partitions.
#[test]
fn topic_apis_repeat_every_requested_topic() {
    check(vec![
        case(
            ApiKey::Metadata,
            13,
            MetadataRequest {
                topics: Some(vec![
                    MetadataRequestTopic {
                        name: Some("orders".to_owned()),
                        ..Default::default()
                    },
                    MetadataRequestTopic {
                        topic_id: TOPIC_ID,
                        name: None,
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
            Some(MetadataResponse {
                topics: vec![
                    MetadataResponseTopic {
                        error_code: ILLEGAL_SASL_STATE,
                        name: Some("orders".to_owned()),
                        ..Default::default()
                    },
                    MetadataResponseTopic {
                        error_code: ILLEGAL_SASL_STATE,
                        name: Some(String::new()),
                        topic_id: TOPIC_ID,
                        ..Default::default()
                    },
                ],
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeTopicPartitions,
            0,
            DescribeTopicPartitionsRequest {
                topics: vec![DescribedTopic {
                    name: "orders".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(DescribeTopicPartitionsResponse {
                topics: vec![DescribeTopicPartitionsResponseTopic {
                    error_code: ILLEGAL_SASL_STATE,
                    name: Some("orders".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
    ]);
}

/// The apis whose error response is the top-level code alone.
#[test]
fn connection_apis_answer_the_top_level_code() {
    check(vec![
        case(
            ApiKey::InitProducerId,
            5,
            InitProducerIdRequest {
                producer_id: 42,
                producer_epoch: 3,
                ..Default::default()
            },
            Some(InitProducerIdResponse {
                error_code: ILLEGAL_SASL_STATE,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::GetTelemetrySubscriptions,
            0,
            GetTelemetrySubscriptionsRequest::default(),
            Some(GetTelemetrySubscriptionsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::PushTelemetry,
            0,
            PushTelemetryRequest::default(),
            Some(PushTelemetryResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ApiVersions,
            3,
            ApiVersionsRequest::default(),
            Some(ApiVersionsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::SaslHandshake,
            1,
            SaslHandshakeRequest {
                mechanism: "PLAIN".to_owned(),
                ..Default::default()
            },
            Some(SaslHandshakeResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::SaslAuthenticate,
            2,
            SaslAuthenticateRequest::default(),
            Some(SaslAuthenticateResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(exception_message()),
                ..Default::default()
            }),
        ),
    ]);
}
