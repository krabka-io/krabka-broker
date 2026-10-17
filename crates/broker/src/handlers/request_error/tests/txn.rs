use {
    super::{case, check},
    crate::codes::ILLEGAL_SASL_STATE,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            add_offsets_to_txn_request::AddOffsetsToTxnRequest,
            add_offsets_to_txn_response::AddOffsetsToTxnResponse,
            add_partitions_to_txn_request::AddPartitionsToTxnRequest,
            add_partitions_to_txn_response::AddPartitionsToTxnResponse,
            common::{
                add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
                add_partitions_to_txn_response::{
                    add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
                    add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
                },
            },
            describe_transactions_request::DescribeTransactionsRequest,
            describe_transactions_response::{DescribeTransactionsResponse, TransactionState},
            end_txn_request::EndTxnRequest,
            end_txn_response::EndTxnResponse,
            list_transactions_request::ListTransactionsRequest,
            list_transactions_response::ListTransactionsResponse,
            txn_offset_commit_request::{
                TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition,
                TxnOffsetCommitRequestTopic,
            },
            txn_offset_commit_response::{
                TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition,
                TxnOffsetCommitResponseTopic,
            },
            write_txn_markers_request::{
                WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
            },
            write_txn_markers_response::{
                WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
                WritableTxnMarkerTopicResult, WriteTxnMarkersResponse,
            },
        },
    },
};

/// `AddPartitionsToTxn` puts the code on each partition before v4, and at the
/// top level from v4.
#[test]
fn add_partitions_to_txn_answers_by_version() {
    let request = AddPartitionsToTxnRequest {
        v3_and_below_transactional_id: "t1".to_owned(),
        v3_and_below_topics: vec![AddPartitionsToTxnTopic {
            name: "orders".to_owned(),
            partitions: vec![0, 1],
            ..Default::default()
        }],
        ..Default::default()
    };
    let row = |partition_index| AddPartitionsToTxnPartitionResult {
        partition_index,
        partition_error_code: ILLEGAL_SASL_STATE,
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::AddPartitionsToTxn,
            3,
            request,
            Some(AddPartitionsToTxnResponse {
                results_by_topic_v3_and_below: vec![AddPartitionsToTxnTopicResult {
                    name: "orders".to_owned(),
                    results_by_partition: vec![row(0), row(1)],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::AddPartitionsToTxn,
            4,
            AddPartitionsToTxnRequest::default(),
            Some(AddPartitionsToTxnResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}

/// `WriteTxnMarkers` answers one result per producer id, keeping the last
/// marker of a repeated one, and one row per distinct partition of a topic.
#[test]
fn write_txn_markers_merges_as_its_maps_do() {
    let marker = |producer_id, topics: Vec<(&str, Vec<i32>)>| WritableTxnMarker {
        producer_id,
        topics: topics
            .into_iter()
            .map(|(name, partition_indexes)| WritableTxnMarkerTopic {
                name: name.to_owned(),
                partition_indexes,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let topic = |name: &str, partitions: &[i32]| WritableTxnMarkerTopicResult {
        name: name.to_owned(),
        partitions: partitions
            .iter()
            .map(|&partition_index| WritableTxnMarkerPartitionResult {
                partition_index,
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    check(vec![case(
        ApiKey::WriteTxnMarkers,
        1,
        WriteTxnMarkersRequest {
            markers: vec![
                marker(5, vec![("dropped", vec![9])]),
                marker(6, vec![
                    ("orders", vec![0, 1]),
                    ("payments", vec![2]),
                    ("orders", vec![1, 3]),
                ]),
                marker(5, vec![("kept", vec![4])]),
            ],
            ..Default::default()
        },
        Some(WriteTxnMarkersResponse {
            markers: vec![
                WritableTxnMarkerResult {
                    producer_id: 5,
                    topics: vec![topic("kept", &[4])],
                    ..Default::default()
                },
                WritableTxnMarkerResult {
                    producer_id: 6,
                    topics: vec![topic("orders", &[0, 1, 3]), topic("payments", &[2])],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }),
    )]);
}

/// The other transaction apis.
#[test]
fn transaction_apis() {
    check(vec![
        case(
            ApiKey::AddOffsetsToTxn,
            4,
            AddOffsetsToTxnRequest::default(),
            Some(AddOffsetsToTxnResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::EndTxn,
            5,
            EndTxnRequest::default(),
            Some(EndTxnResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::TxnOffsetCommit,
            5,
            TxnOffsetCommitRequest {
                topics: vec![TxnOffsetCommitRequestTopic {
                    name: "orders".to_owned(),
                    partitions: vec![TxnOffsetCommitRequestPartition {
                        partition_index: 3,
                        committed_offset: 10,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(TxnOffsetCommitResponse {
                topics: vec![TxnOffsetCommitResponseTopic {
                    name: "orders".to_owned(),
                    partitions: vec![TxnOffsetCommitResponsePartition {
                        partition_index: 3,
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeTransactions,
            0,
            DescribeTransactionsRequest {
                transactional_ids: vec!["t1".to_owned()],
                ..Default::default()
            },
            Some(DescribeTransactionsResponse {
                transaction_states: vec![TransactionState {
                    error_code: ILLEGAL_SASL_STATE,
                    transactional_id: "t1".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ListTransactions,
            2,
            ListTransactionsRequest::default(),
            Some(ListTransactionsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}
