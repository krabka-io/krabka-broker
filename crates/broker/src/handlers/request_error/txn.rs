//! The transaction apis.

use {
    super::{ErrorAnswer, Request},
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            add_offsets_to_txn_request::AddOffsetsToTxnRequest,
            add_offsets_to_txn_response::AddOffsetsToTxnResponse,
            add_partitions_to_txn_request::AddPartitionsToTxnRequest,
            add_partitions_to_txn_response::AddPartitionsToTxnResponse,
            common::add_partitions_to_txn_response::{
                add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
                add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
            },
            describe_transactions_request::DescribeTransactionsRequest,
            describe_transactions_response::{DescribeTransactionsResponse, TransactionState},
            end_txn_request::EndTxnRequest,
            end_txn_response::EndTxnResponse,
            list_transactions_request::ListTransactionsRequest,
            list_transactions_response::ListTransactionsResponse,
            txn_offset_commit_request::TxnOffsetCommitRequest,
            txn_offset_commit_response::{
                TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition,
                TxnOffsetCommitResponseTopic,
            },
            write_txn_markers_request::{WritableTxnMarker, WriteTxnMarkersRequest},
            write_txn_markers_response::{
                WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
                WritableTxnMarkerTopicResult, WriteTxnMarkersResponse,
            },
        },
    },
};

/// `AddPartitionsToTxnRequest.EARLIEST_BROKER_VERSION`: the first version
/// whose error response is one top-level code.
const ADD_PARTITIONS_TO_TXN_BROKER_MIN_VERSION: i16 = 4;

pub(super) fn answer(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let version = request.version;
    Some(match api {
        ApiKey::AddPartitionsToTxn => {
            request.respond(|req| add_partitions_to_txn(&req, version, code))
        }
        ApiKey::AddOffsetsToTxn => {
            request.respond(|_: AddOffsetsToTxnRequest| AddOffsetsToTxnResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::EndTxn => request.respond(|_: EndTxnRequest| EndTxnResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::WriteTxnMarkers => request.respond(|req| write_txn_markers(&req, code)),
        ApiKey::TxnOffsetCommit => request.respond(|req| txn_offset_commit(&req, code)),
        ApiKey::DescribeTransactions => {
            request.respond(
                |req: DescribeTransactionsRequest| DescribeTransactionsResponse {
                    transaction_states: req
                        .transactional_ids
                        .into_iter()
                        .map(|transactional_id| TransactionState {
                            error_code: code,
                            transactional_id,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
        }
        ApiKey::ListTransactions => {
            request.respond(|_: ListTransactionsRequest| ListTransactionsResponse {
                error_code: code,
                ..Default::default()
            })
        }
        _ => return None,
    })
}

/// `AddPartitionsToTxnRequest.getErrorResponse`: before v4 every requested
/// partition carries the code, from v4 the top level does.
fn add_partitions_to_txn(
    req: &AddPartitionsToTxnRequest,
    version: i16,
    code: i16,
) -> AddPartitionsToTxnResponse {
    if version < ADD_PARTITIONS_TO_TXN_BROKER_MIN_VERSION {
        AddPartitionsToTxnResponse {
            results_by_topic_v3_and_below: req
                .v3_and_below_topics
                .iter()
                .map(|topic| AddPartitionsToTxnTopicResult {
                    name: topic.name.clone(),
                    results_by_partition: topic
                        .partitions
                        .iter()
                        .map(|&partition_index| AddPartitionsToTxnPartitionResult {
                            partition_index,
                            partition_error_code: code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    } else {
        AddPartitionsToTxnResponse {
            error_code: code,
            ..Default::default()
        }
    }
}

/// `WriteTxnMarkersRequest.getErrorResponse`: one result per producer id and
/// one row per distinct topic partition of it, each with the code.
///
/// Kafka keys the markers by producer id in a `HashMap`, so a repeated
/// producer id keeps only the partitions of its last marker.
fn write_txn_markers(req: &WriteTxnMarkersRequest, code: i16) -> WriteTxnMarkersResponse {
    let mut producer_ids: Vec<i64> = Vec::new();
    for marker in &req.markers {
        if !producer_ids.contains(&marker.producer_id) {
            producer_ids.push(marker.producer_id);
        }
    }
    WriteTxnMarkersResponse {
        markers: producer_ids
            .into_iter()
            .filter_map(|producer_id| {
                req.markers
                    .iter()
                    .rfind(|marker| marker.producer_id == producer_id)
            })
            .map(|marker| marker_result(marker, code))
            .collect(),
        ..Default::default()
    }
}

/// One marker's result: its partitions grouped by topic name, each once.
fn marker_result(marker: &WritableTxnMarker, code: i16) -> WritableTxnMarkerResult {
    let mut topics: Vec<WritableTxnMarkerTopicResult> = Vec::new();
    for topic in &marker.topics {
        let index = topics
            .iter()
            .position(|known| known.name == topic.name)
            .unwrap_or_else(|| {
                topics.push(WritableTxnMarkerTopicResult {
                    name: topic.name.clone(),
                    ..Default::default()
                });
                topics.len() - 1
            });
        let partitions = &mut topics[index].partitions;
        for &partition_index in &topic.partition_indexes {
            if partitions
                .iter()
                .all(|known| known.partition_index != partition_index)
            {
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index,
                    error_code: code,
                    ..Default::default()
                });
            }
        }
    }
    WritableTxnMarkerResult {
        producer_id: marker.producer_id,
        topics,
        ..Default::default()
    }
}

/// `TxnOffsetCommitRequest.getErrorResponse`: every requested partition with
/// the code, under its topic's name.
fn txn_offset_commit(req: &TxnOffsetCommitRequest, code: i16) -> TxnOffsetCommitResponse {
    TxnOffsetCommitResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| TxnOffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| TxnOffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
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
