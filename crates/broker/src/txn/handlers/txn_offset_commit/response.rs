//! Builders and encoders for the `TxnOffsetCommitResponse`.
//!
//! Every exit from the handler answers with the same nested topic and
//! partition row shape, so this module is the single place that decides which
//! error code lands on which row: one shared code for the whole response, with
//! `TOPIC_AUTHORIZATION_FAILED` overriding it on the topics the per-topic
//! `Read` ACL denied, and `UNKNOWN_TOPIC_OR_PARTITION` overriding it on the
//! rows the existence check flagged.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::{
        txn_offset_commit_request::TxnOffsetCommitRequest,
        txn_offset_commit_response::{
            TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        },
    },
};

use crate::{codes, error::BrokerError};

pub(super) fn build_response(
    req: &TxnOffsetCommitRequest,
    code: i16,
    denied_topics: &std::collections::HashSet<String>,
    unknown_rows: &std::collections::HashSet<(String, i32)>,
) -> TxnOffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| {
            let denied = denied_topics.contains(&t.name);
            TxnOffsetCommitResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| {
                        let row_code = if denied {
                            codes::TOPIC_AUTHORIZATION_FAILED
                        } else if unknown_rows.contains(&(t.name.clone(), p.partition_index)) {
                            codes::UNKNOWN_TOPIC_OR_PARTITION
                        } else {
                            code
                        };
                        TxnOffsetCommitResponsePartition {
                            partition_index: p.partition_index,
                            error_code: row_code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect();
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    }
}

pub(super) fn encode_resp(
    version: i16,
    resp: &TxnOffsetCommitResponse,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

pub(super) fn encode_err_all(
    version: i16,
    req: &TxnOffsetCommitRequest,
    code: i16,
) -> Result<Bytes, BrokerError> {
    let empty_topics: std::collections::HashSet<String> = std::collections::HashSet::new();
    let empty_rows: std::collections::HashSet<(String, i32)> = std::collections::HashSet::new();
    encode_resp(
        version,
        &build_response(req, code, &empty_topics, &empty_rows),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::assert;

    use super::*;
    use crate::txn::handlers::txn_offset_commit::test_support::request;

    fn decode_response(bytes: &Bytes, version: i16) -> TxnOffsetCommitResponse {
        crate::test_support::decode_response(bytes, version)
    }

    fn assert_response_rows(resp: &TxnOffsetCommitResponse, code: i16) {
        let expected = TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![TxnOffsetCommitResponseTopic {
                name: "orders".into(),
                partitions: vec![
                    TxnOffsetCommitResponsePartition {
                        partition_index: 2,
                        error_code: code,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                    TxnOffsetCommitResponsePartition {
                        partition_index: 3,
                        error_code: code,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                ],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(*resp == expected);
    }

    #[test]
    fn build_response_preserves_topic_partition_rows_and_error_codes() {
        let req = request();
        let resp = build_response(
            &req,
            codes::GROUP_AUTHORIZATION_FAILED,
            &HashSet::new(),
            &HashSet::new(),
        );

        assert_response_rows(&resp, codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn build_response_overrides_denied_topics_with_topic_authorization_error() {
        let req = request();
        let denied = maplit::hashset! {"orders".to_string()};

        let resp = build_response(&req, codes::NONE, &denied, &HashSet::new());

        assert_response_rows(&resp, codes::TOPIC_AUTHORIZATION_FAILED);
    }

    #[test]
    fn build_response_overrides_unknown_rows_with_unknown_topic_or_partition() {
        let req = request();
        let unknown = maplit::hashset! {("orders".to_string(), 3)};

        let resp = build_response(&req, codes::NONE, &HashSet::new(), &unknown);

        let expected = TxnOffsetCommitResponse {
            throttle_time_ms: 0,
            topics: vec![TxnOffsetCommitResponseTopic {
                name: "orders".into(),
                partitions: vec![
                    TxnOffsetCommitResponsePartition {
                        partition_index: 2,
                        error_code: codes::NONE,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                    TxnOffsetCommitResponsePartition {
                        partition_index: 3,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    },
                ],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
    }

    #[test]
    fn encode_resp_round_trips_non_empty_response() {
        let req = request();
        let resp = build_response(
            &req,
            codes::INVALID_TXN_STATE,
            &HashSet::new(),
            &HashSet::new(),
        );

        let bytes = encode_resp(5, &resp).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 5);

        assert_response_rows(&decoded, codes::INVALID_TXN_STATE);
    }

    #[test]
    fn encode_err_all_round_trips_rows_for_whole_request_error() {
        let req = request();

        let bytes = encode_err_all(5, &req, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
            .expect("encode all-error response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 5);

        assert_response_rows(&decoded, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
}
