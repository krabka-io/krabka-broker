//! Response assembly for `AlterPartitionReassignments`: the per-partition
//! result rows, the whole-request error envelope, and the encode step.
//!
//! `mark_submit_failed` rewrites the rows that were accepted but whose
//! metadata submit then failed, and it leaves an earlier per-row rejection in
//! place.

use std::collections::HashMap;

use bytes::Bytes;
use krabka_protocol::{
    Encode,
    owned::{
        alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
        alter_partition_reassignments_response::{
            AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
            ReassignableTopicResponse,
        },
    },
};

use crate::codes::COORDINATOR_NOT_AVAILABLE;

pub(super) fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        ..Default::default()
    }
}

pub(super) fn err_row(
    partition_index: i32,
    code: i16,
    msg: String,
) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: code,
        error_message: Some(msg),
        ..Default::default()
    }
}

pub(super) fn mark_submit_failed(
    by_topic: &mut HashMap<String, Vec<ReassignablePartitionResponse>>,
    msg: &str,
) {
    for rows in by_topic.values_mut() {
        for r in rows.iter_mut() {
            if r.error_code == 0 {
                r.error_code = COORDINATOR_NOT_AVAILABLE;
                r.error_message = Some(msg.to_string());
            }
        }
    }
}

pub(super) fn encode_whole_request_error(
    req: &AlterPartitionReassignmentsRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let responses: Vec<ReassignableTopicResponse> = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| err_row(p.partition_index, code, msg.into()))
                .collect(),
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: req.allow_replication_factor_change,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

pub(super) fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(
        resp,
        api_version,
        "encode AlterPartitionReassignments",
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::{
        codes::{CLUSTER_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_OR_PARTITION},
        handlers::alter_partition_reassignments::test_support::{decode_response, request},
    };

    #[test]
    fn row_builders_preserve_non_default_fields() {
        let ok = ok_row(7);
        let expected_ok = ReassignablePartitionResponse {
            partition_index: 7,
            error_code: 0,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let err = err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "missing partition".into());
        let expected_err = ReassignablePartitionResponse {
            partition_index: 8,
            error_code: UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some("missing partition".into()),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(err == expected_err);
    }

    #[test]
    fn encode_whole_request_error_preserves_request_shape() {
        let version = 1;
        let req = request(false, "payments", 8, Some(vec![1, 2]));

        let bytes =
            encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied", version)
                .expect("encode whole request error");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("denied".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn mark_submit_failed_only_rewrites_successful_rows() {
        let mut by_topic = std::collections::HashMap::from([(
            "orders".to_string(),
            vec![
                ok_row(7),
                err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()),
            ],
        )]);

        mark_submit_failed(&mut by_topic, "submit failed: not controller");
        let rows = by_topic.get("orders").expect("topic rows");

        let expected = vec![
            ReassignablePartitionResponse {
                partition_index: 7,
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ReassignablePartitionResponse {
                partition_index: 8,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("unknown partition".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(*rows == expected);
    }
}
