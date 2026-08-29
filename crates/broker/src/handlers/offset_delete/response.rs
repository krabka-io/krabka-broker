//! The `OffsetDelete` response shapes that do not depend on the per-partition
//! decision, plus the encoder every early return goes through.
//!
//! Two of them exist. A whole-response error stamps one code on the top level
//! and on every requested partition, which is what a group-level ACL denial, a
//! missing group and a coordinator-routing failure all return. A late failure
//! instead rewrites the rows that had already resolved to `NONE`, leaving the
//! rows that failed earlier with the code they carry.

use bytes::Bytes;
use krabka_protocol::owned::{
    offset_delete_request::OffsetDeleteRequest,
    offset_delete_response::{
        OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
    },
};

use crate::{codes, error::BrokerError};

pub(super) fn whole_error(req: &OffsetDeleteRequest, code: i16) -> OffsetDeleteResponse {
    OffsetDeleteResponse {
        error_code: code,
        throttle_time_ms: 0,
        topics: req
            .topics
            .iter()
            .map(|t| OffsetDeleteResponseTopic {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .map(|p| OffsetDeleteResponsePartition {
                        partition_index: p.partition_index,
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

pub(super) fn rewrite_success_as(
    topics: Vec<OffsetDeleteResponseTopic>,
    code: i16,
) -> OffsetDeleteResponse {
    let topics = topics
        .into_iter()
        .map(|mut t| {
            for p in &mut t.partitions {
                if p.error_code == codes::NONE {
                    p.error_code = code;
                }
            }
            t
        })
        .collect();
    OffsetDeleteResponse {
        error_code: codes::NONE,
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    }
}

pub(super) fn encode(version: i16, resp: &OffsetDeleteResponse) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::handlers::offset_delete::test_support::{
        expected_row, expected_topic, req_with_topics,
    };

    // ── whole_error ──────────────────────────────────────────────────

    #[test]
    fn whole_error_stamps_top_level_and_each_partition() {
        let req = req_with_topics(&[("t1", &[0, 1]), ("t2", &[5])]);
        let resp = whole_error(&req, codes::GROUP_AUTHORIZATION_FAILED);
        let expected = OffsetDeleteResponse {
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            throttle_time_ms: 0,
            topics: vec![
                expected_topic(
                    "t1",
                    vec![
                        expected_row(0, codes::GROUP_AUTHORIZATION_FAILED),
                        expected_row(1, codes::GROUP_AUTHORIZATION_FAILED),
                    ],
                ),
                expected_topic(
                    "t2",
                    vec![expected_row(5, codes::GROUP_AUTHORIZATION_FAILED)],
                ),
            ],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn whole_error_with_empty_request_returns_empty_topics_list() {
        let req = req_with_topics(&[]);
        let resp = whole_error(&req, codes::GROUP_ID_NOT_FOUND);
        assert!(resp.error_code == codes::GROUP_ID_NOT_FOUND);
        assert!(resp.topics.is_empty());
    }

    // ── rewrite_success_as ───────────────────────────────────────────

    fn resp_topics(rows: &[(&str, &[(i32, i16)])]) -> Vec<OffsetDeleteResponseTopic> {
        rows.iter()
            .map(|(n, ps)| OffsetDeleteResponseTopic {
                name: (*n).to_string(),
                partitions: ps
                    .iter()
                    .map(|(idx, code)| OffsetDeleteResponsePartition {
                        partition_index: *idx,
                        error_code: *code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn rewrite_success_as_overwrites_none_rows_only() {
        let rows = resp_topics(&[(
            "t",
            &[
                (0, codes::NONE),
                (1, codes::TOPIC_AUTHORIZATION_FAILED),
                (2, codes::NONE),
            ],
        )]);
        let resp = rewrite_success_as(rows, codes::UNKNOWN_SERVER_ERROR);
        // NONE rows are rewritten; the denied row stays denied.
        let expected = OffsetDeleteResponse {
            error_code: codes::NONE,
            throttle_time_ms: 0,
            topics: vec![expected_topic(
                "t",
                vec![
                    expected_row(0, codes::UNKNOWN_SERVER_ERROR),
                    expected_row(1, codes::TOPIC_AUTHORIZATION_FAILED),
                    expected_row(2, codes::UNKNOWN_SERVER_ERROR),
                ],
            )],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn rewrite_success_as_noop_when_no_none_rows() {
        let rows = resp_topics(&[("t", &[(0, codes::GROUP_SUBSCRIBED_TO_TOPIC)])]);
        let resp = rewrite_success_as(rows, codes::UNKNOWN_SERVER_ERROR);
        assert!(resp.topics[0].partitions[0].error_code == codes::GROUP_SUBSCRIBED_TO_TOPIC);
    }
}
