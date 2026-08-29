//! The `DeleteTopics` response shapes: one row per requested topic and the
//! envelope that carries them.
//!
//! Field-for-field construction is the contract with the JVM `AdminClient`, so
//! it sits apart from the code that decides which error code a row gets.

use krabka_protocol::{
    owned::delete_topics_response::{DeletableTopicResult, DeleteTopicsResponse},
    primitives::uuid::Uuid as WireUuid,
};

/// Builds one response row for a requested topic.
pub(super) fn delete_topic_result(
    name: Option<String>,
    topic_id: WireUuid,
    error_code: i16,
) -> DeletableTopicResult {
    DeletableTopicResult {
        name,
        topic_id,
        error_code,
        ..Default::default()
    }
}

/// Builds the response envelope over the per-topic rows.
pub(super) fn delete_topics_response(
    responses: Vec<DeletableTopicResult>,
    throttle_time_ms: i32,
) -> DeleteTopicsResponse {
    DeleteTopicsResponse {
        responses,
        throttle_time_ms,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::codes;

    #[test]
    fn response_helpers_preserve_topic_identity_error_and_throttle_fields() {
        let id = WireUuid([9; 16]);
        let unknown_id = delete_topic_result(None, id, codes::UNKNOWN_TOPIC_ID);
        let expected_unknown = DeletableTopicResult {
            name: None,
            topic_id: id,
            error_code: codes::UNKNOWN_TOPIC_ID,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(unknown_id == expected_unknown);

        let denied = delete_topic_result(
            Some("secret".into()),
            WireUuid::ZERO,
            codes::TOPIC_AUTHORIZATION_FAILED,
        );
        let expected_denied = DeletableTopicResult {
            name: Some("secret".into()),
            topic_id: WireUuid::ZERO,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: None,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(denied == expected_denied);

        let resp = delete_topics_response(vec![denied], 123);
        let expected_resp = DeleteTopicsResponse {
            throttle_time_ms: 123,
            responses: vec![expected_denied],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected_resp);
    }
}
