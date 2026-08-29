//! The `CreateAcls` response rows: one result per creation, the envelope that
//! carries them, the bulk stamp a failed controller submit leaves on the
//! creations it covered, and the encoder.
//!
//! Kafka answers `CreateAcls` positionally, so every path through the handler
//! has to produce exactly one row per request creation. Keeping the row
//! constructors together makes that invariant easy to see.

use bytes::Bytes;
use krabka_metadata::MetadataRecord;
use krabka_protocol::{
    Encode,
    owned::create_acls_response::{AclCreationResult, CreateAclsResponse},
};

use crate::codes;

pub(super) fn acl_error_result(code: i16, msg: impl Into<String>) -> AclCreationResult {
    AclCreationResult {
        error_code: code,
        error_message: Some(msg.into()),
        ..Default::default()
    }
}

pub(super) fn create_acls_response(results: Vec<AclCreationResult>) -> CreateAclsResponse {
    CreateAclsResponse {
        results,
        ..Default::default()
    }
}

pub(super) fn apply_submit_error<E: std::fmt::Display>(
    results: &mut [AclCreationResult],
    to_submit: &[(usize, MetadataRecord)],
    err: E,
) {
    let msg = format!("submit failed: {err}");
    for (idx, _) in to_submit {
        results[*idx] = acl_error_result(codes::COORDINATOR_NOT_AVAILABLE, msg.clone());
    }
}

pub(super) fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode CreateAcls")
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::handlers::create_acls::test_support::{
        OPERATION_READ, VERSION, creation, decode_response, validate,
    };

    #[test]
    fn error_and_submit_helpers_preserve_non_default_result_fields() {
        let err = acl_error_result(codes::INVALID_REQUEST, "bad acl");
        assert!(err.error_code == codes::INVALID_REQUEST);
        assert!(err.error_message.as_deref() == Some("bad acl"));

        let mut results = vec![
            AclCreationResult::default(),
            acl_error_result(codes::INVALID_REQUEST, "already invalid"),
        ];
        let submitted = vec![(
            0usize,
            MetadataRecord::V1AccessControlEntry(
                validate(&creation("topic-a", "User:alice", OPERATION_READ))
                    .expect("valid creation"),
            ),
        )];

        apply_submit_error(&mut results, &submitted, "not controller");

        let expected = vec![
            AclCreationResult {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
            AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("already invalid".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ];
        assert!(results == expected);
    }

    #[test]
    fn encode_response_writes_decodable_results() {
        let bytes = encode_response(
            &create_acls_response(vec![acl_error_result(codes::INVALID_REQUEST, "bad acl")]),
            VERSION,
        )
        .expect("encode");
        let decoded = decode_response(&bytes);

        let expected = CreateAclsResponse {
            throttle_time_ms: 0,
            results: vec![AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("bad acl".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(decoded == expected);
    }
}
