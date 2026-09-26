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
use krabka_raft::RaftError;

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

/// Stamps every submitted creation with the error a failed controller write
/// maps to.
///
/// Kafka's controller fails the `createAcls` write with
/// `NotControllerException` when it is not (or is no longer) the active
/// controller, and wraps anything else as an `UnknownServerException`, whose
/// text `ApiError.fromThrowable` drops so no internal detail reaches the
/// client. Neither carries a coordinator error.
pub(super) fn apply_submit_error(
    results: &mut [AclCreationResult],
    to_submit: &[(usize, MetadataRecord)],
    err: &RaftError,
) {
    let code = match err {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
        other => crate::handlers::submit_failure_code(other, codes::UNKNOWN_SERVER_ERROR),
    };
    for (idx, _) in to_submit {
        results[*idx] = AclCreationResult {
            error_code: code,
            ..Default::default()
        };
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
    fn submit_error_maps_controller_failures_and_leaves_rejected_rows() {
        let submitted = vec![(
            0usize,
            MetadataRecord::V1AccessControlEntry(
                validate(&creation("topic-a", "User:alice", OPERATION_READ))
                    .expect("valid creation"),
            ),
        )];
        let cases = [
            (
                RaftError::NotLeader {
                    current_leader: None,
                },
                codes::NOT_CONTROLLER,
            ),
            (RaftError::LeaderUnknown, codes::NOT_CONTROLLER),
            (RaftError::UncommittedTail, codes::NOT_CONTROLLER),
            (RaftError::Shutdown, codes::UNKNOWN_SERVER_ERROR),
            (
                RaftError::ChangeRejected("internal detail".into()),
                codes::UNKNOWN_SERVER_ERROR,
            ),
        ];
        for (error, code) in cases {
            let mut results = vec![
                AclCreationResult::default(),
                acl_error_result(codes::INVALID_REQUEST, "already invalid"),
            ];
            apply_submit_error(&mut results, &submitted, &error);
            let expected = vec![
                AclCreationResult {
                    error_code: code,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
                AclCreationResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("already invalid".into()),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                },
            ];
            assert!(results == expected, "{error}");
        }
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
