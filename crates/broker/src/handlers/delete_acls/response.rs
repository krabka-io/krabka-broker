//! The `DeleteAcls` response rows: the matching-ACL echo Kafka sends back for
//! every entry a filter removed, the per-filter result that carries them, the
//! envelope, the bulk stamp a failed controller submit leaves behind, and the
//! encoder.
//!
//! `kafka-acls --remove` prints the matching-ACL list verbatim, so the wire
//! bytes here are what the operator reads. Keeping the row constructors
//! together makes the one-row-per-filter invariant easy to see.

use bytes::Bytes;
use krabka_metadata::AclEntry;
use krabka_protocol::{
    Encode,
    owned::delete_acls_response::{
        DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
    },
};

use crate::{
    codes,
    handlers::acl_wire::{
        operation_to_wire, pattern_type_to_wire, permission_to_wire, resource_type_to_wire,
    },
};

pub(super) fn matching_acl_result(e: &AclEntry) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl {
        resource_type: resource_type_to_wire(e.resource_type),
        resource_name: e.resource_name.clone(),
        pattern_type: pattern_type_to_wire(e.pattern_type),
        principal: e.principal.clone(),
        host: e.host.clone(),
        operation: operation_to_wire(e.operation),
        permission_type: permission_to_wire(e.permission_type),
        ..Default::default()
    }
}

pub(super) fn filter_result(
    error_code: i16,
    error_message: Option<String>,
    matching_acls: Vec<DeleteAclsMatchingAcl>,
) -> DeleteAclsFilterResult {
    DeleteAclsFilterResult {
        error_code,
        error_message,
        matching_acls,
        ..Default::default()
    }
}

pub(super) fn delete_acls_response(
    filter_results: Vec<DeleteAclsFilterResult>,
) -> DeleteAclsResponse {
    DeleteAclsResponse {
        filter_results,
        ..Default::default()
    }
}

pub(super) fn apply_submit_error<E: std::fmt::Display>(
    filter_results: &mut [DeleteAclsFilterResult],
    err: E,
) {
    let msg = format!("submit failed: {err}");
    for r in filter_results {
        if r.error_code == codes::NONE {
            r.error_code = codes::COORDINATOR_NOT_AVAILABLE;
            r.error_message = Some(msg.clone());
        }
    }
}

pub(super) fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode DeleteAcls")
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{AclOperation, PatternType};
    use krabka_protocol::UnknownTaggedFields;

    use super::*;
    use crate::handlers::delete_acls::test_support::{
        OPERATION_READ, OPERATION_WRITE, PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED,
        PERMISSION_ALLOW, RESOURCE_TYPE_TOPIC, VERSION, acl, decode_response,
    };

    #[test]
    fn helpers_preserve_matching_acl_and_submit_error_fields() {
        let matched = matching_acl_result(&acl("orders", "User:alice", AclOperation::Read));
        let expected_matched = DeleteAclsMatchingAcl {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".into(),
            pattern_type: PATTERN_TYPE_LITERAL,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(matched == expected_matched);

        let mut prefixed_acl = acl("orders-", "User:bob", AclOperation::Write);
        prefixed_acl.pattern_type = PatternType::Prefixed;
        let matched = matching_acl_result(&prefixed_acl);
        assert!(matched.pattern_type == PATTERN_TYPE_PREFIXED);
        assert!(matched.operation == OPERATION_WRITE);

        let mut results = vec![
            filter_result(codes::NONE, None, vec![matched.clone()]),
            filter_result(
                codes::INVALID_REQUEST,
                Some("malformed filter axis".into()),
                Vec::new(),
            ),
        ];
        apply_submit_error(&mut results, "not controller");

        let expected_results = vec![
            DeleteAclsFilterResult {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                matching_acls: vec![matched],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            DeleteAclsFilterResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("malformed filter axis".into()),
                matching_acls: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(results == expected_results);
    }

    #[test]
    fn encode_response_writes_decodable_filter_results() {
        let bytes = encode_response(
            &delete_acls_response(vec![filter_result(
                codes::INVALID_REQUEST,
                Some("malformed filter axis".into()),
                Vec::new(),
            )]),
            VERSION,
        )
        .expect("encode");
        let resp = decode_response(&bytes);

        let expected = DeleteAclsResponse {
            throttle_time_ms: 0,
            filter_results: vec![DeleteAclsFilterResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("malformed filter axis".into()),
                matching_acls: Vec::new(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }
}
