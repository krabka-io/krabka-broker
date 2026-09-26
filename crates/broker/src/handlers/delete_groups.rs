//! `DeleteGroups` (`api_key=42`).
//!
//! This handler deletes empty groups of every kind, tombstoning their offsets
//! and group records, and rejects a group with members with
//! `NON_EMPTY_GROUP`. As in Kafka's `KafkaApis.handleDeleteGroupsRequest`, a
//! duplicate id is answered once, and the rows of the groups the principal may
//! not delete follow the coordinator's rows.

use std::collections::HashSet;

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        delete_groups_request::DeleteGroupsRequest,
        delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse},
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    coordinator::DeleteGroupError,
    error::BrokerError,
};

#[tracing::instrument(
    name = "handle_delete_groups",
    level = "info",
    skip_all,
    fields(api = "DeleteGroups", version, req_bytes = req_bytes.len()),
    err,
)]
// cargo-mutants: coordinator-backed request orchestration; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = DeleteGroupsRequest::decode(&mut cur, version)?;

    // Kafka's `handleDeleteGroupsRequest` drops duplicate ids first
    // (`groupsNames.distinct`), keeping the first-seen order, and then
    // partitions the ids by the `Delete` grant on each group.
    let mut seen = HashSet::with_capacity(req.groups_names.len());
    let groups: Vec<String> = req
        .groups_names
        .into_iter()
        .filter(|gid| seen.insert(gid.clone()))
        .collect();
    let (authorized, denied): (Vec<String>, Vec<String>) = {
        let image = broker.controller.current_image();
        groups.into_iter().partition(|gid| {
            broker.config.authorizer.authorize(
                &*image,
                &AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Group,
                    resource_name: gid.as_str(),
                    operation: AclOperation::Delete,
                },
            ) == AuthorizationResult::Allow
        })
    };

    // The coordinator's results come first, and a `GROUP_AUTHORIZATION_FAILED`
    // row for each denied group follows them.
    let mut results: Vec<DeletableGroupResult> =
        Vec::with_capacity(authorized.len() + denied.len());
    for gid in authorized {
        let error_code = delete_one(broker, &gid).await;
        results.push(DeletableGroupResult {
            group_id: gid,
            error_code,
            ..Default::default()
        });
    }
    results.extend(denied.into_iter().map(|gid| DeletableGroupResult {
        group_id: gid,
        error_code: codes::GROUP_AUTHORIZATION_FAILED,
        ..Default::default()
    }));

    let resp = DeleteGroupsResponse {
        results,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Deletes one authorized group and returns its result's error code.
async fn delete_one(broker: &Broker, group_id: &str) -> i16 {
    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, group_id) {
        return error_code;
    }
    match broker.group_coordinator.delete_group(group_id).await {
        Ok(()) => codes::NONE,
        Err(DeleteGroupError::NotFound) => codes::GROUP_ID_NOT_FOUND,
        Err(DeleteGroupError::NonEmpty) => codes::NON_EMPTY_GROUP,
        Err(DeleteGroupError::ShareState(error_code)) => error_code,
        Err(DeleteGroupError::Internal) => codes::UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_security::Principal;

    use super::*;
    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = 2;

    fn request(groups: &[&str]) -> DeleteGroupsRequest {
        DeleteGroupsRequest {
            groups_names: groups.iter().map(|g| (*g).into()).collect(),
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        DeleteGroupsRequest,
        DeleteGroupsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn drive(
        broker: &Broker,
        req: &DeleteGroupsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> DeleteGroupsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    #[tokio::test]
    async fn handle_denies_delete_for_each_group() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request(&["group-a", "group-b"]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteGroupsResponse {
            throttle_time_ms: 0,
            results: vec![
                DeletableGroupResult {
                    group_id: "group-a".to_string(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
                DeletableGroupResult {
                    group_id: "group-b".to_string(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                },
            ],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Denies every operation on the group named `denied` and allows the
    /// rest.
    #[derive(Debug)]
    struct DenyGroupNamedDenied;

    impl crate::authorizer::Authorizer for DenyGroupNamedDenied {
        fn authorize(
            &self,
            _source: &dyn krabka_authz::AclSource,
            req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if req.resource_name == "denied" {
                AuthorizationResult::Deny
            } else {
                AuthorizationResult::Allow
            }
        }
    }

    /// Kafka answers a duplicate group id once, and appends the
    /// `GROUP_AUTHORIZATION_FAILED` rows after the coordinator's rows.
    #[tokio::test]
    async fn handle_deduplicates_ids_and_appends_denied_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyGroupNamedDenied)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let response = |rows: &[(&str, i16)]| DeleteGroupsResponse {
            results: rows
                .iter()
                .map(|(group_id, error_code)| DeletableGroupResult {
                    group_id: (*group_id).to_string(),
                    error_code: *error_code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let cases = [
            (
                &["denied", "missing"][..],
                response(&[
                    ("missing", codes::GROUP_ID_NOT_FOUND),
                    ("denied", codes::GROUP_AUTHORIZATION_FAILED),
                ]),
            ),
            (
                &["missing", "missing"][..],
                response(&[("missing", codes::GROUP_ID_NOT_FOUND)]),
            ),
            (
                &["denied", "denied"][..],
                response(&[("denied", codes::GROUP_AUTHORIZATION_FAILED)]),
            ),
            (
                &["denied", "a", "denied", "b", "a"][..],
                response(&[
                    ("a", codes::GROUP_ID_NOT_FOUND),
                    ("b", codes::GROUP_ID_NOT_FOUND),
                    ("denied", codes::GROUP_AUTHORIZATION_FAILED),
                ]),
            ),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (groups, response) in cases {
            actual.push((groups, drive(&broker, &request(groups), &p, &peer).await));
            expected.push((groups, response));
        }
        assert!(actual == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_allowed_missing_group_returns_not_found() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(&["missing"]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = DeleteGroupsResponse {
            throttle_time_ms: 0,
            results: vec![DeletableGroupResult {
                group_id: "missing".to_string(),
                error_code: codes::GROUP_ID_NOT_FOUND,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
