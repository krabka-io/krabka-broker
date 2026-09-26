//! `DeleteAcls` handler (`api_key` 31).
//!
//! This handler authorizes `Alter` on `Cluster`. For each filter it decodes
//! the wire axes into Kafka's `AclBindingFilter` and reports the ACL entries
//! that match. It then submits one deletion record per distinct matched entry,
//! as Kafka's `AclControlManager.deleteAcls` does, and returns one filter
//! result per request filter.
//!
//! On a cluster with no authorizer configured every filter is refused with
//! `SECURITY_DISABLED`, the same answer its `DescribeAcls` counterpart gives.
//!
//! This file keeps the whole-request cluster gate and the per-filter loop.
//! Filter decoding lives in `filter`, the response rows and the encoder in
//! `response`, and the audit trail in `audit`.

use bytes::Bytes;
use krabka_metadata::{AclEntry, MetadataRecord};
use krabka_protocol::{
    ProtocolError,
    owned::{delete_acls_request::DeleteAclsRequest, delete_acls_response::DeleteAclsFilterResult},
};

mod audit;
mod filter;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    audit::{audit_deleted_acls, deleted_acl_resources},
    filter::{build_filter, exact_filter},
    response::{
        apply_submit_error, delete_acls_response, encode_response, filter_result,
        matching_acl_result,
    },
};
use super::acl_wire::{
    CLUSTER_RESOURCE_NAME, NO_AUTHORIZER_MESSAGE, binding_filter::UnknownElement,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// The message of a cluster-alter refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs ALTER permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
const CLUSTER_ALTER_DENIED_MESSAGE: &str = "Request DeleteAcls needs ALTER permission.";

#[tracing::instrument(
    name = "handle_delete_acls",
    level = "info",
    skip_all,
    fields(api = "DeleteAcls"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DeleteAclsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Kafka's `DeleteAclsRequest` constructor refuses an `UNKNOWN` element in
    // any filter while the request parses, before any authorization, and the
    // broker closes the connection. The error return is that close.
    let filters = req
        .filters
        .iter()
        .map(build_filter)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|UnknownElement| {
            ProtocolError::InvalidValue("Filters contain UNKNOWN elements")
        })?;

    let image = broker.controller.current_image();

    // Whole-request cluster-alter gate.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: krabka_metadata::ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: krabka_metadata::AclOperation::Alter,
        },
    );
    if allow == AuthorizationResult::Deny {
        let filter_results = filters
            .iter()
            .map(|_| {
                filter_result(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some(CLUSTER_ALTER_DENIED_MESSAGE.into()),
                    Vec::new(),
                )
            })
            .collect();
        return encode_response(&delete_acls_response(filter_results), api_version);
    }

    // No authorizer: there is nothing to delete. Kafka builds this response
    // from `DeleteAclsRequest.getErrorResponse` with a
    // `SecurityDisabledException`, which stamps the same code and message on
    // one filter result per filter and leaves every matching-ACL list empty.
    if !broker.config.authorizer.is_configured() {
        let filter_results = filters
            .iter()
            .map(|_| {
                filter_result(
                    codes::SECURITY_DISABLED,
                    Some(NO_AUTHORIZER_MESSAGE.into()),
                    Vec::new(),
                )
            })
            .collect();
        return encode_response(&delete_acls_response(filter_results), api_version);
    }

    // Kafka's `AclControlManager.deleteAcls` matches every filter against the
    // same ACL set, so an ACL two filters match is listed under both, and it
    // collects the removal records into a set, so that ACL is removed once.
    let mut filter_results: Vec<DeleteAclsFilterResult> = Vec::with_capacity(filters.len());
    let mut doomed: Vec<&AclEntry> = Vec::new();
    for filter in &filters {
        if let Some(message) = filter.unknown_message() {
            filter_results.push(filter_result(
                codes::INVALID_REQUEST,
                Some(message.into()),
                Vec::new(),
            ));
            continue;
        }
        let matched: Vec<&AclEntry> = image.all_acls().filter(|e| filter.matches(e)).collect();
        filter_results.push(filter_result(
            codes::NONE,
            None,
            matched.iter().map(|e| matching_acl_result(e)).collect(),
        ));
        for entry in matched {
            if !doomed.contains(&entry) {
                doomed.push(entry);
            }
        }
    }

    let to_submit: Vec<MetadataRecord> = doomed
        .into_iter()
        .map(|entry| MetadataRecord::V1DeleteAccessControlEntry(exact_filter(entry)))
        .collect();
    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "delete-acls submit failed");
        apply_submit_error(&mut filter_results, &e);
    }

    // Audit: emit one AdminOperation record for successfully-deleted ACLs.
    // Collect resource_name from each matching ACL in every filter result that
    // committed without error (error_code == 0).
    audit_deleted_acls(
        broker.audit_log.as_ref(),
        ctx,
        deleted_acl_resources(&filter_results),
    );

    encode_response(&delete_acls_response(filter_results), api_version)
}
