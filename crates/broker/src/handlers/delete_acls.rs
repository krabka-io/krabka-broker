//! `DeleteAcls` handler (`api_key` 31).
//!
//! This handler authorizes `Alter` on `Cluster`. For each filter it decodes
//! the wire axes, reports the ACL entries that match, and submits one
//! deletion record to the controller. It returns one filter result per
//! request filter.
//!
//! This file keeps the whole-request cluster gate and the per-filter loop.
//! Filter decoding lives in `filter`, the response rows and the encoder in
//! `response`, and the audit trail in `audit`.

use bytes::Bytes;
use krabka_metadata::{AclEntry, MetadataRecord};
use krabka_protocol::owned::{
    delete_acls_request::DeleteAclsRequest, delete_acls_response::DeleteAclsFilterResult,
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
    filter::build_filter,
    response::{
        apply_submit_error, delete_acls_response, encode_response, filter_result,
        matching_acl_result,
    },
};
use super::acl_wire::CLUSTER_RESOURCE_NAME;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

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
        let filter_results = req
            .filters
            .iter()
            .map(|_| {
                filter_result(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("delete-acls denied".into()),
                    Vec::new(),
                )
            })
            .collect();
        return encode_response(&delete_acls_response(filter_results), api_version);
    }

    let mut filter_results: Vec<DeleteAclsFilterResult> = Vec::with_capacity(req.filters.len());
    let mut to_submit: Vec<MetadataRecord> = Vec::with_capacity(req.filters.len());

    for f in &req.filters {
        match build_filter(f) {
            Ok(filter) => {
                let matched: Vec<&AclEntry> =
                    image.all_acls().filter(|e| filter.matches(e)).collect();
                let matching_acls = matched
                    .iter()
                    .map(|e| matching_acl_result(e))
                    .collect::<Vec<_>>();
                filter_results.push(filter_result(codes::NONE, None, matching_acls));
                to_submit.push(MetadataRecord::V1DeleteAccessControlEntry(filter));
            }
            Err(_) => {
                filter_results.push(filter_result(
                    codes::INVALID_REQUEST,
                    Some("malformed filter axis".into()),
                    Vec::new(),
                ));
            }
        }
    }

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
