//! `CreateAcls` handler (`api_key` 30).
//!
//! This handler authorizes `Alter` on `Cluster`. For each binding, it
//! validates the resource shape and submits a `V1AccessControlEntry` to the
//! controller. It returns one result per binding.
//!
//! This file keeps the whole-request cluster gate and the per-binding loop.
//! Binding validation lives in `validate`, the result rows and the encoder in
//! `response`, and the audit trail in `audit`.

use bytes::Bytes;
use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::{
    create_acls_request::CreateAclsRequest, create_acls_response::AclCreationResult,
};
use krabka_units::convert::ByteSizeExt as _;

mod audit;
mod response;
mod validate;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    audit::{audit_created_acls, created_acl_resources},
    response::{acl_error_result, apply_submit_error, create_acls_response, encode_response},
    validate::validate,
};
use super::acl_wire::CLUSTER_RESOURCE_NAME;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

#[tracing::instrument(
    name = "handle_create_acls",
    level = "info",
    skip_all,
    fields(api = "CreateAcls"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: CreateAclsRequest,
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
        let results = req
            .creations
            .iter()
            .map(|_| acl_error_result(codes::CLUSTER_AUTHORIZATION_FAILED, "create-acls denied"))
            .collect();
        return encode_response(&create_acls_response(results), api_version);
    }

    let mut results: Vec<AclCreationResult> = Vec::with_capacity(req.creations.len());
    let mut to_submit: Vec<(usize, MetadataRecord)> = Vec::with_capacity(req.creations.len());

    for c in &req.creations {
        match validate(
            c,
            broker.config.acl_max_principal.bytes_usize(),
            broker.config.acl_max_resource_name.bytes_usize(),
        ) {
            Ok(entry) => {
                let idx = results.len();
                results.push(AclCreationResult::default());
                to_submit.push((idx, MetadataRecord::V1AccessControlEntry(entry)));
            }
            Err((code, msg)) => {
                results.push(acl_error_result(code, msg));
            }
        }
    }

    if !to_submit.is_empty() {
        let records: Vec<MetadataRecord> = to_submit.iter().map(|(_, r)| r.clone()).collect();
        if let Err(e) = broker.controller.submit_change(records).await {
            tracing::warn!(error = %e, "create-acls submit failed");
            apply_submit_error(&mut results, &to_submit, &e);
        }
    }

    // Audit: emit one AdminOperation record for successfully-created ACLs.
    // `to_submit` carries (result_idx, record) for every creation that passed
    // validation; entries whose result slot still has error_code == 0 were committed.
    audit_created_acls(
        broker.audit_log.as_ref(),
        ctx,
        created_acl_resources(&req, &results, &to_submit),
    );

    encode_response(&create_acls_response(results), api_version)
}
