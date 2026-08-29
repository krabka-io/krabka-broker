//! `AlterClientQuotas` (`api_key` 49, KIP-13/124/257).
//!
//! This file holds the wire entry point: the cluster `Alter` authorization
//! preamble, the loop that validates each entry, and the single metadata
//! submit that carries every accepted entry. Entry validation and the records
//! it produces live in `entries`; the response rows live in `response`.

use bytes::Bytes;
use krabka_metadata::{AclOperation, MetadataRecord, ResourceType};
use krabka_protocol::{
    Encode, UnknownTaggedFields,
    owned::{
        alter_client_quotas_request::AlterClientQuotasRequest,
        alter_client_quotas_response::AlterClientQuotasResponse,
    },
};

mod entries;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::entries::process_one_entry;
use self::response::{apply_submit_error, encode_whole_request_error, err_entry, ok_entry};
use super::acl_wire::CLUSTER_RESOURCE_NAME;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::CLUSTER_AUTHORIZATION_FAILED,
};

#[tracing::instrument(
    name = "handle_alter_client_quotas",
    level = "info",
    skip_all,
    fields(api = "AlterClientQuotas"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterClientQuotasRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "alter-client-quotas denied",
            api_version,
        );
    }

    let mut entry_results = Vec::with_capacity(req.entries.len());
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    for entry in &req.entries {
        match process_one_entry(entry) {
            Ok(records) => {
                if !req.validate_only {
                    to_submit.extend(records);
                }
                entry_results.push(ok_entry(&entry.entity));
            }
            Err((code, msg)) => entry_results.push(err_entry(&entry.entity, code, msg)),
        }
    }

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "alter-client-quotas submit failed");
        apply_submit_error(&mut entry_results, e);
    }

    let resp = AlterClientQuotasResponse {
        throttle_time_ms: 0,
        entries: entry_results,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode AlterClientQuotas")
}
