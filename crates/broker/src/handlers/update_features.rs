//! `UpdateFeatures` handler (`api_key` 57, KIP-584).
//!
//! This handler finalizes broker-supported features, at present only
//! `metadata.version`, through a Raft-persisted `V1FeatureLevel` record.
//! `Alter` on `Cluster("kafka-cluster")` gates it.
//!
//! `network::dispatch` intercepts the request inline, as it does for
//! `AlterUserScramCredentials`, so the handler receives the authenticated
//! principal and the peer for the ACL check.

use krabka_metadata::AclOperation;
use krabka_protocol::owned::{
    update_features_request::UpdateFeaturesRequest,
    update_features_response::UpdateFeaturesResponse,
};
use krabka_raft::RaftError;

mod preconditions;
mod response;
mod upgrade_type;
mod validate;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    response::{apply_request_wide, finalize, top_level_error},
    validate::validate_updates,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

#[tracing::instrument(
    name = "handle_update_features",
    level = "info",
    skip_all,
    fields(api = "UpdateFeatures", version)
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: UpdateFeaturesRequest,
    version: i16,
    ctx: &crate::handlers::RequestContext<'_>,
) -> UpdateFeaturesResponse {
    let image = broker.controller.current_image();

    // Whole-request Cluster:Alter gate.
    let authorized = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: krabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Alter,
        },
    ) == AuthorizationResult::Allow;

    if !authorized {
        return top_level_error(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
            version,
        );
    }

    if req.feature_updates.is_empty() {
        return top_level_error(
            codes::INVALID_REQUEST,
            "Can not provide empty feature updates in the request.",
            version,
        );
    }

    let (results, records) = validate_updates(&req, &image, version);

    // validate_only: never persist.
    if req.validate_only {
        return finalize(results, version);
    }

    // Activation must be derived from the validated row. Looking at the raw
    // request here would let a duplicate or otherwise rejected kraft.version
    // row activate the Raft feature despite its error response.
    let kraft_upgrade = image.kraft_version() == 0
        && req
            .feature_updates
            .iter()
            .zip(&results)
            .any(|(update, result)| {
                update.feature == krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                    && result.error_code == codes::NONE
            });
    if kraft_upgrade {
        match broker.controller.finalize_kraft_version(1).await {
            Ok(krabka_raft::ReconfigOutcome::Committed) => {}
            Ok(krabka_raft::ReconfigOutcome::NotLeader { .. })
            | Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return apply_request_wide(
                    results,
                    codes::NOT_CONTROLLER,
                    "This broker is not the active controller.",
                    version,
                );
            }
            Err(error) => {
                tracing::warn!(%error, "UpdateFeatures: kraft.version activation failed");
                return apply_request_wide(
                    results,
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to activate kraft.version.",
                    version,
                );
            }
        }
    }

    if !records.is_empty() {
        match broker.controller.submit_change(records).await {
            Ok(_) => {}
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return apply_request_wide(
                    results,
                    codes::NOT_CONTROLLER,
                    "This broker is not the active controller.",
                    version,
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "UpdateFeatures: submit_change failed");
                return apply_request_wide(
                    results,
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to persist the feature update.",
                    version,
                );
            }
        }
    }

    crate::handlers::audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "UpdateFeatures",
        results
            .iter()
            .filter(|result| result.error_code == codes::NONE)
            .map(|result| crate::handlers::audit_resource("Feature", result.feature.clone()))
            .collect(),
    );

    finalize(results, version)
}
