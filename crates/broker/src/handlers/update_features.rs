//! `UpdateFeatures` handler (`api_key` 57, KIP-584).
//!
//! This handler finalizes the features in the `krabka_metadata` registry
//! through Raft-persisted `V1FeatureLevel` records, and hands a
//! `kraft.version` upgrade to the Raft layer. It applies a request
//! atomically, as Kafka's `FeatureControlManager.updateFeatures` does: the
//! first feature that fails validation fails the whole request and nothing is
//! written. `Alter` on `Cluster("kafka-cluster")` gates it.
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

mod java_order;
mod preconditions;
mod response;
mod upgrade_type;
mod validate;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    response::{feature_error, success, top_level_error},
    validate::{UpdateError, plan_updates},
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// `NOT_CONTROLLER`'s answer when the write reaches a node that has lost the
/// quorum leadership.
const NOT_CONTROLLER_MESSAGE: &str = "This broker is not the active controller.";

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
        );
    }

    let mut plan = match plan_updates(&req, &image, broker.config.node_id) {
        Ok(plan) => plan,
        Err(error) => return feature_error(&error),
    };
    if req.validate_only {
        return success(&req, version);
    }
    // KIP-966: turning ELR on writes its safety config records in the same
    // batch, ahead of the feature record, as Kafka's
    // `ConfigurationControlManager.updateFeatures` does.
    if plan.enables_elr {
        let mut batch =
            validate::elr_safety_records(&image, broker.config.default_min_insync_replicas);
        batch.append(&mut plan.records);
        plan.records = batch;
    }

    if let Some(level) = plan.kraft_upgrade {
        match broker.controller.finalize_kraft_version(level).await {
            Ok(krabka_raft::ReconfigOutcome::Committed) => {}
            Ok(krabka_raft::ReconfigOutcome::NotLeader { .. })
            | Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return top_level_error(codes::NOT_CONTROLLER, NOT_CONTROLLER_MESSAGE);
            }
            // `LeaderState.maybeAppendUpgradedKRaftVersion` refuses with an
            // `InvalidUpdateVersionException`, which Kafka answers as the
            // failed feature's error.
            Err(
                error @ (RaftError::InvalidVoterUpdate(_)
                | RaftError::UnsupportedKraftVersion(_)
                | RaftError::ReconfigInProgress
                | RaftError::ReconfigRejected(_)),
            ) => {
                return feature_error(&UpdateError {
                    code: codes::INVALID_UPDATE_VERSION,
                    message: error.to_string(),
                });
            }
            Err(error) => {
                tracing::warn!(%error, "UpdateFeatures: kraft.version activation failed");
                return top_level_error(
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to activate kraft.version.",
                );
            }
        }
    }

    if !plan.records.is_empty() {
        match broker.controller.submit_change(plan.records).await {
            Ok(_) => {}
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                return top_level_error(codes::NOT_CONTROLLER, NOT_CONTROLLER_MESSAGE);
            }
            Err(e) => {
                tracing::warn!(error = %e, "UpdateFeatures: submit_change failed");
                return top_level_error(
                    codes::FEATURE_UPDATE_FAILED,
                    "Failed to persist the feature update.",
                );
            }
        }
    }

    crate::handlers::audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "UpdateFeatures",
        plan.features
            .into_iter()
            .map(|feature| crate::handlers::audit_resource("Feature", feature))
            .collect(),
    );

    success(&req, version)
}
