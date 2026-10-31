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

use {
    crate::{error::BrokerError, handlers::ApiVersion},
    krabka_protocol::owned::{
        update_features_request::UpdateFeaturesRequest,
        update_features_response::UpdateFeaturesResponse,
    },
    krabka_raft::RaftError,
};

mod java_order;
mod preconditions;
mod response;
mod upgrade_type;
mod validate;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        response::{feature_error, success, top_level_error},
        validate::{UpdateError, plan_updates},
    },
    crate::{
        broker::Broker,
        codes,
        handlers::{
            RequestContext, audit_admin_success, audit_resource, cluster_alter_denied,
            forward_to_controller::{
                ends_guarded_attempt, guarded_operation_on_source, to_active_controller_or_handle,
            },
        },
        metadata_source::MetadataSource,
    },
    krabka_protocol::owned::update_features_request,
    std::{
        sync::{Mutex, PoisonError},
        time::Duration,
    },
    tokio::time::Instant,
};

/// `Errors.REQUEST_TIMED_OUT.message()`, the answer of a write the controller
/// did not admit before the deadline.
const REQUEST_TIMED_OUT_MESSAGE: &str = "The request timed out.";

/// `NOT_CONTROLLER`'s answer when the write reaches a node that has lost the
/// quorum leadership.
const NOT_CONTROLLER_MESSAGE: &str = "This broker is not the active controller.";

/// The answer that refuses a `kraft.version` upgrade the Raft layer would not
/// take, or `None` when it would. The real upgrade and its `validate_only`
/// dry run share this mapping, so a dry run reports what the request would.
fn kraft_upgrade_refusal(
    outcome: Result<krabka_raft::ReconfigOutcome, RaftError>,
) -> Option<UpdateFeaturesResponse> {
    match outcome {
        Ok(krabka_raft::ReconfigOutcome::Committed) => None,
        Ok(krabka_raft::ReconfigOutcome::NotLeader { .. })
        | Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => Some(top_level_error(
            codes::NOT_CONTROLLER,
            NOT_CONTROLLER_MESSAGE,
        )),
        // `LeaderState.maybeAppendUpgradedKRaftVersion` refuses with an
        // `InvalidUpdateVersionException`, which Kafka answers as the failed
        // feature's error.
        Err(
            error @ (RaftError::InvalidVoterUpdate(_)
            | RaftError::UnsupportedKraftVersion(_)
            | RaftError::ReconfigInProgress
            | RaftError::ReconfigRejected(_)),
        ) => Some(feature_error(&UpdateError {
            code: codes::INVALID_UPDATE_VERSION,
            message: error.to_string(),
        })),
        Err(error) => {
            tracing::warn!(%error, "UpdateFeatures: kraft.version activation failed");
            Some(top_level_error(
                codes::FEATURE_UPDATE_FAILED,
                "Failed to activate kraft.version.",
            ))
        }
    }
}

/// The `typed` dispatch entry point: [`answer`], which the generated adapter
/// encodes.
#[tracing::instrument(
    name = "handle_update_features",
    level = "info",
    skip_all,
    fields(api = "UpdateFeatures", version)
)]
pub async fn handle(
    broker: &Broker,
    req: UpdateFeaturesRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<UpdateFeaturesResponse, BrokerError> {
    // Kafka's `KafkaApis.handleUpdateFeatures` forwards the request to the
    // active controller, which validates and writes it against its own image.
    to_active_controller_or_handle(
        broker,
        req,
        update_features_request::API_KEY,
        version,
        ctx,
        request_refusal,
        |req| async move { Ok(answer(broker, req, version, ctx).await) },
    )
    .await
}

/// The answer `ForwardingManagerImpl` makes up when the controller gives none.
fn request_refusal(
    _req: &UpdateFeaturesRequest,
    code: i16,
    message: Option<&str>,
) -> UpdateFeaturesResponse {
    UpdateFeaturesResponse {
        error_code: code,
        error_message: message.map(str::to_owned),
        ..Default::default()
    }
}

/// Runs the `UpdateFeatures` request at `version` and returns the typed
/// response.
pub async fn answer(
    broker: &Broker,
    req: UpdateFeaturesRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> UpdateFeaturesResponse {
    let image = broker.controller.current_image();

    // Whole-request Cluster:Alter gate.
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return top_level_error(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        );
    }

    let local_controller = preconditions::LocalController {
        node_id: broker.config.node_id,
        unstable_features: broker.config.features.unstable_feature_versions,
    };
    let features = match apply_updates(
        broker.controller.as_ref(),
        &req,
        local_controller,
        broker.config.default_min_insync_replicas,
    )
    .await
    {
        Ok(Some(features)) => features,
        Ok(None) => return success(&req, version),
        Err(refusal) => return refusal,
    };

    audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "UpdateFeatures",
        features
            .into_iter()
            .map(|feature| audit_resource("Feature", feature))
            .collect(),
    );

    success(&req, version)
}

/// How long one request may wait for the controller to admit its write,
/// replanning after every intervening commit.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Validates `req` against the metadata image of `source` and writes it.
///
/// Kafka's `QuorumController.updateFeatures` validates the request and
/// generates its records on the controller event thread, so nothing commits
/// between the two. Here the plan and the append run in
/// [`guarded_operation_on_source`]: an append whose image is no longer
/// current is refused, and the request is validated and planned again on the
/// newer image.
///
/// A `kraft.version` upgrade is finalized before the feature records are
/// appended, as `FeatureControlManager.updateFeature` asks the `KRaft` layer to
/// upgrade before it returns the records. A request whose append then fails
/// leaves `kraft.version` upgraded, as it does in Kafka. A retry never
/// finalizes the same level twice.
///
/// Returns the features to audit, `None` for a `validate_only` request that
/// passed, or the response that refuses the request.
async fn apply_updates(
    source: &dyn MetadataSource,
    req: &UpdateFeaturesRequest,
    local_controller: preconditions::LocalController,
    default_min_insync_replicas: i32,
) -> Result<Option<Vec<String>>, UpdateFeaturesResponse> {
    let finalized_kraft = Mutex::new(None);
    let finalized_kraft = &finalized_kraft;
    let result = guarded_operation_on_source(source, Instant::now() + SUBMIT_TIMEOUT, |image| {
        async move {
            let mut plan = match plan_updates(req, &image, local_controller) {
                Ok(plan) => plan,
                Err(error) => return Ok(Err(feature_error(&error))),
            };
            if req.validate_only {
                // Kafka's `FeatureControlManager.updateFeature` asks the KRaft
                // layer to upgrade `kraft.version` with `validateOnly` set, and
                // `LeaderState.maybeAppendUpgradedKRaftVersion` runs every one
                // of its checks and skips only the append.
                if let Some(level) = plan.kraft_upgrade
                    && let Some(refusal) =
                        kraft_upgrade_refusal(source.validate_kraft_version(level).await)
                {
                    return Ok(Err(refusal));
                }
                return Ok(Ok(None));
            }
            // KIP-966: turning ELR on writes its safety config records in the
            // same batch, ahead of the feature record, as Kafka's
            // `ConfigurationControlManager.updateFeatures` does.
            if plan.enables_elr {
                let mut batch = validate::elr_safety_records(&image, default_min_insync_replicas);
                batch.append(&mut plan.records);
                plan.records = batch;
            }

            let already_finalized = *finalized_kraft
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(level) = plan.kraft_upgrade
                && already_finalized != Some(level)
            {
                if let Some(refusal) =
                    kraft_upgrade_refusal(source.finalize_kraft_version(level).await)
                {
                    return Ok(Err(refusal));
                }
                *finalized_kraft
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(level);
            }

            if !plan.records.is_empty() {
                match source.submit_change_if_image(plan.records, image).await {
                    Ok(_) => {}
                    Err(error) if ends_guarded_attempt(&error) => return Err(error),
                    Err(error) => {
                        tracing::warn!(%error, "UpdateFeatures: submit_change failed");
                        return Ok(Err(top_level_error(
                            codes::FEATURE_UPDATE_FAILED,
                            "Failed to persist the feature update.",
                        )));
                    }
                }
            }
            Ok(Ok(Some(plan.features)))
        }
    })
    .await;
    match result {
        Ok(Ok(outcome)) => outcome,
        Err(_) => Err(top_level_error(
            codes::REQUEST_TIMED_OUT,
            REQUEST_TIMED_OUT_MESSAGE,
        )),
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => Err(top_level_error(
            codes::NOT_CONTROLLER,
            NOT_CONTROLLER_MESSAGE,
        )),
        Ok(Err(error)) => {
            tracing::warn!(%error, "UpdateFeatures: submit_change failed");
            Err(top_level_error(
                codes::FEATURE_UPDATE_FAILED,
                "Failed to persist the feature update.",
            ))
        }
    }
}
