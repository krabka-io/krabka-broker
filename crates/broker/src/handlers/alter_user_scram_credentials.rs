//! `AlterUserScramCredentials` handler (`api_key` 51, KIP-554).
//!
//! KIP-554 puts PBKDF2 on the *client* side. The wire request carries the
//! already-stretched PBKDF2 output as `salted_password`. The broker derives
//! `stored_key` and `server_key` from the supplied bytes, and never sees the
//! user's plaintext password.
//!
//! The handler validates each upsertion on its own:
//!
//! - `iterations >= 4096`, or else `UNACCEPTABLE_CREDENTIAL` (93).
//! - `iterations <= 16384`, or else `UNACCEPTABLE_CREDENTIAL`.
//! - An unknown mechanism wire value gives `UNSUPPORTED_SASL_MECHANISM` (33).
//!
//! Authorization needs `Alter` on `Cluster("kafka-cluster")`. On Deny, every
//! per-user result is `CLUSTER_AUTHORIZATION_FAILED` (31). When `super_users`
//! is configured, the authorizer's super-user bypass returns ALLOW from inside
//! `authorize`.
//!
//! Duplicate detection preserves Kafka's first per-user validation/resource
//! error. If the first alteration for a user has already recorded an error,
//! later alterations for that user are ignored and the original error remains.
//! `DUPLICATE_RESOURCE` (92) is returned only when the prior same-user
//! alteration was otherwise valid and pending in the request. An empty username
//! is always an `UNACCEPTABLE_CREDENTIAL` (93) validation error unless the
//! whole request is denied by authorization first.
//!
//! Deletion targets that are not present in the current metadata image get
//! `RESOURCE_NOT_FOUND` (91).
//!
//! On a successful submit, the handler emits one `V1ScramCredential` or
//! `V1DeleteScramCredential` record for each accepted row, through
//! `controller.submit_change`. One batched commit keeps the metadata image
//! consistent across several rows in the same request.

use krabka_metadata::AclOperation;
use krabka_protocol::owned::{
    alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
    alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
};

mod plan;
mod records;
mod response;
mod validation;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    plan::{AlterationPlan, distinct_requested_users, plan_alterations},
    response::{apply_submit_error, err_result},
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// Runs the `AlterUserScramCredentials` request and returns the typed
/// response. The caller, `dispatch.rs`, encodes the response on the wire and
/// prepends the response header.
#[tracing::instrument(
    name = "handle_alter_user_scram_credentials",
    level = "info",
    skip_all,
    fields(api = "AlterUserScramCredentials")
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterUserScramCredentialsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
) -> AlterUserScramCredentialsResponse {
    // ── ACL preamble ────────────────────────────────────────
    // Whole-request Cluster Alter gate. On Deny, every per-user row
    // reports CLUSTER_AUTHORIZATION_FAILED. The authorizer's super-user
    // bypass short-circuits inside `authorize` → ALLOW when `super_users`
    // is configured.
    let image = broker.controller.current_image();
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
        return AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: distinct_requested_users(&req)
                .into_iter()
                .map(|user| err_result(user, codes::CLUSTER_AUTHORIZATION_FAILED, "not super-user"))
                .collect(),
            ..Default::default()
        };
    }

    // KIP-554/KIP-778: KRaft SCRAM requires metadata.version >= 3.5-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        krabka_metadata::metadata_version::SCRAM_MIN_LEVEL,
    )
    .is_err()
    {
        let msg = "SCRAM is not enabled at the cluster's metadata.version.";
        return AlterUserScramCredentialsResponse {
            throttle_time_ms: 0,
            results: distinct_requested_users(&req)
                .into_iter()
                .map(|user| err_result(user, codes::UNSUPPORTED_VERSION, msg))
                .collect(),
            ..Default::default()
        };
    }

    let AlterationPlan {
        mut user_results,
        records,
    } = plan_alterations(broker, req, authorized);

    // Submit accepted records as a single batch. A submit failure converts
    // every pending "ok" row to a generic error (per-row errors already in
    // `user_results` keep their existing codes).
    if !records.is_empty()
        && let Err(e) = broker.controller.submit_change(records).await
    {
        tracing::warn!(error = %e, "AlterUserScramCredentials: submit_change failed");
        let msg = format!("submit failed: {e}");
        apply_submit_error(&mut user_results, &msg);
    }

    // KIP-554 redaction: the request carries the client-side PBKDF2 output and
    // the broker derives `stored_key` and `server_key` from it. None of the
    // three reaches the audit record — only the user whose credential changed.
    crate::handlers::audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "AlterUserScramCredentials",
        user_results
            .iter()
            .filter(|result| result.error_code == codes::NONE)
            .map(|result| crate::handlers::audit_resource("User", result.user.clone()))
            .collect(),
    );

    AlterUserScramCredentialsResponse {
        results: user_results,
        ..Default::default()
    }
}
