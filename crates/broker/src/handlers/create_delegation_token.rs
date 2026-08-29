//! KIP-48: `CreateDelegationToken` (`api_key` 38).
//!
//! Per spec §1.2, which includes act-as, the caller must be
//! SASL-authenticated, and the caller must NOT itself be authenticated with a
//! delegation token. KIP-48 forbids chains where a token creates a token.
//!
//! Owner resolution works as follows:
//!
//! - When both `owner_principal_type` and `owner_principal_name` are empty or
//!   absent, the owner is the caller. This is a self-mint.
//! - When both are present and non-empty, the caller must be a configured
//!   super-user, per `broker.config.super_users`, and the owner becomes the
//!   `KafkaPrincipal` from the wire. The type is limited to `"User"`, because
//!   mTLS-DN owners are not supported. A caller that is not a super-user gets
//!   `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65).
//! - When exactly one is set, the broker returns `INVALID_REQUEST` (42). A
//!   partial act-as is never valid.
//!
//! The HMAC-SHA-256 of `(secret_key, token_id)` becomes the token's password
//! equivalent. Clients re-authenticate with the hex `token_id` as the SCRAM
//! username and the HMAC bytes as the password.
//!
//! This file holds the request flow itself. The owner matrix lives in
//! `owner`, the lifetime clamp and the deadline arithmetic in `lifetime`, and
//! the two response shapes in `wire`.

use std::{collections::HashSet, hash::BuildHasher};

use krabka_metadata::{DelegationTokenRecord, MetadataRecord};
use krabka_protocol::owned::{
    create_delegation_token_request::CreateDelegationTokenRequest,
    create_delegation_token_response::CreateDelegationTokenResponse,
};
use krabka_security::{KafkaPrincipal, SecretBytes};

use crate::{network::auth::ConnectionAuth, time_util::now_ms};

mod lifetime;
mod owner;
mod wire;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    lifetime::{chosen_lifetime_ms, token_deadlines},
    owner::resolve_owner,
    wire::{err_response, minted_response},
};

/// A relative span of milliseconds, such as a token lifetime or a renew
/// period. It is not an absolute epoch timestamp in milliseconds.
pub(crate) type DurationMs = i64;

#[tracing::instrument(
    name = "handle_create_delegation_token",
    level = "info",
    skip_all,
    fields(api = "CreateDelegationToken")
)]
pub(crate) async fn handle<S: BuildHasher>(
    req: &CreateDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    max_lifetime_ms: DurationMs,
    default_renew_period_ms: DurationMs,
    controller: &dyn crate::metadata_source::MetadataSource,
    super_users: &HashSet<String, S>,
) -> CreateDelegationTokenResponse {
    let Some(secret_key) = secret_key else {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    };

    let ConnectionAuth::Authenticated {
        principal,
        authenticated_via_token,
        ..
    } = auth
    else {
        return err_response(crate::codes::INVALID_REQUEST);
    };
    if *authenticated_via_token {
        // KIP-48: a delegation-token-authed caller cannot create more
        // delegation tokens (no token-creating-token chains).
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    }

    let image = controller.current_image();
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if crate::features::require_feature(
        &image,
        crate::features::METADATA_VERSION,
        krabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return err_response(crate::codes::UNSUPPORTED_VERSION);
    }

    // KIP-48 owner resolution.
    let owner = match resolve_owner(req, principal, super_users) {
        Ok(owner) => owner,
        Err(code) => return err_response(code),
    };

    // Validate + clamp `max_lifetime_ms`.
    let Some(chosen_lifetime) = chosen_lifetime_ms(req.max_lifetime_ms, max_lifetime_ms) else {
        return err_response(crate::codes::INVALID_REQUEST);
    };

    let now = now_ms();
    let token_id = uuid::Uuid::new_v4().to_string();
    let hmac = krabka_security::compute_token_hmac(secret_key.as_bytes(), &token_id);

    let renewers: Vec<KafkaPrincipal> = req
        .renewers
        .iter()
        .map(|r| KafkaPrincipal {
            principal_type: r.principal_type.clone(),
            name: r.principal_name.clone(),
        })
        .collect();

    let deadlines = token_deadlines(now, chosen_lifetime, default_renew_period_ms);

    let record = DelegationTokenRecord {
        token_id: token_id.clone(),
        owner: owner.clone(),
        hmac: hmac.clone(),
        issue_timestamp_ms: now,
        expiry_timestamp_ms: deadlines.initial_expiry_ms,
        max_timestamp_ms: deadlines.max_timestamp_ms,
        renewers,
    };

    if let Err(e) = controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(record)])
        .await
    {
        tracing::warn!(error = %e, "CreateDelegationToken: submit_change failed");
        return err_response(crate::codes::INVALID_REQUEST);
    }

    minted_response(
        &owner,
        principal.to_kafka(),
        now,
        &deadlines,
        token_id,
        hmac,
    )
}
