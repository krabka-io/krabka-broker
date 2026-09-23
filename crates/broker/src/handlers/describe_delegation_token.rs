//! KIP-48: `DescribeDelegationToken` (`api_key` 41).
//!
//! Matches Kafka trunk's `KafkaApis.handleDescribeTokensRequest`,
//! `allowTokenRequests`, and `DelegationTokenManager.filterToken`:
//!
//!   - `allowTokenRequests` runs first and unconditionally refuses a
//!     delegation-token-authenticated caller with
//!     `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64), before the
//!     `DELEGATION_TOKEN_AUTH_DISABLED` (61) check even runs. A session
//!     minted from one token must never be able to describe any token,
//!     including its own siblings' HMACs — there is no token-authed
//!     "describe my own tokens" carve-out.
//!   - A present-but-empty `owners` list (`ownersListEmpty`) means "show
//!     nothing", distinct from a missing (null) list, which means "no
//!     filter".
//!   - `filterToken` keeps a token only when the (possibly absent) owner
//!     filter matches it — matching a filter entry against the token's
//!     owner OR a listed renewer, not the owner alone — AND the caller is
//!     that token's owner, a listed renewer, or holds a `Describe` ACL on
//!     `DelegationToken:<token_id>`. The ACL resource name is the token's
//!     own id, matching what Kafka's tooling (and its own authorizer call)
//!     writes; it is not the owner's principal string, which would let one
//!     ACL grant every token of that owner instead of just this one.
//!
//! [`krabka_verified::delegation_token::token_api_admission`] and
//! [`token_describe_visible`] are the two verified kernels this handler
//! drives; see their doc comments for the exact contracts.

use std::net::SocketAddr;

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    describe_delegation_token_request::DescribeDelegationTokenRequest,
    describe_delegation_token_response::DescribeDelegationTokenResponse,
};
use krabka_security::{KafkaPrincipal, SecretBytes};
use krabka_verified::delegation_token::{TokenApi, TokenApiAdmission, token_describe_visible};

mod response;

#[cfg(test)]
mod tests;

use self::response::{describe_token, empty_response, err_response};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    network::auth::ConnectionAuth,
};

// `async` matches the call-site shape used by every other
// `crate::handlers::*::handle`; today the body is purely synchronous.
#[tracing::instrument(
    name = "handle_describe_delegation_token",
    level = "info",
    skip_all,
    fields(api = "DescribeDelegationToken")
)]
pub(crate) fn handle(
    req: &DescribeDelegationTokenRequest,
    auth: &ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    controller: &dyn crate::metadata_source::MetadataSource,
    peer: &SocketAddr,
    authorizer: &dyn Authorizer,
) -> DescribeDelegationTokenResponse {
    // `allowTokenRequests`: refuses an unauthenticated OR
    // delegation-token-authenticated caller before the auth-disabled check
    // even runs. This is what keeps a token-authed caller from ever
    // reaching the per-token filter below and reading a sibling token's
    // HMAC.
    if auth.token_api_admission(TokenApi::Describe) == TokenApiAdmission::Reject {
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    }
    if secret_key.is_none() {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    };
    let caller = principal.to_kafka();

    // `ownersListEmpty`: a present but empty `owners` list means "no
    // tokens are visible", distinct from a missing (null) list, which
    // means "no filter". Only the null case reaches the per-token filter.
    if matches!(&req.owners, Some(list) if list.is_empty()) {
        return empty_response();
    }
    let candidate_owners: Option<Vec<KafkaPrincipal>> = req.owners.as_ref().map(|list| {
        list.iter()
            .map(|o| KafkaPrincipal {
                principal_type: o.principal_type.clone(),
                name: o.principal_name.clone(),
            })
            .collect()
    });

    let image = controller.current_image();

    // Build the visible-token set through the verified per-token predicate.
    let tokens: Vec<krabka_metadata::DelegationToken> = image
        .all_delegation_tokens()
        .filter(|t| {
            // Kafka's `TokenInformation.ownerOrRenewer`: a filter entry
            // matches a token by being its owner OR a listed renewer, not
            // only its owner.
            let owner_filter_matches = candidate_owners.as_ref().is_none_or(|owners| {
                owners
                    .iter()
                    .any(|o| t.owner == *o || t.renewers.contains(o))
            });
            let caller_is_owner = t.owner == caller;
            let caller_is_renewer = t.renewers.contains(&caller);
            // The resource name is the token's own id (Kafka:
            // `authHelper.authorize(..., DESCRIBE, DELEGATION_TOKEN, tokenId)`),
            // so a `Describe` ACL grants exactly one token, not every token
            // of its owner. Skipped when the owner filter already excludes
            // the token, since `token_describe_visible` ANDs it in anyway.
            let acl_allows = owner_filter_matches
                && authorizer.authorize(
                    &*image,
                    &AuthorizationRequest {
                        principal,
                        host: peer,
                        resource_type: ResourceType::DelegationToken,
                        resource_name: &t.token_id,
                        operation: AclOperation::Describe,
                    },
                ) == AuthorizationResult::Allow;
            token_describe_visible(
                owner_filter_matches,
                caller_is_owner,
                caller_is_renewer,
                acl_allows,
            )
        })
        .cloned()
        .collect();

    DescribeDelegationTokenResponse {
        error_code: 0,
        tokens: tokens.into_iter().map(describe_token).collect(),
        throttle_time_ms: 0,
        ..Default::default()
    }
}
