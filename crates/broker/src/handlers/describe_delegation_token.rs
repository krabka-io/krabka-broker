//! KIP-48: `DescribeDelegationToken` (`api_key` 41).
//!
//! Per spec §1.5, SASL-authenticated callers can list tokens visible
//! to them. The filtering rules are:
//!   - Token-authed callers see only their own owned tokens. An owner
//!     filter does not change that. This is KIP-48 isolation, and the
//!     ACL extension below does NOT apply to token-authed callers.
//!   - With an explicit non-empty `owners` filter: tokens whose owner
//!     matches one of the entries AND that the caller can see. The
//!     caller can see a token as its owner, as a listed renewer, OR
//!     with a Describe-ACL on `TOKEN:<owner>`.
//!   - With no `owners` filter, or an empty or null one: every token
//!     where the caller is the owner, is a listed renewer, or holds the
//!     `Describe` ACL on `TOKEN:<owner>`.
//!
//! ACL-based visibility (spec §5.3): when a token's owner has granted
//! `Describe` on `TOKEN:<owner_principal_string>` to the calling
//! principal, the handler puts that token in the visible set even if
//! the caller is not the owner or a renewer.
//!
//! The explicit [`crate::authorizer::Authorizer`] trait governs token
//! visibility on its own. The "no super-users + no ACLs ⇒ Allow"
//! behavior lives in [`crate::authorizer::AllowAllAuthorizer`], which is
//! the documented "allow everything" mode. So showing every token under
//! `AllowAll` is the correct behavior, because that is what the operator
//! asked for. With `SimpleAcl` or `Opa` configured, the authorizer
//! returns Deny for callers without a `Describe`-on-`TOKEN:<owner>` ACL.
//! That filters the visible set.

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

use self::response::{describe_token, err_response};
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
    if secret_key.is_none() {
        return err_response(crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    }
    if auth.token_api_admission(TokenApi::Describe) == TokenApiAdmission::Reject {
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    }
    let ConnectionAuth::Authenticated {
        principal,
        authenticated_via_token,
        ..
    } = auth
    else {
        return err_response(crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    };
    let caller = principal.to_kafka();

    let image = controller.current_image();

    // Optional owner filter: present + non-empty selects tokens whose
    // owner matches one of the entries. A missing or empty list means
    // "no filter".
    let candidate_owners: Option<Vec<KafkaPrincipal>> = match &req.owners {
        Some(list) if !list.is_empty() => Some(
            list.iter()
                .map(|o| KafkaPrincipal {
                    principal_type: o.principal_type.clone(),
                    name: o.principal_name.clone(),
                })
                .collect(),
        ),
        _ => None,
    };

    // Build the visible-token set through the verified per-token predicate.
    let tokens: Vec<krabka_metadata::DelegationToken> = image
        .all_delegation_tokens()
        .filter(|t| {
            let owner_filter_matches = candidate_owners
                .as_ref()
                .is_none_or(|owners| owners.contains(&t.owner));
            let caller_is_owner = t.owner == caller;
            let caller_is_renewer = t.renewers.contains(&caller);
            let acl_allows = !*authenticated_via_token
                && owner_filter_matches
                && authorizer.authorize(
                    &*image,
                    &AuthorizationRequest {
                        principal,
                        host: peer,
                        resource_type: ResourceType::DelegationToken,
                        resource_name: &t.owner.to_string(),
                        operation: AclOperation::Describe,
                    },
                ) == AuthorizationResult::Allow;
            token_describe_visible(
                *authenticated_via_token,
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
