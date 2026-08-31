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
use krabka_verified::delegation_token::{TokenApi, TokenApiAdmission};

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

    // Build the visible-token set per the rules above.
    let tokens: Vec<krabka_metadata::DelegationToken> = if *authenticated_via_token {
        // KIP-48: a token-authed caller is restricted to tokens they
        // own. The wire owner filter is intentionally ignored, and the
        // ACL extension below does NOT apply to token-authed callers.
        image
            .delegation_tokens_by_owner(&caller)
            .into_iter()
            .cloned()
            .collect()
    } else {
        // Step 1: tokens visible via owner / renewer (and the optional
        // owner filter, if present).
        let base: Vec<&krabka_metadata::DelegationToken> = if let Some(owners) = &candidate_owners {
            image
                .all_delegation_tokens()
                .filter(|t| {
                    owners.contains(&t.owner) && (t.owner == caller || t.renewers.contains(&caller))
                })
                .collect()
        } else {
            image.delegation_tokens_visible_to(&caller)
        };

        // Step 2 (spec §5.3): extend with tokens whose owner has
        // granted `Describe` on `TOKEN:<owner_principal_string>` to
        // the calling principal. Apply the same owner filter if one
        // was supplied so the filter remains authoritative.
        //
        // We consult the authorizer for every candidate
        // token. With `AllowAllAuthorizer` every token surfaces (which
        // is correct: the operator opted into "allow everything"), but
        // dedup-by-token_id below means the base owner/renewer set
        // already covers anything the caller would see anyway. With
        // `SimpleAclAuthorizer` (no matching ACL ⇒ default-deny) or
        // `OpaAuthorizer` (policy decides), the extension contributes
        // only tokens the caller is explicitly authorized to Describe.
        let acl_extra: Vec<&krabka_metadata::DelegationToken> = image
            .all_delegation_tokens()
            .filter(|t| match &candidate_owners {
                Some(owners) => owners.contains(&t.owner),
                None => true,
            })
            .filter(|t| {
                let resource = t.owner.to_string();
                authorizer.authorize(
                    &*image,
                    &AuthorizationRequest {
                        principal,
                        host: peer,
                        resource_type: ResourceType::DelegationToken,
                        resource_name: &resource,
                        operation: AclOperation::Describe,
                    },
                ) == AuthorizationResult::Allow
            })
            .collect();

        // Merge + dedup by token_id. Order is unspecified (matches the
        // existing `delegation_tokens_*` accessor contracts).
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut merged: Vec<krabka_metadata::DelegationToken> = Vec::new();
        for t in base.into_iter().chain(acl_extra) {
            if seen.insert(t.token_id.as_str()) {
                merged.push(t.clone());
            }
        }
        merged
    };

    DescribeDelegationTokenResponse {
        error_code: 0,
        tokens: tokens.into_iter().map(describe_token).collect(),
        throttle_time_ms: 0,
        ..Default::default()
    }
}
