//! KIP-48 owner resolution: deciding whether a `CreateDelegationToken` call
//! mints a token for the caller or, on the privileged act-as path, for another
//! principal.
//!
//! The rules that gate act-as are a policy decision rather than a wire detail,
//! so super-user membership, the `User`-only owner type, and the
//! all-or-nothing pairing of the two wire fields live here instead of inside
//! the request flow.

use std::{collections::HashSet, hash::BuildHasher};

use krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest;
use krabka_security::{KafkaPrincipal, Principal};

use crate::codes;

/// The only `KafkaPrincipal` type that the broker supports as an act-as token
/// owner. It is Kafka's `KafkaPrincipal.USER_TYPE`. mTLS-DN owners are not
/// supported.
const USER_PRINCIPAL_TYPE: &str = "User";

/// Wire convention: the JVM admin client serializes "not act-as" in two ways.
/// It omits the compact-nullable string, which gives `None`, or it sends an
/// empty string. Treat both as absent, so that the act-as branch runs only
/// when the caller supplied a principal.
fn is_empty_owner_field(f: Option<&str>) -> bool {
    f.is_none_or(str::is_empty)
}

/// Resolves the owner of the token this request mints.
///
/// The wire `owner_principal_type`/`owner_principal_name` pair drives the
/// privileged "act-as" path: super-users may mint tokens owned by *other*
/// principals so an operator can pre-mint tokens for `KafkaUsers` without
/// first holding their credentials.
///
/// On refusal this returns the Kafka error code the response must carry: 65
/// (`DELEGATION_TOKEN_AUTHORIZATION_FAILED`) for a non-super-user attempting
/// act-as, and 42 (`INVALID_REQUEST`) for a malformed owner pair.
pub(super) fn resolve_owner<S: BuildHasher>(
    req: &CreateDelegationTokenRequest,
    principal: &Principal,
    super_users: &HashSet<String, S>,
) -> Result<KafkaPrincipal, i16> {
    let owner_type_empty = is_empty_owner_field(req.owner_principal_type.as_deref());
    let owner_name_empty = is_empty_owner_field(req.owner_principal_name.as_deref());
    match (owner_type_empty, owner_name_empty) {
        (true, true) => Ok(principal.to_kafka()),
        (false, false) => {
            // Both set → act-as. Only super-users may use this path; the
            // permission is broker-wide because no token exists yet to
            // hang an ACL on.
            if !super_users.contains(&principal.name) {
                return Err(codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
            }
            let owner_type = req.owner_principal_type.as_deref().unwrap_or_default();
            let owner_name = req.owner_principal_name.as_deref().unwrap_or_default();
            // The act-as owner type is restricted to `User`
            // (mTLS-DN owners are not supported). Match Kafka's behavior of
            // returning INVALID_REQUEST for unsupported types here
            // rather than authorization-failed — the request is
            // syntactically wrong, not unauthorized.
            if owner_type != USER_PRINCIPAL_TYPE {
                return Err(codes::INVALID_REQUEST);
            }
            Ok(KafkaPrincipal {
                principal_type: owner_type.to_string(),
                name: owner_name.to_string(),
            })
        }
        // Exactly one set → caller is confused; either both or neither.
        _ => Err(codes::INVALID_REQUEST),
    }
}
