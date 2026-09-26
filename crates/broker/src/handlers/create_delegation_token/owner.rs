//! Owner resolution: which principal owns the token that a
//! `CreateDelegationToken` call mints, and whether the requester may mint for
//! that principal.

use std::{collections::HashSet, hash::BuildHasher};

use krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest;
use krabka_security::KafkaPrincipal;

/// The owner of the token this request mints.
///
/// Matches `KafkaApis.handleCreateTokenRequest` and
/// `DelegationTokenControlManager.createDelegationToken` in Kafka trunk: a
/// null or empty `owner_principal_name` means the requester, and any other
/// name is taken with whatever `owner_principal_type` the request carries.
pub(super) fn resolve_owner(
    req: &CreateDelegationTokenRequest,
    requester: &KafkaPrincipal,
) -> KafkaPrincipal {
    match req.owner_principal_name.as_deref() {
        None | Some("") => requester.clone(),
        Some(name) => KafkaPrincipal {
            principal_type: req.owner_principal_type.clone().unwrap_or_default(),
            name: name.to_string(),
        },
    }
}

/// Whether `requester` may mint a token owned by `owner`.
///
/// Kafka needs no ACL when the owner is the requester, and otherwise
/// authorizes `CreateTokens` on the `User:<owner>` resource. The ACL model
/// here has neither the `User` resource type nor the `CreateTokens`
/// operation, so the only grant that can be expressed is the one every Kafka
/// authorizer gives unconditionally: a configured super user.
pub(super) fn may_create_for<S: BuildHasher>(
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
    super_users: &HashSet<String, S>,
) -> bool {
    owner == requester || super_users.contains(&requester.name)
}
