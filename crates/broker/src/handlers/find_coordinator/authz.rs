//! Per-key authorization for `FindCoordinator`, and the denied-key row Kafka
//! puts in the response.
//!
//! Authorization is decided one coordinator key at a time rather than once per
//! request, so a denied group id in a multi-key v4+ request leaves the
//! authorized keys resolving normally in the same response. This module holds
//! that decision and the partition of the requested keys into denied rows and
//! still-to-resolve keys.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::find_coordinator_response::Coordinator;

use super::{KEY_TYPE_GROUP, KEY_TYPE_TRANSACTION};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// A per-key authorization-failed `Coordinator` entry.
///
/// Kafka stamps the denied key's row with the authorization-failed code.
/// Authorized keys resolve as normal.
fn denied_coordinator(key: String, error_code: i16) -> Coordinator {
    Coordinator {
        key,
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code,
        error_message: Some("authorization failed".into()),
        ..Default::default()
    }
}

/// Authorize a single `FindCoordinator` key against its key-type ACL.
///
/// GROUP needs `Describe` on `Group(key)`. TRANSACTION needs `Describe` on
/// `TransactionalId(key)`. The function returns the authorization-failed code to
/// stamp on Deny. It returns `None` when the ACL allows the key, and also for
/// key-types this handler does not gate, such as SHARE and unknown types.
fn key_authz_failure(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    host: &std::net::SocketAddr,
    key_type: i8,
    key: &str,
) -> Option<i16> {
    let (resource_type, failure_code) = match key_type {
        KEY_TYPE_GROUP => (ResourceType::Group, codes::GROUP_AUTHORIZATION_FAILED),
        KEY_TYPE_TRANSACTION => (
            ResourceType::TransactionalId,
            codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        ),
        _ => return None,
    };
    let allow = authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type,
            resource_name: key,
            operation: AclOperation::Describe,
        },
    );
    (allow == AuthorizationResult::Deny).then_some(failure_code)
}

pub(super) fn authorize_keys(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    key_type: i8,
    keys: Vec<String>,
) -> (Vec<Coordinator>, Vec<String>) {
    let mut denied = Vec::new();
    let mut allowed = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(code) = key_authz_failure(
            broker.config.authorizer.as_ref(),
            image,
            context.principal,
            context.peer,
            key_type,
            &key,
        ) {
            denied.push(denied_coordinator(key, code));
        } else {
            allowed.push(key);
        }
    }
    (denied, allowed)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn deny_authorizer() -> crate::authorizer::SimpleAclAuthorizer {
        crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new())
    }

    fn anon() -> krabka_security::Principal {
        krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        }
    }

    #[test]
    fn group_key_denied_maps_to_group_authorization_failed() {
        let authz = deny_authorizer();
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = key_authz_failure(&authz, &image, &anon(), &peer, KEY_TYPE_GROUP, "g");
        assert!(code == Some(codes::GROUP_AUTHORIZATION_FAILED));
    }

    #[test]
    fn txn_key_denied_maps_to_transactional_id_authorization_failed() {
        let authz = deny_authorizer();
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = key_authz_failure(&authz, &image, &anon(), &peer, KEY_TYPE_TRANSACTION, "t");
        assert!(code == Some(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
    }

    #[test]
    fn denied_entry_carries_the_failure_code() {
        let c = denied_coordinator("g".into(), codes::GROUP_AUTHORIZATION_FAILED);
        assert!(c.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(c.node_id == -1);
    }
}
