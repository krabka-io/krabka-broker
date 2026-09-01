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
use krabka_verified::broker::{FindCoordinatorAdmission, find_coordinator_admission};

use super::{KEY_TYPE_GROUP, KEY_TYPE_SHARE, KEY_TYPE_TRANSACTION, resolve::parse_share_key};
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

fn invalid_coordinator(key: String) -> Coordinator {
    Coordinator {
        key,
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code: codes::INVALID_REQUEST,
        error_message: Some("invalid coordinator key or key type".into()),
        ..Default::default()
    }
}

pub(super) enum KeySlot {
    Resolve(String),
    Rejected(Coordinator),
}

/// Authorize and validate a single `FindCoordinator` key.
///
/// GROUP needs `Describe` on `Group(key)`. TRANSACTION needs `Describe` on
/// `TransactionalId(key)`. SHARE v6+ needs `ClusterAction` on the singleton
/// Cluster resource after its composite key is validated. Unsupported SHARE
/// versions, unknown key types, and malformed SHARE keys fail closed without
/// consulting the authorizer.
fn key_admission(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    host: &std::net::SocketAddr,
    api_version: i16,
    key_type: i8,
    key: &str,
) -> FindCoordinatorAdmission {
    let (resource_type, resource_name, operation, share_key_valid) = match key_type {
        KEY_TYPE_GROUP => (ResourceType::Group, key, AclOperation::Describe, true),
        KEY_TYPE_TRANSACTION => (
            ResourceType::TransactionalId,
            key,
            AclOperation::Describe,
            true,
        ),
        KEY_TYPE_SHARE if api_version < 6 => {
            return find_coordinator_admission(api_version, key_type, false, false);
        }
        KEY_TYPE_SHARE => {
            let Some(_) = parse_share_key(key) else {
                return find_coordinator_admission(api_version, key_type, false, false);
            };
            (
                ResourceType::Cluster,
                crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                AclOperation::ClusterAction,
                true,
            )
        }
        _ => return find_coordinator_admission(api_version, key_type, false, false),
    };
    let acl_allowed = authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type,
            resource_name,
            operation,
        },
    ) == AuthorizationResult::Allow;
    find_coordinator_admission(api_version, key_type, acl_allowed, share_key_valid)
}

pub(super) fn authorize_keys(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    api_version: i16,
    key_type: i8,
    keys: Vec<String>,
) -> Vec<KeySlot> {
    let mut slots = Vec::with_capacity(keys.len());
    for key in keys {
        match key_admission(
            broker.config.authorizer.as_ref(),
            image,
            context.principal,
            context.peer,
            api_version,
            key_type,
            &key,
        ) {
            FindCoordinatorAdmission::AllowGroup
            | FindCoordinatorAdmission::AllowTransaction
            | FindCoordinatorAdmission::AllowShare => slots.push(KeySlot::Resolve(key)),
            FindCoordinatorAdmission::DenyGroup => {
                slots.push(KeySlot::Rejected(denied_coordinator(
                    key,
                    codes::GROUP_AUTHORIZATION_FAILED,
                )));
            }
            FindCoordinatorAdmission::DenyTransaction => {
                slots.push(KeySlot::Rejected(denied_coordinator(
                    key,
                    codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                )));
            }
            FindCoordinatorAdmission::DenyCluster => {
                slots.push(KeySlot::Rejected(denied_coordinator(
                    key,
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                )));
            }
            FindCoordinatorAdmission::InvalidRequest => {
                slots.push(KeySlot::Rejected(invalid_coordinator(key)));
            }
        }
    }
    slots
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn deny_authorizer() -> crate::authorizer::SimpleAclAuthorizer {
        crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new())
    }

    #[derive(Debug)]
    struct AllowOnlyClusterAction;

    impl crate::authorizer::Authorizer for AllowOnlyClusterAction {
        fn authorize(
            &self,
            _source: &dyn krabka_authz::AclSource,
            req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if req.resource_type == ResourceType::Cluster
                && req.resource_name == crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME
                && req.operation == AclOperation::ClusterAction
            {
                AuthorizationResult::Allow
            } else {
                AuthorizationResult::Deny
            }
        }
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
        let decision = key_admission(&authz, &image, &anon(), &peer, 6, KEY_TYPE_GROUP, "g");
        assert!(decision == FindCoordinatorAdmission::DenyGroup);
    }

    #[test]
    fn txn_key_denied_maps_to_transactional_id_authorization_failed() {
        let authz = deny_authorizer();
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let decision = key_admission(&authz, &image, &anon(), &peer, 6, KEY_TYPE_TRANSACTION, "t");
        assert!(decision == FindCoordinatorAdmission::DenyTransaction);
    }

    #[test]
    fn share_key_is_authorized_with_cluster_action() {
        let authz = AllowOnlyClusterAction;
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let key = "share-group:AAAAAAAAAAAAAAAAAAAAAA:0";
        let decision = key_admission(&authz, &image, &anon(), &peer, 6, KEY_TYPE_SHARE, key);
        assert!(decision == FindCoordinatorAdmission::AllowShare);
    }

    #[test]
    fn denied_share_key_maps_to_cluster_authorization_failed() {
        let authz = deny_authorizer();
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let decision = key_admission(
            &authz,
            &image,
            &anon(),
            &peer,
            6,
            KEY_TYPE_SHARE,
            "share-group:AAAAAAAAAAAAAAAAAAAAAA:0",
        );
        assert!(decision == FindCoordinatorAdmission::DenyCluster);
    }

    #[test]
    fn malformed_share_and_unknown_type_are_invalid() {
        let authz = deny_authorizer();
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        assert!(
            key_admission(
                &authz,
                &image,
                &anon(),
                &peer,
                6,
                KEY_TYPE_SHARE,
                "malformed"
            ) == FindCoordinatorAdmission::InvalidRequest
        );
        assert!(
            key_admission(&authz, &image, &anon(), &peer, 6, i8::MAX, "g")
                == FindCoordinatorAdmission::InvalidRequest
        );
        assert!(
            key_admission(
                &AllowOnlyClusterAction,
                &image,
                &anon(),
                &peer,
                5,
                KEY_TYPE_SHARE,
                "share-group:AAAAAAAAAAAAAAAAAAAAAA:0",
            ) == FindCoordinatorAdmission::InvalidRequest
        );
    }

    #[test]
    fn denied_entry_carries_the_failure_code() {
        let c = denied_coordinator("g".into(), codes::GROUP_AUTHORIZATION_FAILED);
        assert!(c.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(c.node_id == -1);
    }
}
