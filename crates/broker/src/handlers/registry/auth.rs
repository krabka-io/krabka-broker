//! Adapters for the apis that authorize against the connection itself: they
//! receive the `ConnectionAuth` and the peer address rather than a
//! [`RequestContext`].
//!
//! `AlterReplicaLogDirs` needs the principal to build a per-partition
//! authorization-failure response, and the four delegation-token apis create,
//! renew, expire and describe tokens for the authenticated principal.
//!
//! [`RequestContext`]: crate::handlers::RequestContext

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_units::convert::TimeExt as _;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId},
};

pub(super) fn alter_replica_log_dirs_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::{
            Decode,
            owned::{
                alter_replica_log_dirs_request::AlterReplicaLogDirsRequest,
                alter_replica_log_dirs_response::{
                    AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
                    AlterReplicaLogDirsResponse,
                },
            },
        };

        let anonymous;
        let principal = if let Some(principal) = auth.principal() {
            principal
        } else {
            anonymous = krabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                auth_method: krabka_security::AuthMethod::Anonymous,
                groups: vec![],
            };
            &anonymous
        };

        let image = broker.controller.current_image();
        let authorized = broker.config.authorizer.authorize(
            &*image,
            &crate::authorizer::AuthorizationRequest {
                principal,
                host: peer,
                resource_type: krabka_metadata::ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: krabka_metadata::AclOperation::Alter,
            },
        ) == crate::authorizer::AuthorizationResult::Allow;

        if !authorized {
            let mut cur = body;
            let req = AlterReplicaLogDirsRequest::decode(&mut cur, version)?;
            // Kafka's `AlterReplicaLogDirsRequest.getErrorResponse` walks the
            // request as sent: one topic result per (directory, topic) pair,
            // in request order, with every listed partition set to the same
            // error code. It never merges or sorts entries, so a topic named
            // under two directories gets two results here too.
            let results = req
                .dirs
                .into_iter()
                .flat_map(|dir| dir.topics)
                .map(|topic| AlterReplicaLogDirTopicResult {
                    topic_name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|partition_index| AlterReplicaLogDirPartitionResult {
                            partition_index,
                            error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect();
            let resp = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results,
                ..Default::default()
            };
            return crate::handlers::encode_response(&resp, version);
        }

        crate::handlers::alter_replica_log_dirs::handle(broker, version, correlation_id, body).await
    })
}

pub(super) fn create_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::create_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker.config.delegation_token_max_lifetime.millis_i64(),
            broker
                .config
                .delegation_token_default_renew_period
                .millis_i64(),
            &*broker.controller,
            &broker.config.super_users,
        )
        .await;
        if resp.error_code == crate::codes::NONE {
            audit_token_operation(broker, auth, peer, "CreateDelegationToken", &resp.token_id);
        }
        crate::handlers::encode_response(&resp, version)
    })
}

pub(super) fn renew_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        // Resolve the token id before the mutation: the response carries only
        // the new expiry, and a delete leaves nothing to look up afterwards.
        let token_id = token_id_for_hmac(broker, req.hmac.as_ref());
        let resp = crate::handlers::renew_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            broker
                .config
                .delegation_token_default_renew_period
                .millis_i64(),
            &*broker.controller,
        )
        .await;
        if resp.error_code == crate::codes::NONE
            && let Some(token_id) = token_id.as_deref()
        {
            audit_token_operation(broker, auth, peer, "RenewDelegationToken", token_id);
        }
        crate::handlers::encode_response(&resp, version)
    })
}

pub(super) fn expire_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let token_id = token_id_for_hmac(broker, req.hmac.as_ref());
        let resp = crate::handlers::expire_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
        )
        .await;
        if resp.error_code == crate::codes::NONE
            && let Some(token_id) = token_id.as_deref()
        {
            audit_token_operation(broker, auth, peer, "ExpireDelegationToken", token_id);
        }
        crate::handlers::encode_response(&resp, version)
    })
}

pub(super) fn describe_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a crate::network::auth::ConnectionAuth,
    peer: &'a std::net::SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::describe_delegation_token_request::DescribeDelegationTokenRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::describe_delegation_token::handle(
            &req,
            auth,
            broker.config.delegation_token_secret_key.as_ref(),
            &*broker.controller,
            peer,
            broker.config.authorizer.as_ref(),
        );
        crate::handlers::encode_response(&resp, version)
    })
}

/// The token id behind an `hmac`, read from the current metadata image.
///
/// `RenewDelegationToken` and `ExpireDelegationToken` address a token by its
/// HMAC, and neither response echoes an id back. The audit record names the
/// id, never the HMAC: the HMAC *is* the token's password equivalent, and an
/// audit topic an auditor can read is not a place to keep one.
fn token_id_for_hmac(broker: &Broker, hmac: &[u8]) -> Option<String> {
    broker
        .controller
        .current_image()
        .delegation_token_by_hmac(hmac)
        .map(|token| token.token_id.clone())
}

/// Emits the `AdminOperation` record for a delegation-token mutation.
///
/// These apis authorize against the connection, so the principal comes from
/// the `ConnectionAuth` rather than from a `RequestContext`. An unauthenticated
/// connection never reaches a success path here, so there is nothing to audit
/// for one.
fn audit_token_operation(
    broker: &Broker,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &std::net::SocketAddr,
    operation: &str,
    token_id: &str,
) {
    if let crate::network::auth::ConnectionAuth::Authenticated { principal, .. } = auth {
        crate::handlers::audit_admin_for(
            broker.audit_log.as_ref(),
            principal,
            peer,
            operation,
            krabka_audit::AuditOutcome::Success,
            vec![crate::handlers::audit_resource("DelegationToken", token_id)],
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use krabka_protocol::owned::{
        alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
        },
        alter_replica_log_dirs_response::{
            AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
            AlterReplicaLogDirsResponse,
        },
    };
    use krabka_security::{AuthMethod, Principal, SaslMechanism};

    use super::*;
    use crate::{
        codes::CLUSTER_AUTHORIZATION_FAILED,
        network::auth::ConnectionAuth,
        test_support::{DenyAll, codec_helpers, start_broker_with_authorizer},
    };

    codec_helpers!(AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse);

    fn authed(name: &str) -> ConnectionAuth {
        ConnectionAuth::Authenticated {
            principal: Principal {
                name: name.into(),
                auth_method: AuthMethod::Anonymous,
                groups: vec![],
            },
            mechanism: SaslMechanism::Plain,
            expires_at_ms: None,
            authenticated_via_token: false,
        }
    }

    fn topic_result(name: &str, partitions: &[i32]) -> AlterReplicaLogDirTopicResult {
        AlterReplicaLogDirTopicResult {
            topic_name: name.to_string(),
            partitions: partitions
                .iter()
                .map(|&partition_index| AlterReplicaLogDirPartitionResult {
                    partition_index,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Kafka's `AlterReplicaLogDirsRequest.getErrorResponse` produces one
    /// topic result per (directory, topic) pair, in request order, never
    /// merging or sorting them by topic name. A denied `Cluster.Alter` must
    /// match that shape exactly, including when the same topic is named
    /// under two directories or when topics are listed out of name order.
    #[tokio::test]
    async fn denial_emits_one_row_per_dir_and_topic_in_request_order() {
        let version = 2;
        for (case, dirs, expected_results) in [
            (
                "one dir, one topic",
                vec![AlterReplicaLogDir {
                    path: "/var/lib/krabka/data0".into(),
                    topics: vec![AlterReplicaLogDirTopic {
                        name: "orders".into(),
                        partitions: vec![0, 1],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                vec![topic_result("orders", &[0, 1])],
            ),
            (
                "same topic under two dirs stays two rows",
                vec![
                    AlterReplicaLogDir {
                        path: "/var/lib/krabka/data0".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    AlterReplicaLogDir {
                        path: "/var/lib/krabka/data1".into(),
                        topics: vec![AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![1],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                vec![topic_result("orders", &[0]), topic_result("orders", &[1])],
            ),
            (
                "topics in reverse name order are not sorted",
                vec![AlterReplicaLogDir {
                    path: "/var/lib/krabka/data0".into(),
                    topics: vec![
                        AlterReplicaLogDirTopic {
                            name: "widgets".into(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                        AlterReplicaLogDirTopic {
                            name: "orders".into(),
                            partitions: vec![0],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                vec![topic_result("widgets", &[0]), topic_result("orders", &[0])],
            ),
        ] {
            let (broker_handle, _dir) = start_broker_with_authorizer(Arc::new(DenyAll)).await;
            let broker = broker_handle.broker_arc_for_test();
            let auth = authed("alice");
            let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let req = AlterReplicaLogDirsRequest {
                dirs,
                ..Default::default()
            };
            let body = encode_request(&req, version);

            let bytes = alter_replica_log_dirs_adapter(&broker, version, 0, &body, &auth, &peer)
                .await
                .expect("adapter");
            let resp = decode_response(&bytes, version);

            let expected = AlterReplicaLogDirsResponse {
                throttle_time_ms: 0,
                results: expected_results,
                ..Default::default()
            };
            assert2::check!(resp == expected, "case {case}");
            broker_handle.shutdown().await;
        }
    }
}
