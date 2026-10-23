//! Adapters for the apis that authorize against the connection itself: they
//! receive the `ConnectionAuth` and the peer address rather than a
//! [`RequestContext`].
//!
//! `AlterReplicaLogDirs` needs the principal to build a per-partition
//! authorization-failure response, and the four delegation-token apis create,
//! renew, expire and describe tokens for the authenticated principal.
//!
//! [`RequestContext`]: crate::handlers::RequestContext

use {
    crate::{
        broker::Broker,
        codes::NONE,
        error::BrokerError,
        handlers::{
            ApiVersion, CorrelationId, alter_replica_log_dirs::handle, audit_admin_for,
            create_delegation_token as token_create, describe_delegation_token as token_describe,
            encode_response, expire_delegation_token as token_expire,
            renew_delegation_token as token_renew,
        },
        network::auth::ConnectionAuth,
    },
    bytes::Bytes,
    futures_util::future::BoxFuture,
    krabka_protocol::owned::{
        create_delegation_token_request::CreateDelegationTokenRequest,
        create_delegation_token_response::CreateDelegationTokenResponse,
        describe_delegation_token_request::DescribeDelegationTokenRequest,
        describe_delegation_token_response::DescribeDelegationTokenResponse,
        expire_delegation_token_request::ExpireDelegationTokenRequest,
        expire_delegation_token_response::ExpireDelegationTokenResponse,
        renew_delegation_token_request::RenewDelegationTokenRequest,
        renew_delegation_token_response::RenewDelegationTokenResponse,
    },
    krabka_units::convert::TimeExt as _,
    std::net::SocketAddr,
};

pub(super) fn alter_replica_log_dirs_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    auth: &'a ConnectionAuth,
    peer: &'a SocketAddr,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    handle(broker, version, correlation_id, body, auth, peer)
}

#[krabka_macros::token_dispatch]
pub(super) async fn create_delegation_token_adapter(
    broker: &Broker,
    req: CreateDelegationTokenRequest,
    auth: &ConnectionAuth,
    peer: &SocketAddr,
) -> CreateDelegationTokenResponse {
    let resp = token_create::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        broker.config.delegation_token_max_lifetime.millis_i64(),
        broker
            .config
            .delegation_token_default_renew_period
            .millis_i64(),
        &*broker.controller,
        token_create::TokenAcl {
            authorizer: broker.config.authorizer.as_ref(),
            peer,
        },
    )
    .await;
    if resp.error_code == NONE {
        audit_token_operation(broker, auth, peer, "CreateDelegationToken", &resp.token_id);
    }
    resp
}

#[krabka_macros::token_dispatch]
pub(super) async fn renew_delegation_token_adapter(
    broker: &Broker,
    req: RenewDelegationTokenRequest,
    auth: &ConnectionAuth,
    peer: &SocketAddr,
) -> RenewDelegationTokenResponse {
    // Resolve the token id before the mutation: the response carries only
    // the new expiry, and a delete leaves nothing to look up afterwards.
    let token_id = token_id_for_hmac(broker, req.hmac.as_ref());
    let resp = token_renew::handle(
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
    audit_token_mutation(
        broker,
        auth,
        peer,
        "RenewDelegationToken",
        token_id.as_deref(),
        resp.error_code,
    );
    resp
}

#[krabka_macros::token_dispatch]
pub(super) async fn expire_delegation_token_adapter(
    broker: &Broker,
    req: ExpireDelegationTokenRequest,
    auth: &ConnectionAuth,
    peer: &SocketAddr,
) -> ExpireDelegationTokenResponse {
    let token_id = token_id_for_hmac(broker, req.hmac.as_ref());
    let resp = token_expire::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        &*broker.controller,
    )
    .await;
    audit_token_mutation(
        broker,
        auth,
        peer,
        "ExpireDelegationToken",
        token_id.as_deref(),
        resp.error_code,
    );
    resp
}

#[krabka_macros::token_dispatch]
pub(super) async fn describe_delegation_token_adapter(
    broker: &Broker,
    req: DescribeDelegationTokenRequest,
    auth: &ConnectionAuth,
    peer: &SocketAddr,
) -> DescribeDelegationTokenResponse {
    token_describe::handle(
        &req,
        auth,
        broker.config.delegation_token_secret_key.as_ref(),
        &*broker.controller,
        peer,
        broker.config.authorizer.as_ref(),
    )
}

fn audit_token_mutation(
    broker: &Broker,
    auth: &ConnectionAuth,
    peer: &std::net::SocketAddr,
    operation: &str,
    token_id: Option<&str>,
    error_code: i16,
) {
    if error_code == NONE
        && let Some(token_id) = token_id
    {
        audit_token_operation(broker, auth, peer, operation, token_id);
    }
}

/// The token id behind an `hmac`, read from the current metadata image.
///
/// `RenewDelegationToken` and `ExpireDelegationToken` address a token by its
/// HMAC, and neither response echoes an id back. The audit record names the
/// id, never the HMAC: the HMAC *is* the token's password equivalent, and an
/// audit topic an auditor can read is not a place to keep one.
fn token_id_for_hmac(broker: &Broker, hmac: &[u8]) -> Option<String> {
    let secret_key = broker.config.delegation_token_secret_key.as_ref()?;
    broker
        .controller
        .current_image()
        .delegation_token_by_hmac(secret_key.as_bytes(), hmac)
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
    auth: &ConnectionAuth,
    peer: &std::net::SocketAddr,
    operation: &str,
    token_id: &str,
) {
    if let ConnectionAuth::Authenticated { principal, .. } = auth {
        audit_admin_for(
            broker.audit_log.as_ref(),
            principal,
            peer,
            operation,
            krabka_audit::AuditOutcome::Success,
            vec![crate::handlers::audit_resource("DelegationToken", token_id)],
        );
    }
}
