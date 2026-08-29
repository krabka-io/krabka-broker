//! Accept loop for the controller TCP listener. Receives inbound KIP-595 RPCs
//! (Fetch=1, Vote=52, BeginQuorumEpoch=53, EndQuorumEpoch=54) plus the
//! Krabka-private observer/forward RPCs and feeds them into the local
//! [`KraftController`] engine.
//!
//! Wire shape matches `krabka_client_core::Connection::raw_request`:
//!
//! - Request: `len(i32) | RequestHeader v1/v2 | body`
//! - Response: `len(i32) | ResponseHeader v0/v1 | body`
//!
//! Both request headers begin with `api_key(i16) api_version(i16)
//! correlation_id(i32) client_id(NULLABLE_STRING)`. Flexible APIs add tagged
//! fields. The body is decoded by the selected controller or Admin handler.

use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

mod api_versions;
mod describe_cluster;
mod dispatch;
mod framing;
mod kip853;
mod metadata_rpc;
mod registration;
#[cfg(test)]
mod test_support;
mod voter_admin;

use self::{
    api_versions::{API_KEY_API_VERSIONS, api_versions_response_body, api_versions_routing_error},
    describe_cluster::{API_KEY_DESCRIBE_CLUSTER, describe_cluster_response_body},
    dispatch::{dispatch_with_router, is_native_raft_api},
    framing::{
        is_eof, read_one_request, write_response, write_response_frame,
        write_response_no_tagged_fields,
    },
    kip853::{
        API_KEY_ADD_RAFT_VOTER, API_KEY_DESCRIBE_QUORUM, API_KEY_REMOVE_RAFT_VOTER,
        API_KEY_UPDATE_RAFT_VOTER, kip853_admin_response, kip853_authorization_failure,
    },
};
use crate::{error::RaftError, kraft::KraftController};

struct ConnectionContext {
    peer: SocketAddr,
    principal: Option<krabka_security::Principal>,
    authenticated_via_token: bool,
    cluster_alter_authorized: bool,
}

pub(crate) async fn run(
    listener: TcpListener,
    engine: KraftController,
    shutdown: CancellationToken,
    handshake: Option<Arc<dyn crate::RaftListenerHandshake>>,
    shard_router: Option<Arc<dyn crate::RaftShardRouter>>,
    admin_router: Option<Arc<dyn crate::ControllerAdminRouter>>,
) {
    match listener.local_addr() {
        Ok(addr) => info!(%addr, "controller listener started"),
        Err(e) => info!(error = %e, "controller listener started (addr unknown)"),
    }
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let engine = engine.clone();
                        let shutdown = shutdown.clone();
                        let handshake = handshake.clone();
                        let shard_router = shard_router.clone();
                        let admin_router = admin_router.clone();
                        tokio::spawn(async move {
                            let connection = if let Some(hs) = handshake {
                                match hs.upgrade(stream).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::debug!(%peer, error = %e, "handshake failed");
                                        return;
                                    }
                                }
                            } else {
                                crate::RaftConnection {
                                    stream: Box::new(stream) as Box<dyn krabka_client_core::ClientDuplex>,
                                    principal: None,
                                    authenticated_via_token: false,
                                    cluster_alter_authorized: true,
                                }
                            };
                            if let Err(e) = handle_conn(
                                connection.stream,
                                engine,
                                shutdown,
                                shard_router,
                                admin_router,
                                ConnectionContext {
                                    peer,
                                    principal: connection.principal,
                                    authenticated_via_token: connection.authenticated_via_token,
                                    cluster_alter_authorized: connection.cluster_alter_authorized,
                                },
                            ).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "controller listener accept failed");
                    }
                }
            }
        }
    }
}

async fn handle_conn<S>(
    mut stream: S,
    engine: KraftController,
    shutdown: CancellationToken,
    shard_router: Option<Arc<dyn crate::RaftShardRouter>>,
    admin_router: Option<Arc<dyn crate::ControllerAdminRouter>>,
    context: ConnectionContext,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = read_one_request(&mut stream, admin_router.as_deref()) => {
                let (api_key_n, api_version, correlation_id, client_id, body, response_flexible) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        // Treat peer EOF as a clean shutdown of this conn.
                        if is_eof(&e) {
                            return Ok(());
                        }
                        return Err(e);
                    }
                };
                // ApiVersions (18) is the bootstrap handshake performed by
                // `Connection::connect`. It arrives at v0 with a header v1 (no
                // tagged-fields byte) and expects a ResponseHeader v0 reply (also
                // no tagged-fields byte) — the documented Kafka asymmetry. We
                // serialize it separately rather than poisoning the generic codec.
                if api_key_n == API_KEY_API_VERSIONS {
                    // ApiVersionsResponse always uses a v0 ResponseHeader (no
                    // tagged-fields byte), but the BODY shape depends on the
                    // request version: v0..=2 are non-flexible (i32 array), v3+
                    // are flexible (compact array). Krabka's own client asks at
                    // v0; the JVM controller asks at v4. The generated codec
                    // speaks the raw `int16`, so unwrap the version here.
                    let image = engine.current_image();
                    let error_code = api_versions_routing_error(
                        api_version.get(),
                        &body,
                        &image.cluster_id().to_string(),
                        engine.node_id().0,
                    )?;
                    let resp = api_versions_response_body(
                        api_version.get(),
                        &image,
                        admin_router.as_deref(),
                        error_code,
                    );
                    write_response_no_tagged_fields(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                // DescribeCluster (60, KIP-919) is served here rather than in
                // `dispatch` because it needs the request version (for the
                // flexible body codec) and the controller's metadata image. The
                // flexible v1 ResponseHeader is supplied by `write_response`.
                if api_key_n == API_KEY_DESCRIBE_CLUSTER {
                    let resp =
                        describe_cluster_response_body(api_version.get(), &body, &engine).await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                if matches!(
                    api_key_n.0,
                    API_KEY_DESCRIBE_QUORUM
                        | API_KEY_ADD_RAFT_VOTER
                        | API_KEY_REMOVE_RAFT_VOTER
                        | API_KEY_UPDATE_RAFT_VOTER
                ) {
                    if matches!(
                        api_key_n.0,
                        API_KEY_ADD_RAFT_VOTER
                            | API_KEY_REMOVE_RAFT_VOTER
                            | API_KEY_UPDATE_RAFT_VOTER
                    ) && !context.cluster_alter_authorized
                    {
                        let resp = kip853_authorization_failure(
                            api_key_n.0,
                            api_version.get(),
                        )?;
                        write_response(&mut stream, correlation_id, resp).await?;
                        continue;
                    }
                    let resp = kip853_admin_response(
                        api_key_n.0,
                        api_version.get(),
                        &body,
                        &engine,
                    )
                    .await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                if registration::is_controller_api(api_key_n.0) {
                    let resp = registration::dispatch(
                        api_key_n.0,
                        api_version.get(),
                        &body,
                        &engine,
                        context.cluster_alter_authorized,
                    )
                    .await?;
                    write_response(&mut stream, correlation_id, resp).await?;
                    continue;
                }
                if !is_native_raft_api(api_key_n.0)
                    && let Some(router) = admin_router.as_deref()
                    && let Some(response) = router
                        .route(crate::ControllerAdminRequest {
                            api_key: api_key_n.get(),
                            api_version: api_version.get(),
                            correlation_id,
                            client_id,
                            body: body.clone(),
                            peer: context.peer,
                            principal: context.principal.clone(),
                            authenticated_via_token: context.authenticated_via_token,
                        })
                        .await?
                {
                    write_response_frame(
                        &mut stream,
                        correlation_id,
                        response.body,
                        response.flexible,
                    )
                    .await?;
                    continue;
                }
                let resp = dispatch_with_router(api_key_n, body, &engine, shard_router.as_deref()).await?;
                write_response_frame(&mut stream, correlation_id, resp, response_flexible).await?;
            }
        }
    }
}
