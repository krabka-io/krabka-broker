//! The request-routing seams the broker crate hangs on a controller.
//!
//! `RaftShardRouter` claims KIP-595 traffic addressed to a non-metadata quorum
//! shard before metadata dispatch sees it, and `ControllerAdminRouter` carries
//! the KIP-919 Admin surface, so a request the controller listener accepts is
//! served by the broker's own handler registry rather than by a second
//! implementation of the same semantics. Both are hooks the broker installs on
//! `ControllerConfig` rather than settings an operator writes down, which is
//! why they sit apart from the configuration itself.

use std::{future::Future, net::SocketAddr, pin::Pin};

use bytes::Bytes;

use crate::error::RaftError;

/// Optional router for KIP-595 traffic addressed to non-metadata quorum shards.
pub type ShardRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Bytes>, RaftError>> + Send + 'a>>;

/// Classifies and serves shard-addressed KIP-595 requests before metadata dispatch.
pub trait RaftShardRouter: Send + Sync {
    fn route(
        &self,
        api_key: i16,
        body: Bytes,
        principal: Option<&krabka_security::Principal>,
    ) -> ShardRouteFuture<'_>;
}

/// One Kafka API version range served by a controller-listener Admin router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub flexible_min: i16,
}

/// Authenticated request handed from the controller listener to the broker's
/// existing Admin handler registry.
#[derive(Clone, Debug)]
pub struct ControllerAdminRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub body: Bytes,
    pub peer: SocketAddr,
    pub principal: Option<krabka_security::Principal>,
    pub authenticated_via_token: bool,
}

/// Encoded Kafka response body plus its response-header shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerAdminResponse {
    pub body: Bytes,
    pub flexible: bool,
}

pub type ControllerAdminRouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<ControllerAdminResponse>, RaftError>> + Send + 'a>>;

/// Optional KIP-919 Admin RPC surface attached by the broker crate.
pub trait ControllerAdminRouter: Send + Sync {
    fn api_versions(&self) -> &[ControllerApiVersion];
    fn route(&self, request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_>;
}
