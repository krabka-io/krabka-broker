//! The real [`PeerSender`]: one cached [`Connection`] per voter, dialed
//! through the injected [`OutboundDialer`].
//!
//! This is the whole outbound half of the controller's KIP-595 transport. It
//! owns the per-peer connection cache, the bootstrap and alias address books a
//! node uses before it has learned the voter set, and the eviction on a failed
//! RPC that makes the next send redial.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_ids::ApiKey;
use krabka_metadata::voters::VoterSet;

use crate::{
    error::RaftError,
    kraft::{transport::PeerSender, types::NodeId},
    network::{
        addressing::{api_version_for, controller_addr},
        dialer::OutboundDialer,
    },
};

#[cfg(test)]
mod tests;

/// Real [`PeerSender`]: it dials each voter's controller listener and issues
/// the KIP-595 RPC over [`krabka_client_core::Connection::raw_request`].
///
/// The sender caches one connection per peer. A failed RPC evicts the cached
/// connection, so the next send dials again.
pub(crate) struct RealPeerSender {
    connections: DashMap<NodeId, Arc<Connection>>,
    voters: RwLock<VoterSet>,
    bootstrap: BTreeMap<NodeId, String>,
    aliases: RwLock<BTreeMap<NodeId, String>>,
    client_id: String,
    dialer: Arc<dyn OutboundDialer>,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
}

impl RealPeerSender {
    pub(crate) fn new(
        voters: VoterSet,
        bootstrap_servers: &[String],
        client_id: String,
        dialer: Arc<dyn OutboundDialer>,
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
        frame_max: krabka_client_core::ClientFrameMax,
    ) -> Self {
        let bootstrap = bootstrap_servers
            .iter()
            .enumerate()
            .filter_map(|(index, address)| {
                u64::try_from(index)
                    .ok()
                    .map(|index| (NodeId(u64::MAX - index), address.clone()))
            })
            .collect();
        Self {
            connections: DashMap::new(),
            voters: RwLock::new(voters),
            bootstrap,
            aliases: RwLock::new(BTreeMap::new()),
            client_id,
            dialer,
            dispatch_queue_capacity,
            frame_max,
        }
    }

    /// Looks up or opens a connection to `peer`.
    #[tracing::instrument(level = "debug", skip_all, fields(peer), err)]
    async fn connect(&self, peer: NodeId) -> Result<Arc<Connection>, RaftError> {
        if let Some(c) = self.connections.get(&peer) {
            return Ok(Arc::clone(c.value()));
        }
        let addr = {
            let voters = self
                .voters
                .read()
                .map_err(|_| RaftError::ChangeRejected("voter endpoint lock poisoned".into()))?;
            controller_addr(&voters, peer)
        }
        .or_else(|| {
            self.aliases
                .read()
                .ok()
                .and_then(|aliases| aliases.get(&peer).cloned())
        })
        .or_else(|| self.bootstrap.get(&peer).cloned())
        .ok_or(RaftError::NotLeader {
            current_leader: None,
        })?;
        let opts = ConnectionOptions {
            client_id: self.client_id.clone(),
            dispatch_queue_capacity: self.dispatch_queue_capacity,
            frame_max: self.frame_max,
            ..ConnectionOptions::default()
        };
        let conn = Arc::new(self.dialer.dial(peer, &addr, opts).await?);
        self.connections.insert(peer, Arc::clone(&conn));
        Ok(conn)
    }
}

#[async_trait]
impl PeerSender for RealPeerSender {
    #[tracing::instrument(level = "debug", skip_all, fields(peer, api_key = key), err)]
    async fn send(&self, peer: NodeId, key: i16, body: Bytes) -> Result<Bytes, RaftError> {
        let conn = self.connect(peer).await?;
        // The transport seam and `raw_request` speak the raw wire `int16`s; the
        // `(api_key, api_version)` pairing is done through the newtypes so the
        // two adjacent `i16`s cannot be transposed, then unwrapped at the wire
        // boundary below.
        let version = api_version_for(ApiKey(key));
        match conn.raw_request(key, version.get(), body).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                // Drop the cached connection on any transport error so the next
                // send redials a fresh socket (a crashed/restarted peer).
                self.connections.remove(&peer);
                Err(RaftError::Network(e))
            }
        }
    }

    async fn probe_kraft_version(
        &self,
        address: &str,
        finalized_version: u16,
    ) -> Result<bool, RaftError> {
        use krabka_protocol::owned::api_versions_request::ApiVersionsRequest;

        let connection = self
            .dialer
            .dial(
                NodeId(u64::MAX),
                address,
                ConnectionOptions {
                    client_id: "krabka-voter-probe".into(),
                    dispatch_queue_capacity: self.dispatch_queue_capacity,
                    frame_max: self.frame_max,
                    ..ConnectionOptions::default()
                },
            )
            .await
            .map_err(RaftError::Network)?;
        let response = connection
            .send(ApiVersionsRequest {
                client_software_name: "krabka".into(),
                client_software_version: env!("CARGO_PKG_VERSION").into(),
                ..Default::default()
            })
            .await
            .map_err(RaftError::Network)?;
        connection.close();
        Ok(response
            .supported_features
            .iter()
            .find(|feature| {
                feature.name == krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
            })
            .is_some_and(|feature| {
                i16::try_from(finalized_version).is_ok_and(|version| {
                    feature.min_version <= version && version <= feature.max_version
                })
            }))
    }

    fn update_voters(&self, voters: &VoterSet) {
        if let Ok(mut current) = self.voters.write() {
            *current = voters.clone();
            // Endpoint updates must force the next request through DNS/dialing.
            self.connections.clear();
        }
    }

    fn discovery_peers(&self) -> Vec<NodeId> {
        self.bootstrap.keys().copied().collect()
    }

    fn remember_peer(&self, source: NodeId, actual: NodeId) {
        if source == actual {
            return;
        }
        let address = self.bootstrap.get(&source).cloned().or_else(|| {
            self.aliases
                .read()
                .ok()
                .and_then(|aliases| aliases.get(&source).cloned())
        });
        if let Some(address) = address
            && let Ok(mut aliases) = self.aliases.write()
        {
            aliases.insert(actual, address);
        }
    }
}
