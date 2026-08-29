//! The outbound peer-RPC seam and its no-op implementation.
//!
//! [`PeerSender`] is the trait the engine sends already-encoded request bodies
//! through. Production backs it with TCP, tests back it in memory, and
//! [`NullPeerSender`] backs the single-voter case where no peer RPC is ever
//! sent.

use bytes::Bytes;

use crate::{error::RaftError, kraft::types::NodeId};

/// Outbound peer RPC sender.
///
/// It encodes nothing itself. The event loop hands it the already-encoded
/// request body (see [`wire`](super::wire)) and the destination peer. The impl then dials
/// the peer, sends the body, and returns the raw response body.
///
/// This matches the `async_trait` mechanism that
/// [`OutboundDialer`](crate::network::OutboundDialer) uses.
#[async_trait::async_trait]
pub trait PeerSender: Send + Sync {
    /// Sends `body`, a request for `api_key`, to `peer` and returns the raw
    /// response body.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the peer is unreachable or the RPC fails.
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError>;

    /// Probe an advertised candidate endpoint through the configured outbound
    /// security dialer and verify its finalized `kraft.version` support.
    async fn probe_kraft_version(
        &self,
        _address: &str,
        _finalized_version: u16,
    ) -> Result<bool, RaftError> {
        Err(RaftError::ChangeRejected(
            "candidate probing is unavailable on this transport".into(),
        ))
    }

    /// Replace the peer endpoint table after applying a `VotersRecord`.
    fn update_voters(&self, _voters: &krabka_metadata::VoterSet) {}

    /// Transport-only bootstrap peers used by an observer with no voter view.
    fn discovery_peers(&self) -> Vec<NodeId> {
        Vec::new()
    }

    /// Associate a leader id with the endpoint used for its discovery reply.
    fn remember_peer(&self, _source: NodeId, _actual: NodeId) {}
}

/// A no-op `PeerSender` for single-voter and no-network tests. Every send fails
/// as unreachable.
///
/// A single voter never sends peer RPCs, because it wins its own election
/// immediately. This sender therefore lets the contract tests run without a
/// real transport.
pub struct NullPeerSender;

#[async_trait::async_trait]
impl PeerSender for NullPeerSender {
    async fn send(&self, peer: NodeId, _api_key: i16, _body: Bytes) -> Result<Bytes, RaftError> {
        Err(RaftError::NotLeader {
            current_leader: Some(peer),
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::kraft::transport::api_key;

    #[tokio::test]
    async fn null_peer_sender_reports_target_as_current_leader() {
        let err = NullPeerSender
            .send(NodeId(7), api_key::FETCH, Bytes::new())
            .await
            .expect_err("null sender should reject peer sends");

        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(7))
            }
        ));
    }
}
