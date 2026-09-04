//! Real KIP-595 peer-RPC body codec.
//!
//! The engine's loop reasons in terms of the flat `PeerRequest` and
//! `PeerResponse` enums. This module maps each variant to and from the genuine
//! generated KIP-595 message bodies. Those bodies are header-less, because the
//! framing layer in `server.rs` and `network.rs` adds the request header and
//! the response header. The captured wire versions are Vote v2,
//! `BeginQuorumEpoch` v1, `EndQuorumEpoch` v1, and Fetch v17. Krabka-to-Krabka
//! replication rides these exact bytes.
//!
//! The metadata log is the single `KRaft` topic `__cluster_metadata`, partition
//! 0, so every RPC body carries exactly one topic and exactly one partition.
//! Kafka's `VoteResponse` carries no pre-vote field. A candidate matches a
//! reply to its round from its own `Prospective` or `Candidate` role, so Krabka
//! encodes a byte-faithful `VoteResponse` and the core infers the round itself
//! (KIP-996).
//!
//! The shared constants and integer conversions live in `codec`, the request
//! bodies in `request`, and the response bodies in `response`.

mod codec;
mod request;
mod response;

/// The pinned `FetchSnapshot` wire version. Public because a broker-only
/// observer sends this RPC itself, and the version on its request header has
/// to be the one [`PeerRequest::FetchSnapshot`] encoded the body at.
pub use self::codec::FETCH_SNAPSHOT_VERSION;
pub(crate) use self::codec::{
    FETCH_VERSION, NOT_LEADER_OR_FOLLOWER, QUORUM_EPOCH_VERSION, VOTE_VERSION,
};
pub use self::{
    request::{
        PeerRequest, decode_begin, decode_end, decode_fetch, decode_fetch_snapshot, decode_vote,
    },
    response::PeerResponse,
};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::{
        PeerRequest, PeerResponse,
        codec::{FETCH_VERSION, METADATA_TOPIC_ID},
    };
    use crate::kraft::types::NodeId;

    #[test]
    fn fetch_wire_carries_metadata_topic_id() {
        use krabka_protocol::{
            Decode,
            owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        };
        let req = PeerRequest::Fetch {
            from: NodeId(2),
            fetch_epoch: 1,
            fetch_offset: 5,
            replica_directory_id: uuid::Uuid::nil(),
        };
        let mut c = &req.encode()[..];
        let dreq = FetchRequest::decode(&mut c, FETCH_VERSION).unwrap();
        assert2::assert!(dreq.topics[0].topic_id == METADATA_TOPIC_ID);

        let resp = PeerResponse::Fetch {
            leader_id: NodeId(1),
            leader_epoch: 4,
            diverging: None,
            snapshot_id: None,
            hwm: 0,
            records: Bytes::new(),
        };
        let mut c2 = &resp.encode()[..];
        let dresp = FetchResponse::decode(&mut c2, FETCH_VERSION).unwrap();
        assert2::assert!(dresp.responses[0].topic_id == METADATA_TOPIC_ID);
    }
}
