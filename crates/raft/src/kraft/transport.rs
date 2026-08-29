//! Transport seam for the [`KraftController`](crate::kraft::controller::KraftController).
//!
//! Outbound peer RPCs go through [`PeerSender`], which is real TCP in
//! production and in-memory in tests. Inbound KIP-595 RPCs arrive as
//! [`Inbound`], which carries a oneshot reply channel. Handle-facing requests
//! arrive as [`Command`].
//!
//! This module is the wire-agnostic boundary: the event loop never touches
//! sockets directly. In-memory tests and real TCP both implement `PeerSender`
//! against the same command and inbound plumbing.
//!
//! ## Peer codec
//!
//! Peer RPC request and response bodies are encoded with the generated KIP-595
//! message codecs in [`wire`]. The engine encodes a `PeerRequest` into the body
//! it hands to [`PeerSender::send`]. The receiving transport drives the peer
//! engine and returns a `PeerResponse`. The sending engine then decodes that
//! response into the matching core [`Event`](crate::kraft::event::Event), which
//! is `ReceiveVoteResponse` or `ReceiveFetchResponse`. This keeps the send path
//! fire-and-forget: the engine never `.await`s a peer RPC inline.

mod command;
mod peer_sender;

pub mod api_key;
pub mod wire;

pub use self::{
    command::{Command, Inbound, MetadataFetchSlice, QuorumStateSnapshot, TimerTick},
    peer_sender::{NullPeerSender, PeerSender},
};
