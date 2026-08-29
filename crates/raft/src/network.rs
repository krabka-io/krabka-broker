//! Real KIP-595 [`PeerSender`](crate::kraft::transport::PeerSender) over Kafka
//! TCP framing, using the existing [`krabka_client_core::Connection`]. One
//! cached connection per peer.
//!
//! The engine's transport seam ([`crate::kraft::transport::PeerSender`]) hands
//! this impl an already-encoded KIP-595 request body, the destination peer, and
//! the api key. The impl resolves the peer's controller endpoint and dials it.
//! The injected [`OutboundDialer`] terminates TLS and SASL on that dial. The
//! impl then issues a `raw_request(api_key, version, body)`. `raw_request`
//! builds the v2 `RequestHeader` and strips the v1 `ResponseHeader`, so the
//! returned bytes are the bare response body. The engine decodes that body back
//! into a `Receive*Response` event.
//!
//! Peer addresses are resolved from the static voter set's CONTROLLER
//! endpoints.
//!
//! This root only wires the parts together. The dial seam lives in `dialer`,
//! the voter-endpoint and api-version lookups in `addressing`, and the
//! [`PeerSender`](crate::kraft::transport::PeerSender) implementation itself in
//! `peer_sender`.

mod addressing;
mod dialer;
mod peer_sender;

pub use self::dialer::{OutboundDialer, PlaintextDialer};
pub(crate) use self::peer_sender::RealPeerSender;
