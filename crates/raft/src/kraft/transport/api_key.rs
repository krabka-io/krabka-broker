//! KIP-595 api keys used by the engine's peer sends.
//!
//! The engine tags every outbound body with one of these `api_key` values, and
//! the inbound dispatch routes on the same numbers.

pub const FETCH: i16 = 1;
pub const VOTE: i16 = 52;
pub const BEGIN_QUORUM_EPOCH: i16 = 53;
pub const END_QUORUM_EPOCH: i16 = 54;
pub const FETCH_SNAPSHOT: i16 = 59;
