//! Shared diskless-WAL range metadata and independently versioned formats.

/// Portable capture state, committed-event projection, and WORM authentication.
pub mod capture;
/// Compacted index-topic key and value codecs.
pub mod index;
/// CKWL object framing codec.
pub mod object;

use {
    serde::{Deserialize, Serialize},
    uuid::Uuid,
};

/// One partition's byte range in a flushed object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalIndexEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
    pub max_timestamp_ms: i64,
}

#[cfg(test)]
mod tests;
