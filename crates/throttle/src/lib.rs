//! Shared throttling primitives: the client quota behind KIP-13, KIP-124 and
//! the KIP-612 `connection_creation_rate`, the KIP-73 replication quota, and
//! a token bucket.

mod client_quota;
mod replication_quota;
mod runtime;
mod sampled_rate;

pub use {
    client_quota::ClientQuota,
    replication_quota::ReplicationQuota,
    runtime::{MICROS_PER_TOKEN, ThrottleState, TokenBucket, whole_token_request},
    sampled_rate::QuotaWindow,
};
