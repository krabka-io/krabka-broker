//! Shared throttling primitives: the KIP-13 and KIP-124 client quota, the
//! KIP-73 replication quota, and the token bucket behind the KIP-612
//! connection rate.

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
