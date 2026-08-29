//! KIP-227 incremental-fetch-session cache.
//!
//! A `FetchSession` lets a Kafka consumer or replicator send the broker its
//! subscription set once. After that it sends small "delta" fetch requests.
//! Each delta carries only the partitions whose desired state has changed (new
//! offset, new max-bytes), plus a `forgotten_topics_data` list of partitions
//! to drop. The broker answers with only the partitions whose state has
//! changed since the previous response.
//!
//! For a caught-up consumer with hundreds of partitions, this reduces a
//! continuous stream of identical fetches to almost no wire traffic until
//! something changes.
//!
//! ## Wire-level state machine
//!
//! Every `FetchRequest` carries `session_id: i32` and `session_epoch: i32`.
//! Four classes of request fall out:
//!
//! | `session_id` | `session_epoch` | Meaning                                |
//! |--------------|-----------------|----------------------------------------|
//! | 0            | -1 (FINAL)      | Sessionless full fetch (no caching).   |
//! | 0            | 0 (INITIAL)     | Open a new session.                    |
//! | N>0          | E (== expected) | Incremental fetch on existing session. |
//! | N>0          | -1 (FINAL)      | Close the existing session.            |
//!
//! A mismatched epoch returns `INVALID_FETCH_SESSION_EPOCH` at the top level
//! of the response. An unknown id returns `FETCH_SESSION_ID_NOT_FOUND`.
//!
//! ## Cache & eviction
//!
//! The cache holds sessions in one bounded map, keyed by allocated id. Its
//! capacity is `BrokerConfig::max_incremental_fetch_session_cache_slots`.
//! When the map is full, an allocation evicts the LRU **non-privileged**
//! session. Only another privileged session evicts a privileged session, which
//! is a follower fetch with `replica_id >= 0`.
//!
//! When there is no eligible victim, `try_allocate` returns
//! `INVALID_SESSION_ID` and the caller falls back to a sessionless response.
//! This matches Apache Kafka. That case arises when the cache is full of
//! privileged sessions and the caller is non-privileged.

mod cache;
mod classify;
mod diff;
mod epoch;
mod eviction;
mod state;
#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(crate) use self::diff::apply_incremental;
pub use self::{
    cache::{FetchSession, FetchSessionCache},
    classify::SessionDecision,
    epoch::{
        FINAL_EPOCH, FetchSessionEpoch, FetchSessionId, INITIAL_EPOCH, INVALID_SESSION_ID,
        next_epoch,
    },
    state::{CachedPartitionState, FetchSessionKey},
};

#[cfg(test)]
#[path = "fetch_session_model.rs"]
mod fetch_session_model;
