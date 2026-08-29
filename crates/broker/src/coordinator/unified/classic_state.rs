//! Classic-protocol per-`group_id` state machine (`Group`).
//!
//! This module holds pure data and transitions. The unified per-group actor
//! owns them. Committed offsets are protocol-agnostic and live on the unified
//! [`super::group::CoordinatorGroup`] container, not here.
//!
//! This file is the module root. The [`ClassicGroup`] record and its
//! rebalance-lifecycle transitions live in `group`, the membership transitions
//! in `membership`, the [`Member`] record in `member`, and the protocol vote in
//! `protocol`. [`OffsetEntry`] stays here: it is a leaf record with no
//! behaviour of its own.

use krabka_log::Offset;

mod group;
mod member;
mod membership;
mod protocol;
mod rebalance;

#[cfg(test)]
mod test_support;

pub use self::{
    group::{ClassicGroup, GroupState},
    member::{AddMemberOutcome, Member},
    protocol::select_protocol,
};

/// A committed offset entry, keyed by `(topic, partition)` in
/// [`Group::committed_offsets`].
#[derive(Debug, Clone)]
pub struct OffsetEntry {
    pub offset: Offset,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
}

#[cfg(test)]
#[path = "classic_state_model.rs"]
mod classic_state_model;
