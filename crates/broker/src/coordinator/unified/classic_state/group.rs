//! The [`ClassicGroup`] record and its [`GroupState`] four-state machine.
//!
//! This module holds the group's fields and the two accessors that do not
//! change membership or drive a rebalance round. The transitions live in the
//! `membership` and `rebalance` siblings.

use std::{collections::HashMap, time::Instant};

use super::member::Member;

/// Four-state machine for a live consumer group, matching the Apache Kafka
/// classic protocol (KIP-62 / KIP-394).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupState {
    /// No members and no committed offsets.
    Empty,
    /// At least one member has called `JoinGroup`. The group waits for the
    /// rebalance deadline or for every expected member.
    PreparingRebalance,
    /// `JoinGroup` returned to all members. The group waits for the leader's
    /// `SyncGroup`.
    CompletingRebalance,
    /// `SyncGroup` completed. Members send heartbeats.
    Stable,
}

#[derive(Debug, Clone)]
pub struct ClassicGroup {
    pub group_id: String,
    pub state: GroupState,
    /// `"consumer"` for `KafkaConsumer`. The broker reads the value only to
    /// reject inconsistent proposals.
    pub protocol_type: Option<String>,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub protocol_name: Option<String>,
    pub members: HashMap<String, Member>,
    /// KIP-345 secondary index that maps `group.instance.id` to the current
    /// `member_id`. It mirrors the `group_instance_id` field on entries in
    /// `members`. The broker uses it to find a static member's slot when a
    /// reconnecting client omits its `member_id` (KIP-394 bootstrap), or
    /// supplies a stale one from a prior session.
    pub static_members: HashMap<String, String>,
    pub rebalance_deadline: Option<Instant>,
    /// Members whose `JoinGroup` arrived since the last transition into
    /// `PreparingRebalance`.
    ///
    /// The `JoinGroup` handler runs the rebalance early, without the full
    /// configured initial delay, once every member still in `members` appears
    /// here. This keeps the leader from running the assignor on a
    /// stale-metadata snapshot when a slow member misses the wait window under
    /// load. In that case the assignor's cooperative-sticky Pass-3 omissions
    /// strand partitions on no member. Every transition into
    /// `PreparingRebalance` clears this set.
    pub joined_this_round: std::collections::HashSet<String>,
    /// `true` while the current `PreparingRebalance` round opened from an
    /// `Empty` group. That is a new group, or one whose members had all left,
    /// for example after a warm-up consumer joins and leaves.
    ///
    /// Such a round keeps the full configured batching window, which mirrors
    /// Apache Kafka's `InitialDelayedJoin`. It does not complete as soon as
    /// the first member appears, so a set of consumers that start together
    /// lands in a single generation. An early completion of a from-`Empty`
    /// round would strand the first joiner in a solo generation. It would then
    /// force an immediate second rebalance when the next member arrives, which
    /// disrupts produce and fetch under load.
    ///
    /// The value is `false` for a rebalance that a membership change triggers
    /// in a group that still had members, that is `Stable` to
    /// `PreparingRebalance`. Such a round completes as soon as every still-live
    /// member rejoins.
    pub rebalance_from_empty: bool,
}

impl ClassicGroup {
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            state: GroupState::Empty,
            protocol_type: None,
            generation_id: 0,
            leader_id: None,
            protocol_name: None,
            members: HashMap::new(),
            static_members: HashMap::new(),
            rebalance_deadline: None,
            joined_this_round: std::collections::HashSet::new(),
            rebalance_from_empty: false,
        }
    }

    /// Looks up the current `member_id` pinned to a `group.instance.id`, if
    /// there is one. This is the KIP-345 entry point that every group RPC
    /// handler uses.
    #[must_use]
    pub fn current_member_id_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.static_members.get(instance_id).map(String::as_str)
    }
}
