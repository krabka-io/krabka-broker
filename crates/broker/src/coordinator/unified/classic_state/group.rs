//! The [`ClassicGroup`] record and its [`GroupState`] four-state machine.
//!
//! This module holds the group's fields and the two accessors that do not
//! change membership or drive a rebalance round. The transitions live in the
//! `membership` and `rebalance` siblings.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

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
    /// Kafka's `pendingJoinMembers`: the member ids a `JoinGroup` v4+ handed
    /// out with `MEMBER_ID_REQUIRED` and that have not joined with them yet,
    /// each with the instant its session timeout removes it. A `JoinGroup`
    /// with one of these ids joins as a new member. Any other unknown id gets
    /// `UNKNOWN_MEMBER_ID`.
    pub pending_members: HashMap<String, Instant>,
    /// Kafka's `InitialDelayedJoin`, while a round that opened from `Empty`
    /// waits out `group.initial.rebalance.delay.ms`. `None` in every other
    /// round.
    pub initial_join: Option<InitialDelayedJoin>,
    /// Kafka's `newMemberAdded`: a new member joined during the current
    /// initial delay, so the delay extends once more when it fires.
    pub new_member_added: bool,
}

/// The state of Kafka's `InitialDelayedJoin` timer: the delay that is running
/// now and the part of the group rebalance timeout still left to extend into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialDelayedJoin {
    pub delay: Duration,
    pub remaining: Duration,
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
            pending_members: HashMap::new(),
            initial_join: None,
            new_member_added: false,
        }
    }

    /// Looks up the current `member_id` pinned to a `group.instance.id`, if
    /// there is one. This is the KIP-345 entry point that every group RPC
    /// handler uses.
    #[must_use]
    pub fn current_member_id_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.static_members.get(instance_id).map(String::as_str)
    }

    /// Kafka's `ClassicGroup.validateMember`, the member check of the classic
    /// `Heartbeat` and `SyncGroup`.
    ///
    /// An instance id that no member holds answers `UNKNOWN_MEMBER_ID`, so a
    /// static member whose session expired can join again. An instance id that
    /// another member holds answers `FENCED_INSTANCE_ID`. A member id the
    /// group does not hold answers `UNKNOWN_MEMBER_ID`.
    ///
    /// # Errors
    /// Returns the Kafka error code of the failed check.
    pub fn validate_member(&self, member_id: &str, instance_id: Option<&str>) -> Result<(), i16> {
        if let Some(instance_id) = instance_id {
            match self.current_member_id_for_instance(instance_id) {
                None => return Err(crate::codes::UNKNOWN_MEMBER_ID),
                Some(pinned) if pinned != member_id => {
                    return Err(crate::codes::FENCED_INSTANCE_ID);
                }
                Some(_) => {}
            }
        }
        if self.members.contains_key(member_id) {
            Ok(())
        } else {
            Err(crate::codes::UNKNOWN_MEMBER_ID)
        }
    }
}
