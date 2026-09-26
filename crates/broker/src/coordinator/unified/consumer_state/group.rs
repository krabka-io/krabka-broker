//! The [`GroupState`] container for a next-gen consumer group and the
//! membership transitions that do not compute an assignment.
//!
//! These are the epoch bump, the add, remove, and session-timeout eviction of
//! members, the static-instance binding, and the `OffsetCommit` epoch fence.
//! Installing a target assignment and reconciling a member against it live in
//! the `reconcile` sibling.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use super::{TargetAssignment, member::MemberState};
use crate::coordinator::unified::{
    expired_member_ids, persistence_next_gen::MemberAssignmentState,
};

#[derive(Debug)]
pub struct GroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, MemberState>,
    pub instance_to_member: HashMap<String, String>,
    pub target: TargetAssignment,
    pub dirty: bool,
    /// The armed rebalance timeouts, by member id: the instant each one fires.
    /// Kafka's `scheduleConsumerGroupRebalanceTimeout` keeps the same deadline
    /// in a timer.
    rebalance_deadlines: HashMap<String, Instant>,
}

impl GroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: 0,
            members: HashMap::new(),
            instance_to_member: HashMap::new(),
            target: TargetAssignment::default(),
            dirty: false,
            rebalance_deadlines: HashMap::new(),
        }
    }

    pub fn bump_epoch(&mut self) -> bool {
        let Some(group_epoch) = crate::metadata_epoch::next_i32(self.group_epoch) else {
            return false;
        };
        self.group_epoch = group_epoch;
        self.dirty = true;
        true
    }

    /// The KIP-848 `OffsetCommit` fencing decision: a member may commit only
    /// with its CURRENT member epoch. `Ok(())` accepts the commit. Any other
    /// result is the Kafka error code.
    ///
    /// This method deliberately does NOT check partition ownership, because
    /// Kafka lets a member with the right epoch commit any partition. The
    /// epoch is the only fence. It therefore rejects a zombie from before a
    /// rebalance, whose epoch the group has since raised.
    ///
    /// The method is pure. It is separate from the actor's `ValidateCommit` so
    /// that the consumer-group composition model can drive the real rule.
    pub(crate) fn validate_commit_decision(&self, member_id: &str, epoch: i32) -> Result<(), i16> {
        match self.members.get(member_id) {
            None => Err(crate::codes::UNKNOWN_MEMBER_ID),
            Some(m) if epoch < m.member_epoch => Err(crate::codes::STALE_MEMBER_EPOCH),
            Some(m) if epoch > m.member_epoch => Err(crate::codes::FENCED_MEMBER_EPOCH),
            Some(_) => Ok(()),
        }
    }

    pub fn add_or_update_member(&mut self, mut m: MemberState) {
        // Ensure the cached compiled regex matches the pattern the caller
        // supplied. Construction sites set `subscribed_topic_regex` via a
        // struct literal and leave `compiled_regex` as `None`; recompile once
        // here so the reconciler never has to.
        m.sync_regex_cache();
        if let Some(iid) = m.instance_id.clone() {
            self.instance_to_member.insert(iid, m.member_id.clone());
        }
        let cached: Option<(HashSet<String>, Option<String>)> =
            self.members.get(&m.member_id).map(|prev| {
                (
                    prev.subscribed_topic_names.clone(),
                    prev.subscribed_topic_regex.clone(),
                )
            });
        let subscription_changed = cached.as_ref().is_none_or(|(names, regex)| {
            names != &m.subscribed_topic_names || regex != &m.subscribed_topic_regex
        });
        self.members.insert(m.member_id.clone(), m);
        if subscription_changed {
            self.dirty = true;
        }
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<MemberState> {
        self.rebalance_deadlines.remove(member_id);
        let m = self.members.remove(member_id)?;
        if let Some(ref iid) = m.instance_id
            && self.instance_to_member.get(iid).map(String::as_str) == Some(member_id)
        {
            self.instance_to_member.remove(iid);
        }
        self.dirty = true;
        Some(m)
    }

    pub fn evict_expired(&mut self, now: Instant, session_timeout: Duration) -> Vec<String> {
        let evicted = expired_member_ids(
            self.members
                .iter()
                .map(|(id, member)| (id.as_str(), member.last_seen)),
            now,
            session_timeout,
        );
        for id in &evicted {
            self.remove_member(id);
        }
        evicted
    }

    /// Arms or cancels the rebalance timeout of `member_id` after a
    /// reconciliation, as Kafka's `GroupMetadataManager.maybeReconcile` does.
    ///
    /// A member that enters the state with partitions to revoke gets a timeout
    /// of its `rebalance_timeout_ms`. The timeout stays armed while the member
    /// stays in that state, so a member that keeps its partitions cannot push
    /// the deadline back with more heartbeats. Kafka gets the same effect
    /// because the member epoch does not move while the member still owns
    /// revoked partitions. Any other state cancels the timeout.
    pub fn track_rebalance_timeout(&mut self, member_id: &str, now: Instant) {
        match self.members.get(member_id) {
            Some(member)
                if member.assignment_state == MemberAssignmentState::UnrevokedPartitions =>
            {
                let timeout = member.rebalance_timeout;
                self.rebalance_deadlines
                    .entry(member_id.to_string())
                    .or_insert(now + timeout);
            }
            _ => {
                self.rebalance_deadlines.remove(member_id);
            }
        }
    }

    /// Cancels the rebalance timeout of every member that no longer has
    /// partitions to revoke.
    ///
    /// A reconciliation can end the obligation of a member that did not send
    /// the heartbeat, for example when another member leaves and the target
    /// gives the pending partition back. Without this pass, a later revocation
    /// of that member would reuse the old, maybe already expired, deadline.
    pub fn prune_rebalance_timeouts(&mut self) {
        let members = &self.members;
        self.rebalance_deadlines.retain(|member_id, _| {
            members.get(member_id).is_some_and(|member| {
                member.assignment_state == MemberAssignmentState::UnrevokedPartitions
            })
        });
    }

    /// The earliest armed rebalance deadline, so the actor can wake at it.
    #[must_use]
    pub fn next_rebalance_deadline(&self) -> Option<Instant> {
        self.rebalance_deadlines.values().min().copied()
    }

    /// Removes every member whose rebalance timeout fired at `now` while the
    /// member still had partitions to revoke. Returns the removed member ids,
    /// sorted.
    ///
    /// This is the fence of Kafka's `scheduleConsumerGroupRebalanceTimeout`
    /// (`consumerGroupFenceMember`). The removal frees the partitions the
    /// member did not revoke, so their new owners can take them.
    ///
    /// A hosted classic member never sends `ConsumerGroupHeartbeat`, so no
    /// heartbeat arms its timeout. This pass arms it the first time it sees
    /// the member with partitions to revoke. Kafka bounds the same member with
    /// its join and sync timers, which also use the rebalance timeout. The
    /// pass also cancels the deadlines that no longer apply, so a past
    /// deadline never stays armed.
    pub fn fence_rebalance_timeouts(&mut self, now: Instant) -> Vec<String> {
        self.prune_rebalance_timeouts();
        let unarmed_classic: Vec<String> = self
            .members
            .values()
            .filter(|member| {
                member.is_classic()
                    && member.assignment_state == MemberAssignmentState::UnrevokedPartitions
                    && !self.rebalance_deadlines.contains_key(&member.member_id)
            })
            .map(|member| member.member_id.clone())
            .collect();
        for member_id in &unarmed_classic {
            self.track_rebalance_timeout(member_id, now);
        }
        let mut fenced: Vec<String> = self
            .rebalance_deadlines
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(member_id, _)| member_id.clone())
            .collect();
        fenced.sort_unstable();
        for member_id in &fenced {
            self.remove_member(member_id);
        }
        fenced
    }

    /// Moves the static member `previous` to the id `member_id` at epoch 0, as
    /// Kafka's `getOrMaybeSubscribeStaticConsumerGroupMember` copies a
    /// released static member for the member that rejoins with its instance
    /// id. The member keeps its subscription, target and assignment, and the
    /// group does not rebalance for the change.
    pub fn replace_static_member(&mut self, previous: &str, member_id: &str) {
        let Some(mut member) = self.members.remove(previous) else {
            return;
        };
        self.rebalance_deadlines.remove(previous);
        // Kafka writes the copy under `member_id` over any member that already
        // has that id. Remove that member through `remove_member`, so its
        // instance id does not keep pointing at the copy.
        if member_id != previous {
            self.remove_member(member_id);
        }
        member.member_id = member_id.to_string();
        member.member_epoch = 0;
        member.previous_member_epoch = 0;
        member.classic = None;
        if let Some(instance_id) = &member.instance_id {
            self.instance_to_member
                .insert(instance_id.clone(), member_id.to_string());
        }
        if let Some(target) = self.target.per_member.remove(previous) {
            self.target.per_member.insert(member_id.to_string(), target);
        }
        self.members.insert(member_id.to_string(), member);
    }

    /// Sets a static member that leaves for a while to epoch -2, as Kafka's
    /// `consumerGroupStaticMemberGroupLeave` does. It keeps its assignment
    /// and drops the partitions it had still to revoke.
    pub fn release_static_member(&mut self, member_id: &str) {
        self.rebalance_deadlines.remove(member_id);
        if let Some(member) = self.members.get_mut(member_id) {
            member.member_epoch = -2;
            member.partitions_pending_revocation.clear();
        }
    }

    pub fn advance_member_epoch(&mut self, member_id: &str) {
        if let Some(m) = self.members.get_mut(member_id) {
            m.previous_member_epoch = m.member_epoch;
            m.member_epoch = self.group_epoch;
        }
    }

    pub fn current_member_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.instance_to_member.get(instance_id).map(String::as_str)
    }

    /// The group state that `ListGroups` reports, from Kafka's
    /// `ConsumerGroup.maybeUpdateGroupState`.
    ///
    /// A group with no members is `Empty`. A group whose epoch is ahead of its
    /// target assignment is `Assigning`. A group with a member that is not
    /// reconciled to the target, which means a member that is not `Stable` or
    /// not at the target epoch, is `Reconciling`. Every other group is
    /// `Stable`.
    #[must_use]
    pub fn state_name(&self) -> &'static str {
        if self.members.is_empty() {
            "Empty"
        } else if self.group_epoch > self.target.epoch {
            "Assigning"
        } else if self.members.values().any(|m| {
            m.assignment_state != MemberAssignmentState::Stable
                || m.member_epoch != self.target.epoch
        }) {
            "Reconciling"
        } else {
            "Stable"
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::consumer_state::test_support::member;

    #[test]
    fn state_name_follows_kafka_consumer_group_state() {
        let at = |epoch, assignment_state| {
            let mut m = member("m1");
            m.member_epoch = epoch;
            m.assignment_state = assignment_state;
            m
        };
        // (group epoch, target epoch, members, expected state)
        let rows = [
            (3, 3, vec![], "Empty"),
            (
                4,
                3,
                vec![at(3, MemberAssignmentState::Stable)],
                "Assigning",
            ),
            (
                3,
                3,
                vec![at(2, MemberAssignmentState::Stable)],
                "Reconciling",
            ),
            (
                3,
                3,
                vec![at(3, MemberAssignmentState::UnreleasedPartitions)],
                "Reconciling",
            ),
            (3, 3, vec![at(3, MemberAssignmentState::Stable)], "Stable"),
        ];
        for (group_epoch, target_epoch, members, expected) in rows {
            let mut g = GroupState::new("g");
            g.group_epoch = group_epoch;
            g.target.epoch = target_epoch;
            for m in members {
                g.members.insert(m.member_id.clone(), m);
            }
            assert!(g.state_name() == expected);
        }
    }

    #[test]
    fn add_member_marks_dirty_first_time() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        assert!(g.dirty);
    }

    #[test]
    fn re_add_same_subscription_keeps_clean_after_reset() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        g.add_or_update_member(member("m1"));
        assert!(!g.dirty);
    }

    #[test]
    fn subscription_change_marks_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        let mut m = member("m1");
        m.subscribed_topic_names.insert("t".into());
        g.add_or_update_member(m);
        assert!(g.dirty);
    }

    #[test]
    fn remove_member_marks_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        g.remove_member("m1");
        assert!(g.dirty);
    }

    #[test]
    fn evict_expired_drops_old_members() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.last_seen = Instant::now().checked_sub(Duration::from_mins(2)).unwrap();
        g.add_or_update_member(m);
        g.add_or_update_member(member("m2"));
        let evicted = g.evict_expired(Instant::now(), Duration::from_mins(1));
        assert!(evicted == vec!["m1".to_string()]);
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn instance_binding_tracked() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.instance_id = Some("inst1".into());
        g.add_or_update_member(m);
        assert!(g.current_member_for_instance("inst1") == Some("m1"));
    }

    /// The rebalance timeout is armed once when a member enters the state with
    /// partitions to revoke, is not pushed back by later reconciliations in
    /// that state, is cancelled by any other state, and is never armed for a
    /// hosted classic member.
    #[test]
    fn rebalance_timeout_arms_once_and_fences_at_the_deadline() {
        struct Row {
            name: &'static str,
            classic: bool,
            /// The member state at the second reconciliation, 30 s later.
            second_state: MemberAssignmentState,
            /// Seconds after the first reconciliation that the check runs.
            check_after_secs: u64,
            fenced: Vec<String>,
        }
        let rows = [
            Row {
                name: "still unrevoked at the deadline",
                classic: false,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 60,
                fenced: vec!["m1".into()],
            },
            Row {
                name: "still unrevoked before the deadline",
                classic: false,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 59,
                fenced: vec![],
            },
            Row {
                name: "revoked before the deadline",
                classic: false,
                second_state: MemberAssignmentState::Stable,
                check_after_secs: 60,
                fenced: vec![],
            },
            Row {
                name: "hosted classic member, same rule",
                classic: true,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 60,
                fenced: vec!["m1".into()],
            },
        ];
        for row in rows {
            let start = Instant::now();
            let mut g = GroupState::new("g");
            let mut m = member("m1");
            m.assignment_state = MemberAssignmentState::UnrevokedPartitions;
            if row.classic {
                m.classic = Some(super::super::ClassicMemberFacade {
                    generation_id: 1,
                    supported_protocols: vec![],
                    session_timeout: Duration::from_secs(45),
                    last_synced_assignment: bytes::Bytes::new(),
                    awaiting_sync: false,
                });
            }
            g.add_or_update_member(m);
            g.track_rebalance_timeout("m1", start);
            g.members.get_mut("m1").unwrap().assignment_state = row.second_state;
            g.track_rebalance_timeout("m1", start + Duration::from_secs(30));
            g.members.get_mut("m1").unwrap().assignment_state =
                MemberAssignmentState::UnrevokedPartitions;

            let fenced =
                g.fence_rebalance_timeouts(start + Duration::from_secs(row.check_after_secs));

            assert!(fenced == row.fenced, "{}", row.name);
            assert!(
                g.members.contains_key("m1") == row.fenced.is_empty(),
                "{}",
                row.name
            );
        }
    }

    /// A reconciliation that ends a member's revocation cancels its deadline,
    /// so a later revocation gets a fresh one. A hosted classic member, which
    /// no heartbeat arms, is armed by the fence pass itself.
    #[test]
    fn stale_deadlines_are_pruned_and_classic_members_are_armed_by_the_sweep() {
        let start = Instant::now();
        let mut g = GroupState::new("g");
        let mut native = member("native");
        native.assignment_state = MemberAssignmentState::UnrevokedPartitions;
        g.add_or_update_member(native);
        g.track_rebalance_timeout("native", start);
        // Another member's change gives the partition back, then takes it
        // away again 50 s later.
        g.members.get_mut("native").unwrap().assignment_state = MemberAssignmentState::Stable;
        g.prune_rebalance_timeouts();
        g.members.get_mut("native").unwrap().assignment_state =
            MemberAssignmentState::UnrevokedPartitions;
        g.track_rebalance_timeout("native", start + Duration::from_secs(50));
        assert!(g.next_rebalance_deadline() == Some(start + Duration::from_secs(110)));
        assert!(
            g.fence_rebalance_timeouts(start + Duration::from_secs(60))
                .is_empty()
        );

        let mut classic = member("classic");
        classic.assignment_state = MemberAssignmentState::UnrevokedPartitions;
        classic.classic = Some(super::super::ClassicMemberFacade {
            generation_id: 1,
            supported_protocols: vec![],
            session_timeout: Duration::from_secs(45),
            last_synced_assignment: bytes::Bytes::new(),
            awaiting_sync: true,
        });
        g.add_or_update_member(classic);
        assert!(g.fence_rebalance_timeouts(start).is_empty());
        assert!(g.next_rebalance_deadline() == Some(start + Duration::from_mins(1)));
        assert!(
            g.fence_rebalance_timeouts(start + Duration::from_mins(1))
                == vec!["classic".to_string()]
        );
    }

    /// A static replacement whose member id another member already holds
    /// takes that id over: the other member and its instance id go, and the
    /// instance index stays coherent.
    #[test]
    fn static_replacement_over_an_occupied_member_id_keeps_the_index_coherent() {
        let mut g = GroupState::new("g");
        let mut released = member("s1");
        released.instance_id = Some("i1".into());
        released.member_epoch = -2;
        g.add_or_update_member(released);
        let mut occupant = member("m1");
        occupant.instance_id = Some("i2".into());
        g.add_or_update_member(occupant);

        g.replace_static_member("s1", "m1");

        let mut members: Vec<(&str, Option<&str>, i32)> = g
            .members
            .values()
            .map(|m| {
                (
                    m.member_id.as_str(),
                    m.instance_id.as_deref(),
                    m.member_epoch,
                )
            })
            .collect();
        members.sort_unstable();
        assert!(members == vec![("m1", Some("i1"), 0)]);
        assert!(g.current_member_for_instance("i1") == Some("m1"));
        assert!(g.current_member_for_instance("i2") == None);
    }

    #[test]
    fn bump_epoch_increments_and_dirties() {
        let mut g = GroupState::new("g");
        g.dirty = false;
        assert!(g.bump_epoch());
        assert!(g.group_epoch == 1);
        assert!(g.dirty);
    }

    #[test]
    fn bump_epoch_rejects_exhaustion() {
        let mut group = GroupState::new("g");
        group.group_epoch = i32::MAX;

        assert!(!group.bump_epoch());
        assert!(group.group_epoch == i32::MAX);
    }

    #[test]
    fn advance_member_epoch_records_previous() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.group_epoch = 5;
        g.advance_member_epoch("m1");
        let m = &g.members["m1"];
        assert!(m.member_epoch == 5);
        assert!(m.previous_member_epoch == 0);
    }
}
