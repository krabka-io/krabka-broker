//! Rebalance-round transitions on a [`ClassicGroup`]: the early-completion
//! check, generation and leader selection, selected-protocol metadata
//! resolution, and assignment install.
//!
//! Together these carry a round from `PreparingRebalance` through
//! `CompletingRebalance` to `Stable`.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::Bytes;

use super::group::{ClassicGroup, GroupState, InitialDelayedJoin};

impl ClassicGroup {
    /// True once every member currently in `members` has sent a `JoinGroup`
    /// since the last transition into `PreparingRebalance`. The `JoinGroup`
    /// handler uses this to end the rebalance wait as soon as all members are
    /// accounted for, so the leader runs the assignor on a fresh snapshot of
    /// every member's owned set.
    #[must_use]
    pub fn all_members_joined_this_round(&self) -> bool {
        if self.members.is_empty() {
            return false;
        }
        self.members
            .keys()
            .all(|id| self.joined_this_round.contains(id))
    }

    /// Kafka's `ClassicGroup.hasAllMembersJoined`: every member waits in
    /// `JoinGroup`, and no `MEMBER_ID_REQUIRED` id is still outstanding.
    #[must_use]
    pub fn has_all_members_joined(&self) -> bool {
        self.pending_members.is_empty()
            && self
                .members
                .keys()
                .all(|id| self.joined_this_round.contains(id))
    }

    /// Kafka's `ClassicGroup.canRebalance`: the states `PreparingRebalance`
    /// may be entered from.
    #[must_use]
    pub fn can_rebalance(&self) -> bool {
        matches!(
            self.state,
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance
        )
    }

    /// Kafka's `ClassicGroup.rebalanceTimeoutMs`: the largest rebalance
    /// timeout of all members.
    #[must_use]
    pub fn rebalance_timeout(&self) -> Duration {
        self.members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or_default()
    }

    /// Kafka's `GroupMetadataManager.prepareRebalance`, the state part. It
    /// moves the group to `PreparingRebalance` and arms the join deadline.
    ///
    /// A round that opens from `Empty` runs Kafka's `InitialDelayedJoin`: the
    /// deadline is the initial delay, and the rest of the group rebalance
    /// timeout is left to extend into. Any other round waits for the group
    /// rebalance timeout. No member counts as joined until it calls
    /// [`Self::mark_awaiting_join`].
    pub fn prepare_rebalance(&mut self, initial_rebalance_delay: Duration, now: Instant) {
        let initial = self.state == GroupState::Empty;
        if initial {
            self.initial_join = Some(InitialDelayedJoin {
                delay: initial_rebalance_delay,
                remaining: self
                    .rebalance_timeout()
                    .saturating_sub(initial_rebalance_delay),
            });
            self.rebalance_deadline = Some(now + initial_rebalance_delay);
        } else {
            self.initial_join = None;
            self.rebalance_deadline = Some(now + self.rebalance_timeout());
        }
        self.new_member_added = false;
        self.rebalance_from_empty = initial;
        self.state = GroupState::PreparingRebalance;
        self.joined_this_round.clear();
    }

    /// Kafka's `tryCompleteInitialRebalanceElseSchedule`, run when the
    /// rebalance deadline fires. It returns `false` and re-arms the deadline
    /// when a new member joined during the initial delay and time is left,
    /// and `true` when the round must complete now.
    pub fn rebalance_deadline_fired(
        &mut self,
        initial_rebalance_delay: Duration,
        now: Instant,
    ) -> bool {
        match self.initial_join {
            Some(InitialDelayedJoin { delay, remaining })
                if self.new_member_added && !remaining.is_zero() =>
            {
                self.new_member_added = false;
                let next = initial_rebalance_delay.min(remaining);
                self.initial_join = Some(InitialDelayedJoin {
                    delay: next,
                    remaining: remaining.saturating_sub(delay),
                });
                self.rebalance_deadline = Some(now + next);
                false
            }
            _ => true,
        }
    }

    /// Kafka's `ClassicGroup.maybeElectNewJoinedLeader`. The current leader
    /// stays while it waits in `JoinGroup`. Otherwise a member that waits
    /// takes over; Kafka takes the first in hash-map order, this the smallest
    /// member id. It returns `false` when no member waits.
    pub fn maybe_elect_new_joined_leader(&mut self) -> bool {
        if let Some(leader) = self.leader_id.as_deref()
            && self.members.contains_key(leader)
            && self.joined_this_round.contains(leader)
        {
            return true;
        }
        match self
            .members
            .keys()
            .filter(|id| self.joined_this_round.contains(*id))
            .min()
            .cloned()
        {
            Some(leader) => {
                self.leader_id = Some(leader);
                true
            }
            None => false,
        }
    }

    /// Completes the rebalance. The current leader stays if it is still a
    /// member, as Kafka's leader does. A group with no leader takes the
    /// smallest member id. It then raises the generation and advances the
    /// state.
    pub fn complete_rebalance(&mut self, protocol_name: impl Into<String>) -> bool {
        let Some(generation_id) = crate::metadata_epoch::next_i32(self.generation_id) else {
            return false;
        };
        let leader = match self.leader_id.as_deref() {
            Some(leader) if self.members.contains_key(leader) => leader.to_string(),
            _ => self
                .members
                .keys()
                .min()
                .cloned()
                .expect("complete_rebalance requires ≥1 member"),
        };
        self.leader_id = Some(leader);
        self.protocol_name = Some(protocol_name.into());
        self.generation_id = generation_id;
        self.state = GroupState::CompletingRebalance;
        self.rebalance_deadline = None;
        self.joined_this_round.clear();
        self.rebalance_from_empty = false;
        self.initial_join = None;
        self.new_member_added = false;
        true
    }

    /// Sets each member's `protocol_metadata` to its proposal for `name`. A
    /// member that did not propose `name` keeps its existing metadata. That
    /// case should not arise after a successful `select_protocol`.
    pub fn resolve_selected_protocol_metadata(&mut self, name: &str) {
        for m in self.members.values_mut() {
            if let Some((_, bytes)) = m.protocols.iter().find(|(n, _)| n == name) {
                m.protocol_metadata = bytes.clone();
            }
        }
    }

    /// Runs when the leader's `SyncGroup` arrives with assignments. It stores
    /// each member's `assignment` and moves the group to `Stable`.
    pub fn install_assignments(&mut self, assignments: HashMap<String, Bytes>) {
        for (member_id, bytes) in assignments {
            if let Some(m) = self.members.get_mut(&member_id) {
                m.assignment = Some(bytes);
            }
        }
        self.state = GroupState::Stable;
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::classic_state::test_support::{
        member_with_protocols, sample_member,
    };

    #[test]
    fn complete_rebalance_bumps_generation() {
        let mut g = ClassicGroup::new("g");
        g.add_member(sample_member("m1"));
        g.add_member(sample_member("m2"));
        assert!(g.complete_rebalance("range"));
        check!(g.generation_id == 1);
        check!(g.leader_id.as_deref() == Some("m1"));
        check!(g.protocol_name.as_deref() == Some("range"));
        check!(g.state == GroupState::CompletingRebalance);
    }

    #[test]
    fn complete_rebalance_rejects_generation_exhaustion_without_mutation() {
        let mut group = ClassicGroup::new("g");
        group.add_member(sample_member("m1"));
        group.generation_id = i32::MAX;

        assert!(!group.complete_rebalance("range"));
        assert!(group.generation_id == i32::MAX);
        assert!(group.leader_id.is_none());
        assert!(group.protocol_name.is_none());
    }

    #[test]
    fn install_assignments_to_stable() {
        let mut g = ClassicGroup::new("g");
        g.add_member(sample_member("m1"));
        g.complete_rebalance("range");
        let mut a = HashMap::new();
        a.insert("m1".into(), Bytes::from_static(b"assignment-bytes"));
        g.install_assignments(a);
        assert!(g.state == GroupState::Stable);
        assert!(g.members["m1"].assignment.is_some());
    }

    #[test]
    fn resolve_metadata_updates_each_member() {
        let mut g = ClassicGroup::new("g");
        g.add_member(member_with_protocols(
            "m1",
            vec![("range", b"r1"), ("cooperative_sticky", b"c1")],
        ));
        g.add_member(member_with_protocols(
            "m2",
            vec![("range", b"r2"), ("cooperative_sticky", b"c2")],
        ));
        g.resolve_selected_protocol_metadata("cooperative_sticky");
        assert!(g.members["m1"].protocol_metadata.as_ref() == b"c1");
        assert!(g.members["m2"].protocol_metadata.as_ref() == b"c2");
    }

    /// #790: Kafka's `InitialDelayedJoin`. Each new member in the delay
    /// extends it by the initial delay, up to the group rebalance timeout.
    #[test]
    fn initial_delay_extends_per_new_member_up_to_the_rebalance_timeout() {
        let delay = Duration::from_secs(3);
        let mut g = ClassicGroup::new("g");
        let mut m = sample_member("m1");
        m.rebalance_timeout = Duration::from_secs(10);
        g.members.insert("m1".into(), m);
        let now = Instant::now();
        g.prepare_rebalance(delay, now);
        check!(g.rebalance_deadline == Some(now + delay));
        let secs = Duration::from_secs;
        // (new member joined before the fire, completes, delay, remaining)
        for (new_member, completes, next, remaining) in [
            (true, false, secs(3), secs(4)),
            (true, false, secs(3), secs(1)),
            (true, false, secs(1), secs(0)),
            (true, true, secs(1), secs(0)),
        ] {
            g.new_member_added = new_member;
            check!(g.rebalance_deadline_fired(delay, now) == completes);
            check!(
                g.initial_join
                    == Some(InitialDelayedJoin {
                        delay: next,
                        remaining,
                    })
            );
            if !completes {
                check!(g.rebalance_deadline == Some(now + next));
            }
        }
    }

    #[test]
    fn rebalance_deadline_without_new_member_completes() {
        let mut g = ClassicGroup::new("g");
        g.members.insert("m1".into(), sample_member("m1"));
        g.prepare_rebalance(Duration::from_secs(3), Instant::now());
        check!(g.rebalance_deadline_fired(Duration::from_secs(3), Instant::now()));
    }

    /// #790: Kafka's `maybeElectNewJoinedLeader`.
    #[test]
    fn leader_stays_while_it_joined() {
        for (name, leader, joined, want_elected, want_leader) in [
            (
                "leader joined",
                Some("m2"),
                &["m1", "m2"][..],
                true,
                Some("m2"),
            ),
            (
                "leader missed the round",
                Some("m2"),
                &["m3", "m1"][..],
                true,
                Some("m1"),
            ),
            ("leader gone", Some("gone"), &["m3"][..], true, Some("m3")),
            ("nobody joined", Some("m2"), &[][..], false, Some("m2")),
        ] {
            let mut g = ClassicGroup::new("g");
            for id in ["m1", "m2", "m3"] {
                g.members.insert(id.into(), sample_member(id));
            }
            g.leader_id = leader.map(str::to_string);
            for id in joined {
                g.mark_awaiting_join(id);
            }
            check!(g.maybe_elect_new_joined_leader() == want_elected, "{name}");
            check!(g.leader_id.as_deref() == want_leader, "{name}");
        }
    }
}
