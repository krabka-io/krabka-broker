//! Rebalance-round transitions on a [`ClassicGroup`]: the early-completion
//! check, generation and leader selection, selected-protocol metadata
//! resolution, and assignment install.
//!
//! Together these carry a round from `PreparingRebalance` through
//! `CompletingRebalance` to `Stable`.

use std::collections::HashMap;

use bytes::Bytes;

use super::group::{ClassicGroup, GroupState};

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

    /// Completes the rebalance. It picks the leader, where the oldest
    /// `member_id` wins, which keeps the choice stable for tests. It then
    /// raises the generation and advances the state.
    pub fn complete_rebalance(&mut self, protocol_name: impl Into<String>) {
        let leader = self
            .members
            .keys()
            .min()
            .cloned()
            .expect("complete_rebalance requires ≥1 member");
        self.leader_id = Some(leader);
        self.protocol_name = Some(protocol_name.into());
        self.generation_id += 1;
        self.state = GroupState::CompletingRebalance;
        self.rebalance_deadline = None;
        self.joined_this_round.clear();
        self.rebalance_from_empty = false;
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
        g.complete_rebalance("range");
        check!(g.generation_id == 1);
        check!(g.leader_id.as_deref() == Some("m1"));
        check!(g.protocol_name.as_deref() == Some("range"));
        check!(g.state == GroupState::CompletingRebalance);
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
}
