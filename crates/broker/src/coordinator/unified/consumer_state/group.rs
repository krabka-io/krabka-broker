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
    /// A member of the consumer protocol that enters the state with partitions
    /// to revoke gets a timeout of its `rebalance_timeout_ms`. The timeout
    /// stays armed while the member stays in that state, so a member that keeps
    /// its partitions cannot push the deadline back with more heartbeats. Kafka
    /// gets the same effect because the member epoch does not move while the
    /// member still owns revoked partitions. Any other state cancels the
    /// timeout. A classic member hosted in the group has only the join and sync
    /// timers in Kafka, so it gets none.
    pub fn track_rebalance_timeout(&mut self, member_id: &str, now: Instant) {
        match self.members.get(member_id) {
            Some(member)
                if !member.is_classic()
                    && member.assignment_state == MemberAssignmentState::UnrevokedPartitions =>
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

    /// Removes every member whose rebalance timeout fired at `now` while the
    /// member still had partitions to revoke. Returns the removed member ids,
    /// sorted.
    ///
    /// This is the fence of Kafka's `scheduleConsumerGroupRebalanceTimeout`
    /// (`consumerGroupFenceMember`). The removal frees the partitions the
    /// member did not revoke, so their new owners can take them.
    pub fn fence_rebalance_timeouts(&mut self, now: Instant) -> Vec<String> {
        let mut fenced: Vec<String> = self
            .rebalance_deadlines
            .iter()
            .filter(|(member_id, deadline)| {
                now >= **deadline
                    && self.members.get(*member_id).is_some_and(|member| {
                        member.assignment_state == MemberAssignmentState::UnrevokedPartitions
                    })
            })
            .map(|(member_id, _)| member_id.clone())
            .collect();
        fenced.sort_unstable();
        for member_id in &fenced {
            self.remove_member(member_id);
        }
        fenced
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
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::consumer_state::test_support::member;

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
                name: "hosted classic member",
                classic: true,
                second_state: MemberAssignmentState::UnrevokedPartitions,
                check_after_secs: 60,
                fenced: vec![],
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
