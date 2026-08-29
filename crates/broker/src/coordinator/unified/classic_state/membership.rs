//! Membership transitions on a [`ClassicGroup`]: adding, removing, and
//! session-timeout expiry of members.
//!
//! These carry the KIP-345 static-membership rules, where a static slot
//! survives a session timeout and a rejoin replaces it in place, and the
//! per-round join bookkeeping the `JoinGroup` handler reads.

use std::time::{Duration, Instant};

use super::{
    group::{ClassicGroup, GroupState},
    member::{AddMemberOutcome, Member},
};

impl ClassicGroup {
    /// Add or refresh a member.
    ///
    /// **Dynamic** (no `group_instance_id`): the group inserts the member and
    /// moves to `PreparingRebalance` if it was `Empty` or `Stable` before. It
    /// returns [`AddMemberOutcome::NewMember`].
    ///
    /// **Static** (KIP-345, `group_instance_id` set): three cases.
    /// 1. The instance id is new. This behaves like a dynamic add and returns
    ///    [`AddMemberOutcome::NewMember`].
    /// 2. The instance id maps to an existing live `member_id` that matches
    ///    the incoming `member.member_id`. It also matches when the incoming
    ///    id differs but the leader-side bootstrap-rejoin path supplied a
    ///    fresh id for the same instance. The group replaces the slot in
    ///    place and keeps the prior `assignment` and the group's `state`, so a
    ///    `Stable` rejoin triggers no rebalance. It returns
    ///    [`AddMemberOutcome::StaticRejoin`] with the prior member id.
    /// 3. The instance id maps to a different live `member_id` and the caller
    ///    did not ask for a takeover. The caller must then reject the request
    ///    with `FENCED_INSTANCE_ID`, and this method returns
    ///    [`AddMemberOutcome::Fenced`]. The handler decides whether a
    ///    non-empty mismatched `req.member_id` is a real fence or a valid
    ///    replacement. `add_member` itself always does the takeover unless the
    ///    caller checks first with
    ///    [`Self::current_member_id_for_instance`].
    pub fn add_member(&mut self, member: Member) -> AddMemberOutcome {
        if let Some(instance_id) = member.group_instance_id.clone() {
            if let Some(prior_member_id) = self.static_members.get(&instance_id).cloned() {
                // Static rejoin: replace slot in-place, preserve assignment.
                let prior = self.members.remove(&prior_member_id);
                let mut next = member;
                if let Some(p) = prior {
                    // Inherit the previously installed assignment so a
                    // Stable-state rejoin can short-circuit the rebalance.
                    if next.assignment.is_none() {
                        next.assignment = p.assignment;
                    }
                }
                let new_member_id = next.id.clone();
                self.static_members
                    .insert(instance_id, new_member_id.clone());
                self.members.insert(new_member_id.clone(), next);
                // If this static rejoin lands during PreparingRebalance,
                // count the (potentially renamed) member as joined so the
                // early-completion check still fires.
                if matches!(self.state, GroupState::PreparingRebalance) {
                    self.joined_this_round.remove(&prior_member_id);
                    self.joined_this_round.insert(new_member_id);
                }
                // Crucially: do NOT touch self.state. Static rejoin from
                // Stable stays Stable; from PreparingRebalance stays
                // PreparingRebalance.
                return AddMemberOutcome::StaticRejoin { prior_member_id };
            }
            // Brand-new instance id: pin it and fall through to a
            // dynamic-style add.
            self.static_members.insert(instance_id, member.id.clone());
        }
        let was_empty = matches!(self.state, GroupState::Empty);
        // Also restart a rebalance when a new member joins while stuck in
        // CompletingRebalance. Real Kafka (KIP-62 AwaitingSync) transitions
        // back to PreparingRebalance on any new join so a dead leader's
        // CompletingRebalance deadlock doesn't strand the group forever.
        let was_first_or_stable = matches!(
            self.state,
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance
        );
        let member_id = member.id.clone();
        self.members.insert(member_id.clone(), member);
        if was_first_or_stable {
            self.state = GroupState::PreparingRebalance;
            self.joined_this_round.clear();
            // A round that opens from `Empty` batches the startup herd over
            // the configured initial delay (see `rebalance_from_empty`); one that
            // opens from `Stable` is a live-membership change and
            // eager-completes once every still-live member rejoins.
            self.rebalance_from_empty = was_empty;
        }
        if matches!(self.state, GroupState::PreparingRebalance) {
            self.joined_this_round.insert(member_id);
        }
        AddMemberOutcome::NewMember
    }

    /// Removes a member. The group moves to `Empty` if no member remains.
    /// KIP-345: this also clears the static-membership index entry.
    pub fn remove_member(&mut self, member_id: &str) {
        if let Some(m) = self.members.remove(member_id)
            && let Some(ref iid) = m.group_instance_id
        {
            // Only clear the index entry if it still points at *this*
            // member_id — a takeover may have already repointed it.
            if self.static_members.get(iid).map(String::as_str) == Some(member_id) {
                self.static_members.remove(iid);
            }
        }
        self.joined_this_round.remove(member_id);
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.leader_id = None;
            self.protocol_name = None;
            self.rebalance_deadline = None;
        }
    }

    /// Drops any **dynamic** member whose `last_heartbeat` is older than its
    /// `session_timeout`. It returns the dropped member IDs. The group moves
    /// to `PreparingRebalance` when it dropped at least one member and still
    /// has members. It moves to `Empty` when it became empty.
    ///
    /// KIP-345: this method **skips** static members, those with
    /// `group_instance_id.is_some()`. Their slot survives the session timeout,
    /// so a restarting client reclaims its assignment on rejoin without a
    /// rebalance for the rest of the group.
    pub fn expire_dead_members(
        &mut self,
        now: Instant,
        initial_rebalance_delay: Duration,
    ) -> Vec<String> {
        // Expire members in all active states (PreparingRebalance,
        // CompletingRebalance, Stable). Empty and Dead have no members to
        // expire. Importantly, expiring in CompletingRebalance lets the broker
        // evict a dead group leader that will never send SyncGroup — otherwise
        // the group is permanently stuck: expire_dead_members only ran in
        // Stable (post-SyncGroup), so a dead leader stranded the group forever.
        //
        // The original Stable-only guard existed to prevent a race where a
        // member with a very small session_timeout could self-evict before its
        // JoinGroup returned (last_heartbeat = add_member time, session_timeout
        // equal to the initial rebalance delay). With the default session timeout
        // 45s and a 3s rebalance delay this race is impossible in practice, and
        // real Kafka expires members regardless of group state.
        if self.state == GroupState::Empty {
            return Vec::new();
        }
        let dropped: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| {
                !m.is_static() && now.duration_since(m.last_heartbeat) > m.session_timeout
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dropped {
            // Dynamic members only — no static_members entry to clear.
            self.members.remove(id);
            // Keep per-round join tracking consistent with `members`: a member
            // expired mid-`PreparingRebalance` (it joined this round, then timed
            // out) must not linger in `joined_this_round`, or the
            // `joined_this_round ⊆ members` invariant breaks and
            // `all_members_joined_this_round` could count a ghost. Mirrors the
            // cleanup `remove_member` already performs.
            self.joined_this_round.remove(id);
        }
        if !dropped.is_empty() {
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.leader_id = None;
                self.protocol_name = None;
                self.rebalance_deadline = None;
                self.joined_this_round.clear();
                self.rebalance_from_empty = false;
            } else {
                self.state = GroupState::PreparingRebalance;
                // Live-membership change (a member timed out), not a
                // start-from-empty herd: eager-complete once the survivors
                // rejoin rather than holding the initial-delay window.
                self.rebalance_from_empty = false;
                // If we just evicted from CompletingRebalance, rebalance_deadline
                // is None (complete_rebalance clears it). Set a fresh deadline so
                // parked joiners/followers wake up via the actor's opt_sleep path
                // rather than waiting indefinitely for a rejoin that never comes.
                if self.rebalance_deadline.is_none() {
                    self.rebalance_deadline = Some(now + initial_rebalance_delay);
                }
            }
        }
        dropped
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::{assert, check};
    use bytes::Bytes;

    use super::*;
    use crate::coordinator::unified::classic_state::test_support::{sample_member, static_member};

    #[test]
    fn empty_to_preparing_on_first_join() {
        let mut g = ClassicGroup::new("g");
        assert!(g.state == GroupState::Empty);
        g.add_member(sample_member("m1"));
        assert!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn remove_last_member_empties_group() {
        let mut g = ClassicGroup::new("g");
        g.add_member(sample_member("m1"));
        g.remove_member("m1");
        assert!(g.state == GroupState::Empty);
        assert!(g.leader_id.is_none());
    }

    #[test]
    fn static_rejoin_preserves_stable_state_and_assignment() {
        let mut g = ClassicGroup::new("g");
        let outcome = g.add_member(static_member("m1", "inst-a"));
        assert!(outcome == AddMemberOutcome::NewMember);
        g.complete_rebalance("range");
        let mut a = HashMap::new();
        a.insert("m1".into(), Bytes::from_static(b"assignment-bytes"));
        g.install_assignments(a);
        assert!(g.state == GroupState::Stable);

        // Rejoin with the same instance id but a fresh `member_id` (the
        // client restarted; KIP-394 bootstrap gave it a new id).
        let outcome = g.add_member(static_member("m2", "inst-a"));
        check!(
            outcome
                == AddMemberOutcome::StaticRejoin {
                    prior_member_id: "m1".into()
                }
        );
        // State preserved: no rebalance kicked off.
        check!(g.state == GroupState::Stable);
        check!(g.members.len() == 1);
        // New member inherited the prior assignment.
        check!(g.members.contains_key("m2"));
        check!(g.members["m2"].assignment.as_deref() == Some(b"assignment-bytes" as &[u8]));
        // Index repointed.
        check!(g.current_member_id_for_instance("inst-a") == Some("m2"));
    }

    #[test]
    fn static_member_timeout_is_suppressed() {
        let mut g = ClassicGroup::new("g");
        let mut m = static_member("m1", "inst-a");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;

        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));
        check!(dropped.is_empty(), "static member must NOT be expired");
        check!(g.state == GroupState::Stable);
        check!(g.members.contains_key("m1"));
        // Index entry retained.
        check!(g.current_member_id_for_instance("inst-a") == Some("m1"));
    }

    #[test]
    fn dynamic_member_timeout_still_drops_in_mixed_group() {
        let mut g = ClassicGroup::new("g");
        g.add_member(static_member("static-1", "inst-a"));
        let mut dyn_m = sample_member("dyn-1");
        dyn_m.session_timeout = Duration::from_millis(1);
        dyn_m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(dyn_m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;

        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));
        check!(dropped == vec!["dyn-1".to_string()]);
        check!(g.state == GroupState::PreparingRebalance);
        check!(g.members.contains_key("static-1"));
    }

    #[test]
    fn completing_rebalance_expiry_uses_configured_initial_delay() {
        let mut g = ClassicGroup::new("g");
        let mut stale = sample_member("stale");
        stale.session_timeout = Duration::from_millis(1);
        stale.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(stale);
        g.add_member(sample_member("survivor"));
        g.complete_rebalance("range");
        check!(g.state == GroupState::CompletingRebalance);
        check!(g.rebalance_deadline.is_none());

        let delay = Duration::from_millis(19);
        let now = Instant::now();
        let dropped = g.expire_dead_members(now, delay);

        check!(dropped == vec!["stale".to_string()]);
        check!(g.state == GroupState::PreparingRebalance);
        assert!(g.rebalance_deadline == Some(now + delay));
    }

    #[test]
    fn remove_static_member_clears_index() {
        let mut g = ClassicGroup::new("g");
        g.add_member(static_member("m1", "inst-a"));
        assert!(g.current_member_id_for_instance("inst-a") == Some("m1"));
        g.remove_member("m1");
        assert!(g.current_member_id_for_instance("inst-a") == None);
        assert!(g.state == GroupState::Empty);
    }

    #[test]
    fn static_takeover_does_not_clear_index_for_prior_id() {
        // After a takeover, the index points at the new id. Removing the
        // *prior* id (e.g. via a stale LeaveGroup from the old session)
        // must NOT wipe the index entry.
        let mut g = ClassicGroup::new("g");
        g.add_member(static_member("m1", "inst-a"));
        g.add_member(static_member("m2", "inst-a"));
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
        // m1 is no longer in members (replaced), so this is a no-op.
        g.remove_member("m1");
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn expire_dead_members_drops_stale() {
        let mut g = ClassicGroup::new("g");
        let mut m = sample_member("m1");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;
        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));
        assert!(dropped == vec!["m1".to_string()]);
        assert!(g.state == GroupState::Empty);
    }

    #[test]
    fn expire_last_dead_member_clears_rebalance_bookkeeping() {
        let mut g = ClassicGroup::new("g");
        let mut m = sample_member("m1");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        g.rebalance_deadline = Some(Instant::now() + Duration::from_secs(3));
        g.rebalance_from_empty = true;
        assert!(g.joined_this_round.contains("m1"));

        let dropped = g.expire_dead_members(Instant::now(), Duration::from_secs(3));

        check!(dropped == vec!["m1".to_string()]);
        check!(g.state == GroupState::Empty);
        check!(g.leader_id.is_none());
        check!(g.protocol_name.is_none());
        check!(g.rebalance_deadline.is_none());
        check!(g.joined_this_round.is_empty());
        check!(!g.rebalance_from_empty);
    }
}
