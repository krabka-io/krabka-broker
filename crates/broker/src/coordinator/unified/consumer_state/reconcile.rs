//! Target-assignment reconciliation for a next-gen consumer group: installing
//! a freshly computed target and turning it into one member's current
//! assignment.
//!
//! This is the KIP-848 `CurrentAssignmentBuilder`, the safety-critical half of
//! the group state. It decides which partitions a member keeps, which it must
//! revoke, and which it may claim, so that the broker never advertises one
//! partition to two members at once.

use std::collections::{HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use super::group::GroupState;
use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;

impl GroupState {
    pub fn install_target(&mut self, per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>) {
        self.target.epoch = self.group_epoch;
        self.target.per_member = per_member;
        for (mid, member) in &mut self.members {
            let target = self.target.per_member.get(mid).cloned().unwrap_or_default();
            // Split everything the member still holds — its current assignment
            // PLUS any partitions already pending revocation — against the new
            // target. Splitting `assigned ∪ pending` (not just `assigned`)
            // preserves in-flight revocations across successive reconciles, so a
            // partition a member is still releasing is never mistaken for "free"
            // by the withholding in `reconcile_member`.
            let mut held = member.assigned_partitions.clone();
            for (tid, parts) in &member.partitions_pending_revocation {
                held.entry(*tid).or_default().extend(parts.iter().copied());
            }
            let (revoke, assigned) = compute_revoke_split(&held, &target);
            member.partitions_pending_revocation = revoke;
            member.assigned_partitions = assigned;
            member.assignment_state = if !member.partitions_pending_revocation.is_empty() {
                MemberAssignmentState::UnrevokedPartitions
            } else if assignment_covers(&member.assigned_partitions, &target) {
                MemberAssignmentState::Stable
            } else {
                MemberAssignmentState::UnreleasedPartitions
            };
        }
    }

    /// KIP-848 per-heartbeat reconciliation, the `CurrentAssignmentBuilder`.
    ///
    /// It computes the authoritative *current* assignment for `member_id` from
    /// the member's reported owned set and the group target. It **withholds
    /// any partition that another member still holds**, whether that member
    /// owns it or has it pending revocation.
    ///
    /// The returned set is what the coordinator grants the member now and
    /// advertises in the heartbeat response. Storing it as
    /// `assigned_partitions` makes the grant authoritative, so the broker
    /// never advertises a partition to two members at once. That is the main
    /// KIP-848 safety property. See `reconciler_model.rs`.
    ///
    /// A member keeps every target partition it already owns, and gains target
    /// partitions that are *free*. A partition it owns but no longer targets
    /// moves to `partitions_pending_revocation`. Another member claims a freed
    /// partition only once its previous owner reports that it released the
    /// partition. That owner's next heartbeat drops the partition from
    /// `reported_owned`, which drains it from both sets.
    ///
    /// It returns `true` when the member's assignment or pending set
    /// changed.
    pub fn reconcile_member(
        &mut self,
        member_id: &str,
        reported_owned: &HashMap<Uuid, Vec<i32>>,
    ) -> bool {
        let target = self
            .target
            .per_member
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        // `assigned ∪ pending` of every OTHER member is exactly what that member
        // still holds; it is invariant under `install_target`'s keep/revoke split.
        let mut held_by_others: HashSet<(Uuid, i32)> = HashSet::new();
        for (mid, m) in &self.members {
            if mid == member_id {
                continue;
            }
            for (tid, parts) in m
                .assigned_partitions
                .iter()
                .chain(m.partitions_pending_revocation.iter())
            {
                for &p in parts {
                    held_by_others.insert((*tid, p));
                }
            }
        }
        // Grant each target partition the member already owns OR that is free.
        let mut new_assigned: HashMap<Uuid, Vec<i32>> = HashMap::new();
        let mut fully_assigned = true;
        for (tid, tparts) in &target {
            for &p in tparts {
                let owned_here = reported_owned.get(tid).is_some_and(|o| o.contains(&p));
                let free = !held_by_others.contains(&(*tid, p));
                if owned_here || free {
                    new_assigned.entry(*tid).or_default().push(p);
                } else {
                    fully_assigned = false;
                }
            }
        }
        // Pending revocation = reported-owned partitions no longer in the target.
        let mut new_pending: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for (tid, oparts) in reported_owned {
            let tset: HashSet<i32> = target
                .get(tid)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
            for &p in oparts {
                if !tset.contains(&p) {
                    new_pending.entry(*tid).or_default().push(p);
                }
            }
        }
        for v in new_assigned.values_mut() {
            v.sort_unstable();
        }
        for v in new_pending.values_mut() {
            v.sort_unstable();
        }
        let m = self
            .members
            .get_mut(member_id)
            .expect("member exists in reconcile_member");
        let changed =
            m.assigned_partitions != new_assigned || m.partitions_pending_revocation != new_pending;
        m.assigned_partitions = new_assigned;
        m.partitions_pending_revocation = new_pending;
        m.assignment_state = if !m.partitions_pending_revocation.is_empty() {
            MemberAssignmentState::UnrevokedPartitions
        } else if fully_assigned {
            MemberAssignmentState::Stable
        } else {
            MemberAssignmentState::UnreleasedPartitions
        };
        changed
    }
}

/// True if `assigned` contains every partition in `target`.
fn assignment_covers(assigned: &HashMap<Uuid, Vec<i32>>, target: &HashMap<Uuid, Vec<i32>>) -> bool {
    target.iter().all(|(tid, tparts)| {
        tparts
            .iter()
            .all(|p| assigned.get(tid).is_some_and(|a| a.contains(p)))
    })
}

fn compute_revoke_split(
    current: &HashMap<Uuid, Vec<i32>>,
    target: &HashMap<Uuid, Vec<i32>>,
) -> (HashMap<Uuid, Vec<i32>>, HashMap<Uuid, Vec<i32>>) {
    let mut revoke: HashMap<Uuid, Vec<i32>> = HashMap::new();
    let mut keep: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, parts) in current {
        let target_parts = target.get(tid).cloned().unwrap_or_default();
        let target_set: HashSet<i32> = target_parts.into_iter().collect();
        for p in parts {
            if target_set.contains(p) {
                keep.entry(*tid).or_default().push(*p);
            } else {
                revoke.entry(*tid).or_default().push(*p);
            }
        }
    }
    (revoke, keep)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::consumer_state::test_support::member;

    #[test]
    fn install_target_computes_revoke_split() {
        let mut g = GroupState::new("g");
        let t = Uuid([1; 16]);
        let mut m = member("m1");
        m.assigned_partitions.insert(t, vec![0, 1, 2]);
        g.add_or_update_member(m);
        let mut target_for_m1 = HashMap::new();
        target_for_m1.insert(t, vec![0, 1]);
        g.install_target([("m1".to_string(), target_for_m1)].into());
        let m = &g.members["m1"];
        check!(m.partitions_pending_revocation[&t] == vec![2]);
        check!(m.assigned_partitions[&t] == vec![0, 1]);
        check!(m.assignment_state == MemberAssignmentState::UnrevokedPartitions);
    }
}
