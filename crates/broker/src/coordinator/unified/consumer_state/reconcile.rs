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

/// The partitions each topic holds, by topic id.
type Partitions = HashMap<Uuid, Vec<i32>>;

impl GroupState {
    /// Installs a freshly computed target at the current group epoch.
    ///
    /// Only the target changes. Kafka reconciles a member inside that
    /// member's own heartbeat, so a target change never rewrites another
    /// member's assignment: the member is told about its new, smaller
    /// assignment by its own next heartbeat, and it stays behind the target
    /// epoch until it acknowledges the revocation. See [`Self::reconcile_member`].
    pub fn install_target(&mut self, per_member: HashMap<String, Partitions>) {
        self.target.epoch = self.group_epoch;
        self.target.per_member = per_member;
    }

    /// Kafka's `GroupMetadataManager.maybeReconcile` and
    /// `CurrentAssignmentBuilder.build`: reconciles the current assignment of
    /// `member_id` towards the target, inside that member's heartbeat.
    ///
    /// `owned` is the `TopicPartitions` the heartbeat reports, and `None` when
    /// the heartbeat sent none. The Java client sends it only when its
    /// assignment changed, so `None` means "unchanged" and the member still
    /// owns everything it was assigned or asked to revoke
    /// (`ownsRevokedPartitions(null)` is true). `has_subscription_changed` is
    /// true when this heartbeat changed the member's subscription.
    ///
    /// A member keeps every target partition it holds and moves the others to
    /// `partitions_pending_revocation`. It stays in its epoch, in the
    /// `UnrevokedPartitions` state, until it reports an owned set that omits
    /// them. Only then does it move to the target epoch and claim the target
    /// partitions that are free. A partition another member holds, assigned or
    /// pending revocation, stays withheld until that member lets it go, so the
    /// broker never advertises a partition to two members. That is the main
    /// KIP-848 safety property. See `reconciler_model.rs`.
    ///
    /// It returns `true` when the member's epoch, state, assignment or pending
    /// set changed.
    pub fn reconcile_member(
        &mut self,
        member_id: &str,
        owned: Option<&Partitions>,
        has_subscription_changed: bool,
    ) -> bool {
        let target_epoch = self.target.epoch;
        let Some(member) = self.members.get(member_id) else {
            return false;
        };
        // `isReconciledTo`: a stable member at the target epoch has nothing to do.
        if !has_subscription_changed
            && member.assignment_state == MemberAssignmentState::Stable
            && member.member_epoch == target_epoch
        {
            return false;
        }
        // A member that still owns the partitions it must revoke cannot move
        // forward, however the target changed meanwhile.
        if member.assignment_state == MemberAssignmentState::UnrevokedPartitions
            && owns_any(owned, &member.partitions_pending_revocation)
        {
            return false;
        }
        let no_target = Partitions::new();
        let target = self.target.per_member.get(member_id).unwrap_or(&no_target);
        // `currentPartitionEpoch`: what every other member still holds, assigned
        // or pending revocation. The member's own holdings need no entry: what
        // it keeps is in `kept`, and what it gives up is not in its target.
        let held_by_others: HashSet<(Uuid, i32)> = self
            .members
            .iter()
            .filter(|(id, _)| id.as_str() != member_id)
            .flat_map(|(_, other)| {
                other
                    .assigned_partitions
                    .iter()
                    .chain(&other.partitions_pending_revocation)
            })
            .flat_map(|(topic_id, parts)| parts.iter().map(|&p| (*topic_id, p)))
            .collect();

        // `computeNextAssignment`.
        let mut kept = Partitions::new();
        let mut to_revoke = Partitions::new();
        let mut to_grant = Partitions::new();
        let mut has_unreleased = false;
        let topic_ids: HashSet<Uuid> = target
            .keys()
            .chain(member.assigned_partitions.keys())
            .copied()
            .collect();
        for topic_id in topic_ids {
            let wanted: &[i32] = target.get(&topic_id).map_or(&[], Vec::as_slice);
            let held: &[i32] = member
                .assigned_partitions
                .get(&topic_id)
                .map_or(&[], Vec::as_slice);
            let pending_here: &[i32] = member
                .partitions_pending_revocation
                .get(&topic_id)
                .map_or(&[], Vec::as_slice);
            for &partition in held {
                let side = if wanted.contains(&partition) {
                    &mut kept
                } else {
                    &mut to_revoke
                };
                side.entry(topic_id).or_default().push(partition);
            }
            for &partition in wanted.iter().filter(|p| !held.contains(p)) {
                // A partition the member itself is pending to revoke is free
                // for it: only one member holds a partition at a time.
                if held_by_others.contains(&(topic_id, partition))
                    && !pending_here.contains(&partition)
                {
                    has_unreleased = true;
                } else {
                    to_grant.entry(topic_id).or_default().push(partition);
                }
            }
        }

        let (state, epoch, mut assigned, mut pending, granted) =
            if !to_revoke.is_empty() && owns_any(owned, &to_revoke) {
                // The member keeps its epoch and waits for the client to
                // acknowledge the revocation.
                (
                    MemberAssignmentState::UnrevokedPartitions,
                    member.member_epoch,
                    kept,
                    to_revoke,
                    Partitions::new(),
                )
            } else {
                // Whatever the member had to revoke it has revoked. It moves to
                // the target epoch with what it keeps and what is free.
                let state = if has_unreleased {
                    MemberAssignmentState::UnreleasedPartitions
                } else {
                    MemberAssignmentState::Stable
                };
                for (topic_id, parts) in &to_grant {
                    kept.entry(*topic_id).or_default().extend(parts);
                }
                (state, target_epoch, kept, Partitions::new(), to_grant)
            };
        for parts in assigned.values_mut().chain(pending.values_mut()) {
            parts.sort_unstable();
        }

        let member = self
            .members
            .get_mut(member_id)
            .expect("member exists in reconcile_member");
        let changed = member.member_epoch != epoch
            || member.assignment_state != state
            || member.assigned_partitions != assigned
            || member.partitions_pending_revocation != pending;
        if member.member_epoch != epoch {
            member.previous_member_epoch = member.member_epoch;
            member.member_epoch = epoch;
        }
        member.assignment_state = state;
        member.assigned_partitions = assigned;
        member.partitions_pending_revocation = pending;
        // A partition granted now is assigned at this epoch, as Kafka stamps it
        // with the target assignment epoch, even one the member held before and
        // gave up. A partition it keeps, or moves to pending revocation, keeps
        // its epoch.
        for (topic_id, parts) in &granted {
            if let Some(epochs) = member.assignment_epochs.get_mut(topic_id) {
                for partition in parts {
                    epochs.remove(partition);
                }
            }
        }
        member.stamp_assignment_epochs(epoch);
        changed
    }
}

/// Kafka's `ownsRevokedPartitions`: `true` when the heartbeat reports any of
/// the `pending` partitions, or reports nothing at all.
fn owns_any(owned: Option<&Partitions>, pending: &Partitions) -> bool {
    owned.is_none_or(|owned| {
        owned.iter().any(|(topic_id, parts)| {
            pending
                .get(topic_id)
                .is_some_and(|pending| parts.iter().any(|p| pending.contains(p)))
        })
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::consumer_state::test_support::member;

    const T: Uuid = Uuid([1; 16]);
    const TARGET_EPOCH: i32 = 6;

    fn parts(partitions: &[i32]) -> Partitions {
        if partitions.is_empty() {
            Partitions::new()
        } else {
            [(T, partitions.to_vec())].into()
        }
    }

    /// What the coordinator stores about a member.
    #[derive(Debug, PartialEq)]
    struct Held {
        epoch: i32,
        state: MemberAssignmentState,
        assigned: Vec<i32>,
        pending: Vec<i32>,
    }

    fn held(epoch: i32, state: MemberAssignmentState, assigned: &[i32], pending: &[i32]) -> Held {
        Held {
            epoch,
            state,
            assigned: assigned.to_vec(),
            pending: pending.to_vec(),
        }
    }

    fn held_by(g: &GroupState, member_id: &str) -> Held {
        let member = &g.members[member_id];
        let list = |partitions: &Partitions| partitions.get(&T).cloned().unwrap_or_default();
        Held {
            epoch: member.member_epoch,
            state: member.assignment_state,
            assigned: list(&member.assigned_partitions),
            pending: list(&member.partitions_pending_revocation),
        }
    }

    /// A group at epoch 6 with member `a`, holding `before`, and member `b`,
    /// holding `other`, assigned and pending revocation, and the target `target`
    /// for `a`.
    fn group(before: &Held, other: (&[i32], &[i32]), target: &[i32]) -> GroupState {
        let mut g = GroupState::new("g");
        g.group_epoch = TARGET_EPOCH;
        let mut a = member("a");
        a.member_epoch = before.epoch;
        a.assignment_state = before.state;
        a.assigned_partitions = parts(&before.assigned);
        a.partitions_pending_revocation = parts(&before.pending);
        a.stamp_assignment_epochs(before.epoch);
        g.members.insert("a".into(), a);
        let mut b = member("b");
        b.member_epoch = TARGET_EPOCH;
        b.assigned_partitions = parts(other.0);
        b.partitions_pending_revocation = parts(other.1);
        g.members.insert("b".into(), b);
        g.install_target([("a".to_string(), parts(target))].into());
        g
    }

    /// Kafka's `CurrentAssignmentBuilder`: one row per rule. `owned` is what
    /// the heartbeat reports, and `None` is a heartbeat without
    /// `TopicPartitions`.
    #[test]
    fn reconcile_member_follows_the_current_assignment_builder() {
        use MemberAssignmentState::{
            Stable as S, UnreleasedPartitions as R, UnrevokedPartitions as U,
        };

        struct Row {
            name: &'static str,
            before: Held,
            other: (&'static [i32], &'static [i32]),
            target: &'static [i32],
            owned: Option<&'static [i32]>,
            subscription_changed: bool,
            after: Held,
            changed: bool,
        }
        let row = |name, before, other, target, owned, after| Row {
            name,
            before,
            other,
            target,
            owned,
            subscription_changed: false,
            changed: false,
            after,
        };
        let rows = [
            // A member that must revoke stays where it is until it reports an
            // owned set without the pending partitions.
            row(
                "an absent owned set keeps a member that must revoke",
                held(5, U, &[0, 1], &[2]),
                (&[], &[]),
                &[0, 1],
                None,
                held(5, U, &[0, 1], &[2]),
            ),
            row(
                "an owned set with the pending partition keeps the member",
                held(5, U, &[0, 1], &[2]),
                (&[], &[]),
                &[0, 1],
                Some(&[0, 1, 2]),
                held(5, U, &[0, 1], &[2]),
            ),
            Row {
                changed: true,
                ..row(
                    "an owned set without the pending partition moves the member on",
                    held(5, U, &[0, 1], &[2]),
                    (&[], &[]),
                    &[0, 1],
                    Some(&[0, 1]),
                    held(6, S, &[0, 1], &[]),
                )
            },
            // A stable member behind the target epoch learns what to revoke.
            Row {
                changed: true,
                ..row(
                    "a shrunk target is a revocation, in the member's epoch",
                    held(5, S, &[0, 1, 2], &[]),
                    (&[], &[]),
                    &[0, 1],
                    None,
                    held(5, U, &[0, 1], &[2]),
                )
            },
            Row {
                changed: true,
                ..row(
                    "a member that already dropped the partition moves on",
                    held(5, S, &[0, 1, 2], &[]),
                    (&[], &[]),
                    &[0, 1],
                    Some(&[0, 1]),
                    held(6, S, &[0, 1], &[]),
                )
            },
            Row {
                changed: true,
                ..row(
                    "a free target partition is granted at the target epoch",
                    held(5, S, &[0], &[]),
                    (&[], &[]),
                    &[0, 1],
                    None,
                    held(6, S, &[0, 1], &[]),
                )
            },
            Row {
                changed: true,
                ..row(
                    "a partition another member holds is withheld",
                    held(5, S, &[0], &[]),
                    (&[1], &[]),
                    &[0, 1],
                    None,
                    held(6, R, &[0], &[]),
                )
            },
            Row {
                changed: true,
                ..row(
                    "a partition another member must still revoke is withheld",
                    held(5, S, &[0], &[]),
                    (&[], &[1]),
                    &[0, 1],
                    None,
                    held(6, R, &[0], &[]),
                )
            },
            Row {
                changed: true,
                ..row(
                    "an unreleased partition is granted once it is free",
                    held(6, R, &[0], &[]),
                    (&[], &[]),
                    &[0, 1],
                    None,
                    held(6, S, &[0, 1], &[]),
                )
            },
            row(
                "an unreleased partition stays withheld while it is held",
                held(6, R, &[0], &[]),
                (&[1], &[]),
                &[0, 1],
                None,
                held(6, R, &[0], &[]),
            ),
            row(
                "a stable member at the target epoch has nothing to do",
                held(6, S, &[0, 1], &[]),
                (&[], &[]),
                &[0, 1],
                None,
                held(6, S, &[0, 1], &[]),
            ),
            Row {
                changed: true,
                ..row(
                    "a revocation waits, and so does a free partition",
                    held(5, S, &[0, 1], &[]),
                    (&[], &[]),
                    &[1, 2],
                    None,
                    held(5, U, &[1], &[0]),
                )
            },
            Row {
                changed: true,
                ..row(
                    "a partition the member gave up is free for it again",
                    held(5, U, &[0], &[1]),
                    (&[], &[]),
                    &[0, 1],
                    Some(&[0]),
                    held(6, S, &[0, 1], &[]),
                )
            },
            Row {
                subscription_changed: true,
                changed: true,
                ..row(
                    "a subscription change reconciles a member at the target epoch",
                    held(6, S, &[0, 1, 2], &[]),
                    (&[], &[]),
                    &[0, 1],
                    None,
                    held(6, U, &[0, 1], &[2]),
                )
            },
        ];
        for r in rows {
            let mut g = group(&r.before, r.other, r.target);
            let owned = r.owned.map(parts);

            let changed = g.reconcile_member("a", owned.as_ref(), r.subscription_changed);

            check!(held_by(&g, "a") == r.after, "{}", r.name);
            check!(changed == r.changed, "{}", r.name);
        }
    }

    /// A target change leaves every member as it was. The member learns about
    /// it in its own heartbeat, and it keeps its epoch until it revokes.
    #[test]
    fn install_target_does_not_reconcile_any_member() {
        let before = held(5, MemberAssignmentState::Stable, &[0, 1, 2], &[]);
        let mut g = group(&before, (&[], &[]), &[0, 1]);

        check!(held_by(&g, "a") == before);
        check!(g.target.epoch == TARGET_EPOCH);

        g.reconcile_member("a", None, false);
        check!(
            held_by(&g, "a") == held(5, MemberAssignmentState::UnrevokedPartitions, &[0, 1], &[2])
        );
    }

    /// A member moved to a new epoch remembers the one it left, and a partition
    /// granted at that epoch is assigned at it, even one the member gave up.
    #[test]
    fn a_granted_partition_is_assigned_at_the_new_epoch() {
        let before = held(5, MemberAssignmentState::UnrevokedPartitions, &[0], &[1]);
        let mut g = group(&before, (&[], &[]), &[0, 1]);

        g.reconcile_member("a", Some(&parts(&[0])), false);

        let member = &g.members["a"];
        check!(member.member_epoch == TARGET_EPOCH);
        check!(member.previous_member_epoch == 5);
        check!(member.assignment_epoch(&T, 0) == Some(5));
        check!(member.assignment_epoch(&T, 1) == Some(TARGET_EPOCH));
    }
}
