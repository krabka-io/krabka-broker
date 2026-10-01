use std::collections::BTreeSet;

use assert2::assert;

use super::{oracle::expected_membership, *};

type ExpectedControl = Option<(
    VoterReconfigurationPlan,
    Vec<u64>,
    Vec<i32>,
    Option<(i64, (usize, usize), u64)>,
)>;

pub(super) fn expected_control(
    old: &[u64],
    state: (ReconfigurationLeadership, CurrentVoterSet),
    request: VoterChangeRequest,
    node: u64,
    candidate: TargetVoter,
    base: i64,
    reports: &[(i64, i64)],
) -> ExpectedControl {
    expected_membership(old, state.0, state.1, request, node, candidate).and_then(|(plan, next)| {
        let count = usize::from(plan.write_kraft_version) + usize::from(plan.write_voters);
        let deltas: Vec<_> = (0..count).map(|i| i32::try_from(i).unwrap()).collect();
        if count == 0 {
            return Some((plan, next, deltas, None));
        }
        let end = i128::from(base) + i128::try_from(count).unwrap();
        if base < 0 || end > i128::from(i64::MAX) {
            return None;
        }
        let end = i64::try_from(end).unwrap();
        let next_ids: BTreeSet<_> = next.iter().copied().collect();
        let old_grants: BTreeSet<_> = old
            .iter()
            .enumerate()
            .filter(|(i, _)| reports[*i].0 >= end)
            .map(|(_, id)| *id)
            .collect();
        let mut new_grants: BTreeSet<_> = old
            .iter()
            .enumerate()
            .filter(|(i, id)| reports[*i].1 >= end && next_ids.contains(*id))
            .map(|(_, id)| *id)
            .collect();
        if !old.contains(&node) && next_ids.contains(&node) && reports[old.len()].1 >= end {
            new_grants.insert(node);
        }
        let counts = (old_grants.len(), new_grants.len());
        if counts.0 <= old.len() / 2 || counts.1 <= next.len() / 2 {
            return None;
        }
        let common = old
            .iter()
            .copied()
            .find(|id| old_grants.contains(id) && new_grants.contains(id))
            .unwrap();
        Some((plan, next, deltas, Some((end, counts, common))))
    })
}

fn check_control(
    old: &[u64],
    state: (ReconfigurationLeadership, CurrentVoterSet),
    request: VoterChangeRequest,
    node: u64,
    candidate: TargetVoter,
    base: i64,
    reports: &[(i64, i64)],
) {
    assert!(
        reconfiguration_control_prefix_support(old, state, request, node, candidate, base, reports)
            == expected_control(old, state, request, node, candidate, base, reports)
    );
}

proptest! {
    #[test]
    fn control_support_matches_actual_prefix_sets(
        old in prop::collection::vec(0u64..12, 0..10), node in 0u64..13,
        bits in any::<u16>(), operation in 0u8..4, membership in 0u8..4,
        version in 0u16..3, requested in 0u16..3, base in any::<i64>(),
        reports in prop::collection::vec((any::<i64>(), any::<i64>()), 11),
    ) {
        let flag = |i: u32| bits & (1u16 << i) != 0u16;
        let leader = ReconfigurationLeadership { is_leader: flag(0), no_pending_change: flag(1), epoch_committed: flag(2) };
        let context = CurrentVoterSet { voter_count: old.len(), kraft_version: version,
            latest_controls_committed: flag(3), all_voters_support_requested: flag(4) };
        let request = VoterChangeRequest { kind: match operation { 0 => VoterChangeKind::Add,
            1 => VoterChangeKind::Remove, 2 => VoterChangeKind::Update, _ => VoterChangeKind::FinalizeKraftVersion },
            requested_kraft_version: requested };
        let candidate = TargetVoter { membership: match membership { 0 => TargetMembership::Absent,
            1 => TargetMembership::PresentUnknownDirectory, 2 => TargetMembership::PresentSameDirectory,
            _ => TargetMembership::PresentOtherDirectory }, version_compatible: flag(5), caught_up: flag(6) };
        check_control(&old, (leader, context), request, node, candidate, base, &reports[..=old.len()]);
    }
}

#[test]
fn complete_control_batches_need_both_actual_prefix_majorities() {
    for n in 1..=5 {
        let old: Vec<_> = (0..n).rev().map(|i| u64::try_from(i).unwrap()).collect();
        for (kind, node, member, version, count) in [
            (
                VoterChangeKind::Add,
                u64::MAX,
                TargetMembership::Absent,
                1,
                1,
            ),
            (
                VoterChangeKind::Remove,
                0,
                TargetMembership::PresentSameDirectory,
                1,
                1,
            ),
            (
                VoterChangeKind::Update,
                0,
                TargetMembership::PresentSameDirectory,
                1,
                1,
            ),
            (
                VoterChangeKind::Update,
                0,
                TargetMembership::PresentUnknownDirectory,
                0,
                0,
            ),
            (
                VoterChangeKind::FinalizeKraftVersion,
                0,
                TargetMembership::Absent,
                0,
                2,
            ),
        ] {
            let end = 13 + count;
            for mask in 0..(1usize << (2 * (n + 1))) {
                let reports: Vec<_> = (0..=n)
                    .map(|i| {
                        (
                            if mask & (1 << (2 * i)) != 0 {
                                end
                            } else {
                                end - 1
                            },
                            if mask & (1 << (2 * i + 1)) != 0 {
                                end
                            } else {
                                end - 1
                            },
                        )
                    })
                    .collect();
                check_control(
                    &old,
                    (leading(), current(n, version)),
                    VoterChangeRequest {
                        kind,
                        requested_kraft_version: 1,
                    },
                    node,
                    target(member),
                    13,
                    &reports,
                );
            }
        }
    }
}

#[test]
fn preflight_has_no_append_and_successor_exhaustion_cannot_alias_a_record() {
    for (kind, node, member, version) in [
        (VoterChangeKind::Add, 3, TargetMembership::Absent, 1),
        (
            VoterChangeKind::Update,
            1,
            TargetMembership::PresentUnknownDirectory,
            0,
        ),
        (
            VoterChangeKind::FinalizeKraftVersion,
            1,
            TargetMembership::Absent,
            0,
        ),
    ] {
        for base in [-1, 0, i64::MAX - 2, i64::MAX - 1, i64::MAX] {
            for reports in [[(i64::MAX, i64::MAX); 4], [(-1, -1); 4]] {
                check_control(
                    &[1, 2, 0],
                    (leading(), current(3, version)),
                    VoterChangeRequest {
                        kind,
                        requested_kraft_version: 1,
                    },
                    node,
                    target(member),
                    base,
                    &reports,
                );
            }
        }
    }
}
