use assert2::assert;

use super::{control_support::expected_control, *};

fn check_waiter(
    old: &[u64],
    state: (ReconfigurationLeadership, CurrentVoterSet),
    request: VoterChangeRequest,
    node: u64,
    candidate: TargetVoter,
    reports: &[(i64, i64)],
    progress: (i64, i64, i64, i64),
) {
    let (base, previous, requested, log_end) = progress;
    let expected = expected_control(old, state, request, node, candidate, base, reports).and_then(
        |(plan, next, deltas, support)| {
            let Some((end, _, common)) = support else {
                return Some((plan, next, Vec::new(), None));
            };
            if !(0..=log_end).contains(&previous) || end > log_end {
                return None;
            }
            let hwm = previous.max(requested.min(log_end));
            let rows: Vec<_> = deltas
                .iter()
                .map(|delta| {
                    let offset = base.checked_add(i64::from(*delta)).unwrap();
                    (
                        offset,
                        plan.write_kraft_version && *delta == 0,
                        offset < hwm,
                    )
                })
                .collect();
            let prefix = rows.iter().filter(|row| row.2).count();
            let ready = rows.iter().all(|row| row.2);
            Some((plan, next, rows, Some((end, hwm, prefix, ready, common))))
        },
    );
    assert!(
        reconfiguration_control_commit_waiter(
            old, state, request, node, candidate, reports, progress
        ) == expected
    );
}

proptest! {
    #[test]
    fn control_commitment_matches_actual_row_visibility(
        old in prop::collection::vec(0u64..12, 0..10), node in 0u64..13,
        bits in any::<u16>(), operation in 0u8..4, membership in 0u8..4,
        version in 0u16..3, requested_version in 0u16..3,
        progress in (any::<i64>(), any::<i64>(), any::<i64>(), any::<i64>()),
        reports in prop::collection::vec((any::<i64>(), any::<i64>()), 11),
    ) {
        let flag = |i: u32| bits & (1u16 << i) != 0u16;
        let leader = ReconfigurationLeadership { is_leader: flag(0), no_pending_change: flag(1), epoch_committed: flag(2) };
        let context = CurrentVoterSet { voter_count: old.len(), kraft_version: version,
            latest_controls_committed: flag(3), all_voters_support_requested: flag(4) };
        let request = VoterChangeRequest { kind: match operation { 0 => VoterChangeKind::Add,
            1 => VoterChangeKind::Remove, 2 => VoterChangeKind::Update, _ => VoterChangeKind::FinalizeKraftVersion },
            requested_kraft_version: requested_version };
        let candidate = TargetVoter { membership: match membership { 0 => TargetMembership::Absent,
            1 => TargetMembership::PresentUnknownDirectory, 2 => TargetMembership::PresentSameDirectory,
            _ => TargetMembership::PresentOtherDirectory }, version_compatible: flag(5), caught_up: flag(6) };
        check_waiter(&old, (leader, context), request, node, candidate, &reports[..=old.len()], progress);
    }
}

#[test]
fn supported_controls_commit_in_record_order_at_every_frontier() {
    let old = [1, 2, 3];
    for (kind, node, member, version) in [
        (VoterChangeKind::Add, 4, TargetMembership::Absent, 1),
        (
            VoterChangeKind::Remove,
            1,
            TargetMembership::PresentSameDirectory,
            1,
        ),
        (
            VoterChangeKind::Update,
            1,
            TargetMembership::PresentSameDirectory,
            1,
        ),
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
        for base in [-1, 0, 5, i64::MAX - 2, i64::MAX - 1, i64::MAX] {
            for previous in [-1, 0, base, base.saturating_add(1)] {
                for requested in [i64::MIN, 0, base, base.saturating_add(1), i64::MAX] {
                    for reports in [[(i64::MAX, i64::MAX); 4], [(base, base); 4]] {
                        check_waiter(
                            &old,
                            (leading(), current(old.len(), version)),
                            VoterChangeRequest {
                                kind,
                                requested_kraft_version: 1,
                            },
                            node,
                            target(member),
                            &reports,
                            (base, previous, requested, base.saturating_add(3)),
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn equal_membership_values_do_not_commit_the_final_voters_record() {
    let old = [1, 2, 3];
    let (plan, next, rows, frontier) = reconfiguration_control_commit_waiter(
        &old,
        (leading(), current(3, 0)),
        VoterChangeRequest {
            kind: VoterChangeKind::FinalizeKraftVersion,
            requested_kraft_version: 1,
        },
        1,
        target(TargetMembership::Absent),
        &[(7, 7); 4],
        (5, 5, 6, 10),
    )
    .unwrap();
    assert!(plan.write_kraft_version && plan.write_voters);
    assert!(next == old);
    assert!(rows == [(5, true, true), (6, false, false)]);
    assert!(frontier == Some((7, 6, 1, false, 1)));
}

#[test]
fn later_appends_and_repeated_advances_preserve_whole_batch_readiness() {
    for (previous, requested, expected_hwm, ready) in [
        (5, 6, 6, false),
        (6, 7, 7, true),
        (6, 8, 8, true),
        (8, 5, 8, true),
        (8, i64::MAX, 10, true),
    ] {
        let (_, _, rows, frontier) = reconfiguration_control_commit_waiter(
            &[1, 2, 3],
            (leading(), current(3, 0)),
            VoterChangeRequest {
                kind: VoterChangeKind::FinalizeKraftVersion,
                requested_kraft_version: 1,
            },
            1,
            target(TargetMembership::Absent),
            &[(7, 7); 4],
            (5, previous, requested, 10),
        )
        .unwrap();
        assert!(rows == [(5, true, true), (6, false, ready)]);
        assert!(frontier == Some((7, expected_hwm, if ready { 2 } else { 1 }, ready, 1)));
    }
}
