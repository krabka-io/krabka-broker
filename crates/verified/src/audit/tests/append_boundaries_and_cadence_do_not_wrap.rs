use super::*;

#[test]
fn append_boundaries_and_cadence_do_not_wrap() {
    check!(
        spool_append_decision(u64::MAX - 1, 1, u64::MAX, 1, 3)
            == SpoolAppendDecision {
                accepted: true,
                new_bytes: u64::MAX,
                sync: false,
                next_unsynced: 2,
            }
    );
    check!(
        spool_append_decision(u64::MAX, 1, u64::MAX, 2, 3)
            == SpoolAppendDecision {
                accepted: false,
                new_bytes: u64::MAX,
                sync: false,
                next_unsynced: 2,
            }
    );
    check!(
        spool_append_decision(0, 0, 0, u64::MAX - 1, u64::MAX)
            == SpoolAppendDecision {
                accepted: true,
                new_bytes: 0,
                sync: true,
                next_unsynced: 0,
            }
    );
}

#[test]
fn loss_settlement_keeps_later_losses_in_a_fresh_generation() {
    let losses = |generation, count| AuditLosses { generation, count };
    // (case, pending, marker's batch, pending after settlement)
    for (case, state, batch, expected) in [
        (
            "the marker reports every pending loss",
            losses(4, 2),
            losses(4, 2),
            losses(4, 0),
        ),
        (
            "a loss emitted after the snapshot moves to a fresh generation",
            losses(4, 3),
            losses(4, 2),
            losses(5, 1),
        ),
        (
            "another generation's marker settles nothing",
            losses(5, 1),
            losses(4, 2),
            losses(5, 1),
        ),
        (
            "an over-reporting batch clamps to zero",
            losses(4, 1),
            losses(4, 2),
            losses(4, 0),
        ),
        (
            "the last generation saturates",
            losses(u64::MAX, 3),
            losses(u64::MAX, 1),
            losses(u64::MAX, 2),
        ),
    ] {
        check!(settle_loss_batch(state, batch) == expected, "{case}");
    }
}

#[test]
fn checkpoint_requires_an_exact_nonempty_position() {
    use AuditCheckpointAdmission::{Admit, RejectHead, RejectSequence, RejectSignature};

    check!(audit_checkpoint_admission(true, true, 3, 2) == Admit);
    check!(audit_checkpoint_admission(false, true, 3, 2) == RejectSignature);
    check!(audit_checkpoint_admission(true, false, 3, 2) == RejectHead);
    check!(audit_checkpoint_admission(true, true, 0, 0) == RejectSequence);
    check!(audit_checkpoint_admission(true, true, u64::MAX, u64::MAX) == RejectSequence);
}

#[test]
fn loss_marker_shape_count_and_generation_are_exact() {
    use AuditLossMarkerAdmission::{Admit, Reject};

    // (header matches, field count, count, generation, previous, expected)
    for (header, fields, count, generation, previous, expected) in [
        (true, 2, 3, Some(2), 1, Admit { generation: 2 }),
        (true, 2, 3, Some(1), 0, Admit { generation: 1 }),
        // A one-field body without a generation is not a marker.
        (true, 1, 3, None, 0, Reject),
        (true, 2, 3, None, 0, Reject),
        (true, 3, 3, Some(2), 1, Reject),
        (false, 2, 3, Some(2), 1, Reject),
        (true, 2, 0, Some(2), 1, Reject),
        (true, 2, 3, Some(1), 1, Reject),
        (true, 2, 3, Some(u64::MAX), u64::MAX, Reject),
    ] {
        check!(
            audit_loss_marker_admission(header, fields, count, generation, previous) == expected
        );
    }
}
