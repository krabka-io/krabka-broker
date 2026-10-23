//! Application of one piggybacked acknowledgement batch to a share
//! partition's acquisition state.
//!
//! `ShareAcknowledge` applies the same batches without a fetch, so this step
//! is shared and not folded into the acquire pass.

use {
    crate::{
        codes,
        share_partition::state::{AckAction, AckType, AcquisitionState},
    },
    krabka_log::Offset,
    std::time::{Duration, Instant},
};

/// The KIP-1222 acknowledge type `Renew`.
const ACK_RENEW: i8 = 4;

/// How an acknowledgement request treats the acknowledge type `Renew`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Renewal {
    /// The request set `IsRenewAck`.
    pub(crate) requested: bool,
    /// The group allows renewals: `share.renew.acknowledge.enable`, as
    /// [`GroupShareSettings`](crate::share_partition::group_settings::GroupShareSettings)
    /// resolves it.
    pub(crate) enabled: bool,
    /// The new lock length of a renewed record.
    pub(crate) lock_duration: Duration,
}

/// The highest acknowledge type of a request version without `Renew`:
/// `Reject`.
const MAX_ACK_TYPE: i8 = 3;

/// Kafka's `KafkaApis.validateAcknowledgementBatches` for the batches of one
/// partition, as `(first_offset, last_offset, acknowledge_types)`.
///
/// A partition is refused with `INVALID_REQUEST` when a batch has its first
/// offset past its last, starts before the last offset of the batch in front
/// of it, has no acknowledge type, has more than one type but not one per
/// offset, has a type outside `[0, 3]` (`[0, 4]` when the version supports
/// `Renew`), or has the type `Renew` in a request without `IsRenewAck`.
pub(crate) fn acknowledgement_batches_are_valid<'a>(
    batches: impl IntoIterator<Item = (i64, i64, &'a [i8])>,
    supports_renew: bool,
    is_renew_ack: bool,
) -> bool {
    let max_type = if supports_renew {
        ACK_RENEW
    } else {
        MAX_ACK_TYPE
    };
    let mut previous_last = -1_i64;
    for (first, last, types) in batches {
        let per_offset = types.len() > 1;
        let covers_range = i64::try_from(types.len())
            .is_ok_and(|count| last.checked_sub(first) == Some(count - 1));
        let valid = first <= last
            && first >= previous_last
            && !types.is_empty()
            && (!per_offset || covers_range)
            && types.iter().all(|ack| (0..=max_type).contains(ack))
            && (is_renew_ack || !types.contains(&ACK_RENEW));
        if !valid {
            return false;
        }
        previous_last = last;
    }
    true
}

/// What one member's acknowledgements apply with.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AckApplication<'a> {
    pub(crate) member: &'a str,
    pub(crate) now: Instant,
    pub(crate) renewal: Renewal,
    /// The group's delivery count limit: a released record at the limit is
    /// archived.
    pub(crate) max_attempts: i16,
}

/// Kafka's `SharePartition.acknowledge`: applies the acknowledgement batches
/// of one partition, as `(first_offset, last_offset, acknowledge_types)`, as
/// one unit.
///
/// It first expires every lock whose deadline has passed. Kafka arms a timer
/// on each acquired batch (`releaseAcquisitionLockOnTimeout`), so at the
/// deadline the record is `AVAILABLE` (or `ARCHIVED` at the delivery limit)
/// and a later acknowledgement or renewal of it is `INVALID_RECORD_STATE`.
/// The expiry stays even when an acknowledgement fails, because the timer
/// fires whatever a member sends.
///
/// It stops at the first batch that fails and restores the state that `st`
/// held before the first batch, so nothing of a failed acknowledgement stays
/// or is persisted, as `rollbackOrProcessStateUpdates` does. It returns that
/// batch's error code, or `NONE`.
pub(crate) fn apply_acknowledgements<'b>(
    st: &mut AcquisitionState,
    application: &AckApplication<'_>,
    batches: impl IntoIterator<Item = (i64, i64, &'b [i8])>,
) -> i16 {
    st.expire_locks(application.now, application.max_attempts);
    let before = st.clone();
    for (first, last, types) in batches {
        if let Err(code) = apply_one_ack(st, application, first, last, types) {
            *st = before;
            return code;
        }
    }
    codes::NONE
}

fn has_live_ack_offsets(
    state: &AcquisitionState,
    first: i64,
    last: i64,
    types: &[i8],
) -> Result<bool, i16> {
    if first > last || types.is_empty() {
        return Err(codes::INVALID_REQUEST);
    }
    Ok(state.ack_bounds(Offset(first), Offset(last))?.is_some())
}

fn apply_ack_runs(
    state: &mut AcquisitionState,
    first: i64,
    types: &[i8],
    mut apply: impl FnMut(&mut AcquisitionState, i64, i64, i8) -> Result<(), i16>,
) -> Result<(), i16> {
    let mut run_first = first;
    for run in types.chunk_by(|left, right| left == right) {
        let run_last =
            run_first + i64::try_from(run.len() - 1).expect("validated run length fits i64");
        apply(state, run_first, run_last, run[0])?;
        run_first = run_last.saturating_add(1);
    }
    Ok(())
}

/// Applies one acknowledgement batch to the state machine.
///
/// The batch as a whole must lie inside the records that were handed out, as
/// Kafka's `fetchSubMapForAcknowledgementBatch` checks before it looks at any
/// offset. A singleton `acknowledge_types` applies that type across the whole
/// range; otherwise each entry maps to one offset, starting at `first`. This
/// function applies valid mixed types in one window pass. Invalid types use
/// the sequential path to preserve the first failing offset's error code.
///
/// The type `Renew` (4) renews the lock of its offsets and leaves their
/// state as it is. The other types take their normal transition in the same
/// batch, as in Kafka's `SharePartition.acknowledgePerOffsetBatchRecords` and
/// `acknowledgeCompleteBatch`. A `Renew` in a request without `IsRenewAck`
/// is `INVALID_REQUEST`, and a `Renew` for a group that does not allow it is
/// `INVALID_RECORD_STATE`.
fn apply_one_ack(
    st: &mut AcquisitionState,
    application: &AckApplication<'_>,
    first: i64,
    last: i64,
    types: &[i8],
) -> Result<(), i16> {
    let &AckApplication {
        member,
        now,
        renewal,
        max_attempts,
    } = application;
    if !has_live_ack_offsets(st, first, last, types)? {
        return Ok(());
    }
    let apply = |st: &mut AcquisitionState, run_first: i64, run_last: i64, ack_type: i8| {
        if ack_type == ACK_RENEW {
            if !renewal.requested {
                return Err(codes::INVALID_REQUEST);
            }
            if !renewal.enabled {
                return Err(codes::INVALID_RECORD_STATE);
            }
            return st.renew(
                member,
                Offset(run_first),
                Offset(run_last),
                now,
                renewal.lock_duration,
            );
        }
        let ack = AckType::from_i8(ack_type).ok_or(codes::INVALID_REQUEST)?;
        st.acknowledge(
            member,
            Offset(run_first),
            Offset(run_last),
            ack,
            max_attempts,
        )
    };
    if let [ack_type] = types {
        return apply(st, first, last, *ack_type);
    }
    let range_len = last
        .checked_sub(first)
        .and_then(|len| len.checked_add(1))
        .and_then(|len| usize::try_from(len).ok());
    if range_len != Some(types.len()) {
        return Err(codes::INVALID_REQUEST);
    }
    if types.iter().all(|ack_type| *ack_type == types[0]) {
        return apply(st, first, last, types[0]);
    }
    let valid_actions = types.iter().all(|ack_type| {
        AckType::from_i8(*ack_type).is_some()
            || (*ack_type == ACK_RENEW && renewal.requested && renewal.enabled)
    });
    if !valid_actions {
        // Parsing every action first must not hide an earlier ownership
        // error. The enclosing request rolls back these transitions on
        // failure, just as it does for the normal mixed path.
        return apply_ack_runs(st, first, types, apply);
    }
    let deadline = now + renewal.lock_duration;
    // The factory receives only the owned live suffix. Slice the request before
    // mapping, so skipped prefix actions need no conversion or storage.
    let result = st.acknowledge_mixed_with(
        member,
        Offset(first),
        Offset(last),
        types.len(),
        |live| {
            types[live].iter().map(|ack_type| {
                if *ack_type == ACK_RENEW {
                    AckAction::Renew { deadline }
                } else {
                    AckAction::Disposition(
                        AckType::from_i8(*ack_type)
                            .expect("all acknowledgement actions were validated"),
                    )
                }
            })
        },
        max_attempts,
    );
    if result == Err(codes::INVALID_RECORD_STATE) {
        // An earlier Accept run can advance the SPSO over settled islands,
        // making later runs valid even when the initial range is not all
        // acquired. The fast path rejects before invoking its factory or
        // changing state; preserve the sequential ordering in that case.
        return apply_ack_runs(st, first, types, apply);
    }
    result
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_partition::state::{
            AcquisitionState, RecordState, test_support::canonical_state,
        },
        assert2::assert,
    };

    #[test]
    fn singleton_ack_type_applies_to_the_whole_range() {
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(200), 200);
        assert!(
            state
                .acquire(
                    "member",
                    200,
                    krabka_log::Offset(i64::MAX),
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .len()
                == 1
        );

        let application = AckApplication {
            member: "member",
            now: Instant::now(),
            renewal: Renewal {
                requested: false,
                enabled: true,
                lock_duration: Duration::from_secs(30),
            },
            max_attempts: 5,
        };
        apply_one_ack(&mut state, &application, 0, 199, &[1]).expect("acknowledge");

        assert!(state.start_offset == Offset(200));
        assert!(
            state
                .acquire(
                    "other",
                    200,
                    krabka_log::Offset(i64::MAX),
                    Instant::now(),
                    Duration::from_secs(30),
                    5
                )
                .is_empty()
        );
    }

    /// The setup a [`Row`] starts from.
    #[derive(Debug, Clone, Copy)]
    enum Setup {
        /// `member` holds offsets 0 to 9 at delivery count 1.
        Acquired,
        /// Offsets 0 to 4 are accepted, so the SPSO is 5, and `member` holds
        /// 5 to 9.
        HalfAccepted,
        /// `member` holds 0 to 4 and `other` holds 5 to 9.
        Split,
        /// `member` holds 0 to 9 at delivery count 2, the limit of these rows.
        AtTheLimit,
    }

    /// One acknowledgement batch of a row.
    #[derive(Clone, Copy)]
    struct TestBatch {
        first: i64,
        last: i64,
        types: &'static [i8],
    }

    /// One row: the setup, the batches, and what Kafka's
    /// `SharePartition.acknowledge` leaves behind.
    struct Row {
        name: &'static str,
        setup: Setup,
        batches: &'static [TestBatch],
        error: i16,
        spso: i64,
        states: Vec<(i64, RecordState)>,
    }

    const LIMIT: i16 = 2;
    const ACCEPT: i8 = 1;
    const RELEASE: i8 = 2;

    fn apply_one_ack_by_runs(
        st: &mut AcquisitionState,
        application: &AckApplication<'_>,
        first: i64,
        last: i64,
        types: &[i8],
    ) -> Result<(), i16> {
        if !has_live_ack_offsets(st, first, last, types)? {
            return Ok(());
        }
        let range_len = last
            .checked_sub(first)
            .and_then(|len| len.checked_add(1))
            .and_then(|len| usize::try_from(len).ok());
        if types.len() > 1 && range_len != Some(types.len()) {
            return Err(codes::INVALID_REQUEST);
        }
        let mut run_start = first;
        for run in types.chunk_by(|a, b| a == b) {
            let run_end = if types.len() == 1 {
                last
            } else {
                run_start + i64::try_from(run.len()).expect("test run length fits i64") - 1
            };
            if run[0] == ACK_RENEW {
                if !application.renewal.requested {
                    return Err(codes::INVALID_REQUEST);
                }
                if !application.renewal.enabled {
                    return Err(codes::INVALID_RECORD_STATE);
                }
                st.renew(
                    application.member,
                    Offset(run_start),
                    Offset(run_end),
                    application.now,
                    application.renewal.lock_duration,
                )?;
            } else {
                let ack = AckType::from_i8(run[0]).ok_or(codes::INVALID_REQUEST)?;
                st.acknowledge(
                    application.member,
                    Offset(run_start),
                    Offset(run_end),
                    ack,
                    application.max_attempts,
                )?;
            }
            run_start = run_end + 1;
        }
        Ok(())
    }

    fn apply_by_runs<'a>(
        st: &mut AcquisitionState,
        application: &AckApplication<'_>,
        batches: &'a [(i64, i64, &'a [i8])],
    ) -> i16 {
        st.expire_locks(application.now, application.max_attempts);
        let before = st.clone();
        for &(first, last, types) in batches {
            if let Err(code) = apply_one_ack_by_runs(st, application, first, last, types) {
                *st = before;
                return code;
            }
        }
        codes::NONE
    }

    fn compare_with_run_by_run(
        state: AcquisitionState,
        application: &AckApplication<'_>,
        batches: &[(i64, i64, &[i8])],
    ) {
        let mut actual = state.clone();
        let mut expected = state;
        let actual_code = apply_acknowledgements(&mut actual, application, batches.iter().copied());
        let expected_code = apply_by_runs(&mut expected, application, batches);
        // Renew can leave adjacent equal batches split in the sequential
        // implementation. Compare complete canonical state on success;
        // errors still require exact representation equality for rollback.
        if actual_code == codes::NONE && expected_code == codes::NONE {
            actual = canonical_state(actual);
            expected = canonical_state(expected);
        }
        assert!((actual_code, actual) == (expected_code, expected));
    }

    #[test]
    fn mixed_pass_matches_run_by_run_transitions() {
        const MIXED: &[i8] = &[RELEASE, ACCEPT, 3, 0, ACK_RENEW, 1, 2, 3, 0, 4, 1, 2];
        const DEAD_LETTER_MIX: &[i8] = &[RELEASE, 3, 0, 1, 2, 3, 0, 1, 2, 3];
        let acquired_at = Instant::now();
        let mut fragmented = AcquisitionState::new(Offset(0));
        fragmented.materialize(Offset(12), 100);
        for part in 0..3 {
            fragmented.acquire(
                "member",
                4,
                Offset(i64::MAX),
                acquired_at + Duration::from_secs(part),
                Duration::from_secs(30),
                2,
            );
        }
        let application = AckApplication {
            member: "member",
            now: acquired_at + Duration::from_secs(10),
            renewal: Renewal {
                requested: true,
                enabled: true,
                lock_duration: Duration::from_secs(45),
            },
            max_attempts: 2,
        };
        compare_with_run_by_run(fragmented.clone(), &application, &[(0, 11, MIXED)]);
        let mut across_boundaries = [ACCEPT; 12];
        across_boundaries[0] = RELEASE;
        across_boundaries[10] = ACK_RENEW;
        across_boundaries[11] = RELEASE;
        compare_with_run_by_run(fragmented, &application, &[(0, 11, &across_boundaries)]);
        compare_with_run_by_run(start(Setup::Acquired), &application, &[(
            0,
            9,
            &[ACCEPT; 10],
        )]);

        let mut at_limit_with_dlq = start(Setup::AtTheLimit);
        at_limit_with_dlq.set_dlq_enabled(true);
        compare_with_run_by_run(at_limit_with_dlq, &application, &[(0, 9, DEAD_LETTER_MIX)]);

        compare_with_run_by_run(start(Setup::Split), &application, &[(0, 9, &[
            ACCEPT, RELEASE, ACCEPT, RELEASE, ACCEPT, RELEASE, ACCEPT, RELEASE, ACCEPT, RELEASE,
        ])]);
        compare_with_run_by_run(start(Setup::Acquired), &application, &[
            (0, 9, &MIXED[..10]),
            (10, 12, &[ACCEPT]),
        ]);

        let mut settled_island = start(Setup::Acquired);
        settled_island
            .acknowledge("member", Offset(2), Offset(3), AckType::Accept, LIMIT)
            .unwrap();
        for first_action in [ACCEPT, RELEASE] {
            let mut types = [ACCEPT; 10];
            types[..2].fill(first_action);
            types[2..4].fill(RELEASE);
            compare_with_run_by_run(settled_island.clone(), &application, &[(0, 9, &types)]);
        }

        let expired_application = AckApplication {
            now: acquired_at + Duration::from_secs(60),
            ..application
        };
        let mut expiring = AcquisitionState::new(Offset(0));
        expiring.materialize(Offset(12), 100);
        expiring.acquire(
            "member",
            12,
            Offset(i64::MAX),
            acquired_at,
            Duration::from_secs(30),
            2,
        );
        compare_with_run_by_run(expiring, &expired_application, &[(0, 11, MIXED)]);
    }

    #[test]
    fn invalid_later_action_preserves_earlier_ownership_error() {
        let application = AckApplication {
            member: "other",
            now: Instant::now(),
            renewal: Renewal {
                requested: false,
                enabled: true,
                lock_duration: Duration::from_secs(30),
            },
            max_attempts: LIMIT,
        };
        for later_action in [99, ACK_RENEW] {
            let mut types = [ACCEPT; 10];
            types[9] = later_action;
            for member in ["member", "other"] {
                let application = AckApplication {
                    member,
                    ..application
                };
                compare_with_run_by_run(start(Setup::Acquired), &application, &[(0, 9, &types)]);
            }
        }
    }

    #[test]
    fn mixed_completed_prefix_preserves_live_transitions_and_validation() {
        const PREFIX: usize = 100_000;
        let first_live = Offset(i64::try_from(PREFIX).unwrap());
        let now = Instant::now();
        let mut state = AcquisitionState::new(first_live);
        state.materialize(first_live + 3, 100);
        state.acquire(
            "member",
            3,
            Offset(i64::MAX),
            now,
            Duration::from_secs(30),
            LIMIT,
        );
        let application = AckApplication {
            member: "member",
            now,
            renewal: Renewal {
                requested: true,
                enabled: true,
                lock_duration: Duration::from_secs(45),
            },
            max_attempts: LIMIT,
        };
        let last = first_live.0 + 2;
        let mut types = vec![RELEASE; PREFIX + 3];
        types[PREFIX + 1] = ACCEPT;
        types[PREFIX + 2] = ACK_RENEW;
        compare_with_run_by_run(state.clone(), &application, &[(0, last, &types)]);

        // A completed prefix is skipped for mutation, but its invalid types
        // still have the same validation outcome as the sequential algorithm.
        types[0] = 99;
        compare_with_run_by_run(state, &application, &[(0, last, &types)]);
    }

    #[test]
    fn mixed_small_windows_match_the_sequential_state_machine_exhaustively() {
        const STATE_KINDS: usize = 5;
        const ACTIONS: [i8; 6] = [ACCEPT, RELEASE, 3, 0, ACK_RENEW, 99];
        let acquired_at = Instant::now();
        let application = AckApplication {
            member: "member",
            now: acquired_at + Duration::from_secs(10),
            renewal: Renewal {
                requested: true,
                enabled: true,
                lock_duration: Duration::from_secs(45),
            },
            max_attempts: LIMIT,
        };
        for mut encoded_state in 0..STATE_KINDS.pow(3) {
            // Every three-offset combination of owned, other-owned, accepted,
            // archived and available records, including completed islands.
            let mut kinds = [0; 3];
            for kind in &mut kinds {
                *kind = encoded_state % STATE_KINDS;
                encoded_state /= STATE_KINDS;
            }
            let mut state = AcquisitionState::new(Offset(0));
            state.materialize(Offset(3), 100);
            for (index, kind) in kinds.iter().enumerate() {
                let owner = if *kind == 1 { "other" } else { "member" };
                state.acquire(
                    owner,
                    1,
                    Offset(i64::MAX),
                    acquired_at + Duration::from_millis(u64::try_from(index).unwrap()),
                    Duration::from_secs(30),
                    LIMIT,
                );
            }
            for (index, kind) in kinds.iter().enumerate() {
                let disposition = match kind {
                    2 => AckType::Accept,
                    3 => AckType::Gap,
                    4 => AckType::Release,
                    _ => continue,
                };
                let offset = Offset(i64::try_from(index).unwrap());
                state
                    .acknowledge("member", offset, offset, disposition, LIMIT)
                    .unwrap();
            }
            for mut encoded_actions in 0..ACTIONS.len().pow(3) {
                let mut types = [0; 3];
                for action in &mut types {
                    *action = ACTIONS[encoded_actions % ACTIONS.len()];
                    encoded_actions /= ACTIONS.len();
                }
                compare_with_run_by_run(state.clone(), &application, &[(0, 2, &types)]);
            }
        }
    }

    fn states(first: i64, last: i64, state: RecordState) -> Vec<(i64, RecordState)> {
        (first..=last).map(|offset| (offset, state)).collect()
    }

    fn start(setup: Setup) -> AcquisitionState {
        let now = Instant::now();
        let lock = Duration::from_secs(30);
        let mut state = AcquisitionState::new(Offset(0));
        state.materialize(Offset(10), 100);
        match setup {
            Setup::Acquired | Setup::HalfAccepted => {
                state.acquire("member", 10, Offset(i64::MAX), now, lock, LIMIT);
            }
            Setup::Split => {
                state.acquire("member", 5, Offset(i64::MAX), now, lock, LIMIT);
                state.acquire("other", 5, Offset(i64::MAX), now, lock, LIMIT);
            }
            Setup::AtTheLimit => {
                state.acquire("member", 10, Offset(i64::MAX), now, lock, LIMIT);
                state.release_member("member", LIMIT);
                state.acquire("member", 10, Offset(i64::MAX), now, lock, LIMIT);
            }
        }
        if let Setup::HalfAccepted = setup {
            state
                .acknowledge("member", Offset(0), Offset(4), AckType::Accept, LIMIT)
                .expect("accept the first half");
        }
        state
    }

    fn rows() -> Vec<Row> {
        use RecordState::Acquired;
        vec![
            Row {
                name: "a failed second batch rolls the first back",
                setup: Setup::Split,
                batches: &[
                    TestBatch {
                        first: 0,
                        last: 4,
                        types: &[ACCEPT],
                    },
                    TestBatch {
                        first: 5,
                        last: 9,
                        types: &[ACCEPT],
                    },
                ],
                error: codes::INVALID_RECORD_STATE,
                spso: 0,
                states: states(0, 9, Acquired),
            },
            Row {
                name: "a batch below the SPSO is skipped",
                setup: Setup::HalfAccepted,
                batches: &[TestBatch {
                    first: 0,
                    last: 2,
                    types: &[ACCEPT],
                }],
                error: codes::NONE,
                spso: 5,
                states: states(5, 9, Acquired),
            },
            Row {
                name: "a batch across the SPSO acknowledges the part above it",
                setup: Setup::HalfAccepted,
                batches: &[TestBatch {
                    first: 3,
                    last: 7,
                    types: &[ACCEPT],
                }],
                error: codes::NONE,
                spso: 8,
                states: states(8, 9, Acquired),
            },
            Row {
                name: "per-offset types across the SPSO",
                setup: Setup::HalfAccepted,
                batches: &[TestBatch {
                    first: 4,
                    last: 6,
                    types: &[RELEASE, ACCEPT, ACCEPT],
                }],
                error: codes::NONE,
                spso: 7,
                states: states(7, 9, Acquired),
            },
            Row {
                name: "a batch past the records handed out",
                setup: Setup::Acquired,
                batches: &[
                    TestBatch {
                        first: 0,
                        last: 4,
                        types: &[ACCEPT],
                    },
                    TestBatch {
                        first: 8,
                        last: 12,
                        types: &[ACCEPT],
                    },
                ],
                error: codes::INVALID_REQUEST,
                spso: 0,
                states: states(0, 9, Acquired),
            },
            Row {
                name: "a release at the delivery limit archives",
                setup: Setup::AtTheLimit,
                batches: &[TestBatch {
                    first: 0,
                    last: 9,
                    types: &[RELEASE],
                }],
                error: codes::NONE,
                spso: 10,
                states: Vec::new(),
            },
            Row {
                name: "a release under the limit offers the records again",
                setup: Setup::Acquired,
                batches: &[TestBatch {
                    first: 0,
                    last: 9,
                    types: &[RELEASE],
                }],
                error: codes::NONE,
                spso: 0,
                states: states(0, 9, RecordState::Available),
            },
            Row {
                name: "a release of part of the records at the limit",
                setup: Setup::AtTheLimit,
                batches: &[TestBatch {
                    first: 0,
                    last: 4,
                    types: &[RELEASE],
                }],
                error: codes::NONE,
                spso: 5,
                states: states(5, 9, Acquired),
            },
            Row {
                // The first batch archives 0 to 9 and moves the SPSO past
                // them, so the second batch is below the SPSO.
                name: "an archived run is below the SPSO for a later batch",
                setup: Setup::AtTheLimit,
                batches: &[
                    TestBatch {
                        first: 0,
                        last: 9,
                        types: &[RELEASE],
                    },
                    TestBatch {
                        first: 0,
                        last: 9,
                        types: &[ACCEPT],
                    },
                ],
                error: codes::NONE,
                spso: 10,
                states: Vec::new(),
            },
        ]
    }

    #[test]
    fn acknowledgements_apply_as_kafka_applies_them() {
        let application = AckApplication {
            member: "member",
            now: Instant::now(),
            renewal: Renewal {
                requested: false,
                enabled: true,
                lock_duration: Duration::from_secs(30),
            },
            max_attempts: LIMIT,
        };
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for row in rows() {
            let mut state = start(row.setup);
            let error = apply_acknowledgements(
                &mut state,
                &application,
                row.batches
                    .iter()
                    .map(|batch| (batch.first, batch.last, batch.types)),
            );
            actual.push((row.name, error, state.start_offset.0, state.record_states()));
            expected.push((row.name, row.error, row.spso, row.states));
        }
        assert!(actual == expected);
    }

    /// One row of the lock-deadline table: the setup, the seconds after the
    /// acquisition, whether the request renews, the acknowledge type, and the
    /// error, SPSO and states expected.
    struct LockCase {
        name: &'static str,
        setup: Setup,
        after: u64,
        requested: bool,
        ack_type: i8,
        error: i16,
        spso: i64,
        want: Vec<(i64, RecordState)>,
    }

    /// Kafka's lock timer fires at the deadline, so an acknowledgement or a
    /// renewal that arrives after it finds the records `AVAILABLE`, or
    /// `ARCHIVED` at the delivery limit, and no sweep has to run first.
    #[test]
    fn an_acknowledgement_after_the_lock_deadline_is_refused() {
        use RecordState::Available;
        const RENEW: i8 = 4;
        let rows: Vec<LockCase> = vec![
            LockCase {
                name: "an acknowledgement inside the lock",
                setup: Setup::Acquired,
                after: 29,
                requested: false,
                ack_type: ACCEPT,
                error: codes::NONE,
                spso: 10,
                want: Vec::new(),
            },
            LockCase {
                name: "an acknowledgement past the lock",
                setup: Setup::Acquired,
                after: 31,
                requested: false,
                ack_type: ACCEPT,
                error: codes::INVALID_RECORD_STATE,
                spso: 0,
                want: states(0, 9, Available),
            },
            LockCase {
                name: "a renewal past the lock",
                setup: Setup::Acquired,
                after: 31,
                requested: true,
                ack_type: RENEW,
                error: codes::INVALID_RECORD_STATE,
                spso: 0,
                want: states(0, 9, Available),
            },
            LockCase {
                name: "an acknowledgement past a lock at the delivery limit",
                setup: Setup::AtTheLimit,
                after: 31,
                requested: false,
                ack_type: ACCEPT,
                error: codes::NONE,
                spso: 10,
                want: Vec::new(),
            },
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for LockCase {
            name,
            setup,
            after,
            requested,
            ack_type,
            error,
            spso,
            want,
        } in rows
        {
            let mut state = start(setup);
            let application = AckApplication {
                member: "member",
                now: Instant::now() + Duration::from_secs(after),
                renewal: Renewal {
                    requested,
                    enabled: true,
                    lock_duration: Duration::from_secs(30),
                },
                max_attempts: LIMIT,
            };
            let got = apply_acknowledgements(&mut state, &application, [(0, 9, &[ack_type][..])]);
            actual.push((name, got, state.start_offset.0, state.record_states()));
            expected.push((name, error, spso, want));
        }
        assert!(actual == expected);
    }

    /// A lock that runs out at the delivery limit archives its records at
    /// once, and a lock under the limit offers them again.
    #[test]
    fn a_lock_expiry_at_the_limit_archives() {
        let expired = Instant::now() + Duration::from_secs(60);
        let mut actual = Vec::new();
        for setup in [Setup::Acquired, Setup::AtTheLimit] {
            let mut state = start(setup);
            state.expire_locks(expired, LIMIT);
            actual.push((state.start_offset.0, state.record_states()));
        }
        assert!(actual == vec![(0, states(0, 9, RecordState::Available)), (10, Vec::new()),]);
    }
}
