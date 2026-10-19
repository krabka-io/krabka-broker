use {
    super::{
        Ack, Boundary, CommitAck, LoadRecord, PathRecovery, ProducerAcks, RecoveryEvidence,
        RestoredReadings, acknowledged_by, bound_evidence, check_recovery, committed_by,
        path_recovery,
    },
    crate::soak::{
        contract::{Isolation, Semantics, fixture},
        evidence::{Evidence, ProducerLedger, ReaderLedger},
        ledger::SeqSet,
        reconcile::FindingKind,
        synthetic::clean_run,
        verdict::judge,
    },
    assert2::check,
    std::collections::BTreeMap,
};

/// The capture starts at 1 000 ms and the source is lost 20 s later, after
/// the fixture's declared `post_capture_load`.
const BOUNDARY: Boundary = Boundary {
    capture_started_unix_ms: 1_000,
    disaster_unix_ms: 21_000,
};

/// Ten sends, acknowledged ten milliseconds apart from 955 ms: seqs 0..=4 by
/// the boundary, 5..=9 after it.
fn acks() -> Vec<Ack> {
    (0..10)
        .map(|seq| Ack {
            seq,
            unix_ms: 955 + i64::try_from(seq).unwrap() * 10,
        })
        .collect()
}

fn ledger() -> ProducerLedger {
    ProducerLedger {
        path: "tiered-idempotent".into(),
        producer: "tiered-idempotent-0".into(),
        semantics: Semantics::Idempotent,
        attempted: 12,
        acked: SeqSet::range(0, 10),
        failed: SeqSet::new(),
        retried: SeqSet::new(),
        transactions: Vec::new(),
    }
}

fn reader(isolation: Isolation, seen: SeqSet) -> ReaderLedger {
    ReaderLedger {
        path: "tiered-idempotent".into(),
        isolation,
        producer: "tiered-idempotent-0".into(),
        seen,
        duplicates: Vec::new(),
        latest_by_key: BTreeMap::new(),
    }
}

fn seqs(values: &[u64]) -> SeqSet { values.iter().copied().collect() }

#[yare::parameterized(
    before_the_first = { 900, &[] },
    at_an_ack = { 975, &[0, 1, 2] },
    at_the_boundary = { 1_000, &[0, 1, 2, 3, 4] },
    after_the_last = { 2_000, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9] },
)]
fn a_record_is_inside_the_boundary_when_acknowledged_by_it(instant: i64, expected: &[u64]) {
    check!(acknowledged_by(&acks(), instant) == seqs(expected));
}

#[test]
fn the_commit_before_the_boundary_is_the_last_one_acknowledged_by_it() {
    let history = vec![
        CommitAck {
            unix_ms: 900,
            offsets: BTreeMap::from([(0, 5), (1, 3)]),
        },
        CommitAck {
            unix_ms: 990,
            offsets: BTreeMap::from([(0, 9)]),
        },
        CommitAck {
            unix_ms: 1_010,
            offsets: BTreeMap::from([(0, 12), (1, 8)]),
        },
    ];
    check!(
        committed_by(&history, BOUNDARY.capture_started_unix_ms)
            == BTreeMap::from([(0, 9), (1, 3)])
    );
}

#[yare::parameterized(
    everything_restored = { &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], &[], None },
    tail_after_the_boundary_lost = { &[0, 1, 2, 3, 4, 5, 6], &[0, 1, 2, 3, 4, 5, 6], &[7, 8, 9], Some(1_025) },
    one_reader_short = { &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], &[0, 1, 2, 3, 4, 5, 6, 7], &[8, 9], Some(1_035) },
    loss_inside_the_boundary = { &[0, 1, 3, 4, 5, 6, 7, 8, 9], &[0, 1, 3, 4, 5, 6, 7, 8, 9], &[], Some(975) },
)]
fn a_path_counts_what_it_lost_beyond_the_boundary_and_its_oldest_loss(
    uncommitted: &[u64],
    committed: &[u64],
    lost_beyond: &[u64],
    oldest_loss: Option<i64>,
) {
    let readers = [
        reader(Isolation::ReadUncommitted, seqs(uncommitted)),
        reader(Isolation::ReadCommitted, seqs(committed)),
    ];
    let readers: Vec<&ReaderLedger> = readers.iter().collect();
    check!(
        path_recovery(&ledger(), &acks(), &readers, BOUNDARY)
            == PathRecovery {
                path: "tiered-idempotent".into(),
                producer: "tiered-idempotent-0".into(),
                acknowledged: 10,
                before_boundary: 5,
                lost_beyond_boundary: seqs(lost_beyond),
                oldest_loss_unix_ms: oldest_loss,
            }
    );
}

/// A drill's load of the fixture's two paths, every send acknowledged as
/// [`acks`] says.
fn load_record() -> LoadRecord {
    let contract = fixture::drill();
    let clean = clean_run(&contract);
    let producers: Vec<ProducerLedger> = clean
        .producers
        .iter()
        .map(|producer| ProducerLedger {
            attempted: 12,
            acked: SeqSet::range(0, 10),
            ..producer.clone()
        })
        .collect();
    LoadRecord {
        acks: producers
            .iter()
            .map(|producer| ProducerAcks {
                producer: producer.producer.clone(),
                acks: acks(),
            })
            .collect(),
        commits: Vec::new(),
        run: Evidence {
            started_unix_ms: 0,
            ended_unix_ms: BOUNDARY.disaster_unix_ms,
            producers,
            readers: Vec::new(),
            commits: Vec::new(),
            ..clean
        },
    }
}

/// What the restored cluster served when every reader found `seen`, `serving`
/// milliseconds after the loss.
fn restored(seen: &SeqSet, serving_after: Option<i64>) -> RestoredReadings {
    let contract = fixture::drill();
    let clean = clean_run(&contract);
    RestoredReadings {
        readers: clean
            .readers
            .into_iter()
            .map(|reader| ReaderLedger {
                seen: seen.clone(),
                ..reader
            })
            .collect(),
        commits: clean.commits,
        serving_unix_ms: serving_after.map(|after| BOUNDARY.disaster_unix_ms + after),
        ended_unix_ms: 5_000,
    }
}

#[test]
fn the_judge_sees_only_the_acknowledgements_from_inside_the_boundary() {
    let evidence = bound_evidence(
        &load_record(),
        BOUNDARY,
        restored(&SeqSet::range(0, 7), Some(60_000)),
    );
    for producer in &evidence.producers {
        check!(producer.acked == SeqSet::range(0, 5));
        check!(producer.attempted == 12);
    }
    let recovery = evidence
        .recovery
        .expect("a drill's evidence has a recovery");
    check!(
        recovery
            == RecoveryEvidence {
                capture_started_unix_ms: 1_000,
                disaster_unix_ms: 21_000,
                serving_unix_ms: Some(81_000),
                paths: load_record()
                    .run
                    .producers
                    .iter()
                    .map(|producer| PathRecovery {
                        path: producer.path.clone(),
                        producer: producer.producer.clone(),
                        acknowledged: 10,
                        before_boundary: 5,
                        lost_beyond_boundary: SeqSet::range(7, 10),
                        oldest_loss_unix_ms: Some(1_025),
                    })
                    .collect(),
            }
    );
}

fn drill_evidence(seen: &SeqSet, serving_after: Option<i64>) -> Evidence {
    bound_evidence(&load_record(), BOUNDARY, restored(seen, serving_after))
}

#[test]
fn a_tail_lost_beyond_the_boundary_within_the_objectives_passes() {
    let verdict = judge(
        &fixture::drill(),
        &drill_evidence(&SeqSet::range(0, 7), Some(60_000)),
    );
    check!(verdict.passed(), "{:#?}", verdict.failures());
    let recovery = verdict.recovery.expect("a drill is judged for recovery");
    check!(recovery.rpo_ms == 19_975);
    check!(recovery.rto_ms == Some(60_000));
    check!(recovery.lost_beyond_boundary == 6);
}

#[test]
fn a_record_lost_inside_the_boundary_fails_the_reconciler() {
    let mut seen = SeqSet::range(0, 10);
    seen.remove(2);
    let verdict = judge(&fixture::drill(), &drill_evidence(&seen, Some(60_000)));
    let lost: Vec<_> = verdict
        .reconciliation
        .findings
        .iter()
        .filter(|finding| finding.kind == FindingKind::Lost)
        .map(|finding| finding.seqs.clone())
        .collect();
    // Two paths, two readers each, every one missing seq 2.
    check!(lost == vec![seqs(&[2]); 4]);
}

#[yare::parameterized(
    never_served = { None, 0, "never served" },
    served_too_late = { Some(11 * 60_000), 0, "past the objective of 10m" },
    loss_too_old = { Some(60_000), 200_000, "recovery point" },
)]
fn a_drill_that_misses_an_objective_fails(
    serving_after: Option<i64>,
    lost_back: i64,
    fragment: &str,
) {
    let mut evidence = drill_evidence(&SeqSet::range(0, 10), serving_after);
    if let Some(recovery) = &mut evidence.recovery
        && lost_back > 0
    {
        recovery.paths[0].oldest_loss_unix_ms = Some(recovery.disaster_unix_ms - lost_back);
    }
    let recovery = check_recovery(&fixture::drill(), &evidence).expect("a drill is judged");
    check!(
        recovery
            .problems
            .iter()
            .any(|problem| problem.contains(fragment)),
        "{:?}",
        recovery.problems
    );
}

#[test]
fn a_drill_lost_before_its_declared_post_capture_load_fails() {
    // The fixture declares 20 s of load after the capture; this source was
    // lost 100 ms after it started.
    let mut evidence = drill_evidence(&SeqSet::range(0, 10), Some(60_000));
    if let Some(recovery) = &mut evidence.recovery {
        recovery.disaster_unix_ms = recovery.capture_started_unix_ms + 100;
    }
    let recovery = check_recovery(&fixture::drill(), &evidence).expect("a drill is judged");
    check!(
        recovery
            .problems
            .iter()
            .any(|problem| problem.contains("post_capture_load"))
    );
}

#[test]
fn a_drill_without_recovery_evidence_fails_and_a_soak_is_not_judged_for_one() {
    let recovery =
        check_recovery(&fixture::drill(), &Evidence::default()).expect("a drill is judged");
    check!(recovery.problems == ["the evidence records no recovery"]);
    check!(check_recovery(&fixture::self_check(), &Evidence::default()).is_none());
}
