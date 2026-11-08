//! Step definitions for offline verification of an audit partition: the hash
//! chain (`tests/features/chain_verification`) and the signed checkpoints over
//! it (`tests/features/checkpoint_signatures`).
//!
//! A `Given` builds the partition's stored history through [`AuditPartition`],
//! the `When` runs the verifier over it as an auditor would, and a `Then`
//! compares the report it returns.

use {
    self::partition::{Alteration, AuditPartition, AuditorTrust},
    crate::{
        RecordCount, Seq, VerifyReport,
        verify::{VerifyBreak, VerifyLoss},
    },
    krabka_bdd::Given,
};

mod partition;

const WRITTEN: &str = "Given a broker has written <count> audit events";
const VERIFIED: &str = "When an auditor verifies the partition";

/// Where the verifier stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BreakPosition {
    /// At the chained record with this sequence number.
    Sequence(Seq),
    /// At the checkpoint record at this offset, which carries no sequence
    /// number of its own.
    Checkpoint { offset: i64 },
}

/// What the verifier found wrong, in the words the scenarios use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum BreakKind {
    /// A record's link to the head before it does not hold.
    #[step_param("broken link")]
    BrokenLink,
    /// A record's sequence number is not the next one.
    #[step_param("sequence gap")]
    SequenceGap,
    /// A checkpoint signature does not verify under the trusted key.
    #[step_param("bad signature")]
    BadSignature,
    /// No key is trusted for the key id a checkpoint names.
    #[step_param("untrusted key")]
    UntrustedKey,
    /// A checkpoint signed a head other than the recomputed one.
    #[step_param("chain head mismatch")]
    ChainHeadMismatch,
}

impl BreakKind {
    /// Classify the verifier's reason for a break, or hand the reason back
    /// when it is none of the kinds the scenarios name.
    fn of(reason: &str) -> Result<Self, String> {
        if reason.starts_with("prev_hash does not match") {
            Ok(Self::BrokenLink)
        } else if reason.starts_with("seq gap") {
            Ok(Self::SequenceGap)
        } else if reason == "checkpoint signature invalid" {
            Ok(Self::BadSignature)
        } else if reason.starts_with("no trusted key for key_id") {
            Ok(Self::UntrustedKey)
        } else if reason.starts_with("checkpoint chain_head does not match") {
            Ok(Self::ChainHeadMismatch)
        } else {
            Err(reason.to_owned())
        }
    }
}

/// A break as the scenarios state it: what, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReportedBreak {
    kind: Result<BreakKind, String>,
    position: BreakPosition,
}

impl From<&VerifyBreak> for ReportedBreak {
    fn from(found: &VerifyBreak) -> Self {
        Self {
            kind: BreakKind::of(&found.reason),
            position: found.seq.map_or(
                BreakPosition::Checkpoint {
                    offset: found.offset,
                },
                BreakPosition::Sequence,
            ),
        }
    }
}

/// Whether the verifier accepted the partition, and the first break if not.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Verdict {
    ok: bool,
    first_break: Option<ReportedBreak>,
}

impl Verdict {
    const CONTINUOUS: Self = Self {
        ok: true,
        first_break: None,
    };

    fn of(report: &VerifyReport) -> Self {
        Self {
            ok: report.ok,
            first_break: report.first_break.as_ref().map(ReportedBreak::from),
        }
    }

    fn broken(kind: BreakKind, position: BreakPosition) -> Self {
        Self {
            ok: false,
            first_break: Some(ReportedBreak {
                kind: Ok(kind),
                position,
            }),
        }
    }
}

/// How much of the partition the verifier attests.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Attestation {
    verdict: Verdict,
    losses: Vec<VerifyLoss>,
    records: RecordCount,
    unattested: RecordCount,
}

impl Attestation {
    fn of(report: &VerifyReport) -> Self {
        Self {
            verdict: Verdict::of(report),
            losses: report.losses.clone(),
            records: report.records,
            unattested: report.unanchored_records,
        }
    }

    fn continuous(records: u64, unattested: u64) -> Self {
        Self {
            verdict: Verdict::CONTINUOUS,
            losses: Vec::new(),
            records: RecordCount(records),
            unattested: RecordCount(unattested),
        }
    }
}

struct VerificationScenario {
    partition: Given<AuditPartition>,
    report: Given<VerifyReport>,
}

#[rstest::fixture]
fn verification() -> VerificationScenario {
    VerificationScenario {
        partition: Given::new("partition", WRITTEN),
        report: Given::new("report", VERIFIED),
    }
}

#[rstest_bdd_macros::given("a broker has written {count} audit events")]
fn broker_has_written(verification: &mut VerificationScenario, count: u64) {
    let mut partition = AuditPartition::new();
    partition.write_events(count);
    verification.partition.set(partition);
}

#[rstest_bdd_macros::given(
    "a broker has written {count} audit events and then a signed checkpoint"
)]
fn broker_has_checkpointed(verification: &mut VerificationScenario, count: u64) {
    let mut partition = AuditPartition::new();
    partition.write_events(count);
    partition.write_signed_checkpoint();
    verification.partition.set(partition);
}

#[rstest_bdd_macros::given("it has written {count} more audit events")]
fn broker_has_written_more(verification: &mut VerificationScenario, count: u64) {
    verification.partition.get_mut().write_events(count);
}

/// The loss marker sits between events, so the chain runs on past it.
#[rstest_bdd_macros::given("a broker has declared {count} lost audit records in its chain")]
fn broker_has_declared_loss(verification: &mut VerificationScenario, count: u64) {
    let mut partition = AuditPartition::new();
    partition.write_events(2);
    partition.declare_lost(count);
    partition.write_events(2);
    verification.partition.set(partition);
}

/// Two events under a checkpoint signed with the old key, then two under one
/// signed with the new key.
#[rstest_bdd_macros::given(
    "a broker has signed checkpoints before and after an audit key rotation"
)]
fn broker_has_rotated_keys(verification: &mut VerificationScenario) {
    let mut partition = AuditPartition::new();
    partition.write_events(2);
    partition.write_checkpoint_signed_by("audit-old");
    partition.write_events(2);
    partition.write_checkpoint_signed_by("audit-new");
    verification.partition.set(partition);
}

#[rstest_bdd_macros::given("the stored event at sequence {seq} has been {alteration}")]
fn stored_event_altered(verification: &mut VerificationScenario, seq: u64, alteration: Alteration) {
    verification.partition.get_mut().alter(alteration, Seq(seq));
}

/// The auditor trusts every key the broker signed with.
#[rstest_bdd_macros::when("an auditor verifies the partition")]
fn auditor_verifies(verification: &mut VerificationScenario) {
    verify_trusting(verification, AuditorTrust::EveryBrokerKey);
}

#[rstest_bdd_macros::when("an auditor verifies the partition trusting {trust}")]
fn auditor_verifies_trusting(verification: &mut VerificationScenario, trust: AuditorTrust) {
    verify_trusting(verification, trust);
}

fn verify_trusting(verification: &mut VerificationScenario, trust: AuditorTrust) {
    let partition = verification.partition.get();
    let report = partition.verify(&partition.trusted_keys(trust));
    verification.report.set(report);
}

#[rstest_bdd_macros::then("the partition is fully attested over {count} events")]
fn fully_attested(verification: &VerificationScenario, count: u64) {
    let report = verification.report.get();
    assert2::assert!(
        (Attestation::of(report), report.checkpoints.0 > 0)
            == (Attestation::continuous(count, 0), true)
    );
}

#[rstest_bdd_macros::then("the chain is continuous with {unattested} of {count} events unattested")]
fn continuous_with_unattested(verification: &VerificationScenario, unattested: u64, count: u64) {
    assert2::assert!(
        Attestation::of(verification.report.get()) == Attestation::continuous(count, unattested)
    );
}

#[rstest_bdd_macros::then("the chain is continuous")]
fn chain_is_continuous(verification: &VerificationScenario) {
    assert2::assert!(Verdict::of(verification.report.get()) == Verdict::CONTINUOUS);
}

#[rstest_bdd_macros::then("the verifier reports a loss of {count} records")]
fn reports_loss(verification: &VerificationScenario, count: u64) {
    let expected: Vec<VerifyLoss> = verification
        .partition
        .get()
        .loss_markers()
        .into_iter()
        .map(|(offset, seq)| VerifyLoss {
            offset,
            seq,
            records: RecordCount(count),
        })
        .collect();
    assert2::assert!(verification.report.get().losses == expected);
}

#[rstest_bdd_macros::then("the verifier reports a {kind} at sequence {seq}")]
fn reports_break_at_sequence(verification: &VerificationScenario, kind: BreakKind, seq: u64) {
    assert2::assert!(
        Verdict::of(verification.report.get())
            == Verdict::broken(kind, BreakPosition::Sequence(Seq(seq)))
    );
}

#[rstest_bdd_macros::then("the verifier reports a {kind} at the checkpoint")]
fn reports_a_break_at_checkpoint(verification: &VerificationScenario, kind: BreakKind) {
    check_break_at_checkpoint(verification, kind);
}

#[rstest_bdd_macros::then("the verifier reports an {kind} at the checkpoint")]
fn reports_an_break_at_checkpoint(verification: &VerificationScenario, kind: BreakKind) {
    check_break_at_checkpoint(verification, kind);
}

/// "The checkpoint" is the first one the partition stores: the only one, or
/// across a key rotation the one the retired key signed.
fn check_break_at_checkpoint(verification: &VerificationScenario, kind: BreakKind) {
    let offset = verification.partition.get().first_checkpoint_offset();
    assert2::assert!(
        Verdict::of(verification.report.get())
            == Verdict::broken(kind, BreakPosition::Checkpoint { offset })
    );
}

krabka_bdd::scenarios!(
    "tests/features/chain_verification",
    fixtures = [verification: VerificationScenario]
);

krabka_bdd::scenarios!(
    "tests/features/checkpoint_signatures",
    fixtures = [verification: VerificationScenario]
);
