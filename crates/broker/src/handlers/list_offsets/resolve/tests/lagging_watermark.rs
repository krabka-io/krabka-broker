//! Scenarios for `tests/features/handler_lagging_watermark`: KIP-207 fences a
//! consumer's lookup while the leader's high watermark lags the start offset
//! of its epoch, and the lookup kind decides which results the fence applies
//! to.

use {
    super::super::{lagging_high_watermark_error, lagging_high_watermark_fences},
    crate::codes,
    assert2::assert,
    krabka_bdd::{Given, given, then, when},
    krabka_verified::ListOffsetsKind,
};

/// The bound every lookup-kind row measures its candidate against.
const BOUND: i64 = 3;

/// Who sends the `ListOffsets` request: its `replica_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Requester {
    #[step_param("a consumer")]
    Consumer,
    #[step_param("a follower")]
    Follower,
    #[step_param("a debugging replica")]
    DebuggingReplica,
}

impl Requester {
    const fn replica_id(self) -> i32 {
        match self {
            Self::Consumer => -1,
            Self::Follower => 3,
            Self::DebuggingReplica => -2,
        }
    }
}

/// What the fence makes of a lookup: no error, or the partition error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Fence {
    #[step_param("not fenced")]
    NotFenced,
    #[step_param("OFFSET_NOT_AVAILABLE")]
    OffsetNotAvailable,
    #[step_param("LEADER_NOT_AVAILABLE")]
    LeaderNotAvailable,
}

impl Fence {
    fn of(error: Option<i16>) -> Self {
        match error {
            None => Self::NotFenced,
            Some(codes::OFFSET_NOT_AVAILABLE) => Self::OffsetNotAvailable,
            Some(codes::LEADER_NOT_AVAILABLE) => Self::LeaderNotAvailable,
            Some(other) => panic!("unexpected lagging high watermark code {other}"),
        }
    }
}

/// The lookup a row's candidate offset was resolved for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum LookupKind {
    Latest,
    Earliest,
    EarliestLocal,
    Timestamp,
    #[step_param("maximum-timestamp")]
    MaxTimestamp,
    LatestTiered,
    EarliestPendingUpload,
}

impl From<LookupKind> for ListOffsetsKind {
    fn from(kind: LookupKind) -> Self {
        match kind {
            LookupKind::Latest => Self::Latest,
            LookupKind::Earliest => Self::Earliest,
            LookupKind::EarliestLocal => Self::EarliestLocal,
            LookupKind::Timestamp => Self::Timestamp,
            LookupKind::MaxTimestamp => Self::MaxTimestamp,
            LookupKind::LatestTiered => Self::LatestTiered,
            LookupKind::EarliestPendingUpload => Self::EarliestPendingUpload,
        }
    }
}

struct LaggingWatermarkScenario {
    epoch_start: Given<Option<i64>>,
    high_watermark: Given<i64>,
    fence: Given<Fence>,
    candidate: Given<(LookupKind, i64)>,
    fenced: Given<bool>,
}

#[rstest::fixture]
fn lagging_watermark() -> LaggingWatermarkScenario {
    LaggingWatermarkScenario {
        epoch_start: Given::new(
            "epoch_start",
            "Given the leader epoch started at offset <offset>",
        ),
        high_watermark: Given::new(
            "high_watermark",
            "Given the high watermark is at offset <high watermark>",
        ),
        fence: Given::new(
            "fence",
            "When <requester> looks up an offset with ListOffsets v<version>",
        ),
        candidate: Given::new(
            "candidate",
            "Given a lagging high watermark bounds <kind> offset candidate <candidate> before offset 3",
        ),
        fenced: Given::new(
            "fenced",
            "When ListOffsets decides whether the lagging high watermark fences the result",
        ),
    }
}

#[given("the leader epoch started at offset {offset}")]
fn epoch_started(lagging_watermark: &mut LaggingWatermarkScenario, offset: i64) {
    lagging_watermark.epoch_start.set(Some(offset));
}

#[given("the leader has no epoch start offset")]
fn no_epoch_start(lagging_watermark: &mut LaggingWatermarkScenario) {
    lagging_watermark.epoch_start.set(None);
}

#[given("the high watermark is at offset {offset}")]
fn high_watermark_at(lagging_watermark: &mut LaggingWatermarkScenario, offset: i64) {
    lagging_watermark.high_watermark.set(offset);
}

#[when("{requester} looks up an offset with ListOffsets v{version}")]
fn look_up_offset(
    lagging_watermark: &mut LaggingWatermarkScenario,
    requester: Requester,
    version: i16,
) {
    let error = lagging_high_watermark_error(
        requester.replica_id(),
        *lagging_watermark.epoch_start.get(),
        *lagging_watermark.high_watermark.get(),
        version,
    );
    lagging_watermark.fence.set(Fence::of(error));
}

#[then("the lookup is {outcome}")]
fn lookup_outcome(lagging_watermark: &LaggingWatermarkScenario, outcome: Fence) {
    assert!(*lagging_watermark.fence.get() == outcome);
}

#[given("a lagging high watermark bounds {kind} offset candidate {candidate} before offset 3")]
fn watermark_candidate(
    lagging_watermark: &mut LaggingWatermarkScenario,
    kind: LookupKind,
    candidate: i64,
) {
    lagging_watermark.candidate.set((kind, candidate));
}

#[when("ListOffsets decides whether the lagging high watermark fences the result")]
fn fence_watermark_candidate(lagging_watermark: &mut LaggingWatermarkScenario) {
    let (kind, candidate) = *lagging_watermark.candidate.get();
    let fenced = lagging_high_watermark_fences(kind.into(), candidate, BOUND);
    lagging_watermark.fenced.set(fenced);
}

#[then("the candidate is fenced {expected}")]
fn check_watermark_candidate(lagging_watermark: &LaggingWatermarkScenario, expected: bool) {
    assert!(*lagging_watermark.fenced.get() == expected);
}

krabka_bdd::scenarios!(
    "tests/features/handler_lagging_watermark",
    fixtures = [lagging_watermark: LaggingWatermarkScenario]
);
