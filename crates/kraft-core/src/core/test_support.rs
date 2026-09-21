//! Shared fixtures for the quorum state machine unit tests: two [`LogView`]
//! stubs and the constructors that build a machine over a bootstrap voter
//! set.
//!
//! The tests live in one module per event family, and every one of them
//! needs these builders, so they sit here rather than in any single family.

use krabka_units::prelude::{Time, secs};

use super::QuorumStateMachine;
use crate::{
    action::Action,
    event::Event,
    types::{Epoch, LogOffsetMetadata, LogView, NodeId, QuorumState, SimInstant},
};

/// The end of `epoch` in a fake log of `end` records that all carry
/// `last_epoch`: Kafka's lookup over a log with one epoch in it.
fn single_epoch_end(end: i64, last_epoch: Epoch, epoch: Epoch) -> LogOffsetMetadata {
    if epoch < last_epoch {
        LogOffsetMetadata { offset: 0, epoch }
    } else {
        LogOffsetMetadata {
            offset: end,
            epoch: last_epoch,
        }
    }
}

pub struct FakeLog {
    pub end: i64,
    pub last_epoch: Epoch,
}
impl LogView for FakeLog {
    fn end_offset(&self) -> i64 {
        self.end
    }
    fn last_epoch(&self) -> Epoch {
        self.last_epoch
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        single_epoch_end(self.end, self.last_epoch, epoch)
    }
}
/// A `LogView` whose `end_offset` can change between calls.
///
/// A test can then model a leader that is promoted at a small log end, that
/// is, a low `epoch_start_offset`, and whose log grows before followers
/// fetch.
pub struct CellLog {
    pub end: std::cell::Cell<i64>,
    pub last_epoch: Epoch,
}
impl LogView for CellLog {
    fn end_offset(&self) -> i64 {
        self.end.get()
    }
    fn last_epoch(&self) -> Epoch {
        self.last_epoch
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        single_epoch_end(self.end.get(), self.last_epoch, epoch)
    }
}
/// A log described by its epoch runs: `(epoch, record count)` in log order.
///
/// Its divergence lookup is the real rule over the records it holds, so a
/// test can put an epoch gap in the log, which neither [`FakeLog`] nor
/// [`CellLog`] can.
pub struct RunsLog {
    epochs: Vec<Epoch>,
}
impl RunsLog {
    pub fn new(runs: &[(Epoch, usize)]) -> Self {
        Self {
            epochs: runs
                .iter()
                .flat_map(|&(epoch, count)| std::iter::repeat_n(epoch, count))
                .collect(),
        }
    }
}
impl LogView for RunsLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("test log length fits in i64")
    }
    fn last_epoch(&self) -> Epoch {
        self.epochs.last().copied().unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        LogOffsetMetadata::end_of_epoch_in(&self.epochs, epoch)
    }
}
pub fn voters(ids: &[NodeId]) -> krabka_voters::VoterSet {
    krabka_voters::VoterSet::from_voters(ids.iter().map(|&id| krabka_voters::Voter {
        id,
        directory_id: uuid::Uuid::nil(),
        endpoints: vec![],
        kraft_version: krabka_voters::KRaftVersionRange::default(),
    }))
}
pub fn machine(me: NodeId, ids: &[NodeId]) -> QuorumStateMachine {
    QuorumStateMachine::new(
        me,
        QuorumState::bootstrap(uuid::Uuid::nil(), voters(ids)),
        TEST_ELECTION_TIMEOUT,
    )
}

/// The base election timeout for every test machine.
pub const TEST_ELECTION_TIMEOUT: Time = secs(1);

pub fn win_election(
    m: &mut QuorumStateMachine,
    log: &dyn LogView,
    peers: &[NodeId],
    now: SimInstant,
) -> Vec<Action> {
    let mut actions = m.on_event(Event::ElectionTimeout, log, now);
    for epoch in [0, 1] {
        for &from in peers {
            actions.extend(m.on_event(
                Event::ReceiveVoteResponse {
                    from,
                    epoch,
                    vote_granted: true,
                },
                log,
                now,
            ));
        }
    }
    actions
}
